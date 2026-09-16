#!/usr/bin/env python3
"""
dashboard_bridge.py -- Feed the FooBot IAQ Dashboard (~/Foobot) from the
local MQTT broker.

Subscribes to device/+/sensor/push on the local broker (foobot_service.py),
decodes raw readings with pollution.py (the same math the Wio LCD and the HA
push use), maps each device UUID to a dashboard location via iaq.device_mappings
and writes one row per reading into iaq.readings -- the exact table and row
shape the Dashboard's own ingestion (~/Foobot/backend/ingestion) writes, so
the web Dashboard displays live local Foobot data without the cloud.

Schema mapping (Foobot calibrated -> iaq.readings):
  ts          unix seconds at receipt (the raw push carries no timestamp)
  temperature tmp    (C, 2dp)        dust   pm  (ugm3, rounded)
  humidity    hum    (%, rounded)     co2    co2 (ppm, rounded)
  voc has no readings column; it is already folded into allpollu upstream.

Pure stdlib except pymysql. Configured via environment variables:
  BROKER_HOST   local broker address            (default 127.0.0.1)
  BROKER_PORT   local broker port               (default 1883)
  DB_HOST/DB_PORT/DB_USER/DB_PASS/DB_NAME       iaq database (defaults php_iaq/iaq)
  DEVICE_GROUP_MAP   JSON {"<uuid>": <group_id>} force-mapping (optional)
  AUTO_REGISTER      create a location+mapping for an unknown device (default 1)
  MAPPING_REFRESH    seconds between device_mappings re-reads     (default 60)

Run:  /home/albert/Foobot/backend/.venv/bin/python dashboard_bridge.py
      (systemd: foobot-dashboard-bridge.service, installed by deploy/install.sh)
"""
import fcntl
import json
import os
import socket
import sys
import time

import pymysql

import pollution
import mqttwire

BROKER_HOST = os.environ.get("BROKER_HOST", "127.0.0.1")
BROKER_PORT = int(os.environ.get("BROKER_PORT", "1883"))
CLIENT_ID = os.environ.get("BRIDGE_CLIENT_ID", "foobot-dashboard-bridge")
TOPIC_FILTER = os.environ.get("BRIDGE_TOPIC", "device/+/sensor/push")
KEEPALIVE_S = 30
PING_EVERY_S = 20
RECONNECT_MAX_S = 60

DB = dict(
    host=os.environ.get("DB_HOST", "127.0.0.1"),
    port=int(os.environ.get("DB_PORT", "3306")),
    user=os.environ.get("DB_USER", "php_iaq"),
    password=os.environ.get("DB_PASS", "php_iaq"),
    database=os.environ.get("DB_NAME", "iaq"),
)
DEVICE_GROUP_MAP = {}
try:
    _raw = os.environ.get("DEVICE_GROUP_MAP", "").strip()
    if _raw:
        DEVICE_GROUP_MAP = {str(k): int(v) for k, v in json.loads(_raw).items()}
except (ValueError, TypeError) as e:
    sys.exit(f"invalid DEVICE_GROUP_MAP JSON: {e}")
AUTO_REGISTER = os.environ.get("AUTO_REGISTER", "1") == "1"
MAPPING_REFRESH_S = int(os.environ.get("MAPPING_REFRESH", "60"))
# two live instances would each see the broker fanout and write duplicate rows
LOCK_FILE = os.environ.get("BRIDGE_LOCK", "/tmp/foobot-dashboard-bridge.lock")

_lock_print = __import__("threading").Lock()


def log(msg: str) -> None:
    line = f"{time.strftime('%H:%M:%S')} {msg}"
    with _lock_print:
        print(line, flush=True)


# --- minimal MQTT 3.1.1 client (wire primitives shared with foobot_service) ----


class MqttClient:
    """CONNECT/SUBSCRIBE/PINGREQ + QoS0 PUBLISH receipt -- all this broker needs.

    Reads go through mqttwire.StreamParser: a keepalive timeout is only acted
    on at a PACKET boundary. A timeout arriving mid-packet (bytes already
    consumed) raises ConnectionError -- dropping the half-read packet would
    desync the stream, so reconnecting is the only safe recovery."""

    def __init__(self, host: str, port: int, client_id: str):
        self.addr = (host, port)
        self.client_id = client_id
        self.sock: socket.socket | None = None
        self._sp = mqttwire.StreamParser()

    # -- wire reading -----------------------------------------------------------

    def _fill(self, deadline: float) -> bool:
        """recv() more bytes. True if bytes arrived; False on a deadline hit;
        raises ConnectionError when the broker closed the socket."""
        assert self.sock is not None
        self.sock.settimeout(max(deadline - time.monotonic(), 0.01))
        try:
            chunk = self.sock.recv(65536)
        except socket.timeout:
            return False
        if not chunk:
            raise ConnectionError("broker closed the connection")
        self._sp.feed(chunk)
        return True

    def _read_packet(self, deadline: float) -> tuple[int, bytes] | None:
        """(header, body) of the next packet, or None on a deadline hit at a
        packet boundary (keepalive time)."""
        while True:
            pkt = self._sp.next_packet()
            if pkt is not None:
                return pkt
            if not self._fill(deadline):
                if self._sp.partial:
                    raise ConnectionError("timeout mid-packet (half-read packet)")
                return None

    # -- protocol ---------------------------------------------------------------

    def connect(self, topic_filter: str) -> None:
        self._sp = mqttwire.StreamParser()
        self.sock = socket.create_connection(self.addr, timeout=10)
        self.sock.sendall(mqttwire.build_connect(self.client_id, KEEPALIVE_S))
        pkt = self._read_packet(time.monotonic() + 10)
        if pkt is None or pkt[0] >> 4 != 2 or len(pkt[1]) < 2 or pkt[1][1] != 0:
            raise ConnectionError(f"broker CONNACK failed: {pkt!r}")
        self.sock.sendall(mqttwire.build_subscribe([topic_filter], pid=1))
        pkt = self._read_packet(time.monotonic() + 10)
        if pkt is None or pkt[0] >> 4 != 9:
            raise ConnectionError(f"broker SUBACK failed: {pkt!r}")
        log(f"[mqtt] connected to {self.addr[0]}:{self.addr[1]}, subscribed {topic_filter}")

    def next_publish(self, deadline: float) -> tuple[str, bytes] | None:
        """Block until the next PUBLISH or the ping deadline. Returns None on ping."""
        while True:
            pkt = self._read_packet(deadline)
            if pkt is None:
                assert self.sock is not None
                self.sock.sendall(mqttwire.PINGREQ)
                return None
            hdr, body = pkt
            if hdr >> 4 != 3:
                continue           # PINGRESP / anything else: not interesting
            parsed = mqttwire.parse_publish(hdr & 0x0F, body)
            if parsed is None:
                continue           # malformed: skip, stream stays framed
            topic, _pid, payload = parsed
            return topic, payload

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.sendall(mqttwire.DISCONNECT)
                self.sock.close()
            except OSError:
                pass
            self.sock = None


# --- dashboard side: group mapping + inserts ----------------------------------

class Dashboard:
    def __init__(self) -> None:
        self.conn = pymysql.connect(**DB)
        self.mappings: dict[str, int] = {}
        self.mappings_loaded_at = 0.0
        self.last_ts: dict[int, int] = {}      # group_id -> last written ts
        self.have_unique = False               # UNIQUE(group_id, ts) confirmed?
        self._ensure_unique_index()

    def _ensure_unique_index(self) -> None:
        """Best effort: make (group_id, ts) UNIQUE. With it confirmed, write()
        skips the pre-INSERT SELECT (one DB round trip less per reading) and a
        lost insert race surfaces as error 1062, handled as a skip. The
        Dashboard's own writer also treats such rows as duplicates-to-skip, so
        this matches its model. Existing duplicate rows (or missing ALTER
        privileges) just leave the dedup purely check-then-insert, still
        guarded by the instance lock."""
        try:
            with self._cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.statistics "
                    "WHERE table_schema = DATABASE() AND table_name = 'readings' "
                    "AND index_name = 'uniq_group_ts'"
                )
                if cur.fetchone() is None:
                    cur.execute(
                        "ALTER TABLE readings ADD UNIQUE KEY uniq_group_ts (group_id, ts)"
                    )
            self.conn.commit()
            self.have_unique = True
            log("[db] UNIQUE(group_id, ts) confirmed on readings")
        except Exception as e:
            log(f"[db] could not add UNIQUE(group_id, ts): {e}; "
                "dedup stays check-then-insert (delete duplicate rows to enable it)")

    def _cursor(self):
        try:
            self.conn.ping()
        except Exception:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn.connect()
        return self.conn.cursor()

    def refresh_mappings(self) -> None:
        if time.monotonic() - self.mappings_loaded_at < MAPPING_REFRESH_S:
            return
        with self._cursor() as cur:
            cur.execute("SELECT device_key, group_id FROM device_mappings")
            self.mappings = {str(k): int(g) for k, g in cur.fetchall()}
        self.mappings_loaded_at = time.monotonic()
        log(f"[db] device_mappings: {len(self.mappings)} known")

    def group_for(self, uuid: str) -> int | None:
        if uuid in DEVICE_GROUP_MAP:
            return DEVICE_GROUP_MAP[uuid]
        if uuid in self.mappings:
            return self.mappings[uuid]
        if not AUTO_REGISTER:
            return None
        name = f"Foobot {uuid[-4:]}"
        with self._cursor() as cur:
            cur.execute("INSERT INTO `groups` (name, tz) VALUES (%s, 'UTC')", (name,))
            group_id = cur.lastrowid
            cur.execute(
                "INSERT INTO device_mappings (device_key, group_id, created_at) "
                "VALUES (%s, %s, %s)",
                (uuid, group_id, int(time.time())),
            )
        self.conn.commit()
        self.mappings[uuid] = group_id
        log(f"[db] auto-registered {uuid} as group {group_id} ('{name}')")
        return group_id

    def write(self, group_id: int, enriched: dict) -> bool:
        ts = int(time.time())
        # the raw push carries no timestamp; keep rows of a group strictly
        # increasing so replayed bursts never collapse onto one second
        ts = max(ts, self.last_ts.get(group_id, 0) + 1)
        with self._cursor() as cur:
            try:
                if not self.have_unique:       # fast path: let the UNIQUE key
                    cur.execute(               # decide, no SELECT round trip
                        "SELECT 1 FROM readings WHERE group_id = %s AND ts = %s LIMIT 1",
                        (group_id, ts),
                    )
                    if cur.fetchone() is not None:
                        return False
                cur.execute(
                    "INSERT INTO readings "
                    "(ts, group_id, temperature, humidity, dust, co2) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (
                        ts,
                        group_id,
                        enriched.get("tmp"),
                        round(enriched["hum"]) if "hum" in enriched else None,
                        round(enriched["pm"]) if "pm" in enriched else None,
                        round(enriched["co2"]) if "co2" in enriched else None,
                    ),
                )
            except pymysql.err.IntegrityError as e:
                if e.args and e.args[0] == 1062:   # duplicate (group_id, ts):
                    self.conn.rollback()           # lost a race; same as a skip
                    return False
                raise
        self.conn.commit()
        self.last_ts[group_id] = ts
        return True


def handle_payload(dash: Dashboard, topic: str, payload: bytes) -> None:
    parts = topic.split("/")
    if len(parts) != 4 or not parts[1] or parts[2] != "sensor" or parts[3] != "push":
        return
    uuid = parts[1]
    try:
        data = json.loads(payload.decode(errors="replace"))
    except ValueError:
        log(f"[skip] non-JSON payload from {uuid}: {payload[:80]!r}")
        return
    rows = data if isinstance(data, list) else [data]
    dash.refresh_mappings()
    group_id = dash.group_for(uuid)
    if group_id is None:
        log(f"[skip] {uuid} has no group mapping (set it in Dashboard Settings)")
        return
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        enriched = pollution.enrich(raw)
        if "tmp" not in enriched and "co2" not in enriched:
            continue
        if dash.write(group_id, enriched):
            log(f"[db] {uuid} -> group {group_id}: {enriched}")
        else:
            log(f"[db] duplicate skipped for group {group_id} ({uuid})")


def main() -> None:
    lock_fh = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        sys.exit("dashboard_bridge: another instance already holds the lock; exiting")
    dash = Dashboard()
    log(f"[bridge] starting; db {DB['user']}@{DB['host']}:{DB['port']}/{DB['database']}")
    backoff = 2
    client = MqttClient(BROKER_HOST, BROKER_PORT, CLIENT_ID)
    while True:
        try:
            client.connect(TOPIC_FILTER)
            backoff = 2
            next_ping = time.monotonic() + PING_EVERY_S
            while True:
                got = client.next_publish(deadline=next_ping)
                if got is not None:
                    handle_payload(dash, *got)
                next_ping = time.monotonic() + PING_EVERY_S
        except Exception as e:
            log(f"[mqtt] {e!r}; reconnecting in {backoff}s")
            client.close()
            time.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_MAX_S)


if __name__ == "__main__":
    main()
