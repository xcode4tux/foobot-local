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


# --- minimal MQTT 3.1.1 client (mirror of foobot_service.py's server wire) ----

def _enc_len(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n % 128
        n //= 128
        if n > 0:
            b |= 0x80
        out.append(b)
        if n == 0:
            return bytes(out)


def _enc_str(s: str) -> bytes:
    b = s.encode()
    return len(b).to_bytes(2, "big") + b


def _recv_exact(sock: socket.socket, n: int) -> bytes | None:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def _read_packet(sock: socket.socket) -> tuple[int, bytes] | None:
    hdr = _recv_exact(sock, 1)
    if hdr is None:
        return None
    mult, rem = 1, 0
    for _ in range(4):
        b = _recv_exact(sock, 1)
        if b is None:
            return None
        rem += (b[0] & 0x7F) * mult
        if not b[0] & 0x80:
            return hdr[0] >> 4, _recv_exact(sock, rem) or b""
        mult *= 128
    return None


class MqttClient:
    """CONNECT/SUBSCRIBE/PINGREQ + QoS0 PUBLISH receipt -- all this broker needs."""

    def __init__(self, host: str, port: int, client_id: str):
        self.addr = (host, port)
        self.client_id = client_id
        self.sock: socket.socket | None = None

    def connect(self, topic_filter: str) -> None:
        self.sock = socket.create_connection(self.addr, timeout=10)
        self.sock.settimeout(1.0)
        vh = _enc_str("MQTT") + bytes([4, 0x02]) + KEEPALIVE_S.to_bytes(2, "big")
        vh += _enc_str(self.client_id)
        self.sock.sendall(bytes([0x10]) + _enc_len(len(vh)) + vh)
        pkt = _read_packet(self.sock)
        if pkt is None or pkt[0] != 2 or pkt[1][1] != 0:
            raise ConnectionError(f"broker CONNACK failed: {pkt!r}")
        body = b"\x00\x01" + _enc_str(topic_filter) + b"\x00"  # pid 1, qos 0
        self.sock.sendall(bytes([0x82]) + _enc_len(len(body)) + body)
        pkt = _read_packet(self.sock)
        if pkt is None or pkt[0] != 9:
            raise ConnectionError(f"broker SUBACK failed: {pkt!r}")
        log(f"[mqtt] connected to {self.addr[0]}:{self.addr[1]}, subscribed {topic_filter}")

    def next_publish(self, deadline: float) -> tuple[str, bytes] | None:
        """Block until the next PUBLISH or the ping deadline. Returns None on ping."""
        assert self.sock is not None
        while True:
            timeout = max(deadline - time.monotonic(), 0.01)
            self.sock.settimeout(timeout)
            try:
                pkt = _read_packet(self.sock)
            except socket.timeout:
                self.sock.sendall(bytes([0xD0, 0x00]))  # PINGREQ
                return None
            if pkt is None:
                raise ConnectionError("broker closed the connection")
            ptype, body = pkt
            if ptype == 13:          # PINGRESP
                continue
            if ptype != 3:           # anything else is not interesting here
                continue
            qos = 0
            i = 0
            tlen = (body[i] << 8) | body[i + 1]
            i += 2
            topic = body[i:i + tlen].decode(errors="replace")
            i += tlen
            if qos:                  # broker only fanouts QoS0, but stay safe
                i += 2
            return topic, body[i:]

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.sendall(bytes([0xE0, 0x00]))  # DISCONNECT
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
            cur.execute(
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
