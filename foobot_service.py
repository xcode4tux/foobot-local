#!/usr/bin/env python3
"""
foobot_service.py -- Permanent local service for the Foobot (replaces the cloud).

A permissive MQTT broker (emulating broker-gw-nc.foobot.io) plus the full
pipeline:
  device --MQTT--> [this service] --REST--> Home Assistant (optional)
  sensor/push (raw) -> decode + allpollu (pollution.py) -> dashboard sensors
  + LED command injection via the ./inject file
  + optional night LED schedule

Pure standard library (stdlib + urllib). Configured via environment variables:
  FOOBOT_PORT   MQTT listen port              (default 1883)
  FOOBOT_UUID   device clientID / UUID         (REQUIRED for LED + cloud relay)
  HA_URL        Home Assistant REST base        (default http://127.0.0.1:8123)
  HA_TOKEN      HA long-lived token             (if unset -> DRY-RUN, log only)
  LED_SCHED     enable night LED schedule       (default 1; "0" disables;
                 needs FOOBOT_UUID + FOOBOT_IP, else it stays off)
  LED_OFF_H     LED off hour                    (default 22)
  LED_ON_H      LED on hour                     (default 7)
  LED_ON_VAL    daytime brightness              (default 47)

Optional cloud fallback (NOT needed for local operation -- see README):
  FOOBOT_TOKEN  api.foobot.io API key           (only if you want the fallback)
  FOOBOT_USER   api.foobot.io username
  CLOUD_POLL    seconds between cloud reads      (default 300)

Usage:  python3 foobot_service.py                       (broker + bridge, prod)
        FOOBOT_PORT=11883 HA_TOKEN= python3 foobot_service.py   (sandbox, dry-run)
"""
import queue, socket, threading, datetime, os, time, json
import http.client, urllib.parse, urllib.request, urllib.error

import mqttwire
import pollution

PORT      = int(os.environ.get("FOOBOT_PORT", "1883"))
UUID      = os.environ.get("FOOBOT_UUID", "")
HA_URL    = os.environ.get("HA_URL", "http://127.0.0.1:8123").rstrip("/")
HA_TOKEN  = os.environ.get("HA_TOKEN", "")
LED_OFF_H = int(os.environ.get("LED_OFF_H", "22"))
LED_ON_H  = int(os.environ.get("LED_ON_H", "7"))
LED_ON_VAL = int(os.environ.get("LED_ON_VAL", "47"))

BASE   = os.path.dirname(os.path.abspath(__file__))
LOGDIR = "/app" if os.path.isdir("/app") else BASE
LOG    = os.path.join(LOGDIR, "service.log")
INJECT = os.path.join(LOGDIR, "inject")
LED_CFG = os.path.join(LOGDIR, "led_config.json")   # editable night schedule
RETAINED_FILE = os.path.join(LOGDIR, "retained.json")  # last readings, for restart-parity

LOG_MAX_BYTES = 5 * 1024 * 1024     # rotate service.log above 5 MiB (one .1 backup)

_lock = threading.Lock()

# persistent unbuffered handle: one write syscall per line (the old
# open/stat/write/close-per-line cost 4), size tracked incrementally
_log_fh = None
_log_bytes = 0

def _open_log():
    global _log_fh, _log_bytes
    _log_fh = open(LOG, "ab", buffering=0)
    _log_bytes = os.path.getsize(LOG)

def log(msg):
    global _log_fh, _log_bytes
    line = f"{datetime.datetime.now().strftime('%H:%M:%S')} {msg}\n"
    with _lock:
        print(line, end="", flush=True)
        try:
            if _log_fh is None:
                _open_log()
            elif _log_bytes > LOG_MAX_BYTES:
                _log_fh.close(); _log_fh = None
                os.replace(LOG, LOG + ".1")
                _open_log()
            data = line.encode()
            _log_fh.write(data)
            _log_bytes += len(data)
        except Exception:
            _log_fh = None            # retry the open on the next line; never
            pass                      # let logging take the service down

# --- MQTT wire (minimal 3.1.1, enough for the Foobot) -------------------------
MAX_PACKET  = 1 << 20     # 1 MiB cap on a single MQTT packet (sensor bursts are KBs)
MAX_CLIENTS = 32          # concurrent broker connections
OUT_Q_MAX   = 64          # outbound packets buffered per client before dropping
IDLE_DROP_S = 900         # drop a client silent in both directions for 15 min

class Client:
    """One connected MQTT client: an outbound queue drained by a dedicated
    writer thread, plus activity timestamps. Every send is queued so a stalled
    receiver (e.g. a Wio whose radio went deaf and stopped ACKing) can never
    block a publisher's thread: its queue fills and packets are dropped instead.
    The idle reaper closes fully silent clients (half-open TCP connections)."""
    __slots__ = ("sock", "outq", "dead", "last_recv", "last_send", "writer")

    def __init__(self, sock):
        self.sock = sock
        self.outq = queue.Queue(maxsize=OUT_Q_MAX)
        self.dead = False
        self.last_recv = self.last_send = time.monotonic()
        with clients_lock:
            all_clients.add(self)
        self.writer = threading.Thread(target=self._writer, daemon=True)
        self.writer.start()

    def _writer(self):
        while True:
            pkt = self.outq.get()
            if pkt is None:
                return
            try:
                self.sock.sendall(pkt)
                self.last_send = time.monotonic()
            except OSError:
                self.dead = True
                self._shutdown()
                with clients_lock:
                    all_clients.discard(self)
                return

    def send(self, pkt):
        """Queue one packet (never blocks). False if the client is dead or its
        queue is full (packet dropped -- preferable to blocking a publisher)."""
        if self.dead:
            return False
        try:
            self.outq.put_nowait(pkt)
            return True
        except queue.Full:
            return False

    def _shutdown(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass

    def close(self):
        was_dead = self.dead
        self.dead = True
        if not was_dead:
            try:
                self.outq.put_nowait(None)   # stop the writer after pending sends
            except queue.Full:
                pass
        self._shutdown()
        with clients_lock:
            all_clients.discard(self)


clients = {}         # clientID -> Client
subs = {}            # clientID -> set of topic filters (local fan-out extension)
all_clients = set()  # every live Client, registered or not (caps + idle reaper)
clients_lock = threading.Lock()

# Retained sensor readings: exact-topic -> last sensor/push payload. Replayed to
# a client the moment it SUBSCRIBEs, so a subscriber that connects between
# readings (e.g. a Wio Terminal, which caches nothing and renders only on a live
# push) paints immediately instead of sitting blank until the next ~5-min reading.
# In-RAM only: cleared on service restart, refilled by the next publish. Capped so
# a flood of spoofed device UUIDs on the LAN cannot grow it without bound.
RETAINED_MAX = 16
retained = {}        # topic -> payload bytes  (guarded by clients_lock)
_retained_io_lock = threading.Lock()   # serialize disk writes across conn threads


def load_retained():
    """Seed `retained` from disk at startup so a broker restart does not blank a
    subscriber (e.g. the Wio) until the next ~5-min reading. Payloads are stored
    latin-1-encoded (a lossless byte<->str mapping) so the exact raw bytes -- VOC
    included -- are replayed, which is what the Wio firmware requires."""
    try:
        with open(RETAINED_FILE) as f:
            data = json.load(f)
    except FileNotFoundError:
        return
    except Exception as e:
        log(f"retained: seed load failed ({e!r})")
        return
    if not isinstance(data, dict):
        return
    for t, s in data.items():
        if isinstance(t, str) and isinstance(s, str) and len(retained) < RETAINED_MAX:
            retained[t] = s.encode("latin-1")
    if retained:
        log(f"retained: seeded {len(retained)} topic(s) from {RETAINED_FILE}")


def save_retained(snapshot):
    """Atomically persist a snapshot of `retained` (temp file + os.replace)."""
    tmp = RETAINED_FILE + ".tmp"
    try:
        with _retained_io_lock:
            with open(tmp, "w") as f:
                json.dump({t: p.decode("latin-1") for t, p in snapshot.items()}, f)
            os.replace(tmp, RETAINED_FILE)
    except Exception as e:
        log(f"retained: save failed ({e!r})")

def fanout(topic, payload):
    """Deliver a received PUBLISH to every subscriber whose filter matches.
    Upstream service only forwarded injected commands to devices; local
    dashboard clients (e.g. a Wio Terminal) need real subscription delivery."""
    pkt = mqttwire.build_publish(topic, payload, qos=0)
    with clients_lock:
        targets = [(cid, c) for cid, c in clients.items()
                   if any(mqttwire.topic_matches(f, topic) for f in subs.get(cid, ()))]
    for cid, c in targets:
        if c.send(pkt):
            log(f"   fanout -> {cid} {topic}")
        else:
            log(f"   fanout dropped for {cid} (dead or queue full) {topic}")

# --- Home Assistant REST (optional dashboard target) --------------------------
class HaClient:
    """HA REST with ONE persistent HTTP connection. Six entities are pushed
    per reading plus a 20 s keepalive re-push, so a fresh TCP handshake per
    POST (what urllib does) dominated the service's syscall budget; HA is a
    local HTTP server and happily keeps the connection open. A broken or
    server-closed connection is retried once on a fresh socket. Used only
    from the single HA worker thread, so no locking is needed."""

    def __init__(self, base_url, token, timeout=5):
        u = urllib.parse.urlsplit(base_url)
        self.host, self.port = u.hostname, u.port or (443 if u.scheme == "https" else 80)
        self.tls = u.scheme == "https"
        self.token, self.timeout = token, timeout
        self.conn = None

    def _connect(self):
        cls = http.client.HTTPSConnection if self.tls else http.client.HTTPConnection
        self.conn = cls(self.host, self.port, timeout=self.timeout)

    def post_state(self, entity, body):
        """POST /api/states/<entity>; returns the HTTP status code or None
        (both error paths already logged by the caller-facing wrapper)."""
        for attempt in (1, 2):
            if self.conn is None:
                self._connect()
            try:
                self.conn.request("POST", f"/api/states/{entity}", body=body, headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                })
                resp = self.conn.getresponse()
                resp.read()                      # drain: keeps the connection reusable
                return resp.status
            except (http.client.HTTPException, OSError):
                try: self.conn.close()
                except Exception: pass          # nosec B110 - best-effort close
                self.conn = None
                if attempt == 2:
                    return None

    def close(self):
        if self.conn is not None:
            try: self.conn.close()
            except Exception: pass              # nosec B110 - best-effort close
            self.conn = None


HA_HTTP = HaClient(HA_URL, HA_TOKEN) if HA_TOKEN else None

def ha_state(entity, state, attrs):
    """POST /api/states/<entity>. In DRY-RUN (no token): log only."""
    if HA_HTTP is None:
        log(f"   [dry-run] HA {entity} = {state}  {attrs}")
        return
    body = json.dumps({"state": state, "attributes": attrs}).encode()
    status = HA_HTTP.post_state(entity, body)
    if status is None:
        log(f"   HA push failed {entity}: connection error")
    elif status >= 400:
        log(f"   HA push failed {entity}: HTTP {status}")

# entity + unit + device_class per reading (so HA renders them nicely).
HA_MAP = {
    "co2":      ("sensor.foobot_co2",         "ppm",   "carbon_dioxide", "Foobot CO2"),
    "voc":      ("sensor.foobot_voc",         "ppb",   "volatile_organic_compounds_parts", "Foobot VOC"),
    "pm":       ("sensor.foobot_pm25",        "µg/m³", "pm25", "Foobot PM2.5"),
    "tmp":      ("sensor.foobot_temperature", "°C",    "temperature", "Foobot Temperature"),
    "hum":      ("sensor.foobot_humidity",    "%",     "humidity", "Foobot Humidity"),
    "allpollu": ("sensor.foobot_pollution",   "%",     None, "Foobot Pollution"),
}

LAST_MEASURES = None            # last pushed measure (for the keepalive)
LAST_LOCK = threading.Lock()

# HA pushes run on a single worker off the connection threads: a slow or
# restarted HA (up to ~30 s of timeouts per reading) must never stall the
# broker's read loops. Producers enqueue; the worker pushes.
HA_QUEUE = queue.Queue(maxsize=100)

def ha_enqueue(measures):
    try:
        HA_QUEUE.put_nowait(dict(measures))
    except queue.Full:
        log("   HA queue full -- push dropped (is Home Assistant hung?)")

def ha_worker():
    while True:
        m = HA_QUEUE.get()
        try:
            push_measures(m, remember=True)
        except Exception as e:
            log(f"   HA worker error: {e!r}")

def push_measures(enriched, remember=True):
    for key, (entity, unit, dclass, name) in HA_MAP.items():
        if key not in enriched:
            continue
        attrs = {"unit_of_measurement": unit, "friendly_name": name}
        if dclass:
            attrs["device_class"] = dclass
        if enriched.get("source"):
            attrs["source"] = enriched["source"]        # "local" | "cloud"
        if key == "allpollu":
            attrs["band"] = enriched.get("band")
            attrs["ring_color"] = enriched.get("color")
        ha_state(entity, enriched[key], attrs)
    if remember:
        global LAST_MEASURES
        with LAST_LOCK:
            LAST_MEASURES = dict(enriched)

def keepalive_pusher():
    """Re-push the last measure every 20 s. Purpose: after a HA restart the
    `sensor.foobot_*` states (pushed over REST, not persisted) come back in <=20 s
    instead of waiting for the device's next reading (~5 min). HA ignores
    identical states, so history is not polluted."""
    if not HA_TOKEN:
        return          # dry-run: no HA to repopulate; would only spam the log
    while True:
        time.sleep(20)
        with LAST_LOCK:
            m = dict(LAST_MEASURES) if LAST_MEASURES else None
        if m:
            ha_enqueue(m)

def handle_sensor_push(payload_txt):
    """sensor/push is a JSON ARRAY of raw readings -> enrich and push."""
    try:
        data = json.loads(payload_txt)
    except ValueError:
        log(f"   non-JSON sensor/push ignored: {payload_txt[:80]}")
        return
    rows = data if isinstance(data, list) else [data]
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        enriched = pollution.enrich(raw)
        enriched["source"] = "local"
        log(f"   * measures -> {enriched}")
        ha_enqueue(enriched)

# --- optional cloud relay (api.foobot.io) -------------------------------------
# NOT required for local operation. Only active if FOOBOT_TOKEN is set. Used as a
# fallback: when the device is NOT on the local broker (still on the cloud), relay
# its cloud readings so the same `sensor.foobot_*` entities stay populated.
CLOUD_POLL = int(os.environ.get("CLOUD_POLL", "300"))
CLOUD_BASE = "https://api.foobot.io"

def _cloud_creds():
    tok = os.environ.get("FOOBOT_TOKEN", "")
    usr = os.environ.get("FOOBOT_USER", "")
    if not (tok and usr):
        try:
            j = json.load(open(os.path.join(LOGDIR, "cloud_secret.json")))
            tok = tok or j.get("token", ""); usr = usr or j.get("username", "")
        except Exception:
            pass
    return tok, usr

def cloud_fetch_last():
    tok, _usr = _cloud_creds()
    if not (tok and UUID):
        return None
    try:
        req = urllib.request.Request(f"{CLOUD_BASE}/v2/device/{UUID}/datapoint/0/last/0/",
            headers={"X-API-KEY-TOKEN": tok, "Accept": "application/json"})
        d = json.loads(urllib.request.urlopen(req, timeout=10).read().decode())
        rec = dict(zip(d["sensors"], d["datapoints"][-1]))   # time,pm,tmp,hum,co2,voc,allpollu
    except Exception as e:
        log(f"   cloud fetch failed: {e!r}")
        return None
    out = {}
    for k in ("co2", "voc", "pm", "tmp", "hum"):
        if k in rec:
            out[k] = round(float(rec[k]), 2)
    if "allpollu" in rec:
        ap = round(float(rec["allpollu"]), 1)
        out["allpollu"] = ap
        out["band"] = pollution.band(ap)
        out["color"] = pollution.color(ap)
    return out

def cloud_poller():
    """Only runs the relay while the device is NOT on the local broker."""
    tok, _ = _cloud_creds()
    if not tok:
        return                       # cloud fallback disabled -> pure local
    time.sleep(45)                   # let the device reconnect locally first
    while True:
        with clients_lock:
            local = UUID in clients
        if not local:
            data = cloud_fetch_last()
            if data:
                data["source"] = "cloud"
                log(f"   cloud -> {data}")
                ha_enqueue(data)
        time.sleep(CLOUD_POLL)

# --- LED injection (./inject file) + schedule ---------------------------------
def inject_now(topic, payload):
    pkt = mqttwire.build_publish(topic, payload, qos=0)
    with clients_lock:
        targets = list(clients.items())
    if not targets:
        log(f"   inject SKIPPED (no device connected) topic={topic}")
    for cid, c in targets:
        if c.send(pkt):
            log(f"   >>> INJECT -> {cid} {topic} {payload}")
        else:
            log(f"   inject dropped for {cid} (dead or queue full) {topic}")

def inject_watcher():
    while True:
        try:
            if os.path.exists(INJECT) and os.path.getsize(INJECT) > 0:
                lines = [l.rstrip("\n") for l in open(INJECT) if l.strip()]
                open(INJECT, "w").close()
                for line in lines:
                    if "|" in line:
                        t, p = line.split("|", 1); inject_now(t, p)
        except Exception as e:
            log(f"inject_watcher err: {e!r}")
        time.sleep(1)

def load_led_cfg():
    """Night-schedule config read HOT from led_config.json (falls back to env).
    Either returns a fully validated config or the defaults -- never a mix."""
    cfg = {"enabled": True, "off_h": LED_OFF_H, "on_h": LED_ON_H, "day_val": LED_ON_VAL}
    try:
        with open(LED_CFG) as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("expected a JSON object")
        cfg["off_h"]   = int(data.get("off_h", cfg["off_h"])) % 24
        cfg["on_h"]    = int(data.get("on_h", cfg["on_h"])) % 24
        cfg["day_val"] = max(0, min(47, int(data.get("day_val", cfg["day_val"]))))
        cfg["enabled"] = bool(data.get("enabled", cfg["enabled"]))
    except FileNotFoundError:
        pass
    except Exception as e:
        log(f"led_config.json invalid, using defaults: {e!r}")
    return cfg

def reboot_ring():
    """Wake the ring via a module REBOOT (AT+Z) -- the only reliable way. Once the
    ring is off, brightness/refresh do NOT redraw it; only a reboot does. Does not
    touch WSDNS. Requires FOOBOT_IP (see foobot_at.py)."""
    if not os.environ.get("FOOBOT_IP"):
        # foobot_at.open_session sys.exits when FOOBOT_IP is unset; SystemExit
        # is not an Exception and would silently kill this scheduler thread.
        log("   reboot_ring: FOOBOT_IP not set -- cannot wake the ring "
            "(set FOOBOT_IP in the unit file to enable morning wake)")
        return
    try:
        import foobot_at as fa
    except Exception as e:
        log(f"   reboot_ring: foobot_at unavailable ({e!r})")
        return
    try:
        s = fa.open_session(attempts=3, delay=3, verbose=False)
        if s is None:
            log("   reboot_ring: module unreachable (UDP 48899)")
            return
        try:
            fa.at(s, "AT+Z"); log("   reboot_ring: AT+Z sent (waking the ring)")
        finally:
            try: s.close()
            except Exception: pass
    except SystemExit as e:
        log(f"   reboot_ring aborted: {e}")
    except Exception as e:
        log(f"   reboot_ring failed: {e!r}")

def led_scheduler():
    """Ring schedule: OFF at night (brightness=0), WAKE in the morning via REBOOT
    (brightness does not relight a ring that is off). Acts ONLY on day/night
    transitions; does NOTHING at startup. Params read hot from led_config.json."""
    last = None
    while True:
        cfg = load_led_cfg()
        if cfg["enabled"]:
            h = datetime.datetime.now().hour
            off_h, on_h = cfg["off_h"], cfg["on_h"]
            night = (off_h <= h < on_h) if off_h <= on_h else (h >= off_h or h < on_h)
            phase = "night" if night else "day"
            if last is not None and phase != last:
                if phase == "night":
                    inject_now(f"device/{UUID}/attribute/brightness", json.dumps({"brightness": 0}))
                    log(f"   LED schedule -> NIGHT: off (brightness=0) (h={h})")
                else:
                    log(f"   LED schedule -> DAY: waking the ring via reboot (h={h})")
                    reboot_ring()
            last = phase
        else:
            last = None
        time.sleep(60)

# --- broker loop --------------------------------------------------------------
def handle(conn, addr):
    peer = f"{addr[0]}:{addr[1]}"; myid = None
    c = Client(conn)
    log(f"++ connection {peer}")
    try:
        while True:
            hdr = mqttwire.recv_exact(conn, 1)
            if hdr is None: break
            ptype, flags = hdr[0] >> 4, hdr[0] & 0x0f
            rem = mqttwire.read_remaining_length(conn)
            if rem is None or rem > MAX_PACKET:
                log(f"[{peer}] malformed or oversized packet (rem={rem}) -- closing")
                break
            body = mqttwire.recv_exact(conn, rem) if rem else b""
            if body is None: break
            c.last_recv = time.monotonic()
            if ptype == 1:      # CONNECT
                myid = mqttwire.parse_connect(body)
                if myid is None:
                    log(f"[{peer}] malformed CONNECT -- closing")
                    break
                log(f"[{peer}] CONNECT clientID={myid}")
                with clients_lock: clients[myid] = c
                c.send(mqttwire.build_connack())
            elif ptype == 3:    # PUBLISH
                parsed = mqttwire.parse_publish(flags, body)
                if parsed is None:
                    log(f"[{peer}] malformed PUBLISH -- closing")
                    break
                t, pid, payload = parsed
                log(f"[{peer}] PUBLISH {t}")
                if pid is not None:
                    c.send(mqttwire.build_puback(pid))
                if t.endswith("/sensor/push"):
                    handle_sensor_push(payload.decode(errors="replace"))
                    snapshot = None
                    with clients_lock:                       # retain for late subscribers
                        if t in retained or len(retained) < RETAINED_MAX:
                            retained[t] = payload
                            snapshot = dict(retained)
                    if snapshot is not None:                 # persist for restart-parity
                        save_retained(snapshot)
                elif t.endswith("/debug/push") or t.endswith("/init/push"):
                    log(f"   {t.rsplit('/',2)[-2]} -> {payload.decode(errors='replace')[:400]}")
                fanout(t, payload)
            elif ptype == 8:    # SUBSCRIBE
                parsed = mqttwire.parse_subscribe(body)
                if parsed is None:
                    log(f"[{peer}] malformed SUBSCRIBE -- closing")
                    break
                pid, sub_list = parsed
                filters = [f for f, _ in sub_list]
                if myid is not None:
                    # MQTT semantics: SUBSCRIBE adds filters; replacing the set
                    # would silently drop earlier subscriptions of this client.
                    with clients_lock: subs.setdefault(myid, set()).update(filters)
                    log(f"[{peer}] SUBSCRIBE {filters}")
                c.send(mqttwire.build_suback(pid, [q for _, q in sub_list]))
                # Replay retained readings AFTER the SUBACK (MQTT ordering) so a
                # freshly-connected subscriber renders the last reading at once.
                with clients_lock:
                    hits = [(t, p) for t, p in retained.items()
                            if any(mqttwire.topic_matches(f, t) for f in filters)]
                for t, p in hits:
                    if c.send(mqttwire.build_publish(t, p, qos=0)):
                        log(f"   replay -> {myid or peer} {t}")
            elif ptype == 12:   # PINGREQ
                c.send(mqttwire.PINGRESP)
            elif ptype == 14:   # DISCONNECT
                break
    except Exception as e:
        log(f"[{peer}] socket err: {e!r}")
    finally:
        try: conn.close()
        except Exception: pass
        c.close()
        if myid is not None:
            with clients_lock:
                if clients.get(myid) is c: del clients[myid]
                subs.pop(myid, None)
        log(f"-- disconnect {peer}")

def idle_reaper():
    """Close clients silent in BOTH directions for IDLE_DROP_S. Clears half-open
    TCP connections (peer slept, Wio radio died) so their clientID slot frees up
    and their writer thread exits; a peer that only receives (never pings nor
    publishes) for 15 min is gone in practice anyway."""
    while True:
        time.sleep(60)
        now = time.monotonic()
        with clients_lock:
            stale = [cl for cl in all_clients
                     if not cl.dead and now - max(cl.last_recv, cl.last_send) > IDLE_DROP_S]
        for cl in stale:
            log("   reaper: closing idle connection")
            cl.close()

def main():
    load_retained()          # restart-parity: replay last readings to early subscribers
    threading.Thread(target=inject_watcher, daemon=True).start()
    threading.Thread(target=cloud_poller, daemon=True).start()
    threading.Thread(target=keepalive_pusher, daemon=True).start()
    threading.Thread(target=ha_worker, daemon=True).start()
    threading.Thread(target=idle_reaper, daemon=True).start()
    if os.environ.get("LED_SCHED", "1") == "1":
        # night dim needs FOOBOT_UUID (topic), morning wake needs FOOBOT_IP (AT).
        # Running with only one would leave the ring stuck off overnight.
        if UUID and os.environ.get("FOOBOT_IP"):
            threading.Thread(target=led_scheduler, daemon=True).start()
        else:
            log("LED scheduler OFF: set FOOBOT_UUID and FOOBOT_IP to enable "
                "(night dim needs the UUID, morning wake needs the IP)")
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", PORT)); srv.listen(50)
    mode = "PROD (push HA)" if HA_TOKEN else "DRY-RUN (log only)"
    log(f"=== foobot_service listening 0.0.0.0:{PORT} -- {mode} ===")
    while True:
        conn, addr = srv.accept()
        with clients_lock:
            n = len(all_clients)
        if n >= MAX_CLIENTS:
            log(f"   refusing {addr[0]}:{addr[1]} ({n} connections >= cap {MAX_CLIENTS})")
            try: conn.close()
            except OSError: pass
            continue
        threading.Thread(target=handle, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    main()
