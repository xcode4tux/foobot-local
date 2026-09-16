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
  LED_SCHED     enable night LED schedule       (default 1; "0" disables)
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
import socket, threading, datetime, os, time, json, urllib.request, urllib.error

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

_lock = threading.Lock()
def log(msg):
    line = f"{datetime.datetime.now().strftime('%H:%M:%S')} {msg}"
    with _lock:
        print(line, flush=True)
        try:
            open(LOG, "a").write(line + "\n")
        except Exception:
            pass

# --- MQTT wire (minimal 3.1.1, enough for the Foobot) -------------------------
clients = {}
subs = {}            # clientID -> set of topic filters (local fan-out extension)
clients_lock = threading.Lock()

def topic_matches(filt, topic):
    """MQTT wildcard match ('+' one level, '#' multi-level suffix)."""
    fp, tp = filt.split("/"), topic.split("/")
    for i, f in enumerate(fp):
        if f == "#":
            return True
        if i >= len(tp) or (f != "+" and f != tp[i]):
            return False
    return len(fp) == len(tp)

def fanout(topic, payload):
    """Deliver a received PUBLISH to every subscriber whose filter matches.
    Upstream service only forwarded injected commands to devices; local
    dashboard clients (e.g. a Wio Terminal) need real subscription delivery."""
    pkt = build_publish(topic, payload, qos=0)
    with clients_lock:
        targets = [(cid, c) for cid, c in clients.items()
                   if any(topic_matches(f, topic) for f in subs.get(cid, ()))]
    for cid, c in targets:
        try:
            c.sendall(pkt)
            log(f"   fanout -> {cid} {topic}")
        except Exception as e:
            log(f"   fanout failed {cid}: {e!r}")

def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        c = sock.recv(n - len(buf))
        if not c: return None
        buf += c
    return buf

def read_remaining_length(sock):
    mult, value = 1, 0
    for _ in range(4):
        b = recv_exact(sock, 1)
        if b is None: return None
        value += (b[0] & 0x7f) * mult
        if not (b[0] & 0x80): return value
        mult *= 128
    return None

def enc_len(n):
    out = bytearray()
    while True:
        b = n % 128; n //= 128
        if n > 0: b |= 0x80
        out.append(b)
        if n == 0: break
    return bytes(out)

def build_publish(topic, payload, qos=0):
    tb = topic.encode()
    pb = payload if isinstance(payload, (bytes, bytearray)) else payload.encode()
    vh = len(tb).to_bytes(2, "big") + tb + pb
    return bytes([0x30 | (qos << 1)]) + enc_len(len(vh)) + bytes(vh)

def read_str(data, i):
    ln = (data[i] << 8) | data[i+1]; i += 2
    return data[i:i+ln], i + ln

# --- Home Assistant REST (optional dashboard target) --------------------------
def ha_state(entity, state, attrs):
    """POST /api/states/<entity>. In DRY-RUN (no token): log only."""
    if not HA_TOKEN:
        log(f"   [dry-run] HA {entity} = {state}  {attrs}")
        return
    body = json.dumps({"state": state, "attributes": attrs}).encode()
    req = urllib.request.Request(f"{HA_URL}/api/states/{entity}", data=body,
        method="POST", headers={"Authorization": f"Bearer {HA_TOKEN}",
                                "Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=5).read()
    except urllib.error.URLError as e:
        log(f"   HA push failed {entity}: {e!r}")

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
    while True:
        time.sleep(20)
        with LAST_LOCK:
            m = dict(LAST_MEASURES) if LAST_MEASURES else None
        if m:
            push_measures(m, remember=False)

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
        push_measures(enriched)

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
                push_measures(data)
        time.sleep(CLOUD_POLL)

# --- LED injection (./inject file) + schedule ---------------------------------
def inject_now(topic, payload):
    pkt = build_publish(topic, payload, qos=0)
    with clients_lock:
        targets = list(clients.items())
    if not targets:
        log(f"   inject SKIPPED (no device connected) topic={topic}")
    for cid, c in targets:
        try:
            c.sendall(pkt); log(f"   >>> INJECT -> {cid} {topic} {payload}")
        except Exception as e:
            log(f"   inject failed {cid}: {e!r}")

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
    """Night-schedule config read HOT from led_config.json (falls back to env)."""
    cfg = {"enabled": True, "off_h": LED_OFF_H, "on_h": LED_ON_H, "day_val": LED_ON_VAL}
    try:
        with open(LED_CFG) as f:
            data = json.load(f)
        for k in cfg:
            if k in data:
                cfg[k] = data[k]
        cfg["off_h"]  = int(cfg["off_h"]) % 24
        cfg["on_h"]   = int(cfg["on_h"]) % 24
        cfg["day_val"] = max(0, min(47, int(cfg["day_val"])))
        cfg["enabled"] = bool(cfg["enabled"])
    except FileNotFoundError:
        pass
    except Exception as e:
        log(f"led_config.json invalid, using defaults: {e!r}")
    return cfg

def reboot_ring():
    """Wake the ring via a module REBOOT (AT+Z) -- the only reliable way. Once the
    ring is off, brightness/refresh do NOT redraw it; only a reboot does. Does not
    touch WSDNS. Requires FOOBOT_IP (see foobot_at.py)."""
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
    log(f"++ connection {peer}")
    try:
        while True:
            hdr = recv_exact(conn, 1)
            if hdr is None: break
            ptype, flags = hdr[0] >> 4, hdr[0] & 0x0f
            rem = read_remaining_length(conn)
            if rem is None: break
            body = recv_exact(conn, rem) if rem else b""
            if body is None: break
            if ptype == 1:      # CONNECT
                i = 0; _, i = read_str(body, i); i += 1; i += 1; i += 2
                cid, i = read_str(body, i); myid = cid.decode(errors="replace")
                log(f"[{peer}] CONNECT clientID={myid}")
                with clients_lock: clients[myid] = conn
                conn.sendall(bytes([0x20, 0x02, 0x00, 0x00]))     # CONNACK ok
            elif ptype == 3:    # PUBLISH
                qos = (flags >> 1) & 0x03; i = 0
                topic, i = read_str(body, i)
                pid = None
                if qos > 0: pid = (body[i] << 8) | body[i+1]; i += 2
                payload = body[i:]; t = topic.decode(errors="replace")
                log(f"[{peer}] PUBLISH {t}")
                if qos == 1 and pid is not None:
                    conn.sendall(bytes([0x40, 0x02, (pid >> 8) & 0xff, pid & 0xff]))
                if t.endswith("/sensor/push"):
                    handle_sensor_push(payload.decode(errors="replace"))
                elif t.endswith("/debug/push") or t.endswith("/init/push"):
                    log(f"   {t.rsplit('/',2)[-2]} -> {payload.decode(errors='replace')[:400]}")
                fanout(t, payload)
            elif ptype == 8:    # SUBSCRIBE
                i = 0; pid = (body[0] << 8) | body[1]; i = 2; grants = []
                filters = []
                while i < len(body):
                    f, i = read_str(body, i); filters.append(f.decode(errors="replace"))
                    grants.append(min(body[i], 2)); i += 1
                if myid is not None:
                    with clients_lock: subs[myid] = set(filters)
                    log(f"[{peer}] SUBSCRIBE {filters}")
                conn.sendall(bytes([0x90, 2+len(grants), (pid>>8)&0xff, pid&0xff]) + bytes(grants))
            elif ptype == 12:   # PINGREQ
                conn.sendall(bytes([0xd0, 0x00]))
            elif ptype == 14:   # DISCONNECT
                break
    except Exception as e:
        log(f"[{peer}] socket err: {e!r}")
    finally:
        try: conn.close()
        except Exception: pass
        if myid is not None:
            with clients_lock:
                if clients.get(myid) is conn: del clients[myid]
                subs.pop(myid, None)
        log(f"-- disconnect {peer}")

def main():
    threading.Thread(target=inject_watcher, daemon=True).start()
    threading.Thread(target=cloud_poller, daemon=True).start()
    threading.Thread(target=keepalive_pusher, daemon=True).start()
    if os.environ.get("LED_SCHED", "1") == "1":
        threading.Thread(target=led_scheduler, daemon=True).start()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", PORT)); srv.listen(50)
    mode = "PROD (push HA)" if HA_TOKEN else "DRY-RUN (log only)"
    log(f"=== foobot_service listening 0.0.0.0:{PORT} -- {mode} ===")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    main()
