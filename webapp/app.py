#!/usr/bin/env python3
"""
app.py -- Web UI for the whole Foobot local setup (one page, no dependencies).

Replaces the manual scan.sh -> join.sh -> provision.py -> foobot_at.py dance with
a single page served on http://<host>:8099. It offers:

  - Status: is the Foobot seen on the LAN (by its MAC)?
  - Wi-Fi reprovisioning: scan for the Foobot's config AP, then push
    w;<SSID>;<password> to 10.10.100.254:1337 (protocol reconstructed from the
    official APK, see ../provision.py), release wlan0, wait for it to rejoin.
  - LED ring: brightness / off, "on" = module reboot (the only reliable relight),
    plus an editable night schedule (written to ../led_config.json, hot-reloaded
    by foobot_service.py).
  - Network mode: flip the module's DNS (WSDNS) between your local host and the
    cloud (../foobot_at.py). Switch back to cloud before shutting your host down.

Pure standard library. The Wi-Fi password is never stored or logged. Meant to run
as a systemd service (user with sudo NOPASSWD for nmcli/rfkill) -- see
foobot-web.service.

Configuration (env vars):
  FOOBOT_WEB_PORT  listen port                 (default 8099)
  WLAN_IFACE       Wi-Fi interface for the AP    (default wlan0)
  FOOBOT_MAC       device MAC (for LAN detection) e.g. aa:bb:cc:dd:ee:ff
  FOOBOT_IP        device's expected/last LAN IP  (helps refresh ARP)
  FOOBOT_UUID      device UUID (for LED topics)
  HOME_SSID        default SSID prefilled in the form
  LOCAL_IP         this host's IP for WSDNS       (default: auto-detected)
  FOOBOT_DIR       dir shared with foobot_service.py (inject / led_config.json;
                                                   default: the repo root)
"""
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("FOOBOT_WEB_PORT", "8099"))
IFACE = os.environ.get("WLAN_IFACE", "wlan0")
FOOBOT_MAC = os.environ.get("FOOBOT_MAC", "").lower()   # e.g. aa:bb:cc:dd:ee:ff
FOOBOT_IP_LAN = os.environ.get("FOOBOT_IP", "")         # expected / last-known LAN IP
FOOBOT_UUID = os.environ.get("FOOBOT_UUID", "")
MODULE_HOST = "10.10.100.254"       # Hi-Flying module in AP (config) mode
MODULE_PORT = 1337
DEFAULT_SSID = os.environ.get("HOME_SSID", "")
CANDIDATE_RE = re.compile(r"foobot|airbox|hf-a11|hf-lpb|lpb100", re.I)
WAIT_LAN_RETURN = 120               # seconds to wait after a reboot/reprovision

# --- shared dir with foobot_service.py (inject file + LED schedule) ------------
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
FOOBOT_DIR = os.environ.get("FOOBOT_DIR", REPO_ROOT)
INJECT_FILE = os.path.join(FOOBOT_DIR, "inject")
LED_CFG = os.path.join(FOOBOT_DIR, "led_config.json")
LED_MAX = 47                        # known-safe daytime brightness
LED_DEFAULTS = {"enabled": True, "off_h": 22, "on_h": 7, "day_val": 47}

# --- module DNS switch (WSDNS) via ../foobot_at.py -----------------------------
WSDNS_CLOUD = "10.10.100.254"       # factory value -> Foobot goes back to the cloud
sys.path.insert(0, REPO_ROOT)
try:
    import foobot_at as fa
except Exception:                   # missing -> network-switch buttons disabled server-side
    fa = None
WSDNS_LOCAL = os.environ.get("LOCAL_IP", "") or (fa._auto_local_ip() if fa else "")


def _at_session():
    """Open an AT session to the module, pointing foobot_at at the device's
    current LAN IP. Returns a socket or None."""
    if fa is None:
        raise RuntimeError("foobot_at module not found (expected in the repo root)")
    present, ip, _ = foobot_on_lan()
    fa.FOOBOT_IP = ip or FOOBOT_IP_LAN
    if not fa.FOOBOT_IP:
        raise RuntimeError("unknown Foobot IP (set FOOBOT_IP or FOOBOT_MAC)")
    return fa.open_session(attempts=3, delay=3, verbose=False)


# --- LED control (via foobot_service.py's inject file) -------------------------
def led_set_brightness(val):
    """Write an immediate brightness command to the broker's inject file.
    NOTE: brightness only reliably DIMS/turns off while the ring is lit. To
    relight a ring that is off, only a reboot works (the "on" button -> /api/led/on)."""
    val = max(0, min(LED_MAX, int(val)))
    line = 'device/%s/attribute/brightness|{"brightness": %d}\n' % (FOOBOT_UUID, val)
    with open(INJECT_FILE, "a") as f:
        f.write(line)
    return val


def led_cfg_read():
    cfg = dict(LED_DEFAULTS)
    try:
        with open(LED_CFG) as f:
            cfg.update({k: v for k, v in json.load(f).items() if k in cfg})
    except (FileNotFoundError, ValueError):
        pass
    return cfg


def led_cfg_write(data):
    cfg = {
        "enabled": bool(data.get("enabled", True)),
        "off_h": int(data.get("off_h", 22)) % 24,
        "on_h": int(data.get("on_h", 7)) % 24,
        "day_val": max(0, min(LED_MAX, int(data.get("day_val", 47)))),
    }
    tmp = LED_CFG + ".tmp"
    with open(tmp, "w") as f:
        json.dump(cfg, f)
    os.replace(tmp, LED_CFG)
    return cfg


# ---------------------------------------------------------------- utilities

def run(cmd, timeout=95):
    """Run a command, return (code, combined stdout+stderr)."""
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, (p.stdout + p.stderr).strip()
    except subprocess.TimeoutExpired:
        return 124, f"timed out: {' '.join(cmd)}"
    except FileNotFoundError:
        return 127, f"command not found: {cmd[0]}"


def foobot_on_lan():
    """Look for the Foobot on the LAN by MAC. Returns (present, ip, detail)."""
    if not FOOBOT_MAC:
        return False, None, "FOOBOT_MAC not set -- cannot detect the device"
    # Pinging the expected IP refreshes the ARP table if the lease is unchanged.
    if FOOBOT_IP_LAN:
        run(["ping", "-c", "1", "-W", "1", FOOBOT_IP_LAN], timeout=5)
    code, out = run(["ip", "neigh"], timeout=5)
    for line in out.splitlines():
        if FOOBOT_MAC in line.lower():
            ip = line.split()[0]
            state = line.split()[-1]
            if state in ("REACHABLE", "STALE", "DELAY", "PROBE"):
                return True, ip, f"seen at {ip} (ARP {state})"
    return False, None, "MAC absent from the ARP table"


def wlan_state():
    code, out = run(["nmcli", "-t", "-f", "DEVICE,STATE,CONNECTION", "device"], timeout=10)
    for line in out.splitlines():
        parts = line.split(":")
        if parts and parts[0] == IFACE:
            return ":".join(parts[1:])
    return "unknown"


def scan_wifi():
    """Turn the radio on and list networks visible on wlan0."""
    run(["sudo", "rfkill", "unblock", "wifi"], timeout=10)
    run(["sudo", "nmcli", "radio", "wifi", "on"], timeout=15)
    time.sleep(2)
    run(["sudo", "nmcli", "device", "wifi", "rescan", "ifname", IFACE], timeout=20)
    time.sleep(4)
    code, out = run(["nmcli", "-t", "-f", "SSID,SIGNAL,SECURITY",
                     "device", "wifi", "list", "ifname", IFACE], timeout=20)
    nets, seen = [], set()
    for line in out.splitlines():
        parts = line.rsplit(":", 2)     # SSID may contain ':' -> split from the right
        if len(parts) != 3 or not parts[0] or parts[0] in seen:
            continue
        ssid, signal, sec = parts
        seen.add(ssid)
        nets.append({
            "ssid": ssid,
            "signal": int(signal) if signal.isdigit() else 0,
            "security": sec or "open",
            "candidate": bool(CANDIDATE_RE.search(ssid)),
        })
    nets.sort(key=lambda r: (not r["candidate"], -r["signal"]))
    return nets


def send_frame(ssid, password, log):
    """Send w;SSID;password to the module (cf. provision.py) and read its reply."""
    cmd = f"w;{ssid};{password}"
    log(f"Sending config frame to {MODULE_HOST}:{MODULE_PORT} "
        f"(SSID \"{ssid}\", password hidden)...")
    with socket.create_connection((MODULE_HOST, MODULE_PORT), timeout=8) as s:
        s.settimeout(6)
        s.sendall((cmd + "\n").encode("utf-8"))
        time.sleep(1)
        chunks = []
        try:
            while True:
                data = s.recv(1024)
                if not data:
                    break
                chunks.append(data)
        except socket.timeout:
            pass
    reply = b"".join(chunks)
    if reply:
        log(f"Module reply: {reply.decode('utf-8', 'replace').strip()}")
    else:
        log("No reply read (the module likely rebooted already -- not necessarily a failure).")


# ---------------------------------------------------------------- background task

LOCK = threading.Lock()
TASK = {"state": "idle", "log": [], "start": None}   # idle | running | success | failure


def log(msg):
    TASK["log"].append(f"{time.strftime('%H:%M:%S')}  {msg}")


def cleanup_wlan(ap_ssid):
    """Disconnect wlan0 and delete the temporary connection profile."""
    run(["sudo", "nmcli", "device", "disconnect", IFACE], timeout=20)
    code, out = run(["nmcli", "-t", "-f", "NAME", "connection", "show"], timeout=10)
    for name in out.splitlines():
        if name.strip() == ap_ssid:
            run(["sudo", "nmcli", "connection", "delete", name.strip()], timeout=15)
    log("wlan0 released (temporary profile deleted).")


def task_provision(ap_ssid, target_ssid, target_pass):
    try:
        log(f"-- Step 1/4: connecting {IFACE} to AP \"{ap_ssid}\"...")
        run(["sudo", "rfkill", "unblock", "wifi"], timeout=10)
        run(["sudo", "nmcli", "radio", "wifi", "on"], timeout=15)
        code, out = run(["sudo", "nmcli", "device", "wifi", "connect",
                         ap_ssid, "ifname", IFACE], timeout=95)
        if code != 0:
            raise RuntimeError(f"cannot connect to the AP: {out}")
        log("Connected to the Foobot's AP.")

        log("-- Step 2/4: checking access to the module (10.10.100.x)...")
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe.connect((MODULE_HOST, MODULE_PORT))
        local_ip = probe.getsockname()[0]
        probe.close()
        log(f"Local IP on the AP: {local_ip}")
        if not local_ip.startswith("10.10.100."):
            raise RuntimeError("local IP is not 10.10.100.x -- wrong network?")

        log("-- Step 3/4: sending the Wi-Fi configuration...")
        send_frame(target_ssid, target_pass, log)

        log("-- Step 4/4: releasing wlan0, then waiting for the LAN return...")
        cleanup_wlan(ap_ssid)
        deadline = time.time() + WAIT_LAN_RETURN
        while time.time() < deadline:
            present, ip, detail = foobot_on_lan()
            if present:
                log(f"OK: Foobot back on the home network -- {detail}.")
                log("The Home Assistant sensors (sensor.foobot_*) will refresh.")
                TASK["state"] = "success"
                return
            time.sleep(5)
        log("x Foobot not seen on the LAN in time. Check the LED (oscillating blue = "
            "normal), the password entered, and your router's device list -- the DHCP "
            "lease may have changed its IP.")
        TASK["state"] = "failure"
    except Exception as e:
        log(f"x Failure: {e}")
        try:
            cleanup_wlan(ap_ssid)
        except Exception:
            pass
        TASK["state"] = "failure"


def task_led_on():
    """Turn the ring on = REBOOT the module (AT+Z), WITHOUT touching WSDNS. Only a
    reboot reliably relights a ring that is off (brightness/refresh do not redraw
    an off ring)."""
    try:
        log("-- Turn ring on: rebooting the module (the only reliable way)")
        s = _at_session()
        if s is None:
            raise RuntimeError("Foobot unreachable on UDP 48899 -- is it online?")
        try:
            log(f"WSDNS (unchanged): {fa.at(s, 'AT+WSDNS')}")
            log("Rebooting the module (AT+Z)...")
            fa.at(s, "AT+Z")
        finally:
            try: s.close()
            except Exception: pass
        log("Waiting for the Foobot to return...")
        deadline = time.time() + WAIT_LAN_RETURN
        while time.time() < deadline:
            present, ip, detail = foobot_on_lan()
            if present:
                log(f"OK: ring relit -- Foobot back ({detail}).")
                TASK["state"] = "success"
                return
            time.sleep(5)
        log("x Foobot not seen on the LAN in time (check power / Wi-Fi).")
        TASK["state"] = "failure"
    except Exception as e:
        log(f"x Error: {e}")
        TASK["state"] = "failure"


def task_switch(mode):
    """Change the module's DNS (WSDNS) then reboot, and wait for the LAN return.
    mode='local' -> your host's broker ; mode='cloud' -> factory (device leaves your host)."""
    try:
        target = WSDNS_LOCAL if mode == "local" else WSDNS_CLOUD
        label = "your local host" if mode == "local" else "the cloud (factory config)"
        if mode == "local" and not target:
            raise RuntimeError("LOCAL_IP unknown (set LOCAL_IP)")
        log(f"-- Switching network config to: {label}")
        s = _at_session()
        if s is None:
            raise RuntimeError("Foobot unreachable on UDP 48899 -- is it online?")
        try:
            log(f"current WSDNS: {fa.at(s, 'AT+WSDNS')}")
            r = fa.at(s, f"AT+WSDNS={target}")
            log(f"write WSDNS={target} -> {r}")
            back = fa.at(s, "AT+WSDNS")
            log(f"re-read: {back}")
            if not back or target not in back:
                raise RuntimeError("WSDNS re-read does not match the value written")
            log("Rebooting the module (AT+Z)...")
            fa.at(s, "AT+Z")
        finally:
            try: s.close()
            except Exception: pass
        log("Waiting for the Foobot to return...")
        deadline = time.time() + WAIT_LAN_RETURN
        while time.time() < deadline:
            present, ip, detail = foobot_on_lan()
            if present:
                log(f"OK: Foobot back on the network -- {detail}.")
                if mode == "local":
                    log("It reconnects to your local broker (sensor.foobot_* sensors).")
                else:
                    log("It went back to the cloud: your host can be shut down safely.")
                TASK["state"] = "success"
                return
            time.sleep(5)
        log("x Foobot not seen on the LAN in time (check power / Wi-Fi).")
        TASK["state"] = "failure"
    except Exception as e:
        log(f"x Failure: {e}")
        TASK["state"] = "failure"


def _start_task(target, args=()):
    """Guarded task launcher -> (ok, error). Sets state=running and clears the log."""
    with LOCK:
        if TASK["state"] == "running":
            return False, "an operation is already running"
        TASK["state"] = "running"
        TASK["log"] = []
        TASK["start"] = time.time()
    threading.Thread(target=target, args=args, daemon=True).start()
    return True, None


# ---------------------------------------------------------------- HTTP server

class Handler(BaseHTTPRequestHandler):

    def _json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json_body(self):
        size = int(self.headers.get("Content-Length") or 0)
        if not size:
            return {}
        return json.loads(self.rfile.read(size).decode("utf-8"))

    def do_GET(self):
        if self.path == "/" or self.path.startswith("/index"):
            body = PAGE.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/api/status":
            present, ip, detail = foobot_on_lan()
            self._json({"foobot_lan": present, "ip": ip, "detail": detail,
                        "wlan": wlan_state(), "task": TASK["state"]})
        elif self.path == "/api/log":
            self._json({"state": TASK["state"], "log": TASK["log"]})
        elif self.path == "/api/led/config":
            self._json(led_cfg_read())
        else:
            self._json({"error": "unknown"}, 404)

    def do_POST(self):
        if self.path == "/api/scan":
            try:
                self._json({"networks": scan_wifi()})
            except Exception as e:
                self._json({"error": str(e)}, 500)
        elif self.path == "/api/provision":
            data = self._json_body()
            ap = (data.get("ap") or "").strip()
            ssid = (data.get("ssid") or "").strip()
            pwd = data.get("password") or ""
            if not ap or not ssid:
                self._json({"error": "Foobot AP and target SSID are required"}, 400)
                return
            ok, err = _start_task(task_provision, (ap, ssid, pwd))
            self._json({"ok": True} if ok else {"error": err}, 200 if ok else 409)
        elif self.path == "/api/led":
            try:
                val = led_set_brightness(self._json_body().get("brightness", 0))
                self._json({"ok": True, "brightness": val})
            except Exception as e:
                self._json({"error": str(e)}, 500)
        elif self.path == "/api/led/on":
            ok, err = _start_task(task_led_on)
            self._json({"ok": True} if ok else {"error": err}, 200 if ok else 409)
        elif self.path == "/api/led/config":
            try:
                self._json({"ok": True, "config": led_cfg_write(self._json_body())})
            except Exception as e:
                self._json({"error": str(e)}, 500)
        elif self.path == "/api/mode":
            mode = (self._json_body().get("mode") or "").strip()
            if mode not in ("local", "cloud"):
                self._json({"error": "invalid mode (local|cloud)"}, 400)
                return
            ok, err = _start_task(task_switch, (mode,))
            self._json({"ok": True} if ok else {"error": err}, 200 if ok else 409)
        else:
            self._json({"error": "unknown"}, 404)

    def log_message(self, fmt, *args):
        pass  # no HTTP log (the password only ever travels in a POST body)


# ---------------------------------------------------------------- HTML page

PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Foobot - local control</title>
<style>
  :root { --bg:#f4f5f7; --card:#ffffff; --text:#1c2733; --muted:#5c6b7a;
          --accent:#0e7490; --ok:#15803d; --ko:#b91c1c; --border:#dde3ea; }
  @media (prefers-color-scheme: dark) {
    :root { --bg:#111827; --card:#1f2937; --text:#e5e7eb; --muted:#9ca3af;
            --accent:#22d3ee; --ok:#4ade80; --ko:#f87171; --border:#374151; }
  }
  * { box-sizing:border-box; }
  body { margin:0; padding:1.2rem; background:var(--bg); color:var(--text);
         font:16px/1.5 -apple-system, "Segoe UI", Roboto, sans-serif; }
  main { max-width:640px; margin:0 auto; display:grid; gap:1rem; }
  h1 { font-size:1.25rem; margin:.2rem 0 .4rem; }
  .card { background:var(--card); border:1px solid var(--border); border-radius:12px;
          padding:1rem 1.2rem; }
  .card h2 { font-size:1rem; margin:0 0 .6rem; }
  .badge { display:inline-block; padding:.15rem .6rem; border-radius:999px;
           font-size:.85rem; font-weight:600; }
  .ok { background:color-mix(in srgb, var(--ok) 15%, transparent); color:var(--ok); }
  .ko { background:color-mix(in srgb, var(--ko) 15%, transparent); color:var(--ko); }
  .muted { color:var(--muted); font-size:.88rem; }
  button { background:var(--accent); color:#fff; border:0; border-radius:8px;
           padding:.55rem 1rem; font-size:.95rem; font-weight:600; cursor:pointer; }
  @media (prefers-color-scheme: dark) { button { color:#0b2530; } }
  button:disabled { opacity:.45; cursor:default; }
  button.secondary { background:transparent; color:var(--accent);
                     border:1px solid var(--accent); }
  input[type=text], input[type=password] {
    width:100%; padding:.5rem .7rem; border:1px solid var(--border); border-radius:8px;
    background:var(--bg); color:var(--text); font-size:.95rem; }
  label { display:block; margin:.6rem 0 .2rem; font-size:.9rem; font-weight:600; }
  .net { display:flex; align-items:center; gap:.6rem; padding:.45rem .3rem;
         border-bottom:1px solid var(--border); }
  .net:last-child { border-bottom:0; }
  .net .name { flex:1; }
  .star { color:var(--accent); font-weight:700; }
  #log { background:var(--bg); border:1px solid var(--border); border-radius:8px;
         padding:.6rem .8rem; font:.82rem/1.5 ui-monospace, Menlo, monospace;
         white-space:pre-wrap; max-height:16rem; overflow-y:auto; }
  .row { display:flex; gap:.6rem; align-items:center; flex-wrap:wrap; margin-top:.7rem; }
</style>
</head>
<body>
<main>
  <h1>Foobot - local control</h1>

  <section class="card">
    <h2>Status</h2>
    <p id="status">Checking...</p>
    <div class="row">
      <button class="secondary" onclick="refreshStatus()">Re-check</button>
    </div>
  </section>

  <section class="card">
    <h2>LED ring</h2>
    <p class="muted">Driven through the local broker (Foobot must be on your host).
    "On" <b>reboots the module</b> (~6 s) - the only reliable way to relight a ring
    that is off. "Off" and the slider act immediately while the ring is lit.</p>
    <div class="row">
      <button onclick="ledOn()">On</button>
      <button class="secondary" onclick="led(0)">Off</button>
    </div>
    <label for="lum">Brightness: <span id="lumVal">47</span></label>
    <input type="range" id="lum" min="0" max="47" value="47" style="width:100%"
           oninput="document.getElementById('lumVal').textContent=this.value"
           onchange="led(this.value)">

    <hr style="border:0;border-top:1px solid var(--border);margin:1.1rem 0">
    <h2>Night schedule</h2>
    <p class="muted">Turns the LED off at night and back on in the morning. The
    slider above takes over until the next day/night switch.</p>
    <label><input type="checkbox" id="schEn"> Schedule enabled</label>
    <div class="row">
      <span><label for="offH">Off hour</label>
        <input type="text" id="offH" style="width:5rem"></span>
      <span><label for="onH">On hour</label>
        <input type="text" id="onH" style="width:5rem"></span>
      <span><label for="dayV">Day brightness</label>
        <input type="text" id="dayV" style="width:5rem"></span>
    </div>
    <div class="row">
      <button onclick="saveSched()">Save schedule</button>
      <span id="schInfo" class="muted"></span>
    </div>
  </section>

  <section class="card">
    <h2>Network mode</h2>
    <p class="muted">Flips the module's DNS (a setting that survives reboots).
    <b>Before shutting down your host</b>, switch the Foobot back to the cloud so
    you don't lose it; switch it back to local afterwards. The module reboots on
    each switch (~30 s).</p>
    <div class="row">
      <button class="secondary" onclick="switchMode('cloud')">Back to cloud (factory)</button>
      <button onclick="switchMode('local')">Use local host (broker)</button>
    </div>
    <p class="muted" id="modeInfo"></p>
  </section>

  <section class="card">
    <h2>Step 1 - put the Foobot into config mode</h2>
    <p class="muted">With the Foobot powered and running normally, <b>flip it upside
    down</b> until the LED starts blinking (per the manual): it then broadcasts its
    access point. Then start the search.</p>
    <div class="row">
      <button id="btnScan" onclick="scan()">Find the Foobot</button>
      <span id="scanInfo" class="muted"></span>
    </div>
    <div id="nets"></div>
  </section>

  <section class="card">
    <h2>Step 2 - send your home Wi-Fi</h2>
    <label for="ssid">Wi-Fi network (2.4 GHz, WPA/WPA2 - no WPA3)</label>
    <input type="text" id="ssid" value="__DEFAULT_SSID__">
    <label for="pwd">Network password</label>
    <input type="password" id="pwd" autocomplete="off">
    <p class="muted">The password is sent once to the Foobot; it is never stored or
    logged.</p>
    <div class="row">
      <button id="btnGo" onclick="send()" disabled>Send configuration</button>
      <span id="goInfo" class="muted">Pick the Foobot's AP in step 1 first.</span>
    </div>
  </section>

  <section class="card">
    <h2>Log</h2>
    <div id="log">(nothing yet)</div>
  </section>
</main>

<script>
let chosenAp = null;
let timer = null;

async function refreshStatus() {
  const e = document.getElementById('status');
  e.textContent = 'Checking...';
  try {
    const r = await (await fetch('/api/status')).json();
    e.innerHTML = r.foobot_lan
      ? '<span class="badge ok">Foobot on the home network</span> ' +
        '<span class="muted">' + r.detail + '</span>'
      : '<span class="badge ko">Foobot not found on the network</span> ' +
        '<span class="muted">' + r.detail + '</span>';
  } catch (err) { e.textContent = 'Error: ' + err; }
}

async function scan() {
  const btn = document.getElementById('btnScan');
  const info = document.getElementById('scanInfo');
  const zone = document.getElementById('nets');
  btn.disabled = true; info.textContent = 'Scanning (~10 s)...'; zone.innerHTML = '';
  try {
    const r = await (await fetch('/api/scan', {method:'POST'})).json();
    if (r.error) throw r.error;
    if (!r.networks.length) { info.textContent = 'No network seen - scan again.'; return; }
    const n = r.networks.filter(x => x.candidate).length;
    info.textContent = n ? n + ' Foobot candidate(s) found (*).'
                         : 'No obvious Foobot AP - check the blinking LED then rescan.';
    for (const x of r.networks) {
      const div = document.createElement('div');
      div.className = 'net';
      div.innerHTML = '<input type="radio" name="ap" value="' + x.ssid.replace(/"/g,'&quot;') + '">' +
        '<span class="name">' + (x.candidate ? '<span class="star">*</span> ' : '') +
        x.ssid + '</span><span class="muted">' + x.signal + '% . ' + x.security + '</span>';
      div.querySelector('input').addEventListener('change', ev => {
        chosenAp = ev.target.value;
        document.getElementById('btnGo').disabled = false;
        document.getElementById('goInfo').textContent = 'AP chosen: ' + chosenAp;
      });
      zone.appendChild(div);
    }
  } catch (err) { info.textContent = 'Error: ' + err; }
  finally { btn.disabled = false; }
}

async function send() {
  if (!chosenAp) return;
  const ssid = document.getElementById('ssid').value.trim();
  const pwd = document.getElementById('pwd').value;
  if (!ssid) { alert('Enter the home network SSID.'); return; }
  if (!pwd && !confirm('Empty password - send anyway?')) return;
  const btn = document.getElementById('btnGo');
  btn.disabled = true;
  const r = await (await fetch('/api/provision', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ap: chosenAp, ssid: ssid, password: pwd})
  })).json();
  if (r.error) { alert(r.error); btn.disabled = false; return; }
  document.getElementById('goInfo').textContent = 'Configuring...';
  if (timer) clearInterval(timer);
  timer = setInterval(pollLog, 1500);
}

async function pollLog() {
  const r = await (await fetch('/api/log')).json();
  const j = document.getElementById('log');
  j.textContent = r.log.join('\\n') || '(nothing yet)';
  j.scrollTop = j.scrollHeight;
  if (r.state === 'running') {
    if (!timer) timer = setInterval(pollLog, 1500);
  } else {
    if (timer) { clearInterval(timer); timer = null; refreshStatus(); }
    document.getElementById('btnGo').disabled = (chosenAp === null);
    if (r.state === 'success') document.getElementById('goInfo').textContent = 'Done.';
    if (r.state === 'failure') document.getElementById('goInfo').textContent = 'Failed - see the log.';
  }
}

async function led(v) {
  try {
    const r = await (await fetch('/api/led', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({brightness: Number(v)})
    })).json();
    if (r.error) throw r.error;
    document.getElementById('lum').value = r.brightness;
    document.getElementById('lumVal').textContent = r.brightness;
  } catch (e) { alert('LED: ' + e); }
}

async function ledOn() {
  if (!confirm('Relighting the ring reboots the Foobot (~6 s). Continue?')) return;
  try {
    const r = await (await fetch('/api/led/on', {method:'POST'})).json();
    if (r.error) throw r.error;
    document.getElementById('lum').value = 47;
    document.getElementById('lumVal').textContent = 47;
    if (timer) clearInterval(timer);
    timer = setInterval(pollLog, 1500);
  } catch (e) { alert('On: ' + e); }
}

async function loadSched() {
  try {
    const c = await (await fetch('/api/led/config')).json();
    document.getElementById('schEn').checked = !!c.enabled;
    document.getElementById('offH').value = c.off_h;
    document.getElementById('onH').value = c.on_h;
    document.getElementById('dayV').value = c.day_val;
  } catch (e) {}
}

async function saveSched() {
  const info = document.getElementById('schInfo');
  const body = {
    enabled: document.getElementById('schEn').checked,
    off_h: Number(document.getElementById('offH').value),
    on_h:  Number(document.getElementById('onH').value),
    day_val: Number(document.getElementById('dayV').value)
  };
  try {
    const r = await (await fetch('/api/led/config', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify(body)
    })).json();
    if (r.error) throw r.error;
    info.textContent = 'schedule saved';
  } catch (e) { info.textContent = 'x ' + e; }
}

async function switchMode(mode) {
  const txt = mode === 'local'
    ? "Switch the Foobot to your local broker? The module reboots (~30 s)."
    : "Put the Foobot back on the cloud (factory config)? The module reboots (~30 s).";
  if (!confirm(txt)) return;
  const r = await (await fetch('/api/mode', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({mode: mode})
  })).json();
  if (r.error) { alert(r.error); return; }
  document.getElementById('modeInfo').textContent = 'Operation running - watch the log below.';
  if (timer) clearInterval(timer);
  timer = setInterval(pollLog, 1500);
}

refreshStatus();
pollLog();
loadSched();
</script>
</body>
</html>
""".replace("__DEFAULT_SSID__", DEFAULT_SSID.replace('"', "&quot;"))


def main():
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Foobot web control on http://0.0.0.0:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
