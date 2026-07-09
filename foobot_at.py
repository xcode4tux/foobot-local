#!/usr/bin/env python3
"""
foobot_at.py -- Reconfigure the Foobot's Wi-Fi module remotely, over the LAN.

The Foobot's Wi-Fi module (Hi-Flying HF-LPB100) exposes an AT command channel
over UDP port 48899 (handshake "HF-A11ASSISTHREAD" then "+ok"), while in station
mode -- no access-point mode, no physical proximity, no cloud. This lets you
point the device at your OWN infrastructure and, crucially, revert it any time.

THE WINNING TRICK -- WSDNS
--------------------------
You cannot durably change the broker hostname: the module rewrites its Socket-B
destination back to `broker-gw-nc.foobot.io` on every reboot / link loss, so
AT+SOCKB=<your-ip> does NOT stick.

What DOES stick is the module's DNS server, `AT+WSDNS`. Point WSDNS at the host
running your local DNS + broker, make that DNS resolve `broker-gw-nc.foobot.io`
to your host, and the device connects to your local broker after a reboot. This
setting survives reboots (stored in NVRAM) and leaves Wi-Fi/IP untouched.

  factory (cloud) value : AT+WSDNS = 10.10.100.254   (module's built-in DNS)
  local revival value   : AT+WSDNS = <LOCAL_IP>      (your broker/DNS host)

Because the 48899 channel is independent of Wi-Fi and IP, the module stays
reachable after switching, so `--to-cloud` (revert) always works remotely.

Configuration (env vars, or edit the defaults below):
  FOOBOT_IP   the Foobot's IP on your LAN        (REQUIRED)
  LOCAL_IP    the host running your DNS + broker  (default: auto-detected)

Modes:
    python3 foobot_at.py                 # READ config (default, no writes)
    python3 foobot_at.py --selftest      # DRY WRITE: rewrite the CURRENT WSDNS
                                         #   value + reboot + verify the channel
                                         #   comes back and the value persisted.
                                         #   Changes no behaviour (stays on cloud).
    python3 foobot_at.py --to-local      # WSDNS -> LOCAL_IP + reboot (go local)
    python3 foobot_at.py --to-cloud      # WSDNS -> 10.10.100.254 + reboot (revert)

WARNING: --selftest / --to-local / --to-cloud WRITE to the device config and
reboot it. All reversible remotely as long as UDP 48899 answers (Wi-Fi is never
touched). For --to-local to be useful, your DNS must resolve
`broker-gw-nc.foobot.io` -> LOCAL_IP and your broker must listen on LOCAL_IP:1883.
"""
import argparse, os, socket, sys, time

UDP_PORT = 48899
HELLO    = b"HF-A11ASSISTHREAD"
CLOUD_DNS = "10.10.100.254"   # module's factory DNS (routes to the real cloud)
BROKER_HOST = "broker-gw-nc.foobot.io"


def _auto_local_ip():
    """Best-effort primary LAN IP of THIS host (no packet actually sent)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


FOOBOT_IP = os.environ.get("FOOBOT_IP", "")
LOCAL_IP  = os.environ.get("LOCAL_IP", "") or _auto_local_ip()


def _require_foobot_ip():
    if not FOOBOT_IP:
        sys.exit("Set FOOBOT_IP to the Foobot's LAN address, e.g. "
                 "FOOBOT_IP=192.168.1.42 python3 foobot_at.py")


def open_session(attempts=1, delay=3.0, verbose=True):
    """Handshake + '+ok' -> a UDP socket in AT command mode, or None.
    Retries `attempts` times (useful right after the module reboots)."""
    _require_foobot_ip()
    for i in range(attempts):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(3)
        try:
            s.sendto(HELLO, (FOOBOT_IP, UDP_PORT))
            ident, _ = s.recvfrom(1024)
            if verbose:
                print(f"module: {ident.decode(errors='replace')}")
            s.sendto(b"+ok", (FOOBOT_IP, UDP_PORT))   # enter command mode (no reply)
            time.sleep(0.3)
            _drain(s)
            return s
        except socket.timeout:
            s.close()
            if i < attempts - 1:
                if verbose:
                    print(f"  (no answer, retrying in {delay:.0f}s...)")
                time.sleep(delay)
    return None


def _drain(s):
    """Flush pending UDP packets (this protocol desyncs request/response
    otherwise -- always drain before sending an AT command)."""
    s.setblocking(False)
    try:
        while True:
            s.recvfrom(2048)
    except (BlockingIOError, OSError):
        pass
    s.setblocking(True)
    s.settimeout(3)


def at(s, cmd):
    """Send one AT command, return the text reply (or None on timeout)."""
    _drain(s)
    s.sendto((cmd + "\r").encode(), (FOOBOT_IP, UDP_PORT))
    try:
        d, _ = s.recvfrom(2048)
        return d.decode(errors="replace").strip()
    except socket.timeout:
        return None


def read_wsdns(s):
    """Return the raw AT+WSDNS reply (e.g. '+ok=10.10.100.254')."""
    return at(s, "AT+WSDNS")


def wsdns_value(resp):
    """Extract the DNS IP from a WSDNS reply, or None."""
    if not resp:
        return None
    return resp.replace("+ok=", "").strip() or None


def write_wsdns(s, ip, reboot=True):
    """Set WSDNS=<ip>, verify the echo, re-read, then reboot (AT+Z)."""
    print(f"-> AT+WSDNS={ip}")
    r = at(s, f"AT+WSDNS={ip}")
    print(f"  reply: {r}")
    if r is None or "ok" not in r.lower():
        return False
    back = wsdns_value(read_wsdns(s))
    print(f"  re-read WSDNS -> {back}")
    if back != ip:
        print("  WARNING: re-read does not match the value written!")
        return False
    if reboot:
        print("-> rebooting the module (AT+Z)...")
        at(s, "AT+Z")            # module reboots: no reliable reply, expected
    return True


def read_config():
    s = open_session()
    if s is None:
        sys.exit("No answer on UDP 48899 -- is the Foobot online / is FOOBOT_IP right?")
    print("=== current config (read-only) ===")
    for cmd in ("AT+WSDNS", "AT+SOCKB", "AT+WMODE", "AT+WANN", "AT+WSSSID", "AT+VER"):
        print(f"  {cmd:10} -> {at(s, cmd)}")
    s.close()
    print("\n(nothing changed -- use --selftest, --to-local or --to-cloud)")


def selftest():
    """Rewrite the CURRENT WSDNS value + reboot + verify the channel returns and
    the value persisted. Proves the write+reboot+revert mechanics WITHOUT
    changing any behaviour (never leaves the cloud)."""
    print("=== DRY WRITE (no behaviour change) ===")
    s = open_session()
    if s is None:
        sys.exit("Foobot unreachable on 48899.")
    cur = wsdns_value(read_wsdns(s))
    print(f"current WSDNS: {cur}")
    if not cur:
        sys.exit("Cannot read the current WSDNS value -- writing nothing.")
    print(f"-> rewriting the SAME value ({cur}) then rebooting...")
    ok = write_wsdns(s, cur, reboot=True)
    s.close()
    if not ok:
        sys.exit("Write not confirmed -- mechanics NOT validated.")
    print("-> waiting for the module to come back (up to ~45s)...")
    s2 = open_session(attempts=15, delay=3.0)
    if s2 is None:
        sys.exit("Module did not return on 48899 after reboot -- do NOT switch yet.")
    after = wsdns_value(read_wsdns(s2))
    s2.close()
    print(f"WSDNS after reboot: {after}")
    if after == cur:
        print("OK: write persisted, clean reboot, channel 48899 came back.")
        print("    -> --to-local and its revert are reliable on this unit.")
        return 0
    print("FAIL: value did not persist after reboot.")
    return 1


def switch(ip, label):
    s = open_session()
    if s is None:
        sys.exit("Foobot unreachable on 48899.")
    ok = write_wsdns(s, ip, reboot=True)
    s.close()
    if not ok:
        sys.exit("Write not confirmed -- nothing rebooted cleanly.")
    print(f"Switched to {label}. The module reboots (~10-20 s) then reconnects.")
    if ip == LOCAL_IP:
        print("  Check: your broker log should show the device CONNECT within ~30 s.")


def main():
    p = argparse.ArgumentParser(description="Reconfigure the Foobot Wi-Fi module remotely (UDP AT).")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--selftest", action="store_true",
                   help="dry write (rewrite the current WSDNS value + reboot)")
    g.add_argument("--to-local", action="store_true",
                   help=f"WSDNS -> LOCAL_IP ({LOCAL_IP}) + reboot")
    g.add_argument("--to-cloud", action="store_true",
                   help=f"WSDNS -> {CLOUD_DNS} (factory / cloud) + reboot")
    args = p.parse_args()

    if args.selftest:
        sys.exit(selftest())
    elif args.to_local:
        switch(LOCAL_IP, f"local host {LOCAL_IP}")
    elif args.to_cloud:
        switch(CLOUD_DNS, "the cloud (factory DNS)")
    else:
        read_config()


if __name__ == "__main__":
    main()
