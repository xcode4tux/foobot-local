#!/usr/bin/env python3
"""
provision.py -- Reconnect a Foobot to Wi-Fi WITHOUT the (now-dead) mobile app.

If you changed router / Wi-Fi password, or the Foobot simply fell off the
network, it can no longer be set up through the official app. This reproduces
the app's provisioning protocol, reconstructed by decompiling the official APK
(com.airboxlab.foobot, classes setup/TCPClient and setup/manual/TCPClientManual):

  - In access-point (config) mode the Foobot exposes its Hi-Flying module at IP
    10.10.100.254 and listens on TCP port 1337.
  - You send ONE line, terminated by \\n:
        w;<SSID>;<password>
    (static-IP variant : w;<SSID>;<password>;<ip>;<mask>;<gateway>;<dns>)
  - The module replies, then REBOOTS and joins the requested network.

Prerequisite: the machine running this must be connected to the FOOBOT's Wi-Fi
access point (see scan.sh / join.sh). The Foobot only does 2.4 GHz WPA/WPA2
(no WPA3).

Examples:
  python3 provision.py --ssid "MyHome-2.4GHz" --pass 'secret'
  python3 provision.py --ssid "MyHome-2.4GHz" --pass 'x' --dry-run
  python3 provision.py --raw 'w;MyHome-2.4GHz;secret'   # raw frame (fallback)
"""
import argparse
import socket
import sys
import time

HOST = "10.10.100.254"
PORT = 1337


def build_command(ssid, password, static=None):
    """Rebuild the 'w;...' command exactly like the official app."""
    if static:
        ip, mask, gw, dns = static
        return f"w;{ssid};{password};{ip};{mask};{gw};{dns}"
    return f"w;{ssid};{password}"


def send_command(cmd, host=HOST, port=PORT, connect_timeout=8.0, read_timeout=6.0):
    """Open the socket, send the command (+ \\n), read the module's reply."""
    print(f"-> TCP connect {host}:{port} ...")
    with socket.create_connection((host, port), timeout=connect_timeout) as s:
        s.settimeout(read_timeout)
        payload = (cmd + "\n").encode("utf-8")
        print(f"-> send ({len(payload)} bytes): {cmd!r}")
        s.sendall(payload)
        # The module replies then reboots; give it a moment and read what comes.
        time.sleep(1.0)
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
            print(f"<- reply ({len(reply)} bytes): {reply!r}")
        else:
            print("<- no reply read (the module may have rebooted already -- not necessarily a failure).")
        return reply


def preflight(host=HOST):
    """Check we're actually on the Foobot's AP (local IP in 10.10.100.x)."""
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe.connect((host, PORT))
        local_ip = probe.getsockname()[0]
        probe.close()
        print(f"  local IP toward {host}: {local_ip}")
        if not local_ip.startswith("10.10.100."):
            print("  WARNING: local IP is NOT 10.10.100.x -- you are probably not")
            print("           connected to the Foobot's access point. Run join.sh first.")
            return False
        return True
    except OSError as e:
        print(f"  WARNING: cannot route to {host} ({e}). Are you on the Foobot's AP?")
        return False


def main():
    p = argparse.ArgumentParser(description="Reprovision a Foobot's Wi-Fi (Hi-Flying module).")
    p.add_argument("--ssid", help="target Wi-Fi SSID (2.4 GHz WPA/WPA2)")
    p.add_argument("--pass", dest="password", help="target network password")
    p.add_argument("--raw", help="raw frame to send (e.g. 'w;SSID;pass'). Overrides --ssid/--pass.")
    p.add_argument("--static", nargs=4, metavar=("IP", "MASK", "GW", "DNS"),
                   help="optional static IP config")
    p.add_argument("--host", default=HOST, help=f"module IP (default {HOST})")
    p.add_argument("--port", type=int, default=PORT, help=f"TCP port (default {PORT})")
    p.add_argument("--no-preflight", action="store_true", help="skip the AP-membership check")
    p.add_argument("--dry-run", action="store_true", help="print the frame without sending it")
    args = p.parse_args()

    if args.raw:
        cmd = args.raw
    else:
        if not args.ssid or args.password is None:
            p.error("provide --ssid and --pass (or --raw)")
        cmd = build_command(args.ssid, args.password, args.static)

    print("=== Foobot -- Wi-Fi reprovisioning ===")
    print(f"Command: {cmd!r}  -> {args.host}:{args.port}")

    if args.dry_run:
        print("(dry-run: nothing sent)")
        return 0

    if not args.no_preflight:
        preflight(args.host)

    try:
        send_command(cmd, host=args.host, port=args.port)
    except OSError as e:
        print(f"x network failure: {e}")
        print("  Check you are connected to the Foobot's AP and that 10.10.100.254 answers.")
        return 1

    print("OK. The Foobot should blink, then join the requested network.")
    print("  Put your machine back on the LAN and look for the Foobot there.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
