#!/usr/bin/env bash
# scan.sh -- Find the Foobot's Wi-Fi access point from this host (wlan0).
# In config mode the Foobot exposes an OPEN AP whose SSID looks like
# "Foobot-xxxx", "FoobotAP", "foobot-config" or "HF-A11..." / "HF-LPB..."
# (the Hi-Flying module's factory name).
set -euo pipefail

IFACE="${1:-wlan0}"

echo "=== Enabling Wi-Fi ($IFACE) ==="
sudo rfkill unblock wifi 2>/dev/null || true
sudo nmcli radio wifi on
sleep 2

echo "=== Rescan ==="
sudo nmcli device wifi rescan 2>/dev/null || true
sleep 4

echo "=== Visible networks ==="
nmcli -f SSID,BSSID,CHAN,SIGNAL,SECURITY device wifi list

echo
echo "=== Foobot / Hi-Flying candidates ==="
nmcli -f SSID,BSSID,SIGNAL,SECURITY device wifi list \
  | grep -iE 'foobot|airbox|HF-A11|HF-LPB|LPB100' \
  || echo "(no obvious candidate -- power-cycle the Foobot into config mode and retry)"
echo
echo "Tip: to force config mode, flip the Foobot upside-down (watch the LED) per the"
echo "manual, or power-cycle it; it re-broadcasts its AP for a few minutes."
