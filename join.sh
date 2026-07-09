#!/usr/bin/env bash
# join.sh -- Connect this host (wlan0) to the Foobot's open access point, so you
# can reconfigure it. The Hi-Flying module hands out 10.10.100.x by DHCP and
# lives at 10.10.100.254.
#
# Usage: ./join.sh "<FOOBOT_SSID>"          (open AP, no password)
#        ./join.sh "<FOOBOT_SSID>" "<psk>"  (in case the AP is protected)
set -euo pipefail

SSID="${1:?Usage: ./join.sh \"<FOOBOT_SSID>\" [password]}"
PSK="${2:-}"
IFACE="wlan0"

echo "=== Connecting $IFACE to AP \"$SSID\" ==="
sudo rfkill unblock wifi 2>/dev/null || true
sudo nmcli radio wifi on
sudo nmcli device wifi rescan 2>/dev/null || true
sleep 3

if [ -n "$PSK" ]; then
  sudo nmcli device wifi connect "$SSID" password "$PSK" ifname "$IFACE"
else
  sudo nmcli device wifi connect "$SSID" ifname "$IFACE"
fi

sleep 3
echo
echo "=== Address obtained on $IFACE ==="
ip -brief addr show "$IFACE"

echo
echo "=== Reachability test to the Foobot module (10.10.100.254:1337) ==="
if timeout 5 bash -c 'cat < /dev/null > /dev/tcp/10.10.100.254/1337' 2>/dev/null; then
  echo "OK -- 10.10.100.254:1337 reachable. You can run provision.py now."
else
  echo "x Port 1337 unreachable. Check the IP (must be 10.10.100.x) and that the Foobot is in config mode."
fi

echo
echo "Next:"
echo "  python3 provision.py --ssid \"MyHome-2.4GHz\" --pass 'YOUR_PASSWORD'"
echo
echo "To return to normal Wi-Fi or free wlan0:"
echo "  sudo nmcli device disconnect $IFACE"
