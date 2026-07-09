# Recovery — putting the Foobot back on the cloud

The Foobot's redirection lives **inside the device** (the `WSDNS` setting), not on
your server. So reverting is one command, from any machine on the same LAN — even
if your local server is dead.

## Revert to cloud (fast, ~1 min)

Only requirement: be on the same LAN as the Foobot (UDP 48899 reachable).

```bash
FOOBOT_IP=<foobot-lan-ip> python3 foobot_at.py --to-cloud
```

Or manually:

```bash
FOOBOT_IP=<foobot-lan-ip> python3 - <<'PY'
import foobot_at as fa
s = fa.open_session(attempts=3, delay=3)
assert s, "Foobot unreachable on UDP 48899"
print("before:", fa.at(s, "AT+WSDNS"))
fa.at(s, "AT+WSDNS=10.10.100.254")   # factory DNS -> routes to the real cloud
print("after :", fa.at(s, "AT+WSDNS"))
fa.at(s, "AT+Z")                      # reboot to apply
print("reboot sent — the Foobot returns to the cloud in ~30 s")
PY
```

If `10.10.100.254` does not resolve the cloud for you, any DNS that resolves
`broker-gw-nc.foobot.io` to the real cloud works — try `192.168.1.254` (your
router) or `8.8.8.8`.

## If your local server died

The device keeps looking for a host at the WSDNS IP that no longer exists, so its
readings freeze. The device itself is fine. Either:

1. **Revert to cloud** (above), or
2. **Rebuild the local host** and give it the **same LAN IP** as before — the
   `WSDNS` value in the Foobot is unchanged, so once `foobot_service.py` is
   listening on that IP again, the device reconnects on its own.

## Read the current state (safe, no writes)

```bash
FOOBOT_IP=<foobot-lan-ip> python3 foobot_at.py
```

Shows `WSDNS`, `SOCKB`, `WANN`, SSID and firmware version — nothing is modified.
