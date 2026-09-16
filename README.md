# foobot-local

**Bring a Foobot air-quality monitor back to life, fully local, after the cloud is gone -- no account, no app, no API token.**

The [Foobot](https://foobot.io) (by Airboxlab) depends on a cloud backend for its
app and real-time service. As that backend became unreliable, the device turned
into a paperweight: it keeps sensing, but nothing collects or displays the data.

This project makes a stock Foobot report to **your own machine** instead of the
cloud, and decodes its readings **locally**. Once set up, the device needs
nothing from Airboxlab -- it just needs your local host running.

> **No cloud credentials are required.** The device speaks plain MQTT to a local
> broker, and the calibration + pollution index have been reverse-engineered, so
> the raw readings are interpreted entirely offline. (An *optional* cloud fallback
> exists and is the only thing that would use an `api.foobot.io` token -- you never
> need it for local operation.)

Everything here is **pure Python standard library** (no dependencies) plus a tiny
DNS config. Tested on a Raspberry Pi, but any always-on Linux host works.

---

## How it works

The Foobot's Wi-Fi module (a Hi-Flying **HF-LPB100**) connects over **plain MQTT,
TCP 1883, no TLS**, to the hostname `broker-gw-nc.foobot.io`. Two facts make a
local takeover possible:

1. The module exposes a **remote config channel** (Hi-Flying AT commands over
   **UDP 48899**) -- so you can repoint it from your laptop, no wires, no
   proximity, and revert any time.
2. The **`WSDNS`** setting (the module's DNS server) **persists across reboots**.
   Point it at your host, run a DNS there that resolves the broker hostname to
   your host, and the device connects to **your** broker.

```
                 UDP 48899 (AT commands)                  MQTT 1883 (plain)
   your host  <───────────────────────  Foobot  ───────────────────────>  your host
   set WSDNS = your host                (module)   broker-gw-nc.foobot.io
                                                   resolves -> your host (via your DNS)

   your host: [dnsmasq: broker-gw-nc.foobot.io -> me] + [foobot_service.py: MQTT broker + decoder]
                                                                 │
                                            raw sensor/push  ────┘──> decode + allpollu ──> dashboard / Home Assistant
```

Why `WSDNS` and not just changing the broker address? Because the module rewrites
its broker destination (`SOCKB`) back to the cloud hostname on every reboot --
that route does **not** stick. `WSDNS` does. Details in
[`docs/PROTOCOL.md`](docs/PROTOCOL.md).

---

## What's in the box

| File | Role |
|---|---|
| `provision.py` | Reconnect the Foobot to your Wi-Fi without the app (reconstructed from the official APK). Needed only if the device fell off the network. |
| `scan.sh` / `join.sh` | Helpers to find and join the Foobot's config-mode access point before provisioning. |
| `foobot_at.py` | Reconfigure the module remotely over UDP 48899. Read config, `--to-local`, `--to-cloud`, `--selftest` (safe dry-write). |
| `foobot_service.py` | The local service: a permissive MQTT broker that receives the device's readings, decodes them, computes the pollution index, drives the LED, and (optionally) pushes to Home Assistant. |
| `pollution.py` | The reverse-engineered calibration (raw → real units) and the `allpollu` pollution-index formula that drives the ring colour. Self-tests with `python3 pollution.py`. |
| `sandbox_test.py` | End-to-end test with a simulated device, no hardware/network needed. |
| `webapp/app.py` | Optional single-page web UI tying it all together: Wi-Fi setup, LED, and the local/cloud network switch, with a live log. |
| `config/dnsmasq-foobot.conf` | The one DNS rewrite: `broker-gw-nc.foobot.io` → your host. |
| `systemd/foobot-local.service` | Run the service as a systemd unit. |
| `docs/WIFI.md` | How to reconnect the Foobot to Wi-Fi without the app (the provisioning protocol). |
| `docs/PROTOCOL.md` | Full protocol notes: MQTT topics, calibration, LED behaviour, the AT channel. |
| `docs/RECOVERY.md` | Put the device back on the cloud (one command), or recover if your host dies. |

---

## Setup

You need: an always-on Linux host on the same LAN as the Foobot, Python 3, and
`dnsmasq` (or any DNS you can add one record to).

**0 -- (only if the Foobot isn't on your Wi-Fi)** Reconnect it first. Since the
setup app is gone, use the reconstructed provisioning protocol -- see
[`docs/WIFI.md`](docs/WIFI.md): `scan.sh` → `join.sh` → `provision.py`. Skip this
step if the device is already on your LAN.

**1 -- Find your values.** The Foobot's LAN IP (from your router), and your host's
LAN IP. Read the device's current config (safe, read-only):

```bash
FOOBOT_IP=192.168.1.42 python3 foobot_at.py
```

This prints `WSDNS`, `SOCKB`, `WANN`, SSID and firmware. Note the device UUID
appears as the MQTT clientID once it connects (step 4).

**2 -- Point DNS at your host.** Edit `config/dnsmasq-foobot.conf`, replacing
`192.168.1.50` with your host's LAN IP in both places, then:

```bash
sudo cp config/dnsmasq-foobot.conf /etc/dnsmasq.d/foobot.conf
sudo systemctl restart dnsmasq
# verify it rewrites only the broker and forwards the rest:
dig +short broker-gw-nc.foobot.io @127.0.0.1   # -> your host IP
dig +short api.foobot.io          @127.0.0.1   # -> real cloud IP (still forwarded)
```

**3 -- Start the local service** (listens on TCP 1883):

```bash
FOOBOT_UUID=<device-uuid> FOOBOT_IP=192.168.1.42 python3 foobot_service.py
```

With no `HA_TOKEN` it runs in **dry-run** and just logs the decoded readings -- a
good way to confirm the pipeline before wiring up a dashboard. See
[Home Assistant](#home-assistant-optional) below to push real sensors.

**4 -- Repoint the Foobot to your host** (writes `WSDNS`, reboots the module):

```bash
# optional but recommended: prove the write+reboot+revert mechanics first
FOOBOT_IP=192.168.1.42 python3 foobot_at.py --selftest

# go local (LOCAL_IP is auto-detected; override with LOCAL_IP=... if needed)
FOOBOT_IP=192.168.1.42 python3 foobot_at.py --to-local
```

Within ~30 s the service log shows `CONNECT clientID=<uuid>` and, every ~5 min,
decoded measurements. Done -- the Foobot is now yours.

**5 -- (optional) Run it as a service.** Edit `systemd/foobot-local.service`
(user, paths, UUID, IP), then:

```bash
sudo cp systemd/foobot-local.service /etc/systemd/system/
sudo systemctl enable --now foobot-local
```

---

## Home Assistant (optional)

The service can push six sensors (`sensor.foobot_co2`, `_voc`, `_pm25`,
`_temperature`, `_humidity`, `_pollution`) straight into Home Assistant over its
REST API -- no MQTT integration needed. Create a long-lived token and:

```bash
FOOBOT_UUID=<uuid> FOOBOT_IP=<ip> \
HA_URL=http://127.0.0.1:8123 HA_TOKEN=<long-lived-token> \
python3 foobot_service.py
```

The `foobot_pollution` sensor carries `band`, `ring_color` and `source`
attributes.

## LED control (optional)

Once the device is on your broker you can drive the ring by appending
`topic|payload` lines to the `inject` file next to the service:

```bash
echo 'device/<uuid>/attribute/brightness|{"brightness":0}'  >> inject   # off
```

Turning the ring back **on** requires a module reboot (`foobot_at.py`, `AT+Z`) --
brightness alone won't relight a ring that's off. The built-in night scheduler in
`foobot_service.py` (`LED_OFF_H`/`LED_ON_H`, or a hot-reloaded `led_config.json`)
uses this. Full explanation in [`docs/PROTOCOL.md`](docs/PROTOCOL.md).

## Web interface (optional, last step)

If you'd rather not use the command line, `webapp/app.py` serves a single page
(default `http://<host>:8099`) that wraps everything above with a live log:

- **Status** -- whether the Foobot is seen on the LAN (by its MAC).
- **Wi-Fi setup** -- scan for the device's config-mode AP and push your home
  Wi-Fi (the `provision.py` flow, no CLI).
- **LED ring** -- on (reboot) / off / brightness, and the night schedule editor.
- **Network mode** -- one click to switch the device between your local host and
  the cloud (handy: switch it back to cloud before shutting your host down).

Pure standard library. It shells out to `nmcli`/`rfkill` for the Wi-Fi steps, so
run it as the provided systemd unit with passwordless sudo for those two:

```bash
# configure device MAC/IP/UUID in the unit first
sudo cp webapp/foobot-web.service /etc/systemd/system/
sudo systemctl enable --now foobot-web
```

Config is via env vars (`FOOBOT_MAC`, `FOOBOT_IP`, `FOOBOT_UUID`, `HOME_SSID`,
`LOCAL_IP`, `FOOBOT_DIR`); see the top of `webapp/app.py`. The Wi-Fi password is
never stored or logged. It reuses `foobot_at.py` and the service's `inject` /
`led_config.json` files, so keep it alongside the rest of the repo.

---

## Reverting

One command, from any machine on the LAN -- see [`docs/RECOVERY.md`](docs/RECOVERY.md):

```bash
FOOBOT_IP=<ip> python3 foobot_at.py --to-cloud
```

Because the redirection lives in the device (not your server), the revert works
even if your host is gone. The AT channel never touches Wi-Fi, so the module stays
reachable throughout.

---

## Notes & caveats

- **Calibration is per-unit.** The constants in `pollution.py` were reversed from
  one device and match it within ~2.5 points of the cloud. If yours reads off,
  re-fit `PM_DIVISOR` and the `allpollu` coefficients from your own raw/reference
  pairs.
- **Exact ring colour** is computed on the device from thresholds it receives on
  `attribute/thresholds` (never captured here); this project computes the index
  for your dashboard but doesn't reproduce the on-device colour exactly.
- **Your host becomes the Foobot's dependency.** With `WSDNS` pointed at it, the
  device needs that host up to reach its broker. Give it a stable IP; `RECOVERY.md`
  covers failure.
- **Do not publish to** `ota`/`mcuota`/`reboot`/`failsafe` topics -- see
  `docs/PROTOCOL.md`.

## Disclaimer

Reverse-engineered from observation of hardware I own, for interoperability and
repair. Not affiliated with or endorsed by Airboxlab/Foobot. `Foobot` is a
trademark of its owner. Use at your own risk; you are responsible for your own
network and device.

## License

MIT -- see [LICENSE](LICENSE).
