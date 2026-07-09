# Foobot protocol notes (reverse-engineered)

Everything here was observed on a real unit. Values like the device UUID, IPs and
MAC are yours to discover; the *shapes* below are what matter.

## Hardware

- **Sensor**: Foobot (Airboxlab), indoor air-quality monitor.
- **Wi-Fi module**: Hi-Flying **HF-LPB100** (seen on firmware `V1.2.15`). This is a
  common serial-to-Wi-Fi module with an AT command set, reachable over the LAN.

## MQTT (device → broker)

- Plain **MQTT 3.1.1 over TCP port 1883**. **No TLS.**
- The device connects to **`broker-gw-nc.foobot.io`** (the cloud broker was a
  RabbitMQ instance).
- **CONNECT**: `clientID` = the device UUID, `user=device`, a password field,
  `clean-session=0`, keepalive ~10 s. A permissive broker that accepts any
  credentials works fine — you do **not** need to know the password.

### Topics the device PUBLISHES

| Topic | Payload |
|---|---|
| `device/<UUID>/sensor/push` | JSON **array** of raw readings, every ~300 s |
| `device/<UUID>/debug/push` | text logs |
| `device/<UUID>/freefall_init/push` | boot/init info |

Example `sensor/push`:

```json
[{"co2":2638,"voc":728,"temp":27511,"hum":56284,"pm":936,
  "voc_raw":97941,"temp_raw":30229,"hum_raw":51085,"pm_raw":936}]
```

### Topics the device SUBSCRIBES to (broker → device)

`attribute/brightness` (LED brightness ✅ works), `attribute/thresholds` (colour
thresholds), `attribute/mode`, `attribute/auto_th`, `attribute/pm_cal`,
`attribute/hum_cal`, `attribute/tmp_cal`, `attribute/pm_comp_trh`, `pattern`,
`refresh` (*= pull-to-refresh, takes a reading now — NOT a colour command*),
`refresh_off`, `refresh_cal`, `ping`, `reboot`, `failsafe`, `device`, `com`,
`envi`, `at`, `mid_by_mac`, `wifi_quality`, `patate` (debug).

> ⚠️ **Never publish to** `ota` / `mcuota` / `silent_ota` / `silent_mcuota` /
> `reboot` / `failsafe`. These can brick or force-update the device.

## Calibration: raw → real units

Validated against a cloud reading taken at the same instant:

| Quantity | Conversion | Unit |
|---|---|---|
| CO₂ | ×1 | ppm |
| VOC | ×1 | ppb |
| Temperature | **÷1000** | °C |
| Humidity | **÷1000** | % |
| PM2.5 | **÷94.2** (+ humidity compensation) | µg/m³ |
| `allpollu` | reversed — see below | % |

CO₂ is not independent: `co2 ≈ 3.62 × voc` almost always, i.e. the device derives
it from VOC. The real inputs are just **pm** and **voc**.

## The `allpollu` index (drives the ring colour)

Regression on 135 cloud points (7 days, hourly means):

```
allpollu ≈ 0.9531·pm + 0.15775·voc − 7.216e-5·voc² − 17.697     (R²=0.997)
```

Colour: `allpollu < 50` = **blue (good)**, `≥ 50` = **orange → red (bad)**, 6
sub-levels. The colour the ring *actually* shows is computed **on the device**
from thresholds pushed on `attribute/thresholds` (cloud→device, not captured
here), so exact on-device colour control is an open point. `pollution.py`
computes the index locally for your dashboard.

## LED ring behaviour (important, learned the hard way)

- `{"brightness":0}` on `device/<UUID>/attribute/brightness` turns the ring
  **off** reliably.
- Once off, **nothing over MQTT reliably turns it back on** — not `brightness:47`,
  not `refresh`. The ring is not a continuously-drivable lamp; the device lights
  it **at boot** and **on air-quality change**, then lets it fade.
- **The only reliable way to relight it is a module reboot** (`AT+Z`, ~6 s), via
  `foobot_at.py`. `foobot_service.py`'s LED scheduler uses reboot to wake it.

## The remote config channel (Hi-Flying AT over UDP)

- The module answers on **UDP port 48899**: send `HF-A11ASSISTHREAD`, it replies
  with its identity, send `+ok`, then send `AT+...\r` commands in plain text.
- ⚠️ The protocol **desyncs** request/response — always flush the socket before
  each command (`foobot_at.py` does this in `_drain`).
- This channel is **independent of Wi-Fi/IP**, so the module stays reachable after
  you repoint it — the revert always works remotely.

### Why WSDNS and not SOCKB

- `AT+SOCKB` holds the broker destination (`TCP,1883,broker-gw-nc.foobot.io`).
- Writing `AT+SOCKB=TCP,1883,<your-ip>` is accepted **but does not stick**: the
  MCU rewrites SOCKB back to the cloud hostname on every reboot and every link
  loss. So the SOCKB route is a dead end.
- `AT+WSDNS` (the module's DNS server) **does** persist across reboots. Point it
  at your host, resolve `broker-gw-nc.foobot.io` → your host there, and the device
  connects locally. This is the working method.
