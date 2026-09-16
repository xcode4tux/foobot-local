# Reconnecting a Foobot to Wi-Fi (without the app)

If your Foobot lost its Wi-Fi (router change, new password, factory reset), you
normally re-run setup from the mobile app -- but the app is gone. This is the
provisioning protocol the app used, reconstructed by decompiling the official APK
(`com.airboxlab.foobot`, classes `setup/TCPClient` and `setup/manual/TCPClientManual`).

Do this **before** the local-broker steps in the main README: the device has to be
back on your LAN first.

## The protocol

The Foobot's Wi-Fi module is a Hi-Flying **HF-LPB100**. In config mode it becomes
a Wi-Fi access point and accepts one TCP command:

| Element | Value |
|---|---|
| Config-mode AP SSID | `Foobot-xxxx` / `foobot-config` / `HF-A11x` (module factory name) |
| Module IP on that AP | **`10.10.100.254`** (clients get `10.10.100.x` by DHCP) |
| Config TCP port | **`1337`** |
| Command (one line, `\n`-terminated) | **`w;<SSID>;<password>`** |
| Static-IP variant | `w;<SSID>;<password>;<ip>;<mask>;<gateway>;<dns>` |
| Effect | the module replies, **reboots**, then joins the requested network |

> ⚠️ The Foobot only supports **2.4 GHz WPA/WPA2** -- not WPA3. Make sure your
> target SSID is reachable on 2.4 GHz.

## Procedure

You need a machine with a Wi-Fi interface (`wlan0` below) that you can temporarily
attach to the Foobot's AP. On a Raspberry Pi, keep Ethernet on the LAN and use
`wlan0` for the AP.

```bash
# 1. Put the Foobot into config mode (flip it / power-cycle per the manual),
#    then find its access point:
./scan.sh

# 2. Connect this host to that AP (use the real SSID seen in the scan):
./join.sh "Foobot-xxxx"

# 3. Push your home Wi-Fi credentials to the device:
python3 provision.py --ssid "MyHome-2.4GHz" --pass 'YOUR_PASSWORD'

# 4. Release wlan0; the Foobot reboots and joins your LAN:
sudo nmcli device disconnect wlan0
```

`provision.py --dry-run` prints the frame without sending it. If you're unsure of
the SSID/password order, `--raw 'w;SSID;pass'` sends a raw frame and prints the
module's reply.

## After it's back on the LAN

Find the Foobot's new IP (from your router's device list, or `ip neigh` after a
ping). Then continue with the main README: point the device at your local broker
with `foobot_at.py --to-local` and run `foobot_service.py`.

**Security note:** your Wi-Fi password is sent once, in clear, over the Foobot's
own local AP. `provision.py` never stores or logs it. Pass it on the command line
only when you run the step (and clear your shell history if that matters to you).
