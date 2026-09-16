#!/usr/bin/env python3
"""
sandbox_test.py -- End-to-end test of the service IN A SANDBOX.

Touches NO real device, NO network beyond localhost, NO prod broker (1883):
runs foobot_service on port 11883 in DRY-RUN, then simulates a Foobot that
connects and publishes a real captured sensor/push payload. Verifies the service
decodes it, computes allpollu, and would push the right sensors.

Usage: python3 sandbox_test.py
"""
import os, socket, threading, time, io, sys

TEST_UUID = "SANDBOXDEVICE001"

os.environ["FOOBOT_PORT"] = "11883"
os.environ["FOOBOT_UUID"] = TEST_UUID
os.environ["HA_TOKEN"] = ""          # dry-run: no real HA POST
os.environ["LED_SCHED"] = "0"        # no schedule during the test

import foobot_service as fs

# --- minimal MQTT client (CONNECT + PUBLISH QoS1) -----------------------------
def enc_len(n):
    out = bytearray()
    while True:
        b = n % 128; n //= 128
        if n > 0: b |= 0x80
        out.append(b)
        if n == 0: break
    return bytes(out)

def s(x):
    b = x.encode(); return len(b).to_bytes(2, "big") + b

def connect_pkt(cid):
    vh = s("MQTT") + bytes([0x04, 0x02]) + (10).to_bytes(2, "big") + s(cid)
    return bytes([0x10]) + enc_len(len(vh)) + vh

def publish_pkt(topic, payload, pid=1):
    vh = s(topic) + pid.to_bytes(2, "big") + payload.encode()
    return bytes([0x32]) + enc_len(len(vh)) + vh   # QoS1

def main():
    threading.Thread(target=fs.main, daemon=True).start()
    time.sleep(0.4)

    buf = io.StringIO()
    real_log = fs.log
    fs.log = lambda m: (buf.write(m + "\n"), real_log(m))

    sock = socket.create_connection(("127.0.0.1", 11883), timeout=3)
    sock.sendall(connect_pkt(TEST_UUID))
    time.sleep(0.2); sock.recv(4)                    # CONNACK

    real_payload = ('[{"co2":2638,"voc":728,"voc_raw":97941,"temp":27511,'
                    '"hum":56284,"pm":936,"temp_raw":30229,"hum_raw":51085,'
                    '"raw4":935,"pm_raw":936}]')
    sock.sendall(publish_pkt(f"device/{TEST_UUID}/sensor/push", real_payload))
    time.sleep(0.5)
    sock.close()

    out = buf.getvalue()
    ok = True
    checks = [
        ("CONNECT received",     f"CONNECT clientID={TEST_UUID}" in out),
        ("sensor/push handled",  "measures ->" in out),
        ("PM decoded ~9.94",     "'pm': 9.94" in out),
        ("temperature /1000",    "'tmp': 27.51" in out),
        ("allpollu computed",    "'allpollu': 68.4" in out),
        ("colour orange",        "'color': 'orange'" in out),
        ("push HA pollution",    "sensor.foobot_pollution = 68.4" in out),
        ("push HA pm25",         "sensor.foobot_pm25 = 9.94" in out),
    ]
    print("\n================= SANDBOX RESULT =================")
    for label, cond in checks:
        print(f"  [{'OK ' if cond else 'KO!'}] {label}")
        ok = ok and cond
    print("=================================================")
    print("PASS: device->service->HA chain validated (dry-run)." if ok
          else "FAIL: at least one check failed (see logs above).")
    return 0 if ok else 1

if __name__ == "__main__":
    code = main()
    # the in-process service runs daemon threads that print during interpreter
    # shutdown, which can abort the process and mask the real result; exit
    # deterministically instead (flush first: os._exit skips atexit flushing)
    sys.stdout.flush()
    os._exit(code)
