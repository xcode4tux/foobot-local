#!/usr/bin/env python3
"""
sandbox_sub_test.py -- Verify the local fan-out extension: a plain MQTT client
(the role the Wio Terminal will play) that SUBSCRIBEs to `device/+/sensor/push`
must actually receive device publishes. Upstream foobot_service granted SUBACKs
but never delivered; the local patch adds delivery. This proves the Wio side.

No real device, no network beyond localhost, sandbox port 11883.

Usage: python3 sandbox_sub_test.py
"""
import os, socket, threading, time

TEST_UUID = "SANDBOXDEVICE001"
SUB_CID   = "WIO-SUB-TEST"

os.environ["FOOBOT_PORT"] = "11883"
os.environ["FOOBOT_UUID"] = TEST_UUID
os.environ["HA_TOKEN"] = ""
os.environ["LED_SCHED"] = "0"

import foobot_service as fs


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

def connect_pkt(cid, clean=True):
    vh = s("MQTT") + bytes([0x04, 0x02 if clean else 0x00]) + (60).to_bytes(2, "big") + s(cid)
    return bytes([0x10]) + enc_len(len(vh)) + vh

def subscribe_pkt(pid, filt, qos=0):
    vh = pid.to_bytes(2, "big") + s(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(vh)) + vh

def publish_pkt(topic, payload):
    vh = s(topic) + payload.encode()
    return bytes([0x30]) + enc_len(len(vh)) + vh   # QoS0, like the Wio will use

def read_publish(sock):
    """Read one PUBLISH packet off the wire -> (topic, payload) or None."""
    def rx(n):
        buf = b""
        while len(buf) < n:
            c = sock.recv(n - len(buf))
            if not c: raise OSError("closed")
            buf += c
        return buf
    hdr = rx(1)
    mult, rem = 1, 0
    for _ in range(4):
        b = rx(1)[0]; rem += (b & 0x7f) * mult
        if not (b & 0x80): break
        mult *= 128
    body = rx(rem)
    tl = (body[0] << 8) | body[1]
    topic = body[2:2 + tl].decode()
    return topic, body[2 + tl:].decode()


def main():
    threading.Thread(target=fs.main, daemon=True).start()
    time.sleep(0.4)

    # subscriber = the Wio Terminal's role
    sub = socket.create_connection(("127.0.0.1", 11883), timeout=5)
    sub.sendall(connect_pkt(SUB_CID))
    time.sleep(0.2); sub.recv(4)                                   # CONNACK
    sub.sendall(subscribe_pkt(1, "device/+/sensor/push"))
    time.sleep(0.2); sub.recv(5)                                   # SUBACK
    sub.settimeout(3)

    # device publishes: one matching, one that must NOT match
    dev = socket.create_connection(("127.0.0.1", 11883), timeout=5)
    dev.sendall(connect_pkt(TEST_UUID))
    time.sleep(0.2); dev.recv(4)
    payload = '[{"co2":900,"voc":249,"temp":29400,"hum":45000,"pm":471}]'
    dev.sendall(publish_pkt(f"device/{TEST_UUID}/sensor/push", payload))
    dev.sendall(publish_pkt(f"device/{TEST_UUID}/debug/push", "noise"))
    time.sleep(0.5)

    # assert: exactly the sensor/push arrives on the subscriber
    got = None
    try:
        got = read_publish(sub)
    except (socket.timeout, OSError):
        pass
    try:
        extra = read_publish(sub)   # must time out: debug/push was not subscribed
    except (socket.timeout, OSError):
        extra = None

    ok = True
    checks = [
        ("subscriber received a PUBLISH", got is not None),
        ("topic matches wildcard filter", got and got[0] == f"device/{TEST_UUID}/sensor/push"),
        ("payload intact end-to-end", got and got[1] == payload),
        ("non-matching topic filtered out", extra is None),
    ]
    print("\n================ SUBSCRIBER RESULT ================")
    for label, cond in checks:
        print(f"  [{'OK ' if cond else 'KO!'}] {label}")
        ok = ok and cond
    print("===================================================")
    print("PASS: Wio-style subscriber gets live readings." if ok
          else "FAIL: subscriber path broken (see service log above).")
    sub.close(); dev.close()
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
