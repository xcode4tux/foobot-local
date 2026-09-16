#!/usr/bin/env python3
"""
sandbox_wire_test.py -- MQTT wire-level tests, no broker process, no DB.

Two layers, matching the two halves of the wire code:
  1. broker-side packet building/parsing round-trips and malformed-packet
     handling (the code foobot_service.py uses on the receiving end)
  2. the dashboard bridge's MqttClient against a scripted fake broker over a
     real socketpair: handshake, split packets, coalesced packets, and
     keepalive firing only at packet boundaries (must be PINGREQ 0xC0)

pymysql is stubbed before importing dashboard_bridge: this test exercises the
wire only and must run on machines without the dashboard venv.

Usage: python3 sandbox_wire_test.py
"""
import os
import socket
import sys
import threading
import time
import types

sys.modules.setdefault("pymysql", types.ModuleType("pymysql"))   # never touches the DB
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import foobot_service as fs          # noqa: E402,F401  (import sanity: needs pollution, mqttwire)
import dashboard_bridge as db        # noqa: E402  (client-side wire code)
import mqttwire as mw                # noqa: E402  (shared wire code under test)

RESULTS = []


def check(label, cond):
    RESULTS.append((label, bool(cond)))
    print(f"  [{'OK ' if cond else 'KO!'}] {label}")


# ---------------------------------------------------------------- shared wire

class _Sock:
    """Minimal synchronous byte-source standing in for a socket."""

    def __init__(self, data):
        self._d = bytes(data)

    def recv(self, n):
        out, self._d = self._d[:n], self._d[n:]
        return out


def _sp(pkt):
    """Feed a raw packet into a StreamParser, return (hdr, body)."""
    sp = mw.StreamParser()
    sp.feed(pkt)
    return sp.next_packet()


def broker_side():
    print("-- shared wire: build/parse round-trips, wildcards, malformed input")

    # PUBLISH QoS0 and QoS1 round-trips through builder + parser
    hdr, body = _sp(mw.build_publish("device/U1/sensor/push", b'[{"pm":1}]'))
    topic, pid, payload = mw.parse_publish(hdr & 0x0F, body)
    check("PUBLISH QoS0 round-trip", hdr >> 4 == 3
          and topic == "device/U1/sensor/push" and pid is None
          and payload == b'[{"pm":1}]')
    hdr, body = _sp(mw.build_publish("a/b", b"x", qos=1, pid=7))
    topic, pid, payload = mw.parse_publish(hdr & 0x0F, body)
    check("PUBLISH QoS1 keeps packet id", topic == "a/b" and pid == 7 and payload == b"x")

    # CONNECT / SUBSCRIBE builders vs parsers (bridge builds, broker parses)
    hdr, body = _sp(mw.build_connect("wire-cid", keepalive=30))
    check("CONNECT round-trip", hdr == 0x10 and mw.parse_connect(body) == "wire-cid")
    hdr, body = _sp(mw.build_subscribe(["device/+/sensor/push", "other/#"], pid=9))
    parsed = mw.parse_subscribe(body)
    check("SUBSCRIBE round-trip", hdr == 0x82 and parsed[0] == 9
          and [f for f, _ in parsed[1]] == ["device/+/sensor/push", "other/#"]
          and all(q == 0 for _, q in parsed[1]))

    # wildcard matching (fan-out rules)
    tm = mw.topic_matches
    check("wildcard + one level", tm("device/+/sensor/push", "device/U1/sensor/push"))
    check("wildcard # suffix", tm("device/#", "device/U1/attribute/brightness"))
    check("# matches parent", tm("device/#", "device"))
    check("exact filter", tm("a/b", "a/b"))
    check("no cross-level +", not tm("device/+/x", "device/a/b/x"))
    check("no partial match", not tm("a/b/c", "a/b"))

    # remaining-length round-trip incl. multi-byte encodings
    for n in (0, 127, 128, 16383, 16384, 100000):
        check(f"remaining-length {n}",
              mw.read_remaining_length(_Sock(mw.enc_len(n))) == n)

    # malformed packets: parsers must return None, never raise
    check("malformed PUBLISH (topic len > body)",
          mw.parse_publish(0x30, b"\x00\xff\x00\x01") is None)
    check("malformed PUBLISH (pid beyond body)",
          mw.parse_publish(0x32, b"\x00\x01ab") is None)
    check("malformed CONNECT (truncated header)",
          mw.parse_connect(b"\x00\x04MQTT") is None)
    check("malformed SUBSCRIBE (qos byte missing)",
          mw.parse_subscribe(b"\x00\x01\x00\x03a/b") is None)

    # StreamParser: oversize announcement refused, split data reassembled
    try:
        sp = mw.StreamParser(max_body=64)
        sp.feed(bytes([0x30, 0xFB, 0xFF, 0xFF, 0x7F]) + b"x")   # announces >64
        sp.next_packet()
        check("StreamParser enforces max_body", False)
    except ValueError:
        check("StreamParser enforces max_body", True)
    sp = mw.StreamParser()
    pkt = mw.build_publish("t/p", b"payload")
    sp.feed(pkt[:2]); ok1 = sp.next_packet() is None and sp.partial
    sp.feed(pkt[2:]); ok2 = sp.next_packet() == (0x30, b"\x00\x03t/ppayload")
    check("StreamParser reassembles split packets", ok1 and ok2)


# ---------------------------------------------------------------- client side

class FakeBroker(threading.Thread):
    """One-connection fake broker driven from the test thread."""

    def __init__(self):
        self.reqs = []
        self.srv = socket.socket()
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(("127.0.0.1", 0))
        self.srv.listen(1)
        self.port = self.srv.getsockname()[1]
        self.conn = None
        super().__init__(daemon=True)

    def run(self):
        self.conn, _ = self.srv.accept()

    def wait_conn(self):
        t0 = time.time()
        while self.conn is None and time.time() - t0 < 5:
            time.sleep(0.01)
        assert self.conn is not None, "fake broker: no connection"

    def read_packet(self):
        """(header byte, body) of the next packet from the client."""
        def rx(n):
            buf = b""
            while len(buf) < n:
                c = self.conn.recv(n - len(buf))
                if not c:
                    raise EOFError
                buf += c
            return buf

        hdr = rx(1)[0]
        mult, rem = 1, 0
        while True:
            b = rx(1)[0]
            rem += (b & 0x7F) * mult
            if not (b & 0x80):
                break
            mult *= 128
        return hdr, rx(rem)


def enc_len(n):
    out = bytearray()
    while True:
        b = n % 128
        n //= 128
        if n > 0:
            b |= 0x80
        out.append(b)
        if n == 0:
            return bytes(out)


def enc_str(s):
    b = s.encode()
    return len(b).to_bytes(2, "big") + b


def publish_pkt(topic, payload):
    vh = enc_str(topic) + payload.encode()
    return bytes([0x30]) + enc_len(len(vh)) + vh


def client_side():
    print("-- client side: bridge MqttClient against a scripted fake broker")
    br = FakeBroker()
    br.start()
    cli = db.MqttClient("127.0.0.1", br.port, "wire-test")
    t = threading.Thread(target=cli.connect, args=("device/+/sensor/push",), daemon=True)
    t.start()
    br.wait_conn()

    hdr, body = br.read_packet()
    check("client sends CONNECT",
          hdr == 0x10 and body[:8] == b"\x00\x04MQTT\x04\x02"
          and enc_str("wire-test") in body)
    br.conn.sendall(bytes([0x20, 0x02, 0x00, 0x00]))
    hdr, body = br.read_packet()
    check("client sends SUBSCRIBE",
          hdr == 0x82 and enc_str("device/+/sensor/push") in body)
    br.conn.sendall(bytes([0x90, 0x03, 0x00, 0x01, 0x00]))
    t.join(timeout=3)
    check("connect() returns after SUBACK", not t.is_alive())

    # split packet: header now, body after a stall -> must reassemble
    pkt = publish_pkt("device/U1/sensor/push", '[{"pm":936}]')
    br.conn.sendall(pkt[:3])
    got = {}
    th = threading.Thread(
        target=lambda: got.update(zip(("t", "p"), cli.next_publish(time.monotonic() + 3))),
        daemon=True,
    )
    th.start()
    time.sleep(0.4)
    br.conn.sendall(pkt[3:])
    th.join(timeout=3)
    check("mid-packet stall reassembles (no desync)",
          got.get("t") == "device/U1/sensor/push"
          and got.get("p") == b'[{"pm":936}]')

    # coalesced packets in one segment
    br.conn.sendall(publish_pkt("device/U1/sensor/push", '[{"pm":1}]')
                    + publish_pkt("device/U2/sensor/push", '[{"pm":2}]'))
    r1 = cli.next_publish(time.monotonic() + 2)
    r2 = cli.next_publish(time.monotonic() + 2)
    check("coalesced packets parsed in order",
          r1 == ("device/U1/sensor/push", b'[{"pm":1}]')
          and r2 == ("device/U2/sensor/push", b'[{"pm":2}]'))

    # boundary timeout -> PINGREQ (0xC0, not 0xD0) and the stream stays usable
    t0 = time.monotonic()
    r = cli.next_publish(time.monotonic() + 0.3)
    check("idle timeout returns None", r is None and 0.2 < time.monotonic() - t0 < 1.5)
    check("keepalive is PINGREQ (0xC0)", br.read_packet()[0] == 0xC0)
    br.conn.sendall(publish_pkt("device/U1/sensor/push", '[{"pm":3}]'))
    check("stream in sync after keepalive",
          cli.next_publish(time.monotonic() + 2) == ("device/U1/sensor/push", b'[{"pm":3}]'))

    cli.close()


def main():
    broker_side()
    client_side()
    ok = all(c for _, c in RESULTS)
    print("\n================== WIRE RESULT ====================")
    print(f"  {sum(c for _, c in RESULTS)}/{len(RESULTS)} checks passed")
    print("PASS: MQTT wire layer validated." if ok
          else "FAIL: see the KO lines above.")
    return 0 if ok else 1


if __name__ == "__main__":
    code = main()
    sys.stdout.flush()
    os._exit(code)
