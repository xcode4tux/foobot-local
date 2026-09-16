#!/usr/bin/env python3
"""
mqttwire.py -- the MQTT 3.1.1 wire subset this stack speaks, in one place.

Used by:
  - foobot_service.py (broker: socket reads, packet parsing, CONNACK/PUBACK/
    SUBACK builders, fan-out PUBLISH builder, wildcard matching)
  - dashboard_bridge.py (client: CONNECT/SUBSCRIBE builders, buffered packet
    reassembly, PUBLISH parsing)
  - sandbox_wire_test.py (round-trip tests over all of the above)

One implementation means a protocol fix lands everywhere at once. The parsers
are bounds-checked: malformed packets come back as None (the caller drops the
connection cleanly) instead of raising IndexError from the middle of a read.

Purposes of each piece:
  encoders     build_* / enc_len / enc_str -- byte-exact packets
  socket reads recv_exact / read_remaining_length -- blocking broker loop
  parsers      parse_* -- bounds-checked decode of received packets
  StreamParser buffered reassembly for client sockets, where a keepalive
               timeout must only ever fire at a packet boundary
"""
import socket

# --- encoders -------------------------------------------------------------------

def enc_len(n):
    """MQTT variable-length remaining-length encoding."""
    out = bytearray()
    while True:
        b = n % 128; n //= 128
        if n > 0: b |= 0x80
        out.append(b)
        if n == 0: return bytes(out)


def enc_str(s):
    """MQTT UTF-8 string with a 2-byte big-endian length prefix."""
    b = s.encode() if isinstance(s, str) else bytes(s)
    return len(b).to_bytes(2, "big") + b


def build_publish(topic, payload, qos=0, pid=None):
    """PUBLISH packet; pid required for qos>0."""
    tb = topic.encode() if isinstance(topic, str) else bytes(topic)
    pb = payload if isinstance(payload, (bytes, bytearray)) else payload.encode()
    vh = len(tb).to_bytes(2, "big") + tb
    if qos > 0:
        vh += (pid or 1).to_bytes(2, "big")
    return bytes([0x30 | (qos << 1)]) + enc_len(len(vh) + len(pb)) + vh + pb


def build_connect(client_id, keepalive=30, clean=True):
    flags = 0x02 if clean else 0x00
    vh = enc_str("MQTT") + bytes([4, flags]) + keepalive.to_bytes(2, "big")
    vh += enc_str(client_id)
    return bytes([0x10]) + enc_len(len(vh)) + vh


def build_subscribe(filters, pid=1):
    """SUBSCRIBE packet for a list of topic filters (all requested at QoS 0)."""
    body = bytearray(pid.to_bytes(2, "big"))
    for f in filters:
        body += enc_str(f) + b"\x00"
    return bytes([0x82]) + enc_len(len(body)) + bytes(body)


def build_connack(rc=0):
    return bytes([0x20, 0x02, 0x00, rc])


def build_puback(pid):
    return bytes([0x40, 0x02, (pid >> 8) & 0xff, pid & 0xff])


def build_suback(pid, grants):
    return (bytes([0x90, 2 + len(grants), (pid >> 8) & 0xff, pid & 0xff])
            + bytes(grants))


PINGREQ     = bytes([0xC0, 0x00])
PINGRESP    = bytes([0xD0, 0x00])
DISCONNECT  = bytes([0xE0, 0x00])

# --- socket reading (broker side: blocking, incremental) ------------------------

def recv_exact(sock, n):
    """Read exactly n bytes or None on close. bytearray accumulation: no
    O(n^2) concatenation on large packets."""
    buf = bytearray()
    while len(buf) < n:
        c = sock.recv(n - len(buf))
        if not c: return None
        buf += c
    return bytes(buf)


def read_remaining_length(sock):
    """Decode the remaining-length varint (max 4 bytes), or None."""
    mult, value = 1, 0
    for _ in range(4):
        b = recv_exact(sock, 1)
        if b is None: return None
        value += (b[0] & 0x7f) * mult
        if not (b[0] & 0x80): return value
        mult *= 128
    return None

# --- packet parsers (bounds-checked; None = malformed) --------------------------

def _read_str(data, i):
    if i + 2 > len(data): return None, i
    ln = (data[i] << 8) | data[i + 1]; i += 2
    if i + ln > len(data): return None, i
    return bytes(data[i:i + ln]), i + ln


def parse_connect(body):
    """CONNECT -> client id string, or None if malformed."""
    _proto, i = _read_str(body, 0)
    if _proto is None or i + 4 > len(body):      # name + level/flags/keepalive
        return None
    cid, _i = _read_str(body, i + 4)
    return None if cid is None else cid.decode(errors="replace")


def parse_publish(flags, body):
    """PUBLISH -> (topic, pid|None, payload), or None if malformed."""
    topic, i = _read_str(body, 0)
    if topic is None: return None
    pid = None
    if (flags >> 1) & 0x03:                      # qos>0 carries a packet id
        if i + 2 > len(body): return None
        pid = (body[i] << 8) | body[i + 1]; i += 2
    return topic.decode(errors="replace"), pid, bytes(body[i:])


def parse_subscribe(body):
    """SUBSCRIBE -> (pid, [(filter, granted_qos)]), or None if malformed."""
    if len(body) < 2: return None
    pid = (body[0] << 8) | body[1]; i = 2
    out = []
    while i < len(body):
        f, i = _read_str(body, i)
        if f is None or i >= len(body): return None
        out.append((f.decode(errors="replace"), min(body[i], 2))); i += 1
    return pid, out


def topic_matches(filt, topic):
    """MQTT wildcard match ('+' one level, '#' multi-level suffix)."""
    fp, tp = filt.split("/"), topic.split("/")
    for i, f in enumerate(fp):
        if f == "#": return True
        if i >= len(tp) or (f != "+" and f != tp[i]): return False
    return len(fp) == len(tp)

# --- buffered stream parsing (client side) --------------------------------------

class StreamParser:
    """Feed raw socket bytes in; get complete packets out. `partial` says
    whether bytes of an incomplete packet are buffered, so the caller can
    tell an idle packet boundary (safe place to time out and PINGREQ) from a
    mid-packet stall (stream integrity risk: reconnect instead of dropping
    the half-read packet).

    max_body rejects absurd announced sizes before they can allocate."""

    def __init__(self, max_body=1 << 20):
        self.buf = bytearray()
        self.max_body = max_body
        self.partial = False

    def feed(self, chunk):
        self.buf += chunk

    def next_packet(self):
        """(header byte, body bytes) of the next complete packet, else None."""
        buf = self.buf
        if len(buf) < 2:
            self.partial = bool(buf)
            return None
        mult, rem, i = 1, 0, 1
        while True:
            if i >= len(buf):
                self.partial = True             # waiting for more length bytes
                return None
            b = buf[i]; i += 1
            rem += (b & 0x7f) * mult
            if not (b & 0x80):
                break
            if i > 4:
                raise ValueError("malformed remaining-length")
            mult *= 128
        if rem > self.max_body:
            raise ValueError(f"packet body {rem} exceeds cap {self.max_body}")
        if len(buf) < i + rem:
            self.partial = True                 # waiting for the body
            return None
        hdr, body = buf[0], bytes(buf[i:i + rem])
        del buf[:i + rem]
        self.partial = False
        return hdr, body
