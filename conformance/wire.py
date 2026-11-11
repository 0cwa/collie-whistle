"""Minimal HTTP/1.1 and WebSocket client on plain sockets, standard library only.

Written by hand instead of using http.client or a WebSocket package so the suite has no
dependencies, and so that the tests see exactly what is on the wire (chunked streaming
bodies, upgrade handshake, frames) with no library smoothing over differences.
"""
import base64, hashlib, os, socket, ssl, struct
from urllib.parse import urlsplit

WS_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class Closed(Exception):
    pass


class Conn:
    def __init__(self, base_url, timeout=10.0):
        u = urlsplit(base_url)
        self.prefix = u.path.rstrip("/")
        port = u.port or (443 if u.scheme == "https" else 80)
        s = socket.create_connection((u.hostname, port), timeout=timeout)
        if u.scheme == "https":
            s = ssl.create_default_context().wrap_socket(s, server_hostname=u.hostname)
        self.sock = s
        self.host = u.netloc
        self.buf = b""
        self.chunked = False
        self.remaining = None  # content-length left, None = until close
        self.body_done = False

    # -- raw io
    def _fill(self):
        try:
            d = self.sock.recv(65536)
        except socket.timeout:
            raise
        except (ConnectionError, ssl.SSLError):
            d = b""
        if not d:
            raise Closed()
        self.buf += d

    def read_until(self, marker):
        while marker not in self.buf:
            self._fill()
        i = self.buf.index(marker) + len(marker)
        out, self.buf = self.buf[:i], self.buf[i:]
        return out

    def read_n(self, n):
        while len(self.buf) < n:
            self._fill()
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def close(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()

    # -- http
    def send_request(self, method, path, headers=None, body=b""):
        h = {"Host": self.host, "Connection": "close"}
        for k, v in (headers or {}).items():
            h[k] = v
        if body or method in ("POST", "PUT"):
            h["Content-Length"] = str(len(body))
        head = "%s %s%s HTTP/1.1\r\n" % (method, self.prefix, path)
        head += "".join("%s: %s\r\n" % kv for kv in h.items()) + "\r\n"
        self.sock.sendall(head.encode() + body)

    def read_head(self):
        raw = self.read_until(b"\r\n\r\n").decode("latin-1")
        lines = raw.split("\r\n")
        status = int(lines[0].split(" ", 2)[1])
        headers = {}
        for ln in lines[1:]:
            if ":" in ln:
                k, v = ln.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        self.chunked = "chunked" in headers.get("transfer-encoding", "").lower()
        cl = headers.get("content-length")
        self.remaining = int(cl) if cl is not None else None
        self.body_done = False
        return status, headers

    def read_body_piece(self):
        """Next piece of the response body, b'' at the end. Blocks up to the socket timeout."""
        if self.body_done:
            return b""
        if self.chunked:
            size = int(self.read_until(b"\r\n").strip().split(b";")[0], 16)
            if size == 0:
                self.read_until(b"\r\n")
                self.body_done = True
                return b""
            data = self.read_n(size)
            self.read_n(2)
            return data
        if self.remaining is not None:
            if self.remaining == 0:
                self.body_done = True
                return b""
            if not self.buf:
                self._fill()
            data, self.buf = self.buf[:self.remaining], self.buf[self.remaining:]
            self.remaining -= len(data)
            return data
        if not self.buf:
            try:
                self._fill()
            except Closed:
                self.body_done = True
                return b""
        data, self.buf = self.buf, b""
        return data

    def read_body(self):
        out = b""
        while True:
            p = self.read_body_piece()
            if not p:
                return out
            out += p

    # -- websocket
    def ws_handshake(self, path, headers=None):
        key = base64.b64encode(os.urandom(16)).decode()
        h = {"Upgrade": "websocket", "Connection": "Upgrade", "Sec-WebSocket-Key": key,
             "Sec-WebSocket-Version": "13"}
        h.update(headers or {})
        head = "GET %s%s HTTP/1.1\r\nHost: %s\r\n" % (self.prefix, path, self.host)
        head += "".join("%s: %s\r\n" % kv for kv in h.items()) + "\r\n"
        self.sock.sendall(head.encode())
        status, rh = self.read_head()
        if status == 101:
            want = base64.b64encode(hashlib.sha1(key.encode() + WS_GUID).digest()).decode()
            if rh.get("sec-websocket-accept") != want:
                raise AssertionError("bad Sec-WebSocket-Accept: %r" % rh.get("sec-websocket-accept"))
        return status, rh

    def ws_send(self, opcode, payload=b""):
        mask = os.urandom(4)
        n = len(payload)
        hdr = bytes([0x80 | opcode])
        if n < 126:
            hdr += bytes([0x80 | n])
        elif n < 65536:
            hdr += bytes([0x80 | 126]) + struct.pack(">H", n)
        else:
            hdr += bytes([0x80 | 127]) + struct.pack(">Q", n)
        self.sock.sendall(hdr + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))

    def ws_recv(self, on_control=None):
        """Next data message as (opcode, payload); answers ping, raises Closed on close.
        on_control(opcode, payload) is told about ping (9) and pong (10) frames."""
        msg, op0 = b"", None
        while True:
            b0, b1 = self.read_n(2)
            op, fin, n = b0 & 0x0F, b0 & 0x80, b1 & 0x7F
            if n == 126:
                n = struct.unpack(">H", self.read_n(2))[0]
            elif n == 127:
                n = struct.unpack(">Q", self.read_n(8))[0]
            data = self.read_n(n)  # server frames are unmasked
            if op == 9:
                self.ws_send(10, data)
                if on_control:
                    on_control(op, data)
            elif op == 10:
                if on_control:
                    on_control(op, data)
            elif op == 8:
                try:
                    self.ws_send(8, data[:2])
                except OSError:
                    pass
                raise Closed()
            else:
                if op != 0:
                    op0 = op
                msg += data
                if fin:
                    return op0, msg
