#!/usr/bin/env python3
"""A deliberately wrong server, to show the suite notices. Not part of the product.

It looks plausible: accepts POST/PUT, answers ?up=1 discovery, streams /json. It gets
the details the recordings pin down wrong: no "open" event, binary bodies decoded as
text with replacement characters instead of base64, no 4096-byte limit, no since=.
usage: naive_server.py PORT
"""
import json, sys, threading, time, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

subs = {}  # topic -> list of wfile
lock = threading.Lock()


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _topic(self):
        return self.path.split("?")[0].strip("/").split("/")[0]

    def do_GET(self):
        t = self._topic()
        if self.path.split("?")[0].endswith("/json"):
            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            with lock:
                subs.setdefault(t, []).append(self.wfile)
            while True:
                time.sleep(1)
        body = b'{"unifiedpush":{"version":1}}\n'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        t = self._topic()
        data = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        m = {"id": uuid.uuid4().hex[:12], "time": int(time.time()), "event": "message", "topic": t,
             "message": data.decode("utf-8", "replace")}
        out = json.dumps(m).encode() + b"\n"
        with lock:
            for w in subs.get(t, []):
                w.write(b"%x\r\n%s\r\n" % (len(out), out))
                w.flush()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    do_PUT = do_POST


ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
