"""Replay recorded UnifiedPush client conversations against any ntfy-compatible server.

    WHISTLE_URL=http://127.0.0.1:8080 python3 -m unittest discover -s conformance -v

WHISTLE_URL      base URL of the server under test (required). A path prefix is allowed.
WHISTLE_FAST=1   leave out the steps that wait about a minute to see a keepalive.
WHISTLE_TIMEOUT  seconds to wait for each expected stream message (default 15).

Each file in fixtures/ is one recorded scenario: a list of steps run in order. See
docs/CONFORMANCE.md for the format. Nothing here is specific to ntfy or to Whistle, so
the same file runs against stock ntfy and against the Whistle binary.
"""
import base64, json, os, queue, random, re, string, threading, time, unittest

from wire import Closed, Conn

HERE = os.path.dirname(os.path.abspath(__file__))
FIXTURES = os.path.join(HERE, "fixtures")
URL = os.environ.get("WHISTLE_URL", "")
FAST = os.environ.get("WHISTLE_FAST") == "1"
TIMEOUT = float(os.environ.get("WHISTLE_TIMEOUT", "15"))
KEEPALIVE_TIMEOUT = 75.0
ID_RE = re.compile(r"^[A-Za-z0-9]{12}$")
ALPHABET = string.ascii_letters + string.digits


class Mismatch(AssertionError):
    pass


class Run:
    """State for one fixture: topic names, captured ids and open streams."""

    def __init__(self, fx):
        self.fx = fx
        self.vars = {"t%d" % (i + 1): "up" + "".join(random.choice(ALPHABET) for _ in range(12))
                     for i in range(fx.get("topics", 0))}
        self.streams = {}   # name -> Stream
        self.where = ""
        self.last_publish = 0.0

    def fill(self, s):
        return re.sub(r"\{([tm]\d+)\}", lambda m: self.vars[m.group(1)], s)

    def fail(self, msg):
        raise Mismatch("%s: %s" % (self.where, msg))

    def match(self, exp, act, path="$"):
        if isinstance(exp, dict):
            if not isinstance(act, dict):
                self.fail("%s: expected object, got %r" % (path, act))
            if set(exp) != set(act):
                self.fail("%s: keys differ, expected %s, got %s (actual %s)" % (path, sorted(exp), sorted(act), json.dumps(act)[:300]))
            for k in exp:
                self.match(exp[k], act[k], "%s.%s" % (path, k))
        elif isinstance(exp, list):
            if not isinstance(act, list) or len(exp) != len(act):
                self.fail("%s: expected list of %d, got %r" % (path, len(exp), act))
            for i, (e, a) in enumerate(zip(exp, act)):
                self.match(e, a, "%s[%d]" % (path, i))
        elif exp == "<int>":
            if not isinstance(act, int) or isinstance(act, bool):
                self.fail("%s: expected integer, got %r" % (path, act))
        elif isinstance(exp, str) and exp.startswith("<id:"):
            if not isinstance(act, str) or not ID_RE.match(act):
                self.fail("%s: expected a 12-character message id, got %r" % (path, act))
            self.vars[exp[4:-1]] = act
        elif isinstance(exp, str):
            want = self.fill(exp)
            if want != act:
                self.fail("%s: expected %r, got %r" % (path, want, act if not isinstance(act, str) else act[:200]))
        elif exp != act:
            self.fail("%s: expected %r, got %r" % (path, exp, act))

    def check_head(self, exp, status, headers):
        if status != exp["status"]:
            self.fail("expected HTTP %s, got %s" % (exp["status"], status))
        for k, v in exp.get("headers", {}).items():
            got = headers.get(k, "")
            if got.split(";")[0].strip().lower() != v.split(";")[0].strip().lower():
                self.fail("header %s: expected %r, got %r" % (k, v, got))


class Stream:
    """A long-lived subscription (ndjson, SSE or WebSocket); events arrive on a queue."""

    def __init__(self, conn, kind):
        self.conn, self.kind = conn, kind
        self.q = queue.Queue()
        self.t = threading.Thread(target=self._pump, daemon=True)

    def start(self):
        self.conn.sock.settimeout(None)
        self.t.start()

    def _pump(self):
        try:
            if self.kind == "ws":
                while True:
                    op, data = self.conn.ws_recv()
                    if op == 1:
                        self.q.put({"json": json.loads(data)})
            buf = b""
            while True:
                piece = self.conn.read_body_piece()
                if not piece:
                    raise Closed()
                buf += piece
                if self.kind == "json":
                    while b"\n" in buf:
                        ln, buf = buf.split(b"\n", 1)
                        if ln.strip():
                            self.q.put({"json": json.loads(ln)})
                else:
                    buf = buf.replace(b"\r\n", b"\n")
                    while b"\n\n" in buf:
                        block, buf = buf.split(b"\n\n", 1)
                        self.q.put(self._sse(block))
        except Closed:
            self.q.put(None)
        except Exception as e:  # reader died: surface it in the test instead of hanging
            self.q.put(e)

    @staticmethod
    def _sse(block):
        ev, data = {"sse_event": None}, None
        for ln in block.split(b"\n"):
            k, _, v = ln.partition(b":")
            v = v[1:] if v.startswith(b" ") else v
            if k == b"event":
                ev["sse_event"] = v.decode()
            elif k == b"data":
                data = (data or "") + v.decode()
        if data is not None:
            ev["json"] = json.loads(data)
        return ev

    def next(self, timeout):
        try:
            item = self.q.get(timeout=timeout)
        except queue.Empty:
            return "timeout"
        if isinstance(item, Exception):
            raise item
        return item


def is_keepalive(ev):
    return ev.get("sse_event") == "keepalive" or ev.get("json", {}).get("event") == "keepalive"


def run_step(run, i, step):
    op = step["op"]
    run.where = "step %d (%s %s)" % (i, op, step.get("path", step.get("stream", "")))
    if op in ("request", "open"):
        path = run.fill(step["path"])
        headers = dict(step.get("headers", {}))
        body = base64.b64decode(step["body_b64"]) if "body_b64" in step else b""
        if step.get("method") in ("POST", "PUT"):
            # ntfy orders messages by whole seconds, so since=<id> only means "after that
            # message" if the next one is published in a later second. The recordings were
            # made with publishes more than a second apart; keep that when replaying.
            time.sleep(max(0.0, run.last_publish + 1.1 - time.monotonic()))
            run.last_publish = time.monotonic()
        elif "since=" in path:
            # the same second-granularity applies to a reconnect: stock ntfy replays the
            # message named by since=<id> itself if it was published in the current second
            time.sleep(max(0.0, run.last_publish + 1.1 - time.monotonic()))
        conn = Conn(URL, timeout=TIMEOUT)
        exp = step["expect"]
        if op == "open" and step["transport"] == "ws":
            status, rh = conn.ws_handshake(path, headers)
        else:
            conn.send_request(step.get("method", "GET"), path, headers, body)
            status, rh = conn.read_head()
        try:
            run.check_head(exp, status, rh)
        except Mismatch:
            conn.close()
            raise
        if op == "request":
            data = conn.read_body()
            conn.close()
            if "json" in exp:
                try:
                    run.match(exp["json"], json.loads(data))
                except ValueError:
                    run.fail("body is not JSON: %r" % data[:200])
            elif "text" in exp:
                run.match(exp["text"], data.decode(errors="replace"))
            elif "ndjson" in exp:
                run.match(exp["ndjson"], [json.loads(x) for x in data.decode().splitlines() if x.strip()])
        else:
            st = Stream(conn, step["transport"])
            st.start()
            run.streams[step["stream"]] = st
    elif op == "recv":
        st = run.streams[step["stream"]]
        exp = step["expect"]
        want_ka = exp.get("json", {}).get("event") == "keepalive"
        while True:
            ev = st.next(KEEPALIVE_TIMEOUT if want_ka else TIMEOUT)
            if ev == "timeout":
                run.fail("no message on stream %s within the timeout; expected %s" % (step["stream"], json.dumps(exp)[:300]))
            if ev is None:
                run.fail("stream %s closed by the server; expected %s" % (step["stream"], json.dumps(exp)[:300]))
            if is_keepalive(ev) and not want_ka:
                continue
            break
        run.match(exp, ev)
    elif op == "close":
        st = run.streams.pop(step["stream"])
        # nothing may be left over that the client did not get to see
        while True:
            ev = st.next(0.4)
            if ev == "timeout" or ev is None:
                break
            if not is_keepalive(ev):
                st.conn.close()
                run.fail("unexpected extra message on stream %s: %s" % (step["stream"], json.dumps(ev)[:300]))
        st.conn.close()
    else:
        raise ValueError("unknown op %r" % op)


def make_test(fx):
    def test(self):
        run = Run(fx)
        try:
            for i, step in enumerate(fx["steps"]):
                run_step(run, i, step)
        finally:
            for st in run.streams.values():
                st.conn.close()
    return test


class Conformance(unittest.TestCase):
    def test_a_fixtures_present(self):
        self.assertTrue(URL, "set WHISTLE_URL to the base URL of the server under test")
        self.assertTrue(FIXTURE_FILES, "no fixtures found in %s" % FIXTURES)


FIXTURE_FILES = sorted(f for f in os.listdir(FIXTURES) if f.endswith(".json")) if os.path.isdir(FIXTURES) else []
for _f in FIXTURE_FILES:
    _fx = json.load(open(os.path.join(FIXTURES, _f)))
    if FAST:  # leave out the steps that wait about a minute for a keepalive
        _fx["steps"] = [x for x in _fx["steps"]
                        if x.get("expect", {}).get("json", {}).get("event") != "keepalive"]
    _t = make_test(_fx)
    _t = unittest.skipUnless(URL, "WHISTLE_URL not set")(_t)
    setattr(Conformance, "test_" + re.sub(r"\W", "_", _f[:-5]), _t)

if __name__ == "__main__":
    unittest.main()
