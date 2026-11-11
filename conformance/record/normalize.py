#!/usr/bin/env python3
"""Turn a raw proxy recording (JSONL from addon.py) into conformance fixtures.

usage: normalize.py RAW.jsonl OUTDIR [--source client|derived] [--client "label"] [--only scenario ...]

The recording is split into scenarios by marker requests, /_marker/<scenario>/<step>.
Each scenario becomes OUTDIR/<scenario>.json: an ordered list of steps (see
conformance/README in docs/CONFORMANCE.md for the format).

Normalization, so that fixtures hold no secrets and no per-run randomness:
  * topic names become {t1}, {t2}, ... in order of first appearance
  * message ids become <id:m1>, ... where first seen, and {m1} where seen again
    (replayed messages, since=<id>)
  * time and expires become <int>
  * Authorization, Cookie, Host and hop-by-hop request headers are dropped
Request bodies are kept byte for byte (base64), since the test replays them.
"""
import argparse, base64, collections, json, os, re, sys

DROP_HEADERS = {"host", "authorization", "cookie", "content-length", "connection", "accept-encoding",
                "transfer-encoding", "upgrade", "sec-websocket-key", "sec-websocket-version",
                "sec-websocket-extensions", "keep-alive", "proxy-authorization"}
KEEP_RESPONSE_HEADERS = {"content-type"}
STREAM_SUFFIXES = ("/json", "/sse", "/ws")


def load(path):
    flows = collections.OrderedDict()
    markers = []
    for line in open(path):
        r = json.loads(line)
        f = flows.setdefault(r["id"], {"chunks": [], "ws": [], "closed": None})
        ev = r["ev"]
        if ev == "request":
            f["req"] = r
            if r["path"].startswith("/_marker/"):
                markers.append((r["t"], r["path"][len("/_marker/"):], r["id"]))
        elif ev == "response":
            f["resp"] = r
        elif ev == "chunk":
            f["chunks"].append((r["t"], base64.b64decode(r["data"])))
        elif ev == "ws":
            f["ws"].append(r)
        elif ev in ("end", "error", "ws_end"):
            f["closed"] = r["t"]
    return flows, markers


def hdrs(pairs):
    out = {}
    for k, v in pairs:
        out[k.lower()] = v
    return out


def split_path(p):
    path, _, query = p.partition("?")
    return path, query


def sse_events(buf):
    out = []
    for block in buf.replace(b"\r\n", b"\n").split(b"\n\n"):
        if not block.strip():
            continue
        ev = {"sse_event": None, "data": None}
        for ln in block.split(b"\n"):
            k, _, v = ln.partition(b":")
            v = v[1:] if v.startswith(b" ") else v
            if k == b"event":
                ev["sse_event"] = v.decode()
            elif k == b"data":
                ev["data"] = (ev["data"] or "") + v.decode()
        out.append(ev)
    return out


class Norm:
    """Per-scenario value normalizer."""

    def __init__(self):
        self.topics = {}   # real topic -> {tN}
        self.ids = {}      # real id -> N
        self.seen_ids = set()

    def topic_list(self, s):
        for t in s.split(","):
            if t and t not in self.topics:
                self.topics[t] = "t%d" % (len(self.topics) + 1)

    def text(self, s):
        for real, name in sorted(self.topics.items(), key=lambda kv: -len(kv[0])):
            s = s.replace(real, "{%s}" % name)
        return s

    def id_ref(self, real):
        if real not in self.ids:
            self.ids[real] = len(self.ids) + 1
        n = "m%d" % self.ids[real]
        if real in self.seen_ids:
            return "{%s}" % n
        self.seen_ids.add(real)
        return "<id:%s>" % n

    def path(self, p):
        path, q = split_path(p)
        parts = [x for x in path.split("/") if x]
        if parts:
            self.topic_list(parts[0])
        out = self.text(path)
        if q:
            def since(m):
                v = m.group(2)
                if v in self.ids:
                    return m.group(1) + "{m%d}" % self.ids[v]
                return m.group(0)
            q = re.sub(r"(since=)([A-Za-z0-9]{12})(?=&|$)", since, q)
            out += "?" + self.text(q)
        return out

    def message(self, obj):
        out = {}
        for k, v in obj.items():
            if k == "topic":
                self.topic_list(v)
                out[k] = self.text(v)
            elif k in ("id", "sequence_id"):
                out[k] = self.id_ref(v)
            elif k in ("time", "expires"):
                out[k] = "<int>"
            else:
                out[k] = v
        return out


def parse_stream(f, kind):
    """-> list of (t, expect-dict-before-normalisation)"""
    out = []
    if kind == "ws":
        for m in f["ws"]:
            if m["from_client"]:
                continue
            if m["type"] != 1:  # text frames only
                continue
            out.append((m["t"], {"json": json.loads(base64.b64decode(m["data"]))}))
        return out
    if kind == "json":
        buf = b""
        for t, c in f["chunks"]:
            buf += c
            while b"\n" in buf:
                ln, buf = buf.split(b"\n", 1)
                if ln.strip():
                    out.append((t, {"json": json.loads(ln)}))
        return out
    buf = b""
    for t, c in f["chunks"]:
        buf += c
        while True:
            m = re.search(rb"\r?\n\r?\n", buf)
            if not m:
                break
            block, buf = buf[:m.start()], buf[m.end():]
            for ev in sse_events(block + b"\n\n"):
                e = {"sse_event": ev["sse_event"]}
                if ev["data"] is not None:
                    e["json"] = json.loads(ev["data"])
                out.append((t, e))
    return out


def build(scenario, flows, markers, source, client_label):
    ms = [(t, p.split("/", 1)[1] if "/" in p else "", i) for (t, p, i) in markers if p.split("/")[0] == scenario]
    if not ms:
        return None
    t_begin = ms[0][0]
    t_end = ms[-1][0]
    step_t = {name: t for t, name, _ in ms}
    t_idle = step_t.get("idle")
    t_disc = step_t.get("disconnect")
    norm = Norm()
    items = []  # (time, order, step)
    n_stream = 0
    for fid, f in flows.items():
        req = f.get("req")
        if not req or req["path"].startswith("/_marker/") or not (t_begin <= req["t"] <= t_end):
            continue
        resp = f.get("resp") or {}
        path, _ = split_path(req["path"])
        h = hdrs(req["headers"])
        body = base64.b64decode(req["body"])
        step = {"op": "request", "method": req["method"], "path": req["path"]}
        rh = {k: v for k, v in h.items() if k not in DROP_HEADERS}
        if rh:
            step["headers"] = rh
        is_stream = path.endswith(STREAM_SUFFIXES) and resp.get("status") in (200, 101)
        if body:
            step["body_b64"] = base64.b64encode(body).decode()
            try:
                txt = body.decode()
                if len(txt) <= 200 and txt.isprintable():
                    step["body_text"] = txt
            except UnicodeDecodeError:
                pass
        rhd = hdrs(resp.get("headers", []))
        expect = {"status": resp.get("status")}
        ch = {k: v for k, v in rhd.items() if k in KEEP_RESPONSE_HEADERS}
        if ch:
            expect["headers"] = ch
        if is_stream:
            n_stream += 1
            kind = "ws" if resp["status"] == 101 else ("sse" if path.endswith("/sse") else "json")
            name = "s%d" % n_stream
            step.update(op="open", stream=name, transport=kind)
            step.pop("method")
            if kind == "ws":
                expect.pop("headers", None)
            step["expect"] = expect
            items.append((req["t"], 0, step))
            for t, e in parse_stream(f, kind):
                js = e.get("json")
                if js is not None and js.get("event") == "keepalive" and not (t_idle and t >= t_idle):
                    continue
                e = dict(e)
                if js is not None:
                    e["json"] = js
                items.append((t, 1, {"op": "recv", "stream": name, "expect": e}))
            closed = f["closed"]
            if closed:
                last = max([t for t, _ in parse_stream(f, kind)] + [0])
                items.append((max(min(closed, t_end), last) + 1e-6, 2, {"op": "close", "stream": name}))
        else:
            if resp.get("status") == 200 or resp.get("status"):
                bodyresp = b"".join(c for _, c in f["chunks"])
                if "ndjson" in rhd.get("content-type", ""):
                    expect["ndjson"] = [json.loads(x) for x in bodyresp.decode().splitlines() if x.strip()]
                elif bodyresp:
                    try:
                        j = json.loads(bodyresp)
                        expect["json"] = j
                    except ValueError:
                        expect["text"] = bodyresp.decode(errors="replace")
            step["expect"] = expect
            items.append((req["t"], 0, step))
    # topic substitution is applied after the fact so that later-seen topics reach earlier text
    items.sort(key=lambda x: (x[0], x[1]))
    steps = [s for _, _, s in items]
    for s in steps:  # normalise in time order so "first seen" means first in time
        if "path" in s:
            s["path"] = norm.path(s["path"])
        ex = s.get("expect", {})
        if "json" in ex:
            ex["json"] = norm.message(ex["json"])
        if "ndjson" in ex:
            ex["ndjson"] = [norm.message(m) for m in ex["ndjson"]]
    # drop keepalive-free duplicates of nothing; re-run text substitution on the whole thing
    raw = json.dumps(steps)
    for real, name in sorted(norm.topics.items(), key=lambda kv: -len(kv[0])):
        raw = raw.replace(real, "{%s}" % name)
    steps = json.loads(raw)
    return {
        "scenario": scenario,
        "source": source,
        "client": client_label,
        "slow": bool(t_idle),
        "topics": len(norm.topics),
        "steps": steps,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("raw")
    ap.add_argument("outdir")
    ap.add_argument("--source", default="client")
    ap.add_argument("--client", default="")
    ap.add_argument("--only", nargs="*")
    a = ap.parse_args()
    flows, markers = load(a.raw)
    scenarios = []
    for _, p, _ in markers:
        s = p.split("/")[0]
        if s not in scenarios:
            scenarios.append(s)
    n = 0
    os.makedirs(a.outdir, exist_ok=True)
    for s in scenarios:
        if a.only and s not in a.only:
            continue
        fx = build(s, flows, markers, a.source, a.client)
        if fx:
            with open(os.path.join(a.outdir, s + ".json"), "w") as fh:
                json.dump(fx, fh, indent=1)
                fh.write("\n")
            print("wrote", s, len(fx["steps"]), "steps")
            n += 1
    if not n:
        sys.exit("no scenarios found")


if __name__ == "__main__":
    main()
