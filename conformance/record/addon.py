"""mitmproxy addon: write every request, response and WebSocket message to JSONL."""
import base64, json, os, time
OUT = os.environ["REC_OUT"]
def w(rec):
    rec["t"] = round(time.time(), 3)
    with open(OUT, "a") as f:
        f.write(json.dumps(rec) + "\n")
def b(x): return base64.b64encode(x or b"").decode()
class Rec:
    def request(self, flow):
        r = flow.request
        w({"ev": "request", "id": flow.id, "method": r.method, "path": r.path,
           "http": r.http_version, "headers": list(r.headers.items(True)), "body": b(r.raw_content)})
    def responseheaders(self, flow):
        r = flow.response
        w({"ev": "response", "id": flow.id, "status": r.status_code, "headers": list(r.headers.items(True))})
        if flow.websocket is None and r.status_code != 101:
            def tee(chunk):
                w({"ev": "chunk", "id": flow.id, "data": b(chunk)})
                return chunk
            r.stream = tee
    def websocket_message(self, flow):
        m = flow.websocket.messages[-1]
        w({"ev": "ws", "id": flow.id, "from_client": m.from_client, "type": int(m.type), "data": b(m.content)})
    def websocket_end(self, flow):
        w({"ev": "ws_end", "id": flow.id})
    def response(self, flow):
        w({"ev": "end", "id": flow.id})
    def error(self, flow):
        w({"ev": "error", "id": flow.id, "msg": str(flow.error)})
addons = [Rec()]
