"""
mock_api.py - a tiny local API to try the API import and API push features
without any real service. Standard library only.

    python mock_api.py            # listens on http://127.0.0.1:8099

Endpoints (all need the header  Authorization: Bearer demo-token):
    GET  /customers   -> {"data": {"items": [ ...records... ]}}
    POST /ingest      -> accepts a JSON object, a JSON list, or {"records": [...]}.
                         Each record must have "id" and "name", else HTTP 422.
    GET  /received    -> everything accepted so far
"""
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TOKEN = "demo-token"
RECEIVED: list = []
CUSTOMERS = [
    {"id": 1, "name": "Aminata Kamara", "city": "Freetown", "balance": "1,200.50", "joined": "2024-01-15", "tags": ["vip"]},
    {"id": 2, "name": "mohamed sesay", "city": "Bo", "balance": "300", "joined": "2024-02-15", "tags": []},
    {"id": 3, "name": "Fatmata Bangura", "city": "Kenema", "balance": "", "joined": "2024-03-02", "tags": ["new"]},
]


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authed(self):
        if self.headers.get("Authorization") in {TOKEN, f"Bearer {TOKEN}"}:
            return True
        if self.headers.get("Authorization") != f"Bearer {TOKEN}":
            self._send(401, {"error": "invalid or missing token"})
            return False
        return True

    def do_GET(self):
        if not self._authed():
            return
        if self.path.startswith("/customers"):
            self._send(200, {"data": {"items": CUSTOMERS}})
        elif self.path.startswith("/received"):
            self._send(200, RECEIVED)
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        if not self._authed():
            return
        raw = self.rfile.read(int(self.headers.get("Content-Length", 0)) or 0)
        try:
            payload = json.loads(raw or b"null")
        except json.JSONDecodeError:
            return self._send(400, {"error": "body is not valid JSON"})
        if self.path != "/ingest":
            return self._send(404, {"error": "not found"})
        if isinstance(payload, dict) and "records" in payload:
            payload = payload["records"]
        records = payload if isinstance(payload, list) else [payload]
        for i, r in enumerate(records):
            if not isinstance(r, dict) or "id" not in r or "name" not in r:
                return self._send(422, {"error": f"record {i} must have 'id' and 'name'"})
        RECEIVED.extend(records)
        self._send(201, {"accepted": len(records)})

    def log_message(self, *args):  # keep the console quiet
        pass


def make_server(port=8099):
    return ThreadingHTTPServer(("127.0.0.1", port), Handler)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8099
    print(f"Mock API on http://127.0.0.1:{port}  (token: {TOKEN})  - Ctrl+C to stop")
    try:
        make_server(port).serve_forever()
    except KeyboardInterrupt:
        pass
