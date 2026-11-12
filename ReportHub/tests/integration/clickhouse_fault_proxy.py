import http.client
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
from urllib.parse import parse_qs, urlsplit


class FaultState:
    def __init__(self):
        self.lock = threading.Lock()
        self.mode = "pass"
        self.target_query_id = None
        self.blocked = None
        self.release = threading.Event()
        self.forwarded_kills = 0
        self.dropped_kills = 0

    def configure(self, mode, query_id=None):
        with self.lock:
            self.mode = mode
            self.target_query_id = query_id
            self.blocked = None
            self.release.clear()

    def snapshot(self):
        with self.lock:
            return {
                "mode": self.mode,
                "blocked": self.blocked,
                "forwarded_kills": self.forwarded_kills,
                "dropped_kills": self.dropped_kills,
            }

    def hold_if_configured(self, kind, query_id):
        with self.lock:
            mode = self.mode
            is_target_query = (
                self.target_query_id is None
                or self.target_query_id == query_id
            )
            should_hold = (
                (mode == "delay-query" and kind == "query" and is_target_query)
                or (mode == "delay-kill" and kind == "kill")
            )
            if not should_hold:
                return False
            self.blocked = {"kind": kind, "query_id": query_id}
            release = self.release

        if not release.wait(timeout=120):
            raise TimeoutError("Timed out waiting to release delayed ClickHouse request.")

        with self.lock:
            self.blocked = None
            self.mode = "pass"
            self.target_query_id = None
        return True

    def record_kill(self, dropped=False):
        with self.lock:
            self.forwarded_kills += 1
            if dropped:
                self.dropped_kills += 1
            if self.mode == "drop-kill-response":
                self.mode = "pass"


STATE = FaultState()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        return

    def _reply_json(self, status, value):
        body = json.dumps(value).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def _handle_control(self, path):
        if path == "/__fault/state":
            self._reply_json(200, STATE.snapshot())
            return True
        if path == "/__fault/configure" and self.command == "POST":
            content_length = int(self.headers.get("Content-Length", "0"))
            value = json.loads(self.rfile.read(content_length) or b"{}")
            if value.get("mode") not in {
                "pass", "delay-query", "delay-kill", "drop-kill-response",
            }:
                self._reply_json(400, {"error": "Unsupported fault mode"})
            else:
                STATE.configure(value["mode"], value.get("query_id"))
                self._reply_json(200, {"configured": value["mode"]})
            return True
        if path == "/__fault/release" and self.command == "POST":
            STATE.release.set()
            self._reply_json(200, {"released": True})
            return True
        return False

    def _forward(self):
        parsed = urlsplit(self.path)
        if self._handle_control(parsed.path):
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length) if content_length else None
        query_parameters = parse_qs(parsed.query)
        statement = query_parameters.get("query", [b""])[0]
        if isinstance(statement, str):
            statement = statement.encode("utf-8")
        statement = statement or (body or b"")
        is_kill = b"KILL QUERY" in statement.upper()
        query_id = query_parameters.get("query_id", [None])[0]
        kind = "kill" if is_kill else "query"

        STATE.hold_if_configured(kind, query_id)

        upstream = http.client.HTTPConnection(
            os.environ["CLICKHOUSE_UPSTREAM_HOST"],
            int(os.environ.get("CLICKHOUSE_UPSTREAM_PORT", "8123")),
            timeout=180,
        )
        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() not in {
                "host", "connection", "content-length", "transfer-encoding",
            }
        }
        try:
            upstream.request(self.command, self.path, body=body, headers=headers)
            response = upstream.getresponse()
            response_body = response.read()
            dropped = is_kill and STATE.snapshot()["mode"] == "drop-kill-response"
            if is_kill:
                STATE.record_kill(dropped=dropped)
            if dropped:
                self.close_connection = True
                return

            self.send_response(response.status, response.reason)
            for key, value in response.getheaders():
                if key.lower() not in {
                    "connection", "content-length", "transfer-encoding",
                    "server", "date",
                }:
                    self.send_header(key, value)
            self.send_header("Content-Length", str(len(response_body)))
            self.send_header("Connection", "close")
            self.end_headers()
            if response_body:
                self.wfile.write(response_body)
            self.close_connection = True
        finally:
            upstream.close()

    do_GET = _forward
    do_POST = _forward


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", 8123), Handler)
    server.daemon_threads = True
    server.serve_forever()
