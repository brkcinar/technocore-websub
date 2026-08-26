#!/usr/bin/env python3
"""A minimal WebSub subscriber, for trying the hub end to end.

Run this, then POST a subscription to the hub with this script's URL as hub.callback.
It answers the hub's verification GET (echoes hub.challenge back) and prints every
notification it receives afterwards, checking the HMAC signature if SECRET is set below.
"""

from __future__ import annotations

import hashlib
import hmac
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

PORT = 8735
SECRET = "demo-secret-change-me"  # must match hub.secret sent in the subscribe request


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        query = parse_qs(urlparse(self.path).query)
        challenge = query.get("hub.challenge", [None])[0]
        mode = query.get("hub.mode", ["?"])[0]
        topic = query.get("hub.topic", ["?"])[0]
        if challenge is None:
            self.send_response(404)
            self.end_headers()
            return
        print(f"[verify] mode={mode} topic={topic} -> echoing challenge", file=sys.stderr)
        body = challenge.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length)
        signature = self.headers.get("X-Hub-Signature", "")
        expected = "sha256=" + hmac.new(SECRET.encode(), raw, hashlib.sha256).hexdigest()
        verdict = "OK" if hmac.compare_digest(signature, expected) else "MISMATCH (or no secret set)"
        print(f"[notify] {raw.decode('utf-8', 'replace')} - signature: {verdict}", file=sys.stderr)
        self.send_response(200)
        self.end_headers()


if __name__ == "__main__":
    print(f"demo subscriber listening on :{PORT}", file=sys.stderr)
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
