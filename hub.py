#!/usr/bin/env python3
"""
technocore-websub - a real W3C WebSub hub for technocore.chat rooms.

technocore-chat's own docs/interop.md (src/interop.md in flop-labs/technocore-chat)
names six protocols it can be bridged to and gives a shape for each. Five of the six
already have independent implementations circulating in this ecosystem (ActivityPub-
style bridges to Telegram/Discord/Slack, an MCP server, several DID/A2A adapters).
WebSub does not, as of this tool's first commit - this fills that specific gap.

What a WebSub hub is, briefly: subscribers register a callback URL for a topic with the
hub; the hub verifies they actually asked for it (a GET with a random challenge they must
echo back); from then on, every time the topic changes, the hub POSTs a notification to
every verified subscriber. It's the same idea PubSubHubbub popularized for RSS/Atom feeds,
formalized as https://www.w3.org/TR/websub/.

technocore.chat itself can be neither hub nor publisher - it makes no outbound requests,
by design (docs/design.md). So the hub has to be a separate process that long-polls rooms
on subscribers' behalf and fans out - exactly what this is, and exactly what interop.md's
WebSub section describes:

    technocore --GET /r/<room>?since=&wait=10--> hub --POST callback--> subscribers

One hub long-polling a room serves every subscriber off ONE client IP's read budget,
where each subscriber polling directly would spend it separately - the whole reason to
run a hub instead of everyone rolling their own poller (which is what most of the
"technocore-*-bot/watcher/monitor" tools in this ecosystem currently do).

Design choices, and why, straight from interop.md so this is verifiably compliant with
what technocore-chat's own maintainers described rather than a WebSub hub that happens to
also talk to this service:

- The topic is the room's plain read URL with NO cursor (`?since=` names a different
  resource per subscriber; subscriptions would never coalesce onto one poll).
- Notifications are a *thin ping* - "the room advanced to last_seq" - never the message
  content itself. Republishing content under the hub's own signature would read as
  provenance the hub does not have; subscribers re-fetch from their own cursor instead.
- `p-` (private, capability-URL) rooms are refused as topics outright: a hub that
  recorded one would be storing someone's "unguessable" secret in a subscriptions file.

Dependencies: none beyond the standard library, on purpose - matching technocore-chat's
own stated preference (README/CONTRIBUTING) for small, auditable, dependency-free tools
in this ecosystem, and technocore-companion's existing choice (this project's earlier,
unrelated sibling tool) to do the same.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

APP_NAME = "technocore-websub"
APP_VERSION = "0.1.0"

TECHNOCORE_BASE = "https://technocore.chat"
STATE_PATH = Path.home() / ".technocore-websub" / "subscriptions.json"
HUB_HOST = "0.0.0.0"
HUB_PORT = 8734
HUB_SELF_URL = f"http://localhost:{HUB_PORT}/"  # override via --self-url for a public hub

DEFAULT_LEASE_SECONDS = 10 * 24 * 3600  # 10 days, WebSub's own commonly-used default
MIN_LEASE_SECONDS = 3600  # refuse a lease shorter than an hour - not worth a poller thread
MAX_LEASE_SECONDS = 30 * 24 * 3600
VERIFY_TIMEOUT = 10
NOTIFY_TIMEOUT = 10
POLL_WAIT_SECONDS = 10  # matches technocore-chat's documented wait= ceiling headroom

_lock = threading.Lock()
_subscriptions: dict[str, list[dict]] = {}  # topic -> [{callback, secret, expires_at}]
_pollers: dict[str, threading.Thread] = {}  # topic -> running poller thread
_cursors: dict[str, int] = {}  # topic -> last_seq already delivered


# --------------------------------------------------------------------------- persistence


def load_state() -> None:
    if not STATE_PATH.exists():
        return
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return
    with _lock:
        _subscriptions.update(data.get("subscriptions", {}))
        _cursors.update(data.get("cursors", {}))


def save_state() -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with _lock:
        payload = {"subscriptions": _subscriptions, "cursors": _cursors}
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(STATE_PATH)


# ----------------------------------------------------------------------------- topic rules


def room_from_topic(topic: str) -> str | None:
    """The room name a topic names, or None if it isn't a room URL this hub will serve.

    Only `{TECHNOCORE_BASE}/r/<room>` with no query string, per interop.md: a topic with
    `?since=` in it names a different resource per caller and subscriptions would never
    coalesce onto one poll.
    """
    prefix = f"{TECHNOCORE_BASE}/r/"
    if not topic.startswith(prefix) or "?" in topic:
        return None
    room = topic[len(prefix) :]
    if not room or "/" in room:
        return None
    return room


def topic_allowed(topic: str) -> bool:
    """Refuse `p-` topics: an unlisted room name is a capability, and a hub records its
    topics (in `_subscriptions`, on disk) - accepting one would mean holding someone
    else's secret in a file this tool never promised to protect that way."""
    room = room_from_topic(topic)
    return room is not None and not room.startswith("p-")


# ------------------------------------------------------------------------------ verification


def verify_intent(callback: str, mode: str, topic: str, challenge: str, lease_seconds: int) -> bool:
    """GET the subscriber's callback with the challenge; true only if it echoes the exact
    bytes back with a 2xx. This is what stops a hub from being turned into a confirmed-
    delivery oracle against a URL that never asked to subscribe."""
    params = {
        "hub.mode": mode,
        "hub.topic": topic,
        "hub.challenge": challenge,
    }
    if mode == "subscribe":
        params["hub.lease_seconds"] = str(lease_seconds)
    url = callback + ("&" if "?" in callback else "?") + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=VERIFY_TIMEOUT) as resp:
            if not (200 <= resp.status < 300):
                return False
            body = resp.read(len(challenge) + 16)
    except (urllib.error.URLError, OSError, ValueError):
        return False
    return body.decode("utf-8", errors="replace").strip() == challenge


# --------------------------------------------------------------------------------- pub/sub


def subscribe(callback: str, topic: str, lease_seconds: int, secret: str) -> bool:
    challenge = secrets.token_urlsafe(24)
    if not verify_intent(callback, "subscribe", topic, challenge, lease_seconds):
        return False
    expires_at = time.time() + lease_seconds
    with _lock:
        entries = _subscriptions.setdefault(topic, [])
        entries[:] = [e for e in entries if e["callback"] != callback]  # resubscribe replaces
        entries.append({"callback": callback, "secret": secret, "expires_at": expires_at})
    save_state()
    ensure_poller(topic)
    return True


def unsubscribe(callback: str, topic: str) -> bool:
    challenge = secrets.token_urlsafe(24)
    if not verify_intent(callback, "unsubscribe", topic, challenge, 0):
        return False
    with _lock:
        entries = _subscriptions.get(topic, [])
        entries[:] = [e for e in entries if e["callback"] != callback]
    save_state()
    return True


def active_subscribers(topic: str) -> list[dict]:
    now = time.time()
    with _lock:
        entries = _subscriptions.get(topic, [])
        live = [e for e in entries if e["expires_at"] > now]
        if len(live) != len(entries):
            entries[:] = live  # drop expired leases as a side effect of reading them
    return live


def notify(topic: str, last_seq: int) -> None:
    """The thin ping interop.md asks for - "the room advanced to last_seq" - never the
    message text. Each subscriber re-fetches from ITS OWN cursor; a fat ping would carry
    the hub's window instead of theirs, and would restate anonymous room content under the
    hub's signature, which is provenance this hub does not have."""
    body = json.dumps({"topic": topic, "last_seq": last_seq}).encode("utf-8")
    for sub in active_subscribers(topic):
        headers = {
            "Content-Type": "application/json",
            "Link": f'<{topic}>; rel="self", <{HUB_SELF_URL}>; rel="hub"',
            "User-Agent": f"{APP_NAME}/{APP_VERSION}",
        }
        if sub.get("secret"):
            digest = hmac.new(sub["secret"].encode("utf-8"), body, hashlib.sha256).hexdigest()
            headers["X-Hub-Signature"] = f"sha256={digest}"
        request = urllib.request.Request(sub["callback"], data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=NOTIFY_TIMEOUT):
                pass
        except (urllib.error.URLError, OSError):
            pass  # a missed ping costs one poll cycle to a subscriber's own cursor, not a retry storm


# ---------------------------------------------------------------------------------- polling


def ensure_poller(topic: str) -> None:
    with _lock:
        thread = _pollers.get(topic)
        if thread is not None and thread.is_alive():
            return
        thread = threading.Thread(target=_poll_loop, args=(topic,), daemon=True)
        _pollers[topic] = thread
    thread.start()


def _poll_loop(topic: str) -> None:
    """One long-poll loop per topic, running only while it has at least one live
    subscriber - this is the ONE client this hub uses to read the room, no matter how many
    subscribers are behind it (see the module docstring: this is the whole reason a hub
    exists rather than everyone polling directly)."""
    room = room_from_topic(topic)
    since = _cursors.get(topic, 0)
    while active_subscribers(topic):
        url = f"{TECHNOCORE_BASE}/r/{room}?since={since}&wait={POLL_WAIT_SECONDS}&format=json"
        request = urllib.request.Request(url, headers={"User-Agent": f"{APP_NAME}/{APP_VERSION}"})
        try:
            with urllib.request.urlopen(request, timeout=POLL_WAIT_SECONDS + 5) as resp:
                view = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, OSError, json.JSONDecodeError):
            time.sleep(2)  # transient hiccup - back off and retry, this loop is long-lived
            continue
        last_seq = view.get("last_seq", since)
        if last_seq != since:
            since = last_seq
            with _lock:
                _cursors[topic] = since
            save_state()
            notify(topic, since)
    with _lock:
        _pollers.pop(topic, None)


# ------------------------------------------------------------------------------- HTTP server


class HubHandler(BaseHTTPRequestHandler):
    server_version = f"{APP_NAME}/{APP_VERSION}"

    def log_message(self, fmt: str, *args) -> None:  # quieter, structured-enough default log
        sys.stderr.write(f"{self.address_string()} {fmt % args}\n")

    def _reply(self, status: int, body: str) -> None:
        encoded = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler's naming
        if self.path in ("/", "/hub"):
            self._reply(
                200,
                f"{APP_NAME} {APP_VERSION} - a WebSub hub for technocore.chat rooms.\n"
                f"POST hub.mode=subscribe&hub.topic=<{TECHNOCORE_BASE}/r/room>&hub.callback=<url> here.\n"
                "See README.md and https://www.w3.org/TR/websub/.\n",
            )
            return
        self._reply(404, "404 not found\n")

    def do_POST(self) -> None:  # noqa: N802
        if self.path not in ("/", "/hub"):
            self._reply(404, "404 not found\n")
            return
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length else b""
        form = urllib.parse.parse_qs(raw.decode("utf-8", errors="replace"))
        mode = (form.get("hub.mode") or [""])[0]
        topic = (form.get("hub.topic") or [""])[0]
        callback = (form.get("hub.callback") or [""])[0]
        secret = (form.get("hub.secret") or [""])[0]

        if mode not in ("subscribe", "unsubscribe"):
            self._reply(400, "400 hub.mode must be 'subscribe' or 'unsubscribe'\n")
            return
        if not callback.startswith(("http://", "https://")):
            self._reply(400, "400 hub.callback must be an absolute http(s) URL\n")
            return
        if not topic_allowed(topic):
            self._reply(
                400,
                "400 hub.topic must be an exact technocore.chat room read URL "
                f"({TECHNOCORE_BASE}/r/<room>, no query string), and not a p- room\n",
            )
            return

        self._reply(202, "202 accepted, verifying intent asynchronously\n")
        if mode == "subscribe":
            lease = _parse_lease((form.get("hub.lease_seconds") or [""])[0])
            threading.Thread(
                target=subscribe, args=(callback, topic, lease, secret), daemon=True
            ).start()
        else:
            threading.Thread(target=unsubscribe, args=(callback, topic), daemon=True).start()


def _parse_lease(raw: str) -> int:
    try:
        seconds = int(raw)
    except ValueError:
        return DEFAULT_LEASE_SECONDS
    return max(MIN_LEASE_SECONDS, min(MAX_LEASE_SECONDS, seconds))


def main() -> int:
    load_state()
    for topic, entries in list(_subscriptions.items()):
        if any(e["expires_at"] > time.time() for e in entries):
            ensure_poller(topic)
    server = ThreadingHTTPServer((HUB_HOST, HUB_PORT), HubHandler)
    print(f"{APP_NAME} {APP_VERSION} listening on {HUB_HOST}:{HUB_PORT}", file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
