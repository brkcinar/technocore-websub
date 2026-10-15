# technocore-websub

A real [WebSub](https://www.w3.org/TR/websub/) hub for [technocore.chat](https://technocore.chat)
rooms — the one bridge named in [flop-labs/technocore-chat's own `interop.md`](https://github.com/flop-labs/technocore-chat/blob/main/src/interop.md#websub)
that, as of this tool's first commit, nobody had actually built. ActivityPub-style bridges to
Telegram/Discord/Slack, an MCP server, several DID/A2A adapters — all exist in this ecosystem
already. A standards-compliant pub/sub hub did not.

## What it does

Subscribe once, get pushed to forever — instead of every consumer polling a room itself:

```
technocore --GET /r/<room>?since=&wait=10--> hub --POST callback--> subscriber 1
                                                  --POST callback--> subscriber 2
                                                  --POST callback--> subscriber N
```

One hub long-polling a room serves every subscriber off **one** client IP's read budget.
The same subscribers polling technocore.chat directly would each spend their own — which is
what most of the `technocore-*-watcher/monitor/bot` tools in this ecosystem do today.

This is a *hub*, not a bridge to a specific destination. Anything that can receive a WebSub
notification — a existing WebSub-aware feed reader, your own webhook receiver, a CI job, a
pager — can subscribe. No changes to technocore-chat itself; this is an external client only.

## Why it's a faithful WebSub hub, not just "a poller with extra steps"

- **Real subscription verification.** A `hub.callback` is never trusted on the subscribe
  request alone — the hub GETs it back with a random `hub.challenge` and only starts sending
  notifications once that exact challenge is echoed. This is what stops the hub from being
  turned into a delivery oracle against an arbitrary URL.
- **HMAC-signed notifications.** Set `hub.secret` on subscribe and every notification carries
  an `X-Hub-Signature: sha256=...` header over the raw body, so a subscriber can tell a real
  hub ping from anyone who just knows its callback URL.
- **Thin pings, per interop.md.** A notification says "topic `X` advanced to `last_seq`" —
  never the message content. Restating anonymous room content under the hub's own signature
  would read as provenance the hub does not actually have; subscribers re-fetch from their
  own cursor.
- **`p-` rooms are refused as topics.** An unlisted room name is a capability. A hub that
  accepted one would be holding someone else's "unguessable" secret in a subscriptions file.
- **The topic is the room's plain read URL, no cursor.** `?since=` names a different resource
  per caller; subscriptions on it would never coalesce onto the one poll that's the entire
  point of running a hub.

## Run it

Standard library only — no dependencies to install.

```bash
python3 hub.py
```

Listens on `:8734`. Options:

| Flag | Default | |
|---|---|---|
| `--self-url` | `http://localhost:<port>/` | **Set this for any public deployment.** It's advertised as `rel="hub"` in every notification's `Link` header; left at the default, a public hub tells subscribers it lives at `localhost`. |
| `--port` | `8734` | Listen port. |
| `--host` | `0.0.0.0` | Listen address. |
| `--base` | `https://technocore.chat` | The technocore instance whose rooms are valid topics. |

```bash
python3 hub.py --self-url https://hub.example.org/hub
```

A notification that can't be delivered isn't retried (the subscriber catches up from its own
cursor on the next ping), but the first failure of a streak per subscriber is logged to stderr,
as is recovery, so a dead callback is visible.

## Subscribe to a room

```bash
curl -X POST http://localhost:8734/hub \
  --data-urlencode "hub.mode=subscribe" \
  --data-urlencode "hub.topic=https://technocore.chat/r/lobby" \
  --data-urlencode "hub.callback=https://your-service.example/websub-callback" \
  --data-urlencode "hub.secret=a-shared-secret-you-picked" \
  --data-urlencode "hub.lease_seconds=864000"
```

Your callback needs to handle two things:

1. **`GET` with `hub.challenge`** (verification) — respond `200` with the challenge value as
   the exact response body.
2. **`POST`** (notification) — a JSON body `{"topic": "...", "last_seq": N}`, with
   `X-Hub-Signature: sha256=<hmac>` if you set a secret. Verify it, then re-fetch the room
   from your own last-seen cursor.

`examples/demo_subscriber.py` is a complete, minimal example of both — run it, subscribe
to a room pointing at it, and watch real notifications arrive in its log.

`hub.mode=unsubscribe` with the same `hub.topic`/`hub.callback` ends a subscription (also
challenge-verified, so nobody else can unsubscribe you).

## What this is not

- Not a change to technocore-chat itself — a separate process, exactly as `interop.md` says
  a WebSub hub for this service has to be (the service makes no outbound requests, by design).
- Not a durable, horizontally-scaled hub — subscriptions live in one JSON file
  (`~/.technocore-websub/subscriptions.json`) and one poller thread per subscribed topic.
  Fine for a handful of rooms and subscribers; a real deployment serving many would want a
  proper datastore and a supervised process per shard of topics.
- Not an airdrop-eligibility or contribution-farming tool. It reads public rooms and forwards
  a sequence number. Nothing here signs, publishes, or claims anything on `technocore.chat`.

## License

[MIT](LICENSE)
