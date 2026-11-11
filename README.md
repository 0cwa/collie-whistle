# Collie Whistle

A small self-hosted UnifiedPush server, compatible with ntfy clients. Part of the
Collie suite.

See [the spec](docs/SPEC.md).

Protocol conformance suite: [docs/CONFORMANCE.md](docs/CONFORMANCE.md).

## Run it

```
cargo build --release
target/release/collie-whistle
```

It listens on `127.0.0.1:8080` by default, which suits a reverse proxy or tunnel on the same
machine. Set `WHISTLE_LISTEN=0.0.0.0:8080` only to serve a LAN. Point a UnifiedPush
distributor (ntfy Android, KUnifiedPush) at the server's base URL, and put a TLS reverse
proxy in front for anything beyond a local network. The same server is a library:
`collie_whistle::serve(listener, Config::default())` runs it inside another process.

Configuration is by environment variable:

| Variable | Default | Meaning |
| --- | --- | --- |
| `WHISTLE_LISTEN` | `127.0.0.1:8080` | address to listen on |
| `WHISTLE_CACHE_SECONDS` | `3600` (1 h) | how long a message stays available for `since=` |
| `WHISTLE_MAX_MESSAGES` | `10000` | messages kept in total, oldest dropped first |
| `WHISTLE_MAX_PER_TOPIC` | `100` | messages kept per topic; a busy topic loses only its own oldest |
| `WHISTLE_PUBLISH_RATE` | `100` | publishes per second over all topics (429 beyond that), `0` for no limit |
| `WHISTLE_MAX_CONNECTIONS` | `1024` | open connections at once; more wait to be accepted |
| `WHISTLE_KEEPALIVE_SECONDS` | `45` | keepalive event / WebSocket ping interval |

A client gets 10 s to send its request header and a publisher 10 s to send its body. A
WebSocket that sends no frame (pongs count) for two keepalive intervals is dropped.

The cache lives in memory: a restart forgets undelivered messages, and clients that reconnect
with an unknown `since=` id get everything still cached (nothing, after a restart). There is no
authentication yet: anyone who knows a topic name can publish to it and read it, so topics
should stay high-entropy, as the distributors generate them. Message bodies longer than 4096
bytes are cut to their first 4096 bytes, as stock ntfy does; a text body cut in the middle of a
character stops being valid UTF-8, so it is then stored as base64.

`cargo test` runs the unit and socket tests. The protocol conformance suite runs against the
built binary (see [docs/CONFORMANCE.md](docs/CONFORMANCE.md)):

```
WHISTLE_LISTEN=127.0.0.1:8080 target/release/collie-whistle &
conformance/run.sh http://127.0.0.1:8080
```

`tools/android-proof/` has the script that checks a 4096-byte binary payload reaches the stock
ntfy Android app's connector intact, including after a reconnect (needs an emulator).
