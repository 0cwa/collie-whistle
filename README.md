# Collie Whistle

A small self-hosted UnifiedPush server, compatible with ntfy clients. Part of the
Collie suite.

See [the spec](docs/SPEC.md).

Protocol conformance suite: [docs/CONFORMANCE.md](docs/CONFORMANCE.md).

## Run it

```
cargo build --release
WHISTLE_LISTEN=0.0.0.0:8080 target/release/collie-whistle
```

Point a UnifiedPush distributor (ntfy Android, KUnifiedPush) at the server's base URL. Put a
TLS reverse proxy in front for anything beyond a local network. The same server is a library:
`collie_whistle::serve(listener, Config::default())` runs it inside another process.

Configuration is by environment variable:

| Variable | Default | Meaning |
| --- | --- | --- |
| `WHISTLE_LISTEN` | `127.0.0.1:8080` | address to listen on |
| `WHISTLE_CACHE_SECONDS` | `43200` (12 h) | how long a message stays available for `since=` |
| `WHISTLE_MAX_MESSAGES` | `10000` | messages kept in total, oldest dropped first |
| `WHISTLE_KEEPALIVE_SECONDS` | `45` | keepalive event / WebSocket ping interval |

The cache lives in memory: a restart forgets undelivered messages, and clients that reconnect
with an unknown `since=` id get everything still cached (nothing, after a restart). There is no
authentication or quota yet, so do not expose a server to the internet that you do not want
others to publish through. Message bodies longer than 4096 bytes are cut to their first 4096
bytes, as stock ntfy does.

`cargo test` runs the unit and socket tests. The protocol conformance suite runs against the
built binary (see [docs/CONFORMANCE.md](docs/CONFORMANCE.md)):

```
WHISTLE_LISTEN=127.0.0.1:8080 target/release/collie-whistle &
conformance/run.sh http://127.0.0.1:8080
```

`tools/android-proof/` has the script that checks a 4096-byte binary payload reaches the stock
ntfy Android app's connector intact, including after a reconnect (needs an emulator).
