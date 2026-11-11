# Collie Whistle spec

Draft, 2026-11-11.

Collie Whistle is a small self-hosted UnifiedPush server in Rust. It speaks a subset
of the ntfy protocol, so existing distributors work with it unchanged. It ships as a
crate and a standalone binary.

It has no dependency on CollieSocket, Signal or Tauri, and it never decrypts or
parses payloads. Collie Pack bundles it, and Collie Fetch is a client of it, but it
works for any UnifiedPush app.

## Scope

- `POST`/`PUT /{topic}?up=1`, 4096-byte limit, binary bodies preserved (as base64, see below).
- `GET /{topic}?up=1` discovery. Application servers call it, the two distributors do not.
- Subscribe over `/json` (ntfy Android, default), `/ws` (ntfy Android, optional) and `/sse`
  (KUnifiedPush) with `since=none|all|<id>`, multiple topics, `open` and keepalive events.
  `poll=1` is not called by either client but stays, for application servers.
- SQLite cache of about 12 hours, optional bearer token, high-entropy topics, quotas.

Nothing else. No attachments, accounts or web UI.

A 2xx means the server accepted the message, not that a device received it.

## Conformance tests

This repository owns the tests that define the protocol.

1. Record what the ntfy Android app and KUnifiedPush actually call against real ntfy.
2. Turn those recordings into fixtures, and run them against Whistle.
3. Run the same fixtures against stock ntfy, so the tests can't drift from the real thing.

Collie Fetch runs a pinned copy of these fixtures from the client side.

## What recording showed

Recorded 2026-11-11, details and re-record commands in [CONFORMANCE.md](CONFORMANCE.md).

- Neither client makes an HTTP call to register. The distributor invents a topic
  (`up` plus 12 characters) and hands the app `{base}/{topic}?up=1`. Topics are not
  created on the server; the first request that names one is the subscribe.
- KUnifiedPush subscribes with `GET /{t1,t2}/sse?up=1&since=all|<id>`, not `/json`.
  Whistle must serve SSE: `event: open` first, then `data:` lines, `event: keepalive`.
- Android subscribes with `/{t1,t2}/json?since=none|<id>` or `/ws`. Both reopen the
  connection with a longer topic list when a second app registers.
- Publishing with `?up=1` stores a non-UTF-8 body as base64 and adds `"encoding":"base64"`;
  UTF-8 text is stored as is. Stock ntfy without `up=1` refuses binary bodies (400).
- A body over 4096 bytes is accepted with 200 and silently cut to the first 4096. The
  fixtures pin that.
- Stock ntfy sends a keepalive every 45 seconds on all three transports.
- Stock ntfy orders messages by whole seconds: `since=<id>` can return that message again
  when the reconnect lands in the same second. The suite spaces its steps to avoid it;
  Whistle may be stricter.
- With default limits stock ntfy answers 429 after about 60 requests, which a few suite
  runs reach; the recording server raises `visitor-request-limit-burst`.

## To prove

- The exact calls those two clients make. Scope above is a best estimate until then.
- Binary payloads arrive intact through both `/json` and `/ws`.
- Long-lived connections survive idle periods and reconnect with `since=` without loss.

## To decide

- Licence.
- Going public: Collie Pack can't pull a private crate in CI without a token, so this
  should be public by the time Pack needs it.

[iamwehi/upf](https://github.com/iamwehi/upf) is a useful reference, but it requires
FoundationDB, which rules it out for a normal install.
