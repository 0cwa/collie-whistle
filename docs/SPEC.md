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
  (KUnifiedPush) with `since=none|all|latest|<id>`, multiple topics, `open` and keepalive events.
  `poll=1` is not called by either client but stays, for application servers; the
  response must end after the cached messages. `since=latest` is only used by those
  application-server polls.
- A message cache of about 1 hour, kept in memory in v1: a restart drops pending messages
  (a persistent cache can come later). Per-topic and global limits, an optional bearer token
  and high-entropy topics.
- The token guards subscribing only. Application servers publish to the endpoint URL the
  distributor hands out and cannot add one; both distributors can send one on subscribe
  (KUnifiedPush: Bearer or Basic; ntfy Android: Basic from a user, or a custom header).
  Credentials are read as stock ntfy reads them: `Authorization: Bearer`, Basic with the
  token as password, or `?auth=`. None is 403, a wrong one 401.

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
- A body over 4096 bytes: stock ntfy accepts it with 200 and silently cuts it to the first
  4096 bytes. The suite allows either that (exactly the first 4096 bytes delivered) or
  413. Whistle may do either; no client depends on which.
- Keepalives: `/json` and `/sse` get a `keepalive` event every 45 seconds (ntfy Android
  times a `/json` read out after 77 s of silence). On `/ws` stock ntfy sends a WebSocket
  ping frame every 45 s and no JSON keepalive; ntfy Android itself pings every 3 minutes,
  so the server must answer pings with a pong.
- KUnifiedPush keeps one `LastMessageId` for all its topics. After an app unregisters,
  it reconnects with `since=<id of a message on the removed topic>`. A provider must
  accept a `since` id that belongs to another topic (stock ntfy looks the id up globally).
- Fields the clients read: ntfy Android `id`, `time`, `event`, `topic`, `message`,
  `encoding` (`sequence_id` defaults to `id`); KUnifiedPush `id`, `topic`, `message`,
  `encoding` and the SSE event name (absent = message, `open` = connected, other names
  ignored). Extra JSON fields are allowed; the suite does not require `sequence_id`.
- Stock ntfy orders messages by whole seconds: `since=<id>` can return that message again
  when the reconnect lands in the same second. The suite spaces its steps to avoid it;
  Whistle may be stricter.
- With default limits stock ntfy answers 429 after about 60 requests, which a few suite
  runs reach; the recording server raises `visitor-request-limit-burst`.

## To prove

Answered by the recording (see above and CONFORMANCE.md):

- The exact calls the two clients make: recorded, over `/json`, `/ws` and `/sse`.
- Binary payloads arrive intact through `/json`, `/ws` and `/sse`: the suite publishes
  non-UTF-8 bodies of up to 4096 bytes and checks the base64 on every transport, and
  KUnifiedPush delivered the exact bytes to its connector.
- Long-lived connections survive idle periods and reconnect with `since=` without loss:
  the suite idles about a minute on `/json` and `/sse`, checks the ws pong, and checks
  that messages sent while a client was offline arrive after `since=`.

Still open for Whistle itself: the same fixtures must pass against the Whistle binary,
and a connection that stays open for hours is not tested here.

## To decide

- Licence.
- Going public: Collie Pack can't pull a private crate in CI without a token, so this
  should be public by the time Pack needs it.

[iamwehi/upf](https://github.com/iamwehi/upf) is a useful reference, but it requires
FoundationDB, which rules it out for a normal install.
