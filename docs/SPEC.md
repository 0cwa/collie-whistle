# Collie Whistle spec

Draft, 2026-11-11.

Collie Whistle is a small self-hosted UnifiedPush server in Rust. It speaks a subset
of the ntfy protocol, so existing distributors work with it unchanged. It ships as a
crate and a standalone binary.

It has no dependency on CollieSocket, Signal or Tauri, and it never decrypts or
parses payloads. Collie Pack bundles it, and Collie Fetch is a client of it, but it
works for any UnifiedPush app.

## Scope

- `POST`/`PUT /{topic}`, 4096-byte limit, binary bodies preserved.
- `GET /{topic}?up=1` discovery.
- Subscribe over `/json` and `/ws` with `since=`, `poll=1`, multiple topics, keepalives.
- SQLite cache of about 12 hours, optional bearer token, high-entropy topics, quotas.

Nothing else. No attachments, accounts or web UI.

A 2xx means the server accepted the message, not that a device received it.

## Conformance tests

This repository owns the tests that define the protocol.

1. Record what the ntfy Android app and KUnifiedPush actually call against real ntfy.
2. Turn those recordings into fixtures, and run them against Whistle.
3. Run the same fixtures against stock ntfy, so the tests can't drift from the real thing.

Collie Fetch runs a pinned copy of these fixtures from the client side.

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
