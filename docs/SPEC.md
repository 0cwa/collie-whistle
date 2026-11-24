# Collie Whistle spec

Collie Whistle is a small self-hosted UnifiedPush server in Rust. It speaks a subset
of the ntfy protocol, so existing distributors work with it unchanged. It provides a Rust
library and a standalone binary.

It has no dependency on CollieSocket, Signal, Tauri or Collie Pack, and it never
decrypts or parses payloads. Collie Pack bundles the binary; Collie Fetch is a
planned client.

## Scope

- `POST`/`PUT /{topic}?up=1`, 4096-byte limit, binary bodies preserved through base64 when needed.
- `GET /{topic}?up=1` discovery. Application servers call it, the two distributors do not.
- Subscribe over `/json` (ntfy Android, default), `/ws` (ntfy Android, optional) and `/sse`
  (KUnifiedPush) with `since=none|all|latest|<id>`, multiple topics, `open` and keepalive events.
  `poll=1` is not called by either client but stays, for application servers; the
  response must end after the cached messages. `since=latest` is only used by those
  application-server polls.
- An expiring in-memory cache: a restart drops pending messages. Persistence can
  come later. Keep per-topic and global limits and high-entropy topics.
- An optional token guards subscriptions only; publishing, discovery and health
  remain open. Configuration and accepted credentials are documented in the
  [README](../README.md#run-it).

Nothing else. No attachments, accounts or web UI. A normal install must not
require FoundationDB.

A 2xx means the server accepted the message, not that a device received it.

## Conformance tests

This repository owns the recorded-client fixtures and protocol checks described
in [CONFORMANCE.md](CONFORMANCE.md). Run the same fixtures against Whistle and
stock ntfy so they stay grounded in real client behavior. That guide owns the
client/version inventory, recording procedure and fixture coverage.

[CI](../.github/workflows/ci.yml) builds the standalone binary, runs the Rust tests
and runs the conformance suite against it. Historical provider and client proof
is recorded in [PR #4](https://github.com/0cwa/collie-whistle/pull/4).

When Collie Fetch is implemented, it must run a pinned copy of these fixtures
from the client side.

## To prove

Hour-scale connections and behavior beyond the cache window still need acceptance.
Keep the [conformance coverage limits](CONFORMANCE.md#not-covered) explicit;
passing the recorded fixtures does not establish unrecorded real-client behavior.

## To decide

- Licence; the repository and `Cargo.toml` do not declare one.
- Whether and when to make the repository public. Pack's pinned sidecar builds
  currently need access to this private source.
