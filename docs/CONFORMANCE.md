# Conformance suite

`conformance/` replays conversations recorded from real UnifiedPush clients against any
HTTP base URL. It needs only Python 3 and no packages.

```
conformance/run.sh http://127.0.0.1:8080            # whole suite, about 85 s
WHISTLE_FAST=1 conformance/run.sh http://...        # skips the 45 s keepalive waits, about 40 s
conformance/selftest.sh                              # proves the suite fails on a wrong server
```

`run.sh` fails if no test ran. The server needs rate limits high enough for roughly 60
requests per run; for stock ntfy set `visitor-request-limit-burst: 1000`.

## What is in it

| Fixture | Recorded from | Covers |
| --- | --- | --- |
| `android-jsonhttp` | ntfy Android, connection protocol `jsonhttp` | register, `/json` subscribe, publish text and binary up to 4096 and 4097 bytes, second registration (two topics), offline then reconnect with `since=`, keepalive, unregister |
| `android-ws` | ntfy Android, connection protocol `ws` | the same over `/ws` |
| `kup-sse` | KUnifiedPush distributor | the same over `/sse` with `since=all` then `since=<id>`, restart of the daemon |
| `app-server` | curl, **reference only** | discovery `GET /{topic}?up=1`, publish, and `poll=1` with `since=all`, `latest`, `none`, `<id>`, two topics, `/sse`. Neither distributor sends these, so the calls are written by hand and the expected answers are what stock ntfy returned |

A fixture is a JSON list of steps (`request`, `open`, `recv`, `close`) with the exact
bytes sent. Normalization keeps secrets and randomness out: topics are `{t1}`, `{t2}`,
message ids are `<id:m1>` where first seen and `{m1}` after, `time` and `expires` are
`<int>`, and Authorization, cookie and host headers are dropped. Replies must match
field for field, including the set of JSON keys. Response headers are compared for
`Content-Type` (media type only). `recv` skips keepalives unless it expects one.

## Versions recorded (2026-11-11)

- ntfy server 2.37.2, release tarball `ntfy_2.37.2_linux_amd64.tar.gz`, sha256
  `e67f53d7fbed529b83faf3993b584110921ef3567b0ac0b033c6910879a4ffd1`
- ntfy Android 1.25.2, `ntfy-1.25.2-fdroid-debug.apk` (sha256
  `b59533391c68240e9704e0e60e69cb520e8f768196aa57d3b358480a40fe6689`), the F-Droid
  flavour from the same tag as the release; the debug build is used because `run-as`
  can then set its preferences. Emulator: API 35 `google_apis` x86_64, headless.
- KUnifiedPush 26.08.2 (`kunifiedpush-distributor`, Fedora 44 package) with its `Ntfy` push
  provider, in a podman container with a private D-Bus session
- mitmproxy 12.3.1 as the recording reverse proxy

Everything ran against a local ntfy. No public server was contacted.

Two stand-ins, both only on the app side of the clients. Android: the UnifiedPush
`REGISTER` and `UNREGISTER` broadcasts that a connector app would send are sent with
`adb shell am broadcast`; the app answered with `NEW_ENDPOINT` and `MESSAGE` as logged.
KUnifiedPush: `Register` is called over D-Bus with `gdbus`, and `record/connector.py` owns
the connector service names and logs what the distributor delivers, so delivery of the
exact payload bytes (4096 bytes for the 4096 and 4097 byte publishes) was seen at the
connector. Nothing in the fixtures is derived from source: all client traffic was recorded.

## Re-record

Needs podman, an Android SDK with the API 35 `google_apis` x86_64 image, curl, and a venv
with `mitmproxy`. Paths below are examples; scratch must be outside the repository.

```
# 1. stock ntfy behind a recording proxy (config: listen 127.0.0.1:12586,
#    base-url http://10.0.2.2:12587, cache-file, cache-duration 12h,
#    visitor-request-limit-burst 1000, visitor-request-limit-replenish 100ms)
ntfy serve -c server.yml &
REC_OUT=raw.jsonl mitmdump --mode reverse:http://127.0.0.1:12586@0.0.0.0:12587 \
    -s conformance/record/addon.py &

# 2. emulator, Android app, KUnifiedPush container
emulator -avd <api35 avd> -no-window -no-audio -no-snapshot &
adb install ntfy-1.25.2-fdroid-debug.apk
conformance/record/setup-kup.sh whistle-kup

# 3. drive the clients; each script writes its scenario to raw.jsonl through the proxy
export BASE=http://127.0.0.1:12587 RAW=$PWD/raw.jsonl ADB=adb
conformance/record/appserver.sh
conformance/record/android.sh jsonhttp
conformance/record/android.sh ws
conformance/record/kup.sh

# 4. normalize into fixtures
python3 -I conformance/record/normalize.py raw.jsonl conformance/fixtures --only kup-sse \
    --client "KUnifiedPush 26.08.2 ..."          # once per scenario; app-server takes --source reference
```

Remove the emulator, AVD and container afterwards (`podman rm -f whistle-kup`).

## Where the real calls differ from the first spec estimate

See "What recording showed" in [SPEC.md](SPEC.md). In short: KUnifiedPush uses `/sse`,
which was not in scope; neither client calls discovery or `poll=1`; registration makes no
HTTP call; over-limit bodies are truncated, not rejected; stock ntfy has one-second
ordering and a default rate limit that tests have to work around.

## Not covered

- Authenticated topics (`Authorization: Bearer`): both clients can send it, nobody sent it here.
- Long idle beyond one keepalive, and messages older than the cache window.
- Rejected publishes (413, rate limit): not called by the clients.
