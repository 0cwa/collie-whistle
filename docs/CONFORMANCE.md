# Conformance suite

`conformance/` replays real UnifiedPush client recordings and reference calls against any
HTTP base URL. It needs only Python 3 and no packages. Run from the repository root:

```
conformance/run.sh http://127.0.0.1:8080               # whole suite, as run in CI
WHISTLE_FAST=1 conformance/run.sh http://127.0.0.1:8080 # skips keepalive waits
conformance/selftest.sh                              # checks rejection of a deliberately wrong server
```

`run.sh` fails if no test ran. The server needs rate limits high enough for roughly 60
requests per run; for stock ntfy set `visitor-request-limit-burst: 1000`.

## What is in it

| Fixture | Recorded from | Covers |
| --- | --- | --- |
| `android-jsonhttp` | ntfy Android, connection protocol `jsonhttp` | register, `/json` subscribe, publish text and binary up to 4096 and 4097 bytes, second registration (two topics), offline then reconnect with `since=`, keepalive, unregister |
| `android-ws` | ntfy Android, connection protocol `ws` | the same over `/ws` |
| `kup-sse` | KUnifiedPush distributor | the same over `/sse` with `since=all` then `since=<id>`, restart of the daemon |
| `ws-ping` | hand-written, **reference** | `/ws` open event, and a ping answered with a pong, which ntfy Android relies on |
| `app-server` | curl, **reference only** | discovery `GET /{topic}?up=1`, publish, and `poll=1` with `since=all`, `latest`, `none`, `<id>`, two topics, `/sse`; each poll must end by itself. Neither distributor sends these, so the calls are written by hand and the expected answers are what stock ntfy returned |

A fixture is a JSON object with a `steps` list (`request`, `open`, `recv`, `ping`, `close`) and the
exact bytes sent; a fixture with no steps fails. Normalization keeps secrets and
randomness out: topics are `{t1}`, `{t2}`, message ids are `<id:m1>` where first seen
and `{m1}` after, `time` and `expires` are `<int>`, and Authorization, cookie and host
headers are dropped.

What a reply must satisfy, so that a provider need not copy ntfy beyond what clients read:

- Every JSON key the fixture names must be present with the right value; extra keys are
  allowed. `sequence_id` is not in the fixtures (ntfy Android falls back to `id`).
- Ids are any non-empty run of URL-safe characters.
- `Content-Type` is compared by media type only.
- SSE: the `event:` line is `open` for the open event, absent for a message, and any
  other name for a keepalive. These are exactly the three cases KUnifiedPush tells apart.
- `recv` skips keepalives unless it expects one. `poll=1` responses must end.
- A body over 4096 bytes may be answered with 200 and exactly the first 4096 bytes
  delivered, or with 413. Rejected-message expectations are skipped; `since=latest`
  then expects the last accepted message for that topic.
- Steps that publish are spaced 1.1 s apart, and a reconnect with `since=` waits likewise.

## Versions recorded (2026-11-11)

- ntfy server 2.37.2, release tarball `ntfy_2.37.2_linux_amd64.tar.gz`, sha256
  `e67f53d7fbed529b83faf3993b584110921ef3567b0ac0b033c6910879a4ffd1`
- ntfy Android 1.25.2, `ntfy-1.25.2-fdroid-debug.apk` (sha256
  `b59533391c68240e9704e0e60e69cb520e8f768196aa57d3b358480a40fe6689`), the F-Droid
  flavour from the same tag as the release; the debug build is used because `run-as`
  can then set its preferences. Emulator: API 35 `google_apis` x86_64, headless.
- KUnifiedPush 26.08.2 (`kunifiedpush-distributor`, Fedora 44 package `kunifiedpush-26.08.2-1.fc44`)
  with its `Ntfy` push provider, in a podman container (image
  `ghcr.io/ublue-os/fedora-toolbox@sha256:152d578a893dc0d96239f92b50e46d3d238c0b4aebca89094ee118f3a407af77`)
  with a private D-Bus session.
- mitmproxy 12.3.1 (`conformance/record/requirements.txt`) as the recording reverse proxy

Everything ran against a local ntfy. No public server was contacted.

Two stand-ins, both only on the app side of the clients. Android: the UnifiedPush
`REGISTER` and `UNREGISTER` broadcasts that a connector app would send are sent with
`adb shell am broadcast`; the app answered with `NEW_ENDPOINT` and `MESSAGE` as logged.
KUnifiedPush: `Register` is called over D-Bus with `gdbus`, and `record/connector.py` owns
the connector service names and logs what the distributor delivers, so delivery of the
exact payload bytes (4096 bytes for the 4096 and 4097 byte publishes) was seen at the
connector. The three client fixtures use recorded traffic; the two reference fixtures
are identified in the table above.

## Re-record

Needs podman, an Android SDK with the API 35 `google_apis` x86_64 system image, curl, and
a venv with `pip install -r conformance/record/requirements.txt`. Keep scratch outside the repository.

```sh
# Start in the repository root; set the downloaded APK's absolute path.
whistle_repo=$PWD
whistle_apk=/absolute/path/to/ntfy-1.25.2-fdroid-debug.apk
whistle_recording=$(mktemp -d)
cd "$whistle_recording"

# 1. stock ntfy 2.37.2 behind a recording proxy
ntfy serve -c "$whistle_repo/conformance/record/server.yml" &
REC_OUT=raw.jsonl mitmdump --mode reverse:http://127.0.0.1:12586@0.0.0.0:12587 \
    -s "$whistle_repo/conformance/record/addon.py" &

# 2. emulator with the Android app, and the KUnifiedPush container
export ANDROID_SDK_ROOT=$HOME/Android/Sdk ANDROID_AVD_HOME=$PWD/avd
mkdir -p "$ANDROID_AVD_HOME"
echo no | "$ANDROID_SDK_ROOT/cmdline-tools/latest/bin/avdmanager" create avd -n whistle-rec \
    -k "system-images;android-35;google_apis;x86_64" -d pixel_6
"$ANDROID_SDK_ROOT/emulator/emulator" -avd whistle-rec -no-window -no-audio -no-snapshot -no-boot-anim &
adb wait-for-device; adb install "$whistle_apk"
"$whistle_repo/conformance/record/setup-kup.sh" whistle-kup

# 3. drive the clients; each script writes its scenario to raw.jsonl through the proxy
export BASE=http://127.0.0.1:12587 RAW=$PWD/raw.jsonl ADB=adb
"$whistle_repo/conformance/record/appserver.sh"
"$whistle_repo/conformance/record/android.sh" jsonhttp
"$whistle_repo/conformance/record/android.sh" ws
"$whistle_repo/conformance/record/kup.sh"

# 4. normalize into scratch fixtures for review
"$whistle_repo/conformance/record/normalize-all.sh" "$RAW" "$whistle_recording/fixtures"
```

`ws-ping.json` is written by hand; mitmproxy does not log WebSocket control frames.
Review the four generated fixtures before replacing the committed copies. If recording
different versions, update the labels in `normalize-all.sh` and the provenance above.
Afterwards stop ntfy and the proxy, and remove the emulator, AVD and container
(`adb emu kill`, `podman rm -f whistle-kup`).
Two things the drivers had to get right: KUnifiedPush purges clients whose D-Bus service
is not *activatable* (it needs a `.service` file, which `kup.sh` writes), and the Android
foreground service only starts while the app is in the foreground.

## Recorded client behaviour

Neither client calls discovery or `poll=1`, and registration makes no HTTP call.
Stock ntfy truncates over-limit bodies; the suite also accepts 413 as described above.
WebSocket ping/pong is covered by the reference fixture because the recording proxy
does not log control frames.

### Does a provider have to accept a `since` id from another topic?

Yes. In `kup-sse.json`, after the second app unregisters, KUnifiedPush opens
`/{t1}/sse?up=1&since={m11}` where `m11` is a message on `{t2}`, the topic just removed.
Stock ntfy resolves the id globally and returns messages after it. Behaviour for an id
outside the cache is not covered by these recordings.

## Not covered

- Authenticated real-client recordings (`Authorization: Bearer`): neither client sent it here.
  Server token behaviour has separate coverage in [tests/token.rs](../tests/token.rs).
- Long idle beyond one keepalive, and messages older than the cache window.
- Rate-limit rejections (429) and other error replies: no client behaviour depends on them here.
