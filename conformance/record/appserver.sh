#!/bin/bash
# Calls an application server makes, and poll requests, which the two distributors never
# send. These are written by hand and answered by stock ntfy, so the fixture is marked
# "reference" rather than "client". usage: appserver.sh   env: BASE RAW
set -eu
HERE=$(cd "$(dirname "$0")" && pwd); . "$HERE/lib.sh"
S=app-server
BIN=$(mktemp -d); head -c 4096 /dev/urandom > "$BIN/max.bin"; head -c 4097 /dev/urandom > "$BIN/over.bin"
T1=up$(head -c 9 /dev/urandom | base64 | tr '/+' 'ab' | head -c 12); T2=up$(head -c 9 /dev/urandom | base64 | tr '/+' 'ab' | head -c 12)
g() { sleep 1.1; curl -s -A curl-app-server "$@" >/dev/null; }
mark "$S/begin"
mark "$S/discovery"
g "$BASE/$T1?up=1"
mark "$S/publish"
publish_text "$T1" "first"
publish_text "$T1" "second"
publish_file "$T1" "$BIN/max.bin" PUT
publish_file "$T1" "$BIN/over.bin"
publish_text "$T2" "other topic"
mark "$S/poll"
g "$BASE/$T1/json?poll=1"
g "$BASE/$T1/json?poll=1&since=all"
g "$BASE/$T1,$T2/json?poll=1&since=all"
g "$BASE/$T1/json?poll=1&since=latest"
ID=$(curl -s "$BASE/$T1/json?poll=1&since=all" | sed -n 2p | sed 's/.*"id":"\([A-Za-z0-9]*\)".*/\1/')
g "$BASE/$T1/json?poll=1&since=$ID"
g "$BASE/$T1/sse?poll=1&since=all"
g "$BASE/$T2/json?poll=1&since=none"
mark "$S/end"
rm -rf "$BIN"
