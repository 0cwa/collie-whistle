#!/bin/sh
# Shows the suite notices a wrong server: runs it against selftest/naive_server.py and
# succeeds only if the suite FAILS. usage: conformance/selftest.sh [port]
set -u
cd "$(dirname "$0")"
PORT=${1:-12599}
python3 -I selftest/naive_server.py "$PORT" & pid=$!
trap 'kill $pid 2>/dev/null' EXIT
sleep 1
if WHISTLE_FAST=1 WHISTLE_TIMEOUT=3 ./run.sh "http://127.0.0.1:$PORT" >/dev/null 2>&1; then
  echo "selftest FAILED: the suite passed against a deliberately wrong server" >&2; exit 1
fi
echo "selftest ok: the suite rejects the naive server"
