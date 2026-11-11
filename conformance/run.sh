#!/bin/sh
# usage: conformance/run.sh BASE_URL   (extra args go to unittest, for example -k kup)
# Fails if no tests ran: a run that collected nothing is not a pass.
set -eu
[ $# -ge 1 ] || { echo "usage: $0 BASE_URL [unittest args]" >&2; exit 2; }
URL=$1; shift
cd "$(dirname "$0")"
out=$(WHISTLE_URL=$URL python3 -I -m unittest discover -s . -t . -v "$@" 2>&1) && rc=0 || rc=$?
echo "$out"
echo "$out" | grep -Eq '^Ran [1-9][0-9]* test' || { echo "no tests ran" >&2; exit 1; }
exit $rc
