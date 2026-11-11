#!/bin/bash
# usage: normalize-all.sh RAW.jsonl [OUTDIR]  - one fixture per recorded scenario, with its client label
set -eu
RAW=${1:?raw jsonl}; OUT=${2:-$(dirname "$0")/../fixtures}; N=$(dirname "$0")/normalize.py
python3 -I "$N" "$RAW" "$OUT" --only app-server --source reference \
  --client "curl, hand-written app-server and poll calls, answered by ntfy 2.37.2"
python3 -I "$N" "$RAW" "$OUT" --only android-jsonhttp \
  --client "ntfy Android 1.25.2 (fdroid debug build), connection protocol jsonhttp, against ntfy 2.37.2"
python3 -I "$N" "$RAW" "$OUT" --only android-ws \
  --client "ntfy Android 1.25.2 (fdroid debug build), connection protocol ws, against ntfy 2.37.2"
python3 -I "$N" "$RAW" "$OUT" --only kup-sse \
  --client "KUnifiedPush 26.08.2 kunifiedpush-distributor (Fedora 44 package), ntfy provider, against ntfy 2.37.2"
