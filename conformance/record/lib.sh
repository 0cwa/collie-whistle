# Shared helpers for the recording drivers. Sourced, not run.
# Required env: BASE (recording proxy URL), RAW (JSONL file the proxy writes).
# Everything here talks to the proxy on $BASE, never to anything else.
mark() { curl -s "$BASE/_marker/$1" >/dev/null; }
# Newest topic seen in a stream request path, e.g. /upAbc/json or /upAbc,upDef/sse
last_topics() { grep -o '"path": "/[^"?]*/\(json\|ws\|sse\)' "$RAW" | tail -1 | sed 's#"path": "/##; s#/[a-z]*$##'; }
publish_text() { sleep 1.1; curl -s -X POST -A "curl-app-server" --data-binary "$2" "$BASE/$1?up=1" >/dev/null; }
publish_file() { sleep 1.1; curl -s -X "${3:-POST}" -A "curl-app-server" -H 'Content-Type: application/octet-stream' --data-binary "@$2" "$BASE/$1?up=1" >/dev/null; }
wait_for() { # wait_for <seconds> <grep pattern in $RAW after line $2>
  local n=$2 i; for i in $(seq "$1"); do
    tail -n +"$n" "$RAW" | grep -q -- "$3" && return 0; sleep 1; done; echo "timeout waiting for $3" >&2; return 1; }
rawlines() { wc -l < "$RAW"; }
