#!/bin/bash
# Shows the stock ntfy Android app (debug build) relaying binary UnifiedPush payloads from a
# Whistle server to a test connector, intact, including after the app lost its connection.
# usage: proof.sh jsonhttp|ws
# env: ADB, NTFY_APK, RECEIVER_APK, DEVICE_URL (server as the emulator sees it, e.g. http://10.0.2.2:8080),
#      PUBLISH_URL (the same server from this machine), PKG (default io.heckel.ntfy.debug)
set -eu
PROTO=${1:?jsonhttp or ws}
ADB=${ADB:-adb}; PKG=${PKG:-io.heckel.ntfy.debug}; RX=dev.whistle.receiver
DEVICE_URL=${DEVICE_URL:?}; PUBLISH_URL=${PUBLISH_URL:?}
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT

rx_log() { $ADB logcat -d -s WHISTLE_RX:I | tr -d '\r'; }
wait_log() { # wait_log SECONDS PATTERN
  for _ in $(seq "$1"); do rx_log | grep -q -- "$2" && return 0; sleep 1; done
  echo "FAIL: timed out waiting for $2" >&2; rx_log >&2; exit 1; }
publish() { # publish ENDPOINT -> sha256 of the 4096 random bytes sent
  head -c 4096 /dev/urandom > "$TMP/p.bin"
  curl -s -o /dev/null -w '%{http_code}\n' -X POST -H 'Content-Type: application/octet-stream' --data-binary @"$TMP/p.bin" "$PUBLISH_URL/$1?up=1" | grep -q 200
  sha256sum "$TMP/p.bin" | cut -d' ' -f1; }

$ADB shell cmd connectivity airplane-mode disable >/dev/null 2>&1 || true
$ADB install -r "$RECEIVER_APK" >/dev/null; [ -z "${NTFY_APK:-}" ] || $ADB install -r "$NTFY_APK" >/dev/null
$ADB shell pm clear "$PKG" >/dev/null
$ADB shell pm grant "$PKG" android.permission.POST_NOTIFICATIONS
$ADB shell dumpsys deviceidle whitelist +"$PKG" >/dev/null
cat > "$TMP/prefs.xml" <<XML
<?xml version='1.0' encoding='utf-8' standalone='yes' ?>
<map>
    <int name="DeleteWorkerVersion" value="63" />
    <int name="PollWorkerVersion" value="63" />
    <int name="AutoRestartWorkerVersion" value="63" />
    <string name="DefaultBaseURL">$DEVICE_URL</string>
    <string name="ConnectionProtocol">$PROTO</string>
</map>
XML
$ADB push "$TMP/prefs.xml" /data/local/tmp/prefs.xml >/dev/null
$ADB shell "run-as $PKG sh -c 'mkdir -p shared_prefs && cp /data/local/tmp/prefs.xml shared_prefs/MainPreferences.xml'"
$ADB shell monkey -p "$RX" -c android.intent.category.LAUNCHER 1 >/dev/null 2>&1   # un-stop the connector
$ADB shell monkey -p "$PKG" -c android.intent.category.LAUNCHER 1 >/dev/null 2>&1; sleep 5
$ADB logcat -c

echo "== $PROTO: register"
$ADB shell am broadcast -a org.unifiedpush.android.distributor.REGISTER -n "$PKG/io.heckel.ntfy.up.BroadcastReceiver" \
    --es application "$RX" --es token proof-token >/dev/null
wait_log 40 'ENDPOINT '
ENDPOINT=$(rx_log | grep -o 'ENDPOINT .*' | tail -1 | cut -d' ' -f2)
echo "endpoint $ENDPOINT"
TOPIC=$(echo "$ENDPOINT" | sed 's#.*/\([^/?]*\)?up=1#\1#')
sleep 3

echo "== $PROTO: 4096-byte binary payload while connected"
SHA1=$(publish "$TOPIC")
wait_log 30 "sha256=$SHA1"
rx_log | grep "sha256=$SHA1"

echo "== $PROTO: drop the connection, publish while offline, reconnect"
$ADB shell cmd connectivity airplane-mode enable; sleep 8
SHA2=$(publish "$TOPIC")
sleep 2
rx_log | grep -q "sha256=$SHA2" && { echo "FAIL: delivered while offline?" >&2; exit 1; }
$ADB shell cmd connectivity airplane-mode disable
wait_log 120 "sha256=$SHA2"
rx_log | grep "sha256=$SHA2"
n=$(rx_log | grep -c "sha256=$SHA2" || true)
[ "$n" = 1 ] || { echo "FAIL: payload delivered $n times" >&2; exit 1; }
echo "PASS $PROTO: sent $SHA1 and $SHA2, both received intact (len=4096) and the second after reconnect"
