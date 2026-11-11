#!/bin/bash
# Drive the ntfy Android app (debug build, so run-as can set preferences) on a running emulator
# and make it perform UnifiedPush registrations against the recording proxy.
# usage: android.sh jsonhttp|ws      env: BASE RAW ADB PKG
set -eu
PROTO=${1:?jsonhttp or ws}
HERE=$(cd "$(dirname "$0")" && pwd); . "$HERE/lib.sh"
ADB=${ADB:-adb}; PKG=${PKG:-io.heckel.ntfy.debug}
S=android-$PROTO
BIN=$(mktemp -d)   # random payloads, made fresh; the fixtures keep the bytes that were sent
head -c 20 /dev/urandom > "$BIN/small.bin"; printf '\x00\xff\xfe' >> "$BIN/small.bin"
head -c 4096 /dev/urandom > "$BIN/max.bin"; head -c 4097 /dev/urandom > "$BIN/over.bin"

$ADB shell pm clear "$PKG" >/dev/null
$ADB shell pm grant "$PKG" android.permission.POST_NOTIFICATIONS
$ADB shell dumpsys deviceidle whitelist +"$PKG" >/dev/null
cat > "$BIN/prefs.xml" <<XML
<?xml version='1.0' encoding='utf-8' standalone='yes' ?>
<map>
    <int name="DeleteWorkerVersion" value="63" />
    <int name="PollWorkerVersion" value="63" />
    <int name="AutoRestartWorkerVersion" value="63" />
    <string name="DefaultBaseURL">${APP_BASE:-http://10.0.2.2:12587}</string>
    <string name="ConnectionProtocol">$PROTO</string>
</map>
XML
$ADB push "$BIN/prefs.xml" /data/local/tmp/prefs.xml >/dev/null
$ADB shell "run-as $PKG sh -c 'mkdir -p shared_prefs && cp /data/local/tmp/prefs.xml shared_prefs/MainPreferences.xml'"

reg() { $ADB shell am broadcast -a org.unifiedpush.android.distributor.REGISTER -n "$PKG/io.heckel.ntfy.up.BroadcastReceiver" --es application "$1" --es token "$2" >/dev/null; }
unreg() { $ADB shell am broadcast -a org.unifiedpush.android.distributor.UNREGISTER -n "$PKG/io.heckel.ntfy.up.BroadcastReceiver" --es token "$1" >/dev/null; }

mark "$S/begin"
# the service may only start in the foreground on API 35, so open the app first
$ADB shell monkey -p "$PKG" -c android.intent.category.LAUNCHER 1 >/dev/null 2>&1; sleep 5
N=$(rawlines); reg org.example.upapp1 tok-0001
wait_for 40 "$N" '/\(json\|ws\)'; sleep 3
T1=$(last_topics); echo "topic1=$T1"

mark "$S/publish"
publish_text "$T1" "hello from an app server"
publish_text "$T1" "unicode: caf$(printf '\xc3\xa9') $(printf '\xe2\x9c\x93')"
publish_file "$T1" "$BIN/small.bin"
publish_file "$T1" "$BIN/over.bin"
publish_file "$T1" "$BIN/max.bin" PUT
sleep 3

mark "$S/register-second"
N=$(rawlines); reg org.example.upapp2 tok-0002
wait_for 40 "$N" "$T1,\|,$T1"; sleep 3
T2=$(last_topics | sed "s/$T1//; s/,//"); echo "topic2=$T2"
publish_text "$T2" "to the second topic"
publish_text "$T1" "to the first topic again"
sleep 3

mark "$S/disconnect"
$ADB shell cmd connectivity airplane-mode enable; sleep 6
publish_text "$T1" "sent while the client was offline"
publish_text "$T2" "also sent while offline"
sleep 2
mark "$S/reconnect"
N=$(rawlines); $ADB shell cmd connectivity airplane-mode disable
wait_for 120 "$N" 'since=[A-Za-z0-9]'; sleep 5

mark "$S/unregister"
N=$(rawlines); unreg tok-0002; wait_for 30 "$N" '/\(json\|ws\)' || true; sleep 3
# the stream that survives is idle long enough to see a keepalive (ntfy sends one every 45 s)
mark "$S/idle"
sleep "${IDLE:-55}"
mark "$S/end"
rm -rf "$BIN"
