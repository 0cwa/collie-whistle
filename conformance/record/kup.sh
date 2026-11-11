#!/bin/bash
# Drive KUnifiedPush's distributor (KDE, ntfy provider) in a container with a private D-Bus session.
# usage: kup.sh      env: BASE RAW CT (container, default whistle-kup) NTFY_URL (what the distributor connects to)
set -eu
HERE=$(cd "$(dirname "$0")" && pwd); . "$HERE/lib.sh"
CT=${CT:-whistle-kup}; NTFY_URL=${NTFY_URL:-http://127.0.0.1:12587}
S=kup-sse
BIN=$(mktemp -d)
head -c 20 /dev/urandom > "$BIN/small.bin"; printf '\x00\xff\xfe' >> "$BIN/small.bin"
head -c 4096 /dev/urandom > "$BIN/max.bin"; head -c 4097 /dev/urandom > "$BIN/over.bin"
cx() { podman exec -i "$CT" bash -s; }
start_distributor() { cx <<'SH'
export HOME=/root QT_QPA_PLATFORM=offscreen QT_LOGGING_RULES="org.kde.kunifiedpush*=true" DBUS_SESSION_BUS_ADDRESS=$(cat /run/kup/addr)
nohup kunifiedpush-distributor >> /run/kup/distributor.log 2>&1 &
sleep 3
SH
}
dbus() { podman exec "$CT" env DBUS_SESSION_BUS_ADDRESS="$(podman exec "$CT" cat /run/kup/addr)" gdbus call --session -d org.unifiedpush.Distributor.kde -o /org/unifiedpush/Distributor "$@"; }
reg() { dbus -m org.unifiedpush.Distributor1.Register "$1" "$2" "conformance test $1"; }
unreg() { dbus -m org.unifiedpush.Distributor1.Unregister "$1"; }

# fresh state: new bus, empty config and settings, distributor pointed at ntfy
cx <<SH
export HOME=/root
pkill -f kunifiedpush-distributor || true; pkill -f connector.py || true; pkill dbus-daemon || true
rm -rf \$HOME/.config/KDE \$HOME/.config/*unifiedpush* \$HOME/.local/share/*nifiedpush*
mkdir -p \$HOME/.config/KDE /run/kup
printf '[PushProvider]\nType=Ntfy\n\n[Ntfy]\nUrl=$NTFY_URL\n' > \$HOME/.config/KDE/kunifiedpush-distributor.conf
for n in org.example.kupapp1 org.example.kupapp2; do
  printf '[D-BUS Service]\nName=%s\nExec=/usr/bin/true\n' \$n > /usr/share/dbus-1/services/\$n.service
done
dbus-daemon --session --fork --print-address > /run/kup/addr
SH
podman cp "$HERE/connector.py" "$CT":/run/kup/connector.py
cx <<'SH'
export DBUS_SESSION_BUS_ADDRESS=$(cat /run/kup/addr)
: > /run/kup/connector.log
nohup python3 /run/kup/connector.py org.example.kupapp1 org.example.kupapp2 > /run/kup/connector.out 2>&1 &
SH
mark "$S/begin"
start_distributor
N=$(rawlines); reg org.example.kupapp1 tok-kup-0001
wait_for 40 "$N" '/sse'; sleep 3
T1=$(last_topics); echo "topic1=$T1"

mark "$S/publish"
publish_text "$T1" "hello from an app server"
publish_text "$T1" "unicode: caf$(printf '\xc3\xa9') $(printf '\xe2\x9c\x93')"
publish_file "$T1" "$BIN/small.bin"
publish_file "$T1" "$BIN/over.bin"
publish_file "$T1" "$BIN/max.bin" PUT
sleep 3

mark "$S/register-second"
N=$(rawlines); reg org.example.kupapp2 tok-kup-0002
wait_for 40 "$N" "$T1,\|,$T1"; sleep 3
T2=$(last_topics | sed "s/$T1//; s/,//"); echo "topic2=$T2"
publish_text "$T2" "to the second topic"
publish_text "$T1" "to the first topic again"
sleep 3

mark "$S/disconnect"
cx <<'SH'
pkill -f kunifiedpush-distributor
SH
sleep 2
publish_text "$T1" "sent while the client was offline"
publish_text "$T2" "also sent while offline"
sleep 2
mark "$S/reconnect"
N=$(rawlines); start_distributor
wait_for 60 "$N" 'since=[A-Za-z0-9]'; sleep 5

mark "$S/unregister"
N=$(rawlines); unreg tok-kup-0002; wait_for 30 "$N" "/sse" || true; sleep 3
mark "$S/idle"
sleep "${IDLE:-55}"
mark "$S/end"
rm -rf "$BIN"
echo "--- what the connectors received over D-Bus"; cx <<<'cat /run/kup/connector.log'
if [ -n "${KEEP_LOG:-}" ]; then podman exec "$CT" cat /run/kup/distributor.log > "$KEEP_LOG"; fi
