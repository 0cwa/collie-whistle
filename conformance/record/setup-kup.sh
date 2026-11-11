#!/bin/bash
# Create the container that runs KUnifiedPush's own distributor (Fedora 44 package kunifiedpush-26.08.2-1.fc44, image pinned by digest).
# usage: setup-kup.sh [container-name]   (needs podman; uses host networking so 127.0.0.1 is shared)
set -eu
CT=${1:-whistle-kup}
podman run -d --name "$CT" --memory 8g --cpus 8 --network host ghcr.io/ublue-os/fedora-toolbox@sha256:152d578a893dc0d96239f92b50e46d3d238c0b4aebca89094ee118f3a407af77 sleep infinity >/dev/null
podman exec "$CT" dnf install -y kunifiedpush-26.08.2-1.fc44 dbus-daemon python3-gobject-base >/dev/null
podman exec "$CT" rpm -q kunifiedpush
