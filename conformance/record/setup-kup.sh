#!/bin/bash
# Create the container that runs KUnifiedPush's own distributor (Fedora package, pinned by the image's repo state).
# usage: setup-kup.sh [container-name]   (needs podman; uses host networking so 127.0.0.1 is shared)
set -eu
CT=${1:-whistle-kup}
podman run -d --name "$CT" --memory 8g --cpus 8 --network host ghcr.io/ublue-os/fedora-toolbox:latest sleep infinity >/dev/null
podman exec "$CT" dnf install -y kunifiedpush dbus-daemon python3-gobject-base >/dev/null
podman exec "$CT" rpm -q kunifiedpush
