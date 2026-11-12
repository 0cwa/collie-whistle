#!/bin/sh
# Builds receiver.apk with the plain SDK tools (no Gradle). usage: build.sh OUT_DIR
# env: ANDROID_SDK_ROOT (default ~/Android/Sdk), BUILD_TOOLS (default 36.0.0), PLATFORM (default android-34)
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${1:?output directory}
SDK=${ANDROID_SDK_ROOT:-$HOME/Android/Sdk}; BT=$SDK/build-tools/${BUILD_TOOLS:-36.0.0}
JAR=$SDK/platforms/${PLATFORM:-android-34}/android.jar
mkdir -p "$OUT/classes"
javac -source 8 -target 8 -cp "$JAR" -d "$OUT/classes" "$HERE"/src/dev/whistle/receiver/*.java 2>&1 | grep -v '^warning\|^Note\|^[0-9] warning' || true
"$BT/d8" --min-api 26 --lib "$JAR" --output "$OUT" "$OUT"/classes/dev/whistle/receiver/*.class
"$BT/aapt2" link -I "$JAR" --manifest "$HERE/AndroidManifest.xml" --min-sdk-version 26 --target-sdk-version 34 -o "$OUT/unsigned.apk"
(cd "$OUT" && zip -q -j unsigned.apk classes.dex)
"$BT/zipalign" -f 4 "$OUT/unsigned.apk" "$OUT/aligned.apk"
[ -f "$OUT/debug.keystore" ] || keytool -genkeypair -keystore "$OUT/debug.keystore" -storepass android -keypass android \
    -alias debug -keyalg RSA -keysize 2048 -validity 3650 -dname "CN=whistle test" >/dev/null 2>&1
"$BT/apksigner" sign --ks "$OUT/debug.keystore" --ks-pass pass:android --key-pass pass:android --out "$OUT/receiver.apk" "$OUT/aligned.apk"
echo "$OUT/receiver.apk"
