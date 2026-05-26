#!/usr/bin/env bash
# Deep-sign bundled PyInstaller sidecars so the Tauri .app passes Apple notarization.
# Tauri signs the app shell but not binaries shipped as resources; every nested
# Mach-O needs Developer ID + secure timestamp + hardened runtime.
# No-op unless on macOS with APPLE_SIGNING_IDENTITY set.
set -euo pipefail

if [ "$(uname)" != "Darwin" ] || [ -z "${APPLE_SIGNING_IDENTITY:-}" ]; then
  echo "sign_macos_sidecars: skipping (not macOS or APPLE_SIGNING_IDENTITY unset)"
  exit 0
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="$REPO_ROOT/flowfile_frontend/src-tauri/binaries"
ENTITLEMENTS="$REPO_ROOT/flowfile_frontend/src-tauri/entitlements.mac.plist"

if [ ! -d "$BIN_DIR" ]; then
  echo "sign_macos_sidecars: $BIN_DIR not found — run rename_sidecar first" >&2
  exit 1
fi

echo "Signing sidecar binaries in $BIN_DIR with '$APPLE_SIGNING_IDENTITY'..."
count=0
while IFS= read -r -d '' f; do
  if file "$f" | grep -q "Mach-O"; then
    codesign --force --timestamp --options runtime \
      --entitlements "$ENTITLEMENTS" --sign "$APPLE_SIGNING_IDENTITY" "$f"
    count=$((count + 1))
  fi
done < <(find "$BIN_DIR" -type f -print0)
echo "sign_macos_sidecars: signed $count binaries."
