#!/usr/bin/env bash
# Deep-sign bundled PyInstaller sidecars so the Tauri .app passes Apple notarization.
# Tauri signs the app shell but not binaries shipped as resources; every nested
# Mach-O needs Developer ID + secure timestamp + hardened runtime.
#
# CI (actions/setup-python) ships a FRAMEWORK build of CPython, so PyInstaller bundles
# a Python.framework into _internal/. PyInstaller stages it as plain real-file copies
# (top-level Python binary + a real Versions/Current dir, no symlinks), which codesign
# cannot classify -> "bundle format is ambiguous (could be app or framework)". We
# rebuild the canonical symlink layout, then sign the binary and the bundle.
#
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

loose_count=0
bundle_count=0
fail_count=0
failed_paths=()

is_macho() { file -b "$1" 2>/dev/null | grep -q "Mach-O"; }

sign_one() {
  local target="$1" rc=0
  set +e
  codesign --force --timestamp --options runtime \
    --entitlements "$ENTITLEMENTS" --sign "$APPLE_SIGNING_IDENTITY" "$target"
  rc=$?
  set -e
  if [ "$rc" -ne 0 ]; then
    echo "  FAILED ($rc): $target" >&2
    failed_paths+=("$target")
    fail_count=$((fail_count + 1))
    return 1
  fi
  return 0
}

# Rebuild a framework into the canonical symlink layout codesign requires:
#   <fw>/<Name>            -> Versions/Current/<Name>
#   <fw>/Resources         -> Versions/Current/Resources
#   <fw>/Versions/Current  -> <ver>
#   <fw>/Versions/<ver>/{<Name>,Resources/Info.plist}   (real)
# PyInstaller stages these as real copies; we replace the duplicates with symlinks
# so the bundle is unambiguously a framework. Idempotent (no-op once canonical).
normalize_framework() {
  local fw="$1" name ver d
  name="$(basename "$fw")"; name="${name%.framework}"

  if [ ! -d "$fw/Versions" ]; then
    echo "  NOTE: $fw has no Versions/ (flattened); signing as-is" >&2
    return 0
  fi

  # Real version dir (e.g. 3.11): a non-symlink dir under Versions/ other than Current.
  ver=""
  for d in "$fw"/Versions/*; do
    [ -d "$d" ] && [ ! -L "$d" ] || continue
    [ "$(basename "$d")" = "Current" ] && continue
    ver="$(basename "$d")"
    break
  done
  if [ -z "$ver" ]; then
    echo "  WARN: $fw has no real version dir under Versions/; signing as-is" >&2
    return 0
  fi

  # Versions/Current -> <ver> (replace a real dir; the binary lives in <ver>).
  if [ ! -L "$fw/Versions/Current" ] && [ -e "$fw/Versions/$ver/$name" ]; then
    rm -rf "$fw/Versions/Current"
    ( cd "$fw/Versions" && ln -s "$ver" Current )
    echo "  normalized: Versions/Current -> $ver"
  fi

  # Top-level <Name> -> Versions/Current/<Name> (replace the duplicate real binary
  # that triggers "bundle format is ambiguous").
  if [ ! -L "$fw/$name" ] && [ -e "$fw/Versions/Current/$name" ]; then
    rm -f "$fw/$name"
    ( cd "$fw" && ln -s "Versions/Current/$name" "$name" )
    echo "  normalized: $name -> Versions/Current/$name"
  fi

  # Top-level Resources -> Versions/Current/Resources.
  if [ ! -L "$fw/Resources" ] && [ -d "$fw/Versions/Current/Resources" ]; then
    rm -rf "$fw/Resources"
    ( cd "$fw" && ln -s "Versions/Current/Resources" Resources )
    echo "  normalized: Resources -> Versions/Current/Resources"
  fi

  # A framework needs Versions/Current/Resources/Info.plist (CFBundlePackageType=FMWK)
  # for codesign to classify it. Synthesize a minimal one only if missing.
  if [ ! -f "$fw/Versions/Current/Resources/Info.plist" ]; then
    mkdir -p "$fw/Versions/Current/Resources"
    cat > "$fw/Versions/Current/Resources/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleExecutable</key><string>$name</string>
  <key>CFBundleIdentifier</key><string>org.python.$name</string>
  <key>CFBundleName</key><string>$name</string>
  <key>CFBundlePackageType</key><string>FMWK</string>
  <key>CFBundleVersion</key><string>$ver</string>
  <key>CFBundleShortVersionString</key><string>$ver</string>
</dict>
</plist>
PLIST
    echo "  normalized: wrote minimal Versions/Current/Resources/Info.plist"
  fi
  return 0
}

# Phase 1 — loose Mach-Os NOT inside any bundle.
while IFS= read -r -d '' f; do
  case "$f" in *.framework/*|*.app/*) continue ;; esac
  if is_macho "$f"; then
    if sign_one "$f"; then loose_count=$((loose_count + 1)); fi
  fi
done < <(find "$BIN_DIR" -type f ! -type l -print0)

# Phase 2 — frameworks/apps as bundles, deepest-first.
while IFS= read -r -d '' bundle; do
  echo "Signing bundle: ${bundle#"$BIN_DIR"/}"
  if [ "${bundle%.framework}" != "$bundle" ]; then
    echo "  [diag] raw layout (pre-normalize):"
    ls -la "$bundle" 2>/dev/null | sed 's/^/    /' || true
    ls -la "$bundle/Versions" 2>/dev/null | sed 's/^/    /' || true
    normalize_framework "$bundle"
  fi
  while IFS= read -r -d '' inner; do
    if is_macho "$inner"; then sign_one "$inner" || true; fi
  done < <(find "$bundle" -type f ! -type l -print0)
  if sign_one "$bundle"; then bundle_count=$((bundle_count + 1)); fi
done < <(find "$BIN_DIR" \( -name '*.framework' -o -name '*.app' \) -type d -depth -print0)

echo "sign_macos_sidecars: signed $loose_count loose binaries and $bundle_count bundles."

if [ "$fail_count" -ne 0 ]; then
  echo "sign_macos_sidecars: $fail_count signing failure(s):" >&2
  printf '  - %s\n' "${failed_paths[@]}" >&2
  exit 1
fi
