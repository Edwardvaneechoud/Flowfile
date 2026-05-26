#!/usr/bin/env bash
# Deep-sign bundled PyInstaller sidecars so the Tauri .app passes Apple notarization.
# Tauri signs the app shell but not binaries shipped as resources; every nested
# Mach-O needs Developer ID + secure timestamp + hardened runtime.
#
# CI (actions/setup-python) ships a FRAMEWORK build of CPython, so PyInstaller bundles
# a Python.framework into _internal/. A framework must be signed as a bundle, not by
# pointing codesign at a framework-internal Mach-O — that makes codesign classify the
# enclosing bundle and fail with "bundle format is ambiguous (could be app or framework)".
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

# Defensive + idempotent. PyInstaller 6.x usually already produces a canonical
# framework (Versions/Current + top-level symlinks); only repair if missing.
normalize_framework() {
  local fw="$1" name
  name="$(basename "$fw")"; name="${name%.framework}"
  if [ ! -d "$fw/Versions" ]; then
    echo "  NOTE: $fw has no Versions/ (flattened); signing as-is" >&2
    return 0
  fi
  if [ ! -e "$fw/Versions/Current" ]; then
    local versions=() d
    for d in "$fw"/Versions/*; do
      if [ -d "$d" ] && [ ! -L "$d" ]; then versions+=("$(basename "$d")"); fi
    done
    if [ "${#versions[@]}" -eq 1 ]; then
      ( cd "$fw/Versions" && ln -snf "${versions[0]}" Current )
    fi
  fi
  if [ -e "$fw/Versions/Current/$name" ] && [ ! -e "$fw/$name" ]; then
    ( cd "$fw" && ln -snf "Versions/Current/$name" "$name" )
  fi
  if [ -d "$fw/Versions/Current/Resources" ] && [ ! -e "$fw/Resources" ]; then
    ( cd "$fw" && ln -snf "Versions/Current/Resources" Resources )
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
  case "$bundle" in *.framework) normalize_framework "$bundle" ;; esac
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
