#!/usr/bin/env python3
"""Assemble the Tauri updater manifest (`latest.json`) from the release build artifacts."""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_REPO = "Edwardvaneechoud/Flowfile"


def _asset_names(version: str) -> dict[str, str]:
    return {
        "darwin-aarch64": "Flowfile_aarch64.app.tar.gz",
        "darwin-x86_64": "Flowfile_x64.app.tar.gz",
        "windows-x86_64": f"Flowfile_{version}_x64-setup.exe",
        "linux-x86_64": f"Flowfile_{version}_amd64.deb",
    }


def _find_one(artifacts_dir: Path, name: str, key: str) -> Path:
    """Locate a single file by name anywhere under the artifacts dir."""
    matches = sorted(artifacts_dir.rglob(name))
    if len(matches) != 1:
        raise SystemExit(f"{key}: expected exactly one {name!r} under {artifacts_dir}, found {len(matches)}")
    return matches[0]


def build_manifest(version: str, artifacts_dir: Path, repo: str = DEFAULT_REPO) -> dict:
    """Build the manifest dict; the bundle and its signature are looked up independently."""
    platforms = {}
    for key, asset in _asset_names(version).items():
        _find_one(artifacts_dir, asset, key)
        signature_path = _find_one(artifacts_dir, f"{asset}.sig", key)
        signature = signature_path.read_text(encoding="utf-8").strip()
        if not signature:
            raise SystemExit(f"{key}: signature file {signature_path} is empty")
        platforms[key] = {
            "signature": signature,
            "url": f"https://github.com/{repo}/releases/download/v{version}/{asset}",
        }
    return {
        "version": version,
        "pub_date": datetime.now(timezone.utc).isoformat(),
        "platforms": platforms,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the Tauri updater manifest for a release.")
    parser.add_argument("--version", required=True, help="Release version without the leading 'v'.")
    parser.add_argument("--artifacts-dir", required=True, type=Path, help="Directory with the downloaded artifacts.")
    parser.add_argument("--out", required=True, type=Path, help="Where to write the manifest.")
    parser.add_argument("--repo", default=DEFAULT_REPO, help=f"GitHub owner/repo (default {DEFAULT_REPO}).")
    args = parser.parse_args()

    manifest = build_manifest(args.version, args.artifacts_dir, args.repo)
    args.out.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {args.out} for v{args.version} with {len(manifest['platforms'])} platforms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
