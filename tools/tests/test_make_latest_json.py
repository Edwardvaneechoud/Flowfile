"""
Tests for the Tauri updater manifest generator.
"""

import pytest

from tools.make_latest_json import build_manifest

VERSION = "1.2.3"

# target -> (bundle subdirectory, asset file name)
ASSETS = {
    "aarch64-apple-darwin": ("macos", "Flowfile_aarch64.app.tar.gz"),
    "x86_64-apple-darwin": ("macos", "Flowfile_x64.app.tar.gz"),
    "x86_64-pc-windows-msvc": ("nsis", f"Flowfile_{VERSION}_x64-setup.exe"),
    "x86_64-unknown-linux-gnu": ("deb", f"Flowfile_{VERSION}_amd64.deb"),
}


@pytest.fixture
def artifacts_dir(tmp_path):
    """Mirror CI: bundles and signatures download into disjoint directories."""
    root = tmp_path / "artifacts"
    for target, (subdir, asset) in ASSETS.items():
        bundle_dir = root / f"bundle-{target}" / subdir
        bundle_dir.mkdir(parents=True)
        (bundle_dir / asset).write_bytes(b"bundle")

        signature_dir = root / f"signature-{target}" / subdir
        signature_dir.mkdir(parents=True)
        (signature_dir / f"{asset}.sig").write_text(f"signature-for-{asset}\n", encoding="utf-8")
    return root


def test_manifest_covers_every_platform(artifacts_dir):
    """All four platform keys get a url and the stripped signature contents."""
    manifest = build_manifest(VERSION, artifacts_dir)

    assert manifest["version"] == VERSION
    assert manifest["pub_date"].endswith("+00:00")
    assert set(manifest["platforms"]) == {
        "darwin-aarch64",
        "darwin-x86_64",
        "windows-x86_64",
        "linux-x86_64",
    }

    base = f"https://github.com/Edwardvaneechoud/Flowfile/releases/download/v{VERSION}"
    assert manifest["platforms"]["darwin-aarch64"] == {
        "signature": "signature-for-Flowfile_aarch64.app.tar.gz",
        "url": f"{base}/Flowfile_aarch64.app.tar.gz",
    }
    assert manifest["platforms"]["darwin-x86_64"]["url"] == f"{base}/Flowfile_x64.app.tar.gz"
    assert manifest["platforms"]["windows-x86_64"]["url"] == f"{base}/Flowfile_{VERSION}_x64-setup.exe"
    assert manifest["platforms"]["linux-x86_64"]["url"] == f"{base}/Flowfile_{VERSION}_amd64.deb"
    assert manifest["platforms"]["linux-x86_64"]["signature"] == f"signature-for-Flowfile_{VERSION}_amd64.deb"


def test_missing_asset_fails(artifacts_dir):
    """A bundle that never got built stops the release, naming the platform key."""
    (artifacts_dir / "bundle-x86_64-unknown-linux-gnu" / "deb" / f"Flowfile_{VERSION}_amd64.deb").unlink()

    with pytest.raises(SystemExit, match="linux-x86_64"):
        build_manifest(VERSION, artifacts_dir)


def test_empty_signature_fails(artifacts_dir):
    """An empty .sig would silently produce an unverifiable update."""
    signature = artifacts_dir / "signature-x86_64-pc-windows-msvc" / "nsis" / f"Flowfile_{VERSION}_x64-setup.exe.sig"
    signature.write_text("   \n", encoding="utf-8")

    with pytest.raises(SystemExit, match="windows-x86_64"):
        build_manifest(VERSION, artifacts_dir)


def test_duplicate_match_fails(artifacts_dir):
    """Two candidates for one asset are ambiguous, not a pick-the-first situation."""
    stray = artifacts_dir / "bundle-aarch64-apple-darwin" / "dmg"
    stray.mkdir(parents=True)
    (stray / "Flowfile_aarch64.app.tar.gz").write_bytes(b"bundle")

    with pytest.raises(SystemExit, match="darwin-aarch64"):
        build_manifest(VERSION, artifacts_dir)
