"""Stage PyInstaller onedir outputs into Tauri's sidecar convention.

Tauri's bundler resolves `binaries/<name>` to `binaries/<name>-<target-triple>`
at build time. This script:

1. Detects the host triple via `rustc -vV`.
2. Copies each PyInstaller executable from `services_dist/<name>` into
   `flowfile_frontend/src-tauri/binaries/<name>-<triple>`.
3. Copies the shared `_internal/` directory next to those binaries.

PyInstaller onedir bundles look for `_internal/` adjacent to the executable
at runtime. Tauri's sidecar mechanism copies the executable into `target/`
during dev — and our `src-tauri/build.rs` symlinks `binaries/_internal/`
into that same `target/` directory so the runtime lookup succeeds.

Run after `make build_python_services`. Idempotent.
"""

from __future__ import annotations

import argparse
import platform
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SERVICES_DIST = REPO_ROOT / "services_dist"
SIDECAR_DIR = REPO_ROOT / "flowfile_frontend" / "src-tauri" / "binaries"
BINARIES = ("flowfile_core", "flowfile_worker")


def detect_host_triple() -> str:
    """Read `rustc -vV` and return its `host:` line."""
    try:
        out = subprocess.check_output(["rustc", "-vV"], text=True)
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        raise RuntimeError(
            "`rustc` is required to detect the target triple. Install Rust via https://rustup.rs"
        ) from exc

    for line in out.splitlines():
        if line.startswith("host: "):
            return line.split(":", 1)[1].strip()
    raise RuntimeError("`rustc -vV` did not report a host triple")


def exe_suffix() -> str:
    return ".exe" if platform.system() == "Windows" else ""


def stage(binary: str, triple: str) -> None:
    src = SERVICES_DIST / f"{binary}{exe_suffix()}"
    if not src.exists():
        raise FileNotFoundError(
            f"{src} not found. Run `make build_python_services` first."
        )

    dst = SIDECAR_DIR / f"{binary}-{triple}{exe_suffix()}"
    dst.parent.mkdir(parents=True, exist_ok=True)

    if dst.exists() or dst.is_symlink():
        dst.unlink()
    shutil.copy2(src, dst)
    dst.chmod(0o755)
    print(f"  {src.relative_to(REPO_ROOT)} -> {dst.relative_to(REPO_ROOT)}")


def stage_shared_internal() -> None:
    src = SERVICES_DIST / "_internal"
    if not src.exists():
        raise FileNotFoundError(
            f"{src} not found. Did PyInstaller produce a merged _internal/ "
            f"directory? Check that build_backends/main.py is still configured "
            f"for --onedir + combine_packages()."
        )

    dst = SIDECAR_DIR / "_internal"
    if dst.exists() or dst.is_symlink():
        if dst.is_symlink() or dst.is_file():
            dst.unlink()
        else:
            shutil.rmtree(dst)
    shutil.copytree(src, dst, symlinks=True)
    print(f"  {src.relative_to(REPO_ROOT)} -> {dst.relative_to(REPO_ROOT)}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--triple",
        default=None,
        help="Override the target triple (defaults to host triple from rustc).",
    )
    args = parser.parse_args()

    triple = args.triple or detect_host_triple()
    print(f"Target triple: {triple}")
    print(f"Staging binaries into {SIDECAR_DIR.relative_to(REPO_ROOT)}")

    if not SERVICES_DIST.exists():
        print(
            f"error: {SERVICES_DIST} does not exist. Run `make build_python_services` first.",
            file=sys.stderr,
        )
        return 1

    SIDECAR_DIR.mkdir(parents=True, exist_ok=True)

    for binary in BINARIES:
        stage(binary, triple)
    stage_shared_internal()

    print("Done. Re-run `cargo build` (or `npm run dev`) so build.rs links")
    print("`_internal/` into target/<profile>/ where Tauri places the sidecar.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
