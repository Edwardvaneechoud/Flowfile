"""The host folders a kernel's container may read, and where each appears inside it.

A POSIX host folder appears at its own path; a Windows one at ``/host/<drive>/<rest>``. Pure
functions over ``os.environ`` and the filesystem, so they run without Docker.
"""

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath, PureWindowsPath

from flowfile_core.kernel.notebook_support import is_notebook_kernel_config

WINDOWS_HOST_ROOT = "/host"

_RESERVED_TREES = ("/app", "/bin", "/boot", "/dev", "/etc", "/lib", "/lib64", "/proc", "/sbin", "/sys", "/usr")
_RESERVED_EXACT = ("/", "/home", "/host", "/mnt", "/opt", "/root", "/run", "/srv", "/tmp", "/var")
_KERNEL_OWN_MOUNTS = ("/shared", "/catalog_tables")


def is_electron_mode() -> bool:
    return os.environ.get("FLOWFILE_MODE", "electron") == "electron"


def _windows_drive_path(path: str) -> PureWindowsPath | None:
    win = PureWindowsPath(path)
    drive = win.drive
    if len(drive) == 2 and drive[1] == ":" and drive[0].isalpha() and win.root:
        return win
    return None


def kernel_side(host_path: str) -> str:
    """Where ``host_path`` appears in the kernel: the same path on POSIX, ``/host/c/...`` for ``C:\\...``."""
    win = _windows_drive_path(host_path)
    if win is not None:
        return str(PurePosixPath(WINDOWS_HOST_ROOT, win.drive[0].lower(), *win.parts[1:]))
    return str(PurePosixPath(host_path))


def _normalized(host_path: str) -> str:
    win = _windows_drive_path(host_path)
    return str(win) if win is not None else str(PurePosixPath(host_path))


def _is_reserved(kernel_path: str) -> bool:
    if kernel_path in _RESERVED_EXACT:
        return True
    return any(
        kernel_path == root or kernel_path.startswith(root + "/") for root in _RESERVED_TREES + _KERNEL_OWN_MOUNTS
    )


def validate_mounted_folders(folders: list[str]) -> list[str]:
    """Normalise the folders a kernel may read; ``ValueError`` names the first bad entry.

    Folders are refused outright outside electron mode. Each must be an absolute, existing
    directory that does not shadow the kernel's own system folders.
    """
    if not folders:
        return []
    if not is_electron_mode():
        raise ValueError("Kernel folders can only be mounted in the desktop app (FLOWFILE_MODE=electron)")
    out: list[str] = []
    for folder in folders:
        if not isinstance(folder, str) or not folder.strip():
            raise ValueError("A kernel folder must be a non-empty path")
        path = Path(folder)
        if not path.is_absolute():
            raise ValueError(f"Kernel folder must be an absolute path: {folder}")
        if not path.is_dir():
            raise ValueError(f"Kernel folder does not exist or is not a directory: {folder}")
        normalized = _normalized(folder)
        if _is_reserved(kernel_side(normalized)):
            raise ValueError(f"Kernel folder would hide the kernel's own files: {folder}")
        if normalized not in out:
            out.append(normalized)
    return out


def _flowfile_folders() -> list[str]:
    from flowfile_core.flowfile.user_defined.mounts import load_mounts
    from shared.storage_config import storage

    folders = [
        storage.flows_directory,
        storage.user_defined_nodes_directory,
        storage.catalog_tables_directory,
    ]
    return [str(f) for f in folders] + [m.path for m in load_mounts()]


def build_mount_table(kernel) -> dict[str, str]:
    """Host folder -> kernel folder for ``kernel`` (a ``KernelConfig``/``KernelInfo``), electron only.

    A notebook kernel gets the Flowfile folders (flows, custom nodes and their mounts, catalog
    tables; the database is a copy in its shared folder, see ``notebook_db``); every kernel gets its
    ``mounted_folders``. Missing folders are skipped.
    """
    if not is_electron_mode():
        return {}
    hosts: list[str] = []
    if is_notebook_kernel_config(kernel):
        hosts.extend(_flowfile_folders())
    hosts.extend(getattr(kernel, "mounted_folders", None) or [])
    table: dict[str, str] = {}
    for host in hosts:
        normalized = _normalized(host)
        if normalized not in table and os.path.isdir(host):
            table[normalized] = kernel_side(normalized)
    return table


def _under(path: str, prefix: str, case_insensitive: bool) -> str | None:
    """The part of ``path`` below ``prefix`` ('' when equal), or None when it is not under it."""
    p = path.replace("\\", "/").rstrip("/")
    base = prefix.replace("\\", "/").rstrip("/")
    cmp_p, cmp_base = (p.casefold(), base.casefold()) if case_insensitive else (p, base)
    if cmp_p == cmp_base:
        return ""
    if cmp_p.startswith(cmp_base + "/"):
        return p[len(base) + 1 :]
    return None


def translate(path: str, table: dict[str, str]) -> str | None:
    """The kernel-side POSIX path of host ``path`` (longest host prefix wins), or None when not covered."""
    best: tuple[int, str] | None = None
    for host, kernel_path in table.items():
        rest = _under(path, host, case_insensitive=_windows_drive_path(host) is not None)
        if rest is None:
            continue
        if best is None or len(host) > best[0]:
            best = (len(host), f"{kernel_path}/{rest}" if rest else kernel_path)
    return best[1] if best else None


def key_store_dir() -> str:
    """Core's electron ``SecureStorage`` folder (master key, JWT secret), mirroring ``auth/secrets.py``."""
    override = os.environ.get("FLOWFILE_SECURE_STORAGE_PATH")
    if override:
        return override
    app_data = os.environ.get("APPDATA") or os.path.expanduser("~/.config")
    return str(Path(app_data) / "flowfile")


def masked_paths(table: dict[str, str]) -> list[str]:
    """Kernel-side paths to cover with an empty tmpfs: the key store when a mounted folder holds it."""
    masked = translate(key_store_dir(), table)
    return [masked] if masked else []
