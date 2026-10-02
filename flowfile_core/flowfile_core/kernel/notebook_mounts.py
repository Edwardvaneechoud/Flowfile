"""The host folders a kernel's container may read (and write, when marked), and where each appears inside it.

A POSIX host folder appears at its own path; a Windows one at ``/host/<drive>/<rest>``. Pure
functions over ``os.environ`` and the filesystem, so they run without Docker.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path, PurePosixPath, PureWindowsPath

from flowfile_core.auth import sharing
from flowfile_core.kernel.models import MountedFolder
from flowfile_core.kernel.notebook_support import is_notebook_kernel_config

logger = logging.getLogger(__name__)

WINDOWS_HOST_ROOT = "/host"

_RESERVED_TREES = ("/app", "/bin", "/boot", "/dev", "/etc", "/lib", "/lib64", "/proc", "/sbin", "/sys", "/usr")
_RESERVED_EXACT = ("/", "/home", "/host", "/mnt", "/opt", "/root", "/run", "/srv", "/tmp", "/var")
_KERNEL_OWN_MOUNTS = ("/shared", "/catalog_tables")


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


def folder_path(entry: str | MountedFolder) -> str:
    """The host path of a ``mounted_folders`` entry."""
    return entry.path if isinstance(entry, MountedFolder) else entry


def is_writable(entry: str | MountedFolder) -> bool:
    return isinstance(entry, MountedFolder) and entry.writable


def _held_storage(normalized: str) -> str | None:
    """Flowfile's storage folder when ``normalized`` is or holds it, else None.

    A notebook kernel writes into that folder (its ``FLOWFILE_STORAGE_DIR`` is the kernel side
    of it), so a read-only mount over it breaks the kernel.
    """
    from shared.storage_config import storage

    base = str(storage.base_directory)
    held = _under(base, normalized, case_insensitive=_windows_drive_path(normalized) is not None)
    return base if held is not None else None


def validate_mounted_folders(folders: list[str | MountedFolder]) -> list[str | MountedFolder]:
    """Normalise the folders a kernel may use; ``ValueError`` names the first bad entry.

    Folders are refused outright outside electron mode. Each must be an absolute, existing
    directory that neither shadows the kernel's own system folders nor holds Flowfile's storage
    folder. A read-only folder comes back as its path, a writable one as a ``MountedFolder``;
    a repeated path keeps its first entry.
    """
    if not folders:
        return []
    if sharing.sharing_enabled():
        raise ValueError("Kernel folders can only be mounted in the desktop app (FLOWFILE_MODE=electron)")
    out: list[str | MountedFolder] = []
    seen: set[str] = set()
    for entry in folders:
        folder = folder_path(entry)
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
        storage_dir = _held_storage(normalized)
        if storage_dir is not None:
            raise ValueError(
                f"Kernel folder holds Flowfile's own folder ({storage_dir}); "
                f"add a narrower folder, such as one for your data: {folder}"
            )
        if normalized not in seen:
            seen.add(normalized)
            out.append(MountedFolder(path=normalized, writable=True) if is_writable(entry) else normalized)
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
    ``mounted_folders``. Missing folders are skipped, and so is a folder saved before
    ``validate_mounted_folders`` refused one that holds Flowfile's storage folder.
    """
    if sharing.sharing_enabled():
        return {}
    hosts: list[str] = []
    if is_notebook_kernel_config(kernel):
        hosts.extend(_flowfile_folders())
    hosts.extend(folder_path(f) for f in getattr(kernel, "mounted_folders", None) or [])
    table: dict[str, str] = {}
    for host in hosts:
        normalized = _normalized(host)
        if normalized in table or not os.path.isdir(host):
            continue
        if _held_storage(normalized) is not None:
            logger.warning("Not mounting kernel folder %s: it holds Flowfile's own folder", host)
            continue
        table[normalized] = kernel_side(normalized)
    return table


def writable_sources(kernel) -> set[str]:
    """The host folders of ``kernel`` mounted read-write: its ``mounted_folders`` marked writable."""
    return {_normalized(f.path) for f in getattr(kernel, "mounted_folders", None) or [] if is_writable(f)}


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


def host_side(path: str, folders: dict[str, str]) -> str | None:
    """The host path of kernel ``path`` under ``folders`` (kernel folder -> host folder), or None when not covered.

    The inverse of :func:`translate`: the longest kernel folder wins, kernel paths compare
    case-sensitively, and the result takes the host folder's separators.
    """
    best: tuple[int, str] | None = None
    for kernel_folder, host in folders.items():
        rest = _under(path, kernel_folder, case_insensitive=False)
        if rest is None or (best is not None and len(kernel_folder) <= best[0]):
            continue
        if not rest:
            resolved = host
        elif _windows_drive_path(host) is not None:
            resolved = str(PureWindowsPath(host, *rest.split("/")))
        else:
            resolved = str(PurePosixPath(host, rest))
        best = (len(kernel_folder), resolved)
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
