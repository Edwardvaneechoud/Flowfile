"""File-backed registry for user-defined custom nodes (registry v2).

The single loader and the single mint of node-type keys. Broken files stay
registered with ``error`` set — visible-with-error, never silently missing.
``configs.node_store`` attaches the store-sync hooks so ``CUSTOM_NODE_STORE``
and the palette lists track healthy entries only.
"""

import hashlib
import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from flowfile_core.flowfile.user_defined.mounts import load_mounts
from shared import storage
from shared.node_designer.custom_node import CustomNodeBase
from shared.node_designer.loading import NodeLoadError, find_custom_node_class, load_node_module

if TYPE_CHECKING:
    from flowfile_core.schemas.schemas import NodeTemplate

logger = logging.getLogger(__name__)


class KernelRequiredError(ValueError):
    """A kernel-environment custom node was added without a resolvable kernel binding."""

    def __init__(self, node_type: str):
        self.node_type = node_type
        super().__init__(f"Custom node '{node_type}' runs in an isolated kernel environment — select a kernel first.")


def compute_node_key(node_name: str) -> str:
    """Canonical node-type key; identical to ``CustomNodeBase.item`` so saved flows keep resolving."""
    return node_name.replace(" ", "_").lower()


@dataclass
class LoadedNode:
    node_key: str
    file_path: Path
    file_name: str
    source_text: str
    source_hash: str
    mtime: float
    class_name: str | None = None
    node_class: type[CustomNodeBase] | None = None
    template: "NodeTemplate | None" = None
    error: str | None = None
    mount_path: str | None = None  # None = default nodes directory
    module_name: str = ""

    @property
    def is_broken(self) -> bool:
        return self.node_class is None


class CustomNodeRegistry:
    """Registry over the ``storage.user_defined_nodes_directory`` .py files plus any mounted directories.

    Entries are keyed by full file path so equal file names across mounts never
    collide (each gets its own sys.modules name too). The default directory is
    scanned first, so on duplicate node names a default-dir node wins over a
    mounted one. Mounted nodes are read-only for the designer's save/delete
    routes — those operate on the default directory only.
    """

    def __init__(self, directory: Path | None = None):
        self._directory = directory
        self._entries: dict[str, LoadedNode] = {}  # keyed by str(file_path)
        self.on_registered: Callable[[LoadedNode], None] | None = None
        self.on_unregistered: Callable[[LoadedNode], None] | None = None

    @property
    def directory(self) -> Path:
        return self._directory if self._directory is not None else storage.user_defined_nodes_directory

    def mount_directories(self) -> list[Path]:
        """Registered mounts (persisted next to the default dir, so custom-directory registries stay isolated)."""
        return [Path(mount.path) for mount in load_mounts(self.directory)]

    def scan(self) -> list[LoadedNode]:
        """Full rescan of the nodes directory + mounts (non-recursive, so the icons subdir is never scanned)."""
        for key in list(self._entries):
            self._remove_entry(key)
        directory = self.directory
        if not directory.exists() or not directory.is_dir():
            logger.warning("User-defined nodes directory %s does not exist", directory)
            return []
        scan_targets: list[tuple[Path, str | None]] = [(directory, None)]
        scan_targets += [(mount, str(mount)) for mount in self.mount_directories()]
        for base, mount_path in scan_targets:
            if mount_path is not None and (not base.exists() or not base.is_dir()):
                logger.warning("Custom node mount %s does not exist", base)
                continue
            for path in sorted(base.glob("*.py")):
                if path.name.startswith("__"):
                    continue
                self.load_file(path, mount_path=mount_path)
        return self.all()

    @staticmethod
    def _module_name_for(path: Path, mount_path: str | None) -> str:
        if mount_path is None:
            return f"flowfile_udn_{path.stem}"
        digest = hashlib.sha256(mount_path.encode("utf-8")).hexdigest()[:8]
        return f"flowfile_udn_m{digest}_{path.stem}"

    def load_file(self, path: Path, mount_path: str | None = None) -> LoadedNode:
        path = Path(path)
        module_name = self._module_name_for(path, mount_path)
        prior = self._entries.get(str(path))
        fallback_key = prior.node_key if prior is not None else path.stem
        try:
            source_bytes = path.read_bytes()
        except OSError as e:
            entry = LoadedNode(
                node_key=fallback_key,
                file_path=path,
                file_name=path.name,
                source_text="",
                source_hash="",
                mtime=0.0,
                error=f"Cannot read file: {e}",
                mount_path=mount_path,
                module_name=module_name,
            )
            return self._finalize(entry, prior)

        entry = LoadedNode(
            node_key=fallback_key,
            file_path=path,
            file_name=path.name,
            source_text=source_bytes.decode("utf-8", errors="replace"),
            source_hash=hashlib.sha256(source_bytes).hexdigest(),
            mtime=path.stat().st_mtime,
            mount_path=mount_path,
            module_name=module_name,
        )
        try:
            module = load_node_module(path=str(path), module_name=module_name)
            node_class = find_custom_node_class(module)
            node = node_class()
            entry.class_name = node_class.__name__
            entry.node_key = compute_node_key(node.node_name)
            winner = self._holder_of(entry.node_key)
            if winner is not None and winner.file_path != entry.file_path:
                entry.error = f"Duplicate node name '{node.node_name}' — already provided by {winner.file_name}"
            else:
                entry.node_class = node_class
                entry.template = node.to_node_template()
        except SyntaxError as e:
            entry.error = f"Syntax error at line {e.lineno}: {e.msg}"
        except NodeLoadError as e:
            entry.error = str(e)
        except Exception as e:
            entry.error = f"{type(e).__name__}: {e}"
        return self._finalize(entry, prior)

    def remove_file(self, file_name: str) -> LoadedNode | None:
        """Remove a default-directory file's entry; mounted entries leave via ``scan()`` after a mount change."""
        return self._remove_entry(str(self.directory / file_name))

    def _remove_entry(self, key: str) -> LoadedNode | None:
        entry = self._entries.pop(key, None)
        if entry is None:
            return None
        sys.modules.pop(entry.module_name or f"flowfile_udn_{Path(entry.file_name).stem}", None)
        if entry.node_class is not None and self.on_unregistered is not None:
            self.on_unregistered(entry)
        return entry

    def get(self, node_key: str) -> LoadedNode | None:
        broken = None
        for entry in self._entries.values():
            if entry.node_key == node_key:
                if entry.node_class is not None:
                    return entry
                broken = broken or entry
        return broken

    def get_by_file(self, file_name: str) -> LoadedNode | None:
        """Default-directory lookup by file name (mounted entries are keyed by full path)."""
        return self._entries.get(str(self.directory / file_name))

    def all(self, include_broken: bool = True) -> list[LoadedNode]:
        entries = sorted(self._entries.values(), key=lambda e: (e.file_name, e.mount_path or ""))
        if include_broken:
            return entries
        return [entry for entry in entries if entry.node_class is not None]

    def _holder_of(self, node_key: str) -> LoadedNode | None:
        for entry in self._entries.values():
            if entry.node_key == node_key and entry.node_class is not None:
                return entry
        return None

    def _finalize(self, entry: LoadedNode, prior: LoadedNode | None) -> LoadedNode:
        if (
            prior is not None
            and prior.node_class is not None
            and (prior.node_key != entry.node_key or entry.node_class is None)
            and self.on_unregistered is not None
        ):
            self.on_unregistered(prior)
        self._entries[str(entry.file_path)] = entry
        if entry.node_class is not None:
            if self.on_registered is not None:
                self.on_registered(entry)
            logger.info("Loaded custom node '%s' from %s", entry.node_key, entry.file_name)
        else:
            logger.warning("Custom node file %s failed to load: %s", entry.file_name, entry.error)
        return entry


registry = CustomNodeRegistry()


def missing_custom_node_error(node_type: str) -> str:
    """User-facing error for a custom node type that cannot execute on this machine."""
    entry = registry.get(node_type)
    if entry is not None and entry.error:
        return f"Custom node '{node_type}' failed to load: {entry.error}"
    return f"Custom node '{node_type}' is not installed on this machine"


def load_single_node_from_file(file_path: Path) -> type[CustomNodeBase] | None:
    """Legacy loader kept for compatibility: returns the node class or None, raising on invalid files."""
    file_path = Path(file_path)
    if not file_path.exists():
        logger.error("File not found: %s", file_path)
        return None
    module = load_node_module(path=str(file_path), module_name=f"custom_node_{file_path.stem}_{id(file_path)}")
    try:
        node_class = find_custom_node_class(module)
    except NodeLoadError as e:
        if "No CustomNodeBase subclass" in str(e):
            return None
        raise
    node_class()  # validate instantiation; pydantic errors are ValueErrors
    logger.info("Loaded custom node '%s' from %s", node_class.__name__, file_path.name)
    return node_class


def unload_node_by_name(node_name: str) -> bool:
    """Drop a node's cached modules from ``sys.modules`` so it can be cleanly reloaded."""
    module_stem = node_name.lower().replace(" ", "_")
    modules_to_remove = [
        key
        for key in sys.modules
        if key == module_stem or key == f"flowfile_udn_{module_stem}" or key.startswith(f"custom_node_{module_stem}")
    ]
    for mod_name in modules_to_remove:
        del sys.modules[mod_name]
        logger.info("Removed module from cache: %s", mod_name)
    return len(modules_to_remove) > 0
