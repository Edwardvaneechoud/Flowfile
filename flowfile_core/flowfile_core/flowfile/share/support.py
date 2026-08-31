"""What the in-browser (WASM) build can run, read from the shipped manifest.

The truth is ``wasm_node_support.json``, generated from ``flowfile_wasm``'s
palette, dialect map and Pyodide engine by
``tools/generate_wasm_node_manifest.py`` and committed as package data next to
this module. It is read package-relative on purpose: ``flowfile_wasm`` is not
part of any shipped artifact, so a repo-root lookup would leave every wheel,
Docker image and PyInstaller sidecar with no baseline.

Missing or unreadable manifest is **fail-closed**: every node type reports
``ABSENT`` and every capability set is empty, so a share degrades to
placeholders instead of minting links for nodes the browser cannot run.

Run ``make wasm_node_manifest`` after changing the browser palette or engine;
``make check_wasm_node_manifest`` fails CI when the committed file drifts.
"""

import functools
import json
import logging
import sys
from enum import Enum
from pathlib import Path

logger = logging.getLogger(__name__)

_MANIFEST_NAME = "wasm_node_support.json"


class SupportTier(str, Enum):
    """How a core node type fares in the browser build."""

    SUPPORTED = "supported"
    LOCKED = "locked"
    ABSENT = "absent"


def _manifest_path() -> Path:
    """Locate the manifest in a checkout, a wheel, a Docker image or a frozen bundle."""
    if getattr(sys, "frozen", False):
        frozen = Path(getattr(sys, "_MEIPASS", "")) / "flowfile_core" / "flowfile" / "share" / _MANIFEST_NAME
        if frozen.is_file():
            return frozen
    return Path(__file__).resolve().parent / _MANIFEST_NAME


_MANIFEST_PATH = _manifest_path()


@functools.lru_cache(maxsize=1)
def load_manifest() -> dict | None:
    """Parse the shipped manifest, or ``None`` when it is missing or malformed."""
    if not _MANIFEST_PATH.is_file():
        logger.warning(
            "WASM node support manifest not found at %s — every node will be shared as an "
            "unsupported placeholder. Run 'make wasm_node_manifest' and make sure the file ships.",
            _MANIFEST_PATH,
        )
        return None
    try:
        manifest = json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        logger.warning("WASM node support manifest at %s is unreadable (%s)", _MANIFEST_PATH, exc)
        return None
    if not isinstance(manifest, dict) or not isinstance(manifest.get("nodes"), dict):
        logger.warning("WASM node support manifest at %s has an unexpected shape", _MANIFEST_PATH)
        return None
    return manifest


def _node_entry(node_type: str) -> dict:
    manifest = load_manifest()
    if manifest is None:
        return {}
    entry = manifest["nodes"].get(node_type)
    return entry if isinstance(entry, dict) else {}


def tier_for(node_type: str) -> SupportTier:
    """The support tier of a core node type; unknown types are ``ABSENT``."""
    try:
        return SupportTier(_node_entry(node_type).get("tier", SupportTier.ABSENT.value))
    except ValueError:
        return SupportTier.ABSENT


def wasm_type_for(node_type: str) -> str | None:
    """The browser editor's spelling of a core node type, when it has one."""
    return _node_entry(node_type).get("wasm_type")


def docs_anchor_for(node_type: str) -> str | None:
    """The docs heading slug the browser palette links a locked node to."""
    return _node_entry(node_type).get("docs_anchor")


def _capability(name: str) -> frozenset[str]:
    manifest = load_manifest()
    if manifest is None:
        return frozenset()
    values = (manifest.get("capabilities") or {}).get(name)
    return frozenset(values) if isinstance(values, list) else frozenset()


def group_by_aggs() -> frozenset[str]:
    """Aggregations the browser engine's group_by implements."""
    return _capability("group_by_aggs")


def pivot_aggs() -> frozenset[str]:
    """Aggregations the browser engine's pivot implements (a subset of group_by's)."""
    return _capability("pivot_aggs")


def read_file_types() -> frozenset[str]:
    """File formats the browser Read node can open."""
    return _capability("read_file_types")


def output_file_types() -> frozenset[str]:
    """File formats the browser Write node can produce."""
    return _capability("output_file_types")
