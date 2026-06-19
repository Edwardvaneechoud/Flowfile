"""Canonical YAML serialization + determinism normalization for project files.

Determinism is the load-bearing requirement: an unchanged DB must project to
byte-identical files, so ``git diff`` stays clean and meaningful.
"""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from pathlib import Path
from typing import Any

import yaml

_SAFE_STEM_RE = re.compile(r"[^A-Za-z0-9._-]+")


def dump_yaml(data: Any) -> str:
    """The single canonical dumper used for every project file."""
    return yaml.dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True, width=4096)


def atomic_write(path: Path, content: str) -> None:
    """Write-then-rename so a crash never leaves a torn file for the importer."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp-", suffix=path.suffix or ".yaml")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def write_yaml(path: Path, data: Any) -> None:
    atomic_write(path, dump_yaml(data))


def safe_stem(name: str | None) -> str:
    cleaned = _SAFE_STEM_RE.sub("_", name or "").strip("._-")
    return cleaned or "flow"


def unique_stem(name: str | None) -> str:
    """A filesystem-safe stem that stays distinct when cleaning is lossy. ``prod db``/``prod/db``
    both clean to ``prod_db`` and would overwrite each other's file; appending a short hash of the
    original name keeps them separate while staying byte-identical across runs (the suffix is
    derived only from the name). Names needing no cleaning keep their bare stem."""
    base = safe_stem(name)
    if base == (name or ""):
        return base
    digest = hashlib.sha256((name or "").encode()).hexdigest()[:8]
    return f"{base}_{digest}"


def deterministic_flow_id(flow_uuid: str) -> int:
    """A stable 63-bit positive flowfile_id derived from the flow's uuid (no host/time entropy).
    Uses the full positive int64 range so two flows don't collide and evict each other from the
    in-memory handler on import."""
    return int(hashlib.sha256(flow_uuid.encode()).hexdigest(), 16) & 0x7FFFFFFFFFFFFFFF


# (node type → settings key) for nodes whose target namespace must be portable across installs.
_NAMESPACE_NODE_SETTINGS = {
    "catalog_writer": "catalog_write_settings",
    "train_model": "train_input",
    "apply_model": "apply_input",
}


def _strip_node_namespace_ids(data: dict) -> None:
    """Drop the install-local numeric ``namespace_id`` from catalog-writer / model nodes that carry a
    portable ``namespace_full_name`` (set by the editor). The name is authoritative and survives
    recreation; the id goes stale (and may even point at a different namespace) on another machine, so
    keeping it would only churn the committed file. Nodes with only an id are left untouched — a stale
    id can't be reliably mapped back to the intended namespace, so re-save in the editor to store the
    name."""
    for node in data.get("nodes") or []:
        settings_key = _NAMESPACE_NODE_SETTINGS.get(node.get("type"))
        if not settings_key:
            continue
        settings = node.get(settings_key)
        if isinstance(settings, dict) and settings.get("namespace_full_name"):
            settings["namespace_id"] = None


def normalize_flow_data(data: dict, flow_uuid: str, catalog_name: str, namespace: dict | None = None) -> dict:
    """Strip volatile fields from a FlowfileData dict; key it by the stable flow_uuid.

    ``flowfile_id`` (timestamp+host+random) and ``source_registration_id`` (a machine-local
    FK) are the only volatile fields that reach the projected file; everything else in
    ``FlowfileData`` is already deterministic. ``flow_uuid`` and ``catalog_name`` are injected
    for the importer (the loader ignores unknown keys): ``flow_uuid`` re-links the flow,
    ``catalog_name`` restores its friendly catalog label without being the filename key.
    ``namespace`` (``{"catalog", "schema"}`` names — portable across installs) is injected at a
    fixed position so the importer can restore the flow's catalog placement; omitted when None.
    Catalog-writer/model nodes additionally have their install-local ``namespace_id`` dropped when a
    portable ``namespace_full_name`` is present (the runtime resolves the name).
    """
    data = dict(data)
    data["flowfile_id"] = deterministic_flow_id(flow_uuid)
    settings = data.get("flowfile_settings")
    if isinstance(settings, dict):
        settings["source_registration_id"] = None
    _strip_node_namespace_ids(data)
    head: dict = {"flow_uuid": flow_uuid, "catalog_name": catalog_name}
    if namespace is not None:
        head["namespace"] = namespace
    return {**head, **data}
