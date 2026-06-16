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


def deterministic_flow_id(flow_uuid: str) -> int:
    """A stable 32-bit flowfile_id derived from the flow's uuid (no host/time entropy)."""
    return int(hashlib.sha256(flow_uuid.encode()).hexdigest(), 16) & 0xFFFFFFFF


def normalize_flow_data(data: dict, flow_uuid: str) -> dict:
    """Strip volatile fields from a FlowfileData dict; key it by the stable flow_uuid.

    ``flowfile_id`` (timestamp+host+random) and ``source_registration_id`` (a machine-local
    FK) are the only volatile fields that reach the projected file; everything else in
    ``FlowfileData`` is already deterministic. The injected ``flow_uuid`` is ignored by the
    ``FlowfileData`` loader (unknown keys) but read by the importer to re-link the flow.
    """
    data = dict(data)
    data["flowfile_id"] = deterministic_flow_id(flow_uuid)
    settings = data.get("flowfile_settings")
    if isinstance(settings, dict):
        settings["source_registration_id"] = None
    return {"flow_uuid": flow_uuid, **data}
