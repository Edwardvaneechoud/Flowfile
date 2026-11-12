"""Canonical YAML serialization + determinism normalization for project files.

Determinism is the load-bearing requirement: an unchanged DB must project to
byte-identical files, so ``git diff`` stays clean and meaningful.
"""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml

_SAFE_STEM_RE = re.compile(r"[^A-Za-z0-9._-]+")


class _ProjectDumper(yaml.Dumper):
    """PyYAML's dumper with the list style of project files, so their bytes do not depend on what another
    module registered on ``yaml.Dumper``."""


def _represent_list(dumper: yaml.Dumper, data: list):
    flow = len(data) <= 10 and all(isinstance(item, int | str | float | bool | type(None)) for item in data)
    return dumper.represent_sequence("tag:yaml.org,2002:seq", data, flow_style=flow)


_ProjectDumper.add_representer(list, _represent_list)


def dump_yaml(data: Any) -> str:
    """The single canonical dumper used for every project file."""
    return yaml.dump(
        data, Dumper=_ProjectDumper, default_flow_style=False, sort_keys=False, allow_unicode=True, width=4096
    )


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
    """A stable 53-bit positive flowfile_id derived from the flow's uuid (no host/time entropy).
    Capped at JavaScript's Number.MAX_SAFE_INTEGER (2**53 - 1) so the frontend can round-trip
    it losslessly; the 2**53 space still keeps distinct flows from colliding on import."""
    return int(hashlib.sha256(flow_uuid.encode()).hexdigest(), 16) & 0x1FFFFFFFFFFFFF


# (node type, settings key, {id field: value once dropped}, name field, lookup kind)
_ID_NAME_FIELDS = (
    (
        "catalog_reader",
        None,
        {"catalog_table_id": None, "catalog_namespace_id": None},
        "catalog_full_table_name",
        "table",
    ),
    ("catalog_writer", "catalog_write_settings", {"namespace_id": None}, "namespace_full_name", "namespace"),
    ("train_model", "train_input", {"namespace_id": None}, "namespace_full_name", "namespace"),
    ("apply_model", "apply_input", {"namespace_id": None}, "namespace_full_name", "namespace"),
    ("run_flow", "flow_reference", {"registration_id": 0}, "flow_uuid", "flow"),
)

Lookup = Callable[[str, dict], dict | None]


def _name_node_references(data: dict, lookup: Lookup) -> None:
    """Replace the install-local row ids a node's settings hold with the names they resolve to.

    ``_ID_NAME_FIELDS`` lists them; a settings key of ``None`` means the node's ``setting_input`` itself.
    ``lookup(kind, settings)`` answers the name fields the run time would resolve the settings to on this install
    now, in the order it reads them (the catalog reader its id first; the writer and models a stored name that
    resolves, else the id; a Run Flow node its ``flow_uuid``, the id only without one), or ``None`` when nothing
    resolves, which leaves the name fields as they were. The id is dropped either way, so a flow file holds none
    of the row ids the table lists. A Run Flow node's ``registration_id`` is a required int, so it is written as
    ``0``, which no row has.
    """
    for node in data.get("nodes") or []:
        for node_type, settings_key, id_fields, _name_field, kind in _ID_NAME_FIELDS:
            if node.get("type") != node_type:
                continue
            settings = node.get("setting_input")
            if isinstance(settings, dict) and settings_key is not None:
                settings = settings.get(settings_key)
            if not isinstance(settings, dict):
                continue
            if all(settings.get(f) in (None, dropped) for f, dropped in id_fields.items()):
                continue
            names = lookup(kind, settings)
            if names:
                settings.update(names)
            settings.update(id_fields)


def normalize_flow_data(
    data: dict, flow_uuid: str, catalog_name: str, namespace: dict | None = None, *, lookup: Lookup
) -> dict:
    """Strip volatile fields from a FlowfileData dict; key it by the stable flow_uuid.

    ``flowfile_id`` (timestamp+host+random) and ``source_registration_id`` (a machine-local
    FK) are the only volatile fields that reach the projected file; everything else in
    ``FlowfileData`` is already deterministic. ``flow_uuid`` and ``catalog_name`` are injected
    for the importer (the loader ignores unknown keys): ``flow_uuid`` re-links the flow,
    ``catalog_name`` restores its friendly catalog label without being the filename key.
    ``namespace`` (``{"catalog", "schema"}`` names — portable across installs) is injected at a
    fixed position so the importer can restore the flow's catalog placement; omitted when None.
    Node settings that name a catalog table, namespace or flow by row id get the name instead
    (:func:`_name_node_references`, through ``lookup``). The three head keys
    an import wrote into the runtime file are dropped first, so the registration's name and namespace
    win over them.
    """
    data = dict(data)
    data["flowfile_id"] = deterministic_flow_id(flow_uuid)
    settings = data.get("flowfile_settings")
    if isinstance(settings, dict):
        settings["source_registration_id"] = None
    _name_node_references(data, lookup)
    for key in ("flow_uuid", "catalog_name", "namespace"):
        data.pop(key, None)
    head: dict = {"flow_uuid": flow_uuid, "catalog_name": catalog_name}
    if namespace is not None:
        head["namespace"] = namespace
    return {**head, **data}
