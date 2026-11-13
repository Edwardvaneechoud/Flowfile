"""Catalog metadata lookups a notebook kernel session makes through core (``POST /notebook/session/lookup``).

Building a node reads catalog metadata: the table a catalog reader names, the registration a ``run_flow``
reference points at, the connection a cloud, database or Kafka node names, the namespaces, kernels and
connections the frame's own helpers list. In core those reads open the catalog database. A kernel session
sets :data:`metadata_lookup` for each op instead (``flowfile_frame._metadata.installed``), and every one of
those reads asks it, so the kernel opens no database connection and nothing secret-bearing reaches it: not a
plan, not a storage credential, not a ciphertext. The kernel mounts no host folder either, so what a build needs
from the host's filesystem (a flow file's interface, whether a path is a folder, the custom node sources) is read
by core the same way.

Core answers a ``kind`` by calling the function the read calls when the hook is unset (the route's own
context never sets it), so a kernel build sees exactly what core computes. :data:`KINDS` is closed; an
unknown kind is a 422. The module imports nothing beyond the standard library and pydantic at top level,
since ``flow_graph``, ``subflow``, the catalog storage backend, the prechecks and the frame import it.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from contextvars import ContextVar
from typing import Any

from pydantic import BaseModel, Field, ValidationError

Lookup = Callable[[str, dict[str, Any]], Any]

metadata_lookup: ContextVar[Lookup | None] = ContextVar("metadata_lookup", default=None)
"""How this context answers a catalog metadata lookup instead of opening the database: ``(kind, args) -> answer``.

A notebook kernel session sets it for each op; core's own contexts never do, so there the database is read
as always. Set only in the context that reads through it, like ``flow_graph.placement_check``.
"""


def current_lookup() -> Lookup | None:
    """How this context answers a catalog metadata lookup: the session's hook, else core for any other code in a
    kernel, else ``None`` (read the database).

    A kernel container (``FLOWFILE_KERNEL_ID`` set) holds no catalog database, so flowfile running there outside a
    notebook session (a Catalog-page notebook cell, a Python Script node) asks core too
    (``flowfile_frame._metadata.kernel_lookup``). Every read that would open the database asks this function.
    """
    hook = metadata_lookup.get()
    if hook is not None or not os.environ.get("FLOWFILE_KERNEL_ID"):
        return hook
    from flowfile_frame._metadata import kernel_lookup

    return kernel_lookup


PRECHECKED_SETTINGS: tuple[str, ...] = (
    "NodeCloudStorageReader",
    "NodeCloudStorageWriter",
    "NodeDatabaseReader",
    "NodeDatabaseWriter",
    "NodeKafkaSource",
)
"""The settings classes ``notebook.prechecks.placement_refusal`` looks up a connection for."""

FRAME_KINDS: tuple[str, ...] = (
    "namespaces",
    "namespace",
    "namespace_by_name",
    "default_namespace_id",
    "namespace_full_name",
    "namespace_id_by_full_name",
    "tables",
    "flow_registrations",
    "kernels",
    "cloud_connections",
    "database_connection",
    "database_connections",
    "flow_interface",
    "is_directory",
    "custom_node_hashes",
    "custom_node_sources",
)
"""The kinds ``flowfile_frame._metadata`` asks for its own helpers; each is answered by its ``local_*`` twin.

The last four read the host's filesystem, which the kernel mounts nothing of: a registered flow's interface
(its file, after the access check), whether a path a cell names is a folder, and the installed custom node files
(their hashes, then the files of the keys the kernel asks for).
"""


class LookupRequest(BaseModel):
    """Body of ``POST /notebook/session/lookup``: the session's flow, the kind and its arguments.

    ``flow_id`` is ``None`` when the kernel asks outside a notebook session (:func:`current_lookup`).
    """

    flow_id: int | None = None
    kind: str = Field(pattern=r"^[a-z_]{1,40}$")
    args: dict[str, Any] = Field(default_factory=dict)


def _catalog_table(args: dict[str, Any], user_id: int) -> dict[str, Any]:
    from flowfile_core.flowfile.flow_graph import _resolve_catalog_table_info
    from flowfile_core.schemas import input_schema

    settings = input_schema.NodeCatalogReader.model_validate(
        {
            "flow_id": 0,
            "node_id": int(args.get("node_id") or 0),
            "user_id": user_id,
            "catalog_table_id": args.get("catalog_table_id"),
            "catalog_full_table_name": args.get("catalog_full_table_name"),
            "catalog_table_name": args.get("catalog_table_name"),
            "catalog_namespace_id": args.get("catalog_namespace_id"),
        }
    )
    return {**_resolve_catalog_table_info(settings)._asdict(), "serialized_lf": None}


def _catalog_sql_tables(args: dict[str, Any], user_id: int) -> dict[str, Any]:
    from flowfile_core.flowfile.flow_graph import _resolve_catalog_sql_tables

    resolved = _resolve_catalog_sql_tables(int(args.get("node_id") or 0), user_id)
    return {
        "table_paths": resolved.table_paths,
        "virtual_tables": {
            name: [is_optimized, None, table_id, versions]
            for name, (is_optimized, _plan, table_id, versions) in resolved.virtual_tables.items()
        },
        "table_namespaces": resolved.table_namespaces,
    }


def _catalog_storage(args: dict[str, Any], user_id: int) -> dict[str, Any]:
    from flowfile_core.catalog.storage_backend import locate_for_namespace

    namespace_id = args.get("namespace_id")
    try:
        target = locate_for_namespace(None if namespace_id is None else int(namespace_id))
    except ValueError as exc:
        return {"error": str(exc)}
    return {"is_cloud": target.is_cloud, "base": target.base, "connection_name": target.connection_name}


def _flow_registration(args: dict[str, Any], user_id: int) -> dict[str, Any]:
    from flowfile_core.flowfile.subflow import SubflowResolutionError, local_registration
    from flowfile_core.schemas import input_schema

    try:
        return local_registration(input_schema.SubflowReference.model_validate(args), user_id)
    except SubflowResolutionError as exc:
        return {"error": str(exc)}


def _placement_refusal(args: dict[str, Any], user_id: int) -> str | None:
    from flowfile_core.notebook.prechecks import placement_refusal
    from flowfile_core.schemas import input_schema

    name = args.get("settings_type")
    if name not in PRECHECKED_SETTINGS:
        raise ValueError(f"No placement check for {name!r}")
    settings = getattr(input_schema, name).model_validate(args.get("settings") or {})
    return placement_refusal(settings, user_id)


def _frame(kind: str) -> Callable[[dict[str, Any], int], Any]:
    def answer(args: dict[str, Any], user_id: int) -> Any:
        from flowfile_frame import _metadata

        return _metadata.dumped(getattr(_metadata, f"local_{kind}")(**args, user_id=user_id))

    return answer


KINDS: dict[str, Callable[[dict[str, Any], int], Any]] = {
    "catalog_table": _catalog_table,
    "catalog_sql_tables": _catalog_sql_tables,
    "catalog_storage": _catalog_storage,
    "flow_registration": _flow_registration,
    "placement_refusal": _placement_refusal,
    **{kind: _frame(kind) for kind in FRAME_KINDS},
}


def answer(kind: str, args: dict[str, Any], user_id: int) -> Any:
    """The JSON answer to ``kind`` for ``user_id``; an unknown kind or unusable ``args`` is a ``ValueError``."""
    handler = KINDS.get(kind)
    if handler is None:
        raise ValueError(f"Unknown lookup {kind!r}")
    try:
        return handler(args, user_id)
    except (TypeError, ValidationError) as exc:
        raise ValueError(f"Lookup {kind!r} cannot read its arguments: {exc}") from exc


def _with_kernel_paths(kind: str, result: Any, to_kernel_path: Callable[[str], str]) -> Any:
    """``result`` with its catalog table paths as the kernel sees them (its ``/catalog_tables`` mount)."""
    if kind == "catalog_table" and result.get("file_path"):
        return {**result, "file_path": to_kernel_path(result["file_path"])}
    if kind == "catalog_sql_tables":
        paths = {name: to_kernel_path(path) for name, path in result["table_paths"].items()}
        return {**result, "table_paths": paths}
    return result


def answer_request(kernel_id: str, user, body: LookupRequest) -> dict[str, Any]:
    """``{"result": answer}`` for ``kernel_id``, as its owner.

    With a ``flow_id`` the kernel must hold that flow's session or run a call for it
    (``kernel_runner._bound_kernel``; the flow itself need not be open), and core's paths are answered as they
    are, since core reads a session's catalog tables. Without one (``current_lookup`` outside a session) the owner
    check is enough (``kernel_runner._owned_kernel``), and table paths are the kernel's, which reads them itself.
    """
    from fastapi import HTTPException

    from flowfile_core.notebook import kernel_runner, validate

    if body.flow_id is None:
        manager = kernel_runner._owned_kernel(kernel_id, user)
    else:
        kernel_runner._bound_kernel(kernel_id, user, body.flow_id)
    if len(json.dumps(body.args, default=str)) > validate.MAX_PAYLOAD_BYTES:
        raise HTTPException(422, f"The lookup's arguments exceed {validate.MAX_PAYLOAD_BYTES} bytes")
    try:
        result = answer(body.kind, body.args, user.id)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    if body.flow_id is None:
        result = _with_kernel_paths(body.kind, result, manager.to_kernel_path)
    return {"result": result}
