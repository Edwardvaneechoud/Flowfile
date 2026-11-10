"""The storage target of a catalog table for a run, whichever catalog keeps the table.

A table's ``source`` names the catalog that keeps its row: ``"local"`` is this install's own, any other a remote
catalog whose client registered itself here (``register_remote``). The engine asks one question, the target of a
table, or of the schema a new table goes into, for this run and person, and gets what a ``CatalogStorageTarget``
offers either way: a ``base`` (the table's parent), ``scan_kwargs`` and ``to_worker_payload``. A remote catalog answers
from the short-lived credential it was given for the run, so the question is asked when a node runs, never when it is
wired. The dependency points from the remote catalog's client to this module, never the other way.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

import polars as pl

from flowfile_core.catalog.storage_backend import CatalogStorageTarget, resolve_for_namespace, resolve_for_table

LOCAL = "local"


class RemoteCatalog(Protocol):
    """What a remote catalog's client answers for a run; every refusal is a ``LookupError`` with a sentence."""

    def table_named(self, user_id: int | None, qualified_name: str) -> dict:
        """The row of the table ``qualified_name`` as ``user_id`` may read it."""

    def table_target(self, run_id: str | None, user_id: int | None, file_path: str) -> Any:
        """The target of the table stored at ``file_path``, its ``base`` the table's parent."""

    def schema_target(self, run_id: str | None, user_id: int | None, schema: str) -> Any:
        """The target of the schema ``schema`` (``catalog.schema``), where a new table of it goes."""

    def schema_table(self, run_id: str | None, user_id: int | None, schema: str, name: str) -> dict | None:
        """The row of the table ``name`` in ``schema``, ``None`` when it has none."""

    def register_table(self, run_id: str | None, user_id: int | None, schema: str, table: dict) -> None:
        """Record the newly written ``table`` in ``schema``."""

    def run_holds(self, run_id: str | None) -> bool:
        """Whether the run ``run_id`` was given credentials for this catalog's tables."""


_remotes: dict[str, RemoteCatalog] = {}


def register_remote(source: str, catalog: RemoteCatalog) -> None:
    """Make ``catalog`` the answer for every table whose ``source`` is ``source``."""
    _remotes[source] = catalog


def is_remote(source: str | None) -> bool:
    """Whether ``source`` names a catalog other than this install's own."""
    return bool(source) and source != LOCAL


def remote(source: str) -> RemoteCatalog:
    """The remote catalog registered for ``source``.

    Raises:
        LookupError: No client registered for that source in this process.
    """
    if source not in _remotes:
        raise LookupError(f"This Flowfile has no catalog named {source!r} to read or write tables of")
    return _remotes[source]


def table_target(
    source: str | None,
    file_path: str | None,
    namespace_id: int | None = None,
    *,
    run_id: str | None = None,
    user_id: int | None = None,
) -> CatalogStorageTarget:
    """The storage target of an existing table for the run ``run_id`` and ``user_id``.

    A local table's is its namespace's (``resolve_for_table``); a remote one's is its catalog's answer for the run.
    """
    if not is_remote(source):
        return resolve_for_table(file_path, namespace_id)
    return remote(source).table_target(run_id, user_id, file_path)


def schema_target(
    source: str | None,
    schema: int | str | None,
    *,
    run_id: str | None = None,
    user_id: int | None = None,
    db=None,
) -> CatalogStorageTarget:
    """The storage target of the schema a new table goes into: a local namespace id, or a remote schema's name."""
    if not is_remote(source):
        return resolve_for_namespace(schema, db=db)
    return remote(source).schema_target(run_id, user_id, schema)


def scan_delta(
    target: CatalogStorageTarget,
    path: str,
    user_id: int | None,
    columns: Mapping[str, pl.DataType] | None = None,
    **kwargs,
) -> pl.LazyFrame:
    """A lazy scan of the Delta table at ``path`` on ``target``, its plan holding no secret.

    A target whose credential core cannot open scans by its own ``scan_delta`` with ``columns`` as the schema; any
    other through ``scan_kwargs``.
    """
    own_scan = getattr(target, "scan_delta", None)
    if own_scan is not None:
        return own_scan(path, user_id, columns or {}, **kwargs)
    return pl.scan_delta(path, **kwargs, **target.scan_kwargs(user_id))
