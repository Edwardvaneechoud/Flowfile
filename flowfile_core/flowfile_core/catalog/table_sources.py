"""The storage target of a catalog table for a run, whichever catalog keeps the table.

A table's ``source`` names the catalog that keeps its row: ``"local"`` is this install's own, any other a remote
catalog whose client registered itself here (``register_remote``). The engine asks one question, the target of a
table, or of the schema a new table goes into, for this run and person, and gets what a ``CatalogStorageTarget``
offers either way: a ``base`` (the table's parent), ``scan_kwargs`` and ``to_worker_payload``. A remote catalog answers
from the short-lived credential it was given for the run, so the question is asked when a node runs, never when it is
wired. The dependency points from the remote catalog's client to this module, never the other way.
"""

from __future__ import annotations

import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

import polars as pl
from pydantic import BaseModel, ValidationError

from flowfile_core.auth import issuer_accounts
from flowfile_core.catalog.exceptions import TableNotFoundError
from flowfile_core.catalog.storage_backend import CatalogStorageTarget, resolve_for_namespace, resolve_for_table
from flowfile_core.catalog.text_utils import plain_reference, reads_other
from flowfile_core.configs.settings import is_server_mode

if TYPE_CHECKING:
    from flowfile_core.database.models import CatalogTable
    from flowfile_core.schemas.catalog_schema import CatalogTableOut, NamespaceTree

LOCAL = "local"
ON_THIS_COMPUTER = "On this computer"
ON_THIS_SERVER = "On this server"
_REMOTE_ID = re.compile(r"-\d+")


@dataclass(frozen=True)
class RunStart:
    """What a run tells the remote catalog when it starts, so the catalog's client asks its side once for all of it.

    ``tables`` are the schemas (``catalog.schema``) the run's steps read and write, each with whether the step writes
    and the backend it runs on. ``secrets`` and ``connections`` (a kind and a name each) are the stored ones its steps
    use that the catalog's side keeps, ``exchange`` whether the run needs the exchange store that side set, and
    ``servers`` the names of the servers that open what the run is given themselves; empty when the run's own worker
    opens it.
    """

    run_id: str
    user_id: int | None
    tables: tuple[tuple[str, bool, str], ...] = ()
    secrets: tuple[str, ...] = ()
    connections: tuple[tuple[str, str], ...] = ()
    exchange: bool = False
    servers: tuple[str, ...] = ()


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

    def run_starts(self, start: RunStart) -> Any:
        """Ask for what the run ``start`` describes uses, keep it under the run's id, and answer it."""

    def run_grant(self, run_id: str | None) -> Any | None:
        """What the run ``run_id`` was given when it started; ``None`` for a run that asked nothing, or ended."""

    def run_ends(self, run_id: str) -> None:
        """Forget what the run ``run_id`` was given."""

    def table(self, user_id: int | None, table_id: int) -> CatalogTableOut:
        """The row of the table a read outside a run names by ``table_id``, the id its row carries in this install."""

    def read_targets(self, user_id: int | None, tables: Sequence[CatalogTableOut], purpose: str) -> list[Any]:
        """The target of each of ``tables``, in order, for one read outside a run that ``purpose`` names (a preview,
        a query): asked now, for a credential that does not last. Each ``base`` is its table's parent."""

    def tree(self, user_id: int | None) -> list[NamespaceTree]:
        """The catalogs ``user_id`` sees there, each with its schemas and their tables; empty when it cannot be
        asked."""

    def request(self, user_id: int | None, method: str, path: str, params: dict | None, body) -> tuple[int, object]:
        """A metadata request on a row this catalog keeps, sent to its same route (``path`` is core's own catalog
        route, ids as this install shows them) as ``user_id``: its status and its answer, ``None`` for an empty one.
        Never raises: a catalog that cannot be asked answers a status and a ``detail`` of its client's own."""

    def of_record(self) -> bool:
        """Whether this catalog is the one people's work on this install is kept in, in place of the install's own."""


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


def remote_of(table_id: int) -> RemoteCatalog | None:
    """The remote catalog that keeps the table ``table_id``, ``None`` for one of this install's own.

    A remote table's row carries a negative id in this install, where its own are positive, and a core has one
    remote catalog.
    """
    return next(iter(_remotes.values()), None) if table_id < 0 else None


def run_starts(start: RunStart) -> Any:
    """Tell the remote catalog the run ``start`` describes begins: it asks its side for what the run uses, keeps the
    answer under the run's id until ``run_ends``, and answers it (``run_grant``).

    Raises:
        LookupError: The catalog's sentence when it cannot be asked or refuses; no remote catalog in this process.
    """
    catalog = next(iter(_remotes.values()), None)
    if catalog is None:
        raise LookupError("This Flowfile has no remote catalog to ask for what this run uses")
    return catalog.run_starts(start)


def run_grant(run_id: str | None) -> Any | None:
    """What the remote catalog gave the run ``run_id`` when it started: the secrets and connections sealed for its
    steps and the servers they were sealed to. ``None`` between runs and for a run that asked nothing."""
    grants = (catalog.run_grant(run_id) for catalog in _remotes.values()) if run_id else ()
    return next((grant for grant in grants if grant is not None), None)


def run_ends(run_id: str) -> None:
    """Tell the remote catalogs the run ``run_id`` ended, so none keeps what it was given."""
    for catalog in _remotes.values():
        catalog.run_ends(run_id)


def relayed(user_id: int | None, method: str, path: str, params: dict | None, body) -> tuple[int, object] | None:
    """The remote catalog's answer to a metadata request that names one of its rows, ``None`` for a request that
    names none: this install's own to answer.

    A request names a remote row by a negative integer as a segment of ``path``, or as an ``id`` or ``*_id`` field
    of its query or its JSON body. Nothing is decided here: what the catalog refuses, the catalog refuses.
    """
    named = [*path.split("/")]
    for sent in (params, body):
        if isinstance(sent, Mapping):
            named += [value for key, value in sent.items() if key == "id" or key.endswith("_id")]
    catalog = next(iter(_remotes.values()), None)
    if catalog is None or not any(_REMOTE_ID.fullmatch(str(value)) for value in named):
        return None
    return catalog.request(user_id, method, path, params, body)


def remote_answers(user_id: int | None, path: str, params: dict | None = None) -> list[object]:
    """Each remote catalog's answer to the list route ``path`` for ``user_id``, its rows marked as that catalog's.

    A list that names no row is this install's own to answer (``relayed``), so a caller adds these to its own. What
    a person reads of a remote catalog is what that catalog answers them. A catalog that does not answer 200, one the
    person has no session with included, is left out, as it is left out of the tree.
    """
    if user_id is None:
        return []
    answers = [catalog.request(user_id, "GET", path, params, None) for catalog in _remotes.values()]
    return [answer for status, answer in answers if status == 200]


def merged_rows(user_id: int | None, path: str, local: list, row: type[BaseModel], params: dict | None = None) -> list:
    """The rows of the list route ``path`` that ``user_id`` reads: this install's ``local`` first, then the remote
    catalogs' rows of the same list (``remote_answers``), each as ``row``. An answer that is no list of ``row`` is
    left out."""
    merged = list(local)
    for answer in remote_answers(user_id, path, params):
        try:
            merged += [row.model_validate(item) for item in answer]
        except (ValidationError, TypeError):
            continue
    return merged


def local_catalog_offered() -> bool:
    """Whether this install's own catalog is a place a person's work is saved or registered in.

    It is unless a remote catalog says it is the catalog of record here (``RemoteCatalog.of_record``): then nothing
    a person saves, opens from that catalog or publishes is registered in this install's own, whose existing rows
    stay as they are, and the destinations a person is offered are the remote catalog's.
    """
    return not any(catalog.of_record() for catalog in _remotes.values())


def has_its_record_here(executed_on: str | None) -> bool:
    """Whether a remote catalog's record of a run names this install as where it ran, by the name it joined with:
    this install keeps its own record of that run."""
    return executed_on is not None and executed_on == issuer_accounts.joined_name()


def merged_tree(user_id: int, local: list[NamespaceTree]) -> list[NamespaceTree]:
    """The catalog tree ``user_id`` reads: this install's catalogs ``local`` first, then the remote catalogs'.

    With no remote catalog to show, ``local`` is answered as it is. Beside a remote catalog's rows each of this
    install's says where it lives, ``On this computer``, or ``On this server`` in server mode. On a server that
    joined with keys of its own (``issuer_accounts.joined_with_keys``) this install's rows are not ``offered``,
    whatever the remote catalog answers: the people there work in the remote catalogs, so the Catalog panel and the
    table pickers show only those, while the flow pickers still find the server's own.
    """
    listed = [root for catalog in _remotes.values() for root in catalog.tree(user_id)]
    joined = issuer_accounts.joined_with_keys() is not None
    if not listed and not joined:
        return local
    place = ON_THIS_SERVER if is_server_mode() else ON_THIS_COMPUTER
    return [_marked(node, place=place, offered=not joined) for node in local] + listed


def _marked(node: NamespaceTree, **fields) -> NamespaceTree:
    """``node`` and every namespace under it with ``fields`` set."""
    return node.model_copy(update={**fields, "children": [_marked(child, **fields) for child in node.children]})


def table_row(repo, table_id: int, user_id: int | None = None) -> CatalogTable | CatalogTableOut:
    """The row of the table ``table_id`` from the catalog that keeps it, for a read by ``user_id`` outside a run.

    Raises:
        TableNotFoundError: This install's catalog has no such table.
        ValueError: The remote catalog's sentence: no such table there, or one it does not let this person read.
    """
    catalog = remote_of(table_id)
    if catalog is None:
        table = repo.get_table(table_id)
        if table is None:
            raise TableNotFoundError(table_id=table_id)
        return table
    try:
        return catalog.table(user_id, table_id)
    except LookupError as e:
        raise ValueError(str(e)) from e


def row_targets(
    tables: Sequence[CatalogTable | CatalogTableOut], user_id: int | None = None, purpose: str = "read"
) -> list[CatalogStorageTarget]:
    """The storage target of each of ``tables``, in order, for one read by ``user_id`` outside a run.

    A local row's is ``resolve_for_table``'s. The remote rows are asked of their catalog in one call, when the read
    is made: what it answers is short-lived.

    Raises:
        ValueError: The remote catalog's sentence when it refuses the read.
    """
    remote_rows = [table for table in tables if is_remote(getattr(table, "source", None))]
    found: dict[int, CatalogStorageTarget] = {}
    if remote_rows:
        try:
            answered = remote(remote_rows[0].source).read_targets(user_id, remote_rows, purpose)
        except LookupError as e:
            raise ValueError(str(e)) from e
        found = {table.id: target for table, target in zip(remote_rows, answered, strict=True)}
    return [
        found[table.id] if table.id in found else resolve_for_table(table.file_path, table.namespace_id)
        for table in tables
    ]


def remote_sql_tables(query: str, user_id: int | None, own: set[str]) -> dict[str, CatalogTableOut]:
    """The tables of the remote catalogs by their SQL names, for a ``query`` that reads a name ``own`` does not hold.

    ``own`` are the names this install's catalog answers, which win. A query over this install's own tables asks no
    remote catalog and gets none.
    """
    if user_id is None or not _remotes or not reads_other(query, own):
        return {}
    listed = [root for catalog in _remotes.values() for root in catalog.tree(user_id)]
    return {name: table for name, table in _sql_names(listed).items() if name not in own}


def _sql_names(listed: list[NamespaceTree]) -> dict[str, CatalogTableOut]:
    """Each SQL name of the tables in ``listed`` with its row, named as a local table is: ``catalog.schema.table``
    always, ``schema.table`` and the bare name where ``listed`` has one table by it."""
    full: dict[str, CatalogTableOut] = {}
    short: dict[str, dict[int, CatalogTableOut]] = {}
    for root in listed:
        for holder in (root, *root.children):
            for table in holder.tables:
                parts = [root.name, table.name] if holder is root else [root.name, holder.name, table.name]
                full[".".join(parts)] = table
                for name in (".".join(parts[1:]), table.name):
                    short.setdefault(name, {})[table.id] = table
    names = {name: next(iter(found.values())) for name, found in short.items() if len(found) == 1}
    names.update(full)
    return names


LISTED_SECONDS = 10
_listed: dict[int, tuple[float, list[NamespaceTree]]] = {}


def _kernel_tree(user_id: int) -> list[NamespaceTree]:
    """The remote catalogs of ``user_id`` for a kernel's lookups, kept ``LISTED_SECONDS``: one notebook cell asks
    for them several times in a row."""
    held = _listed.get(user_id)
    if held is None or time.monotonic() - held[0] >= LISTED_SECONDS:
        listed = [root for catalog in _remotes.values() for root in catalog.tree(user_id)]
        held = _listed[user_id] = (time.monotonic(), listed)
    return held[1]


def kernel_namespaces(user_id: int, parent_id: int | None) -> list[NamespaceTree]:
    """The remote catalogs as a kernel of ``user_id`` lists them after this install's own (``parent_id`` ``None``),
    or the schemas of the remote catalog whose row id is ``parent_id``."""
    listed = _kernel_tree(user_id)
    if parent_id is None:
        return listed
    return [schema for root in listed if root.id == parent_id for schema in root.children]


def kernel_tables(user_id: int, namespace_id: int) -> list[CatalogTableOut]:
    """The tables of the remote namespace whose row id is ``namespace_id`` as a kernel lists them: each a view.

    A kernel holds no credential, and reads a view through a file core has the worker write
    (``materialize_virtual_table``): that is how a table on a remote catalog's storage reaches a notebook cell.
    """
    holders = (holder for root in _kernel_tree(user_id) for holder in (root, *root.children))
    return [
        table.model_copy(update={"table_type": "virtual"})
        for holder in holders
        if holder.id == namespace_id
        for table in holder.tables
    ]


def kernel_table(user_id: int, reference: str, namespace_id: int | None = None) -> CatalogTableOut | None:
    """The remote table a notebook cell names by ``reference``, as ``kernel_tables`` lists it; ``None`` when the
    remote catalogs hold none by that name. With the row id of a remote namespace, the table of that name in it."""
    name = plain_reference(reference)
    if namespace_id is not None:
        return next((table for table in kernel_tables(user_id, namespace_id) if table.name == name), None)
    found = _sql_names(_kernel_tree(user_id)).get(name)
    return found.model_copy(update={"table_type": "virtual"}) if found is not None else None


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
