"""Read and write one Iceberg table with the storage keys its catalog hands out.

An Iceberg REST catalog can answer a table load with short-lived keys for that table's files. These helpers pick
those keys out of a loaded table, carry them with the table's metadata location, and give them to Polars, so a
process can read the table without any storage key of its own and without talking to the catalog.

A flow names a table by an ``IcebergTableRef`` (the catalog's address, its warehouse, the table's dotted name and at
most the ``$ffsec$`` ciphertext of the catalog's sign-in). ``scan_iceberg_table`` turns one into a lazy frame whose plan
holds that ref and nothing else: the process that runs the plan asks the catalog for the keys itself.

A sign-in is a bearer token, or a client id and secret that PyIceberg trades for a token at the catalog's token
endpoint and trades again when a token runs out (``client_credentials_sign_in`` writes it, ``sign_in_properties``
reads it). ``list_schemas``, ``list_table_names`` and ``describe_table`` browse a catalog without asking it for any
storage key, and ``explain_catalog_error`` turns a refusal into one sentence a person can act on.

Only S3-compatible storage is handled. The keys are not renewed when they expire: load the table again for new ones.
Every ``pyiceberg`` import is inside a function, so importing this module loads only Polars.
"""

from __future__ import annotations

import importlib.util
import json
import logging
import re
import time
import warnings
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

import polars as pl

if TYPE_CHECKING:
    from polars._typing import SchemaDict
    from pyiceberg.table import Table

MIN_CREDENTIAL_SECONDS = 60
"""A read or write is refused when the catalog's credential has less than this left when it starts."""

CATALOG_TIMEOUT = (10, 60)
"""Seconds to connect to the catalog and to wait for each of its answers; PyIceberg sets no limit of its own."""

INTERACTIVE_TIMEOUT = (5, 20)
"""Seconds to connect and to wait when a person is waiting for the answer (a test, a listing)."""

_TOKEN_PREFIX = "$ffsec$"
_EXPIRY_KEY = "s3.session-token-expires-at-ms"
_PLANNER_WARNING = r".*Planning the Iceberg scan with PyIceberg instead"
_SIGN_IN_KEYS = frozenset({"client_id", "client_secret", "scope", "token_endpoint"})
_NO_KEYS_HEADER = "header.X-Iceberg-Access-Delegation"

_run_log: Callable[[int, int | str, str], None] | None = None

_STORAGE_PREFIXES = ("s3.",)
_STORAGE_KEYS = ("client.region",)
_CATALOG_PREFIXES = ("s3.signer",)


@dataclass(frozen=True)
class IcebergTableAccess:
    """What a reader needs to open one table without its catalog: where its metadata is and the keys to its files.

    The keys are left out of ``repr`` so the object can be logged without leaking them.
    """

    metadata_location: str
    storage_properties: dict[str, str] = field(repr=False)


def storage_properties(table: Table) -> dict[str, str]:
    """The storage settings and keys the catalog gave for this table, and nothing about the catalog itself.

    ``table.io.properties`` is the client's own catalog configuration merged with what the catalog answered, so it
    also holds the catalog's address, its token or client secret and an auth manager object. Only ``s3.*`` entries
    (less ``s3.signer.*``, which name the catalog) and ``client.region`` are kept, as strings. This cannot tell keys
    the catalog answered from ``s3.*`` settings the catalog client was given: load the catalog without any.
    """
    return {
        key: str(value)
        for key, value in table.io.properties.items()
        if (key.startswith(_STORAGE_PREFIXES) and not key.startswith(_CATALOG_PREFIXES)) or key in _STORAGE_KEYS
    }


def _handed_out_keys(table: Table) -> dict[str, str]:
    """The table's storage properties, refused when they hold no key Polars can use on their own."""
    properties = storage_properties(table)
    if properties.get("s3.remote-signing-enabled", "false").lower() == "true":
        raise ValueError(
            "The catalog signs each storage request for this table itself (remote signing), which Polars cannot use."
        )
    if not properties.get("s3.access-key-id"):
        raise ValueError("The catalog handed out no storage keys for this table.")
    return properties


def table_access(table: Table) -> IcebergTableAccess:
    """Everything another process needs to read this table, taken from a table loaded from its catalog."""
    return IcebergTableAccess(table.metadata_location, _handed_out_keys(table))


def scan_table(
    access: IcebergTableAccess, *, reader_override: Literal["native", "pyiceberg"] | None = None
) -> pl.LazyFrame:
    """A lazy scan of the table, opened from its metadata file with the handed-over keys and no catalog.

    Polars translates the ``s3.*`` names to its object store's options itself, and uses the same properties to read
    the metadata file. Build the scan in the process that runs it: a serialized Iceberg scan carries its keys.
    """
    return pl.scan_iceberg(
        access.metadata_location, storage_options=access.storage_properties, reader_override=reader_override
    )


def write_table(table: Table, frame: pl.LazyFrame, mode: Literal["append", "overwrite"]) -> None:
    """Append ``frame`` to the table or replace its rows, writing the files with the keys the catalog gave.

    Polars writes the data files and the commit goes through the table's catalog, so ``table`` must be loaded from
    a catalog: a table opened from its metadata file alone can be read but not written.
    """
    frame.sink_iceberg(table, mode=mode, storage_options=_handed_out_keys(table))


def _refuse_clear_sign_in(token: str | None) -> None:
    if token is not None and not token.startswith(_TOKEN_PREFIX):
        raise ValueError(
            "The catalog's token travels encrypted: pass the stored secret's ciphertext (save the secret again if"
            " it predates encryption per user)."
        )


@dataclass(frozen=True)
class IcebergTableRef:
    """One table as a flow names it: its REST catalog's address and warehouse and its dotted name.

    ``table`` is ``namespace.table``; further dots separate nested namespaces. ``token`` is the ``$ffsec$``
    ciphertext of the catalog's sign-in, a bearer token or a client id and secret, decrypted only where the catalog is
    asked, so a clear sign-in can never be serialized with a plan or a request. Nothing here can hold a storage key:
    those come from the catalog each time.
    """

    catalog_uri: str
    table: str
    warehouse: str | None = None
    token: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        _refuse_clear_sign_in(self.token)
        parts = self.table.split(".")
        if len(parts) < 2 or not all(parts) or re.search(r"[/\s]", self.table):
            raise ValueError(f"An Iceberg table is named namespace.table, not {self.table!r}.")


@dataclass(frozen=True)
class IcebergCatalogRef:
    """A REST catalog as Flowfile reaches it: its address, its warehouse and at most the ciphertext of its sign-in."""

    catalog_uri: str
    warehouse: str | None = None
    token: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        _refuse_clear_sign_in(self.token)

    def table(self, name: str) -> IcebergTableRef:
        return IcebergTableRef(self.catalog_uri, name, self.warehouse, self.token)


@dataclass(frozen=True)
class IcebergTableDescription:
    """A table as its catalog describes it, without reading a file: no row count, nothing from a snapshot."""

    columns: dict[str, pl.DataType]
    format_version: int
    location: str
    partitioned_by: tuple[str, ...]
    updated_at_ms: int | None


def client_credentials_sign_in(
    client_id: str, client_secret: str, scope: str | None = None, token_endpoint: str | None = None
) -> str:
    """The text Flowfile stores for a client id and secret: a small JSON object read back by ``sign_in_properties``."""
    if ":" in client_id:
        raise ValueError("A client id cannot hold ':'; the catalog's sign-in separates the id from the secret by it.")
    body = {"client_id": client_id, "client_secret": client_secret}
    if scope:
        body["scope"] = scope
    if token_endpoint:
        body["token_endpoint"] = token_endpoint
    return json.dumps(body)


def sign_in_properties(catalog_uri: str, sign_in: str) -> dict[str, str]:
    """The PyIceberg properties of a stored sign-in: ``token``, or ``credential``, ``scope`` and the token endpoint.

    A text that is not a JSON object is a bearer token. An object must be one ``client_credentials_sign_in`` wrote, so
    no other property (a storage key among them) can come out of a stored sign-in. The token endpoint is always named,
    the catalog's own when none was given: PyIceberg otherwise warns that it will stop assuming it.
    """
    try:
        parsed = json.loads(sign_in)
    except ValueError:
        parsed = None
    if not isinstance(parsed, dict):
        return {"token": sign_in}
    written = (
        set(parsed) <= _SIGN_IN_KEYS
        and all(isinstance(value, str) for value in parsed.values())
        and bool(parsed.get("client_id"))
        and bool(parsed.get("client_secret"))
        and ":" not in parsed.get("client_id", "")
    )
    if not written:
        raise ValueError("The stored sign-in is not one Flowfile wrote; enter it again.")
    properties = {
        "credential": f"{parsed['client_id']}:{parsed['client_secret']}",
        "oauth2-server-uri": parsed.get("token_endpoint") or f"{catalog_uri.rstrip('/')}/v1/oauth/tokens",
    }
    if parsed.get("scope"):
        properties["scope"] = parsed["scope"]
    return properties


def register_run_log(log: Callable[[int, int | str, str], None] | None) -> None:
    """Set where this process writes the lines a read or write says about itself (flow id, node id, message)."""
    global _run_log
    _run_log = log


def _say(flow_id: int, node_id: int | str, message: str) -> None:
    if _run_log is not None:
        _run_log(flow_id, node_id, message)
    else:
        logging.getLogger(__name__).info(message)


def catalog_properties(ref: IcebergTableRef | IcebergCatalogRef, *, keys: bool = True) -> dict[str, str]:
    """The catalog client's whole configuration: the address, the warehouse and the sign-in in plain, nothing else.

    No ``s3.*`` setting can come out of a ref, so every storage property a loaded table holds is one the catalog
    answered. With ``keys=False`` the catalog is asked to hand out no storage key at all (it may still).
    """
    from shared.cloud_credential_provider import decrypt_stored

    properties = {"uri": ref.catalog_uri}
    if ref.warehouse:
        properties["warehouse"] = ref.warehouse
    if ref.token:
        properties.update(sign_in_properties(ref.catalog_uri, decrypt_stored(ref.token, "the catalog's sign-in")))
    if not keys:
        properties[_NO_KEYS_HEADER] = ""
    return properties


def _timeout() -> tuple[float, float]:
    return CATALOG_TIMEOUT


def _catalog(
    ref: IcebergTableRef | IcebergCatalogRef, *, keys: bool = True, timeout: tuple[float, float] | None = None
):
    """The ref's REST catalog, built directly so no ``~/.pyiceberg.yaml`` or ``PYICEBERG_*`` setting is merged in.

    Every request to it gives up after ``timeout``, else ``CATALOG_TIMEOUT``: a catalog that never answers would
    otherwise hold a run before its first node, where a cancel does not reach. With a client id and secret the
    catalog is asked for a token here, when it is built.
    """
    from pyiceberg.catalog.rest import RestCatalog

    class _TimedRestCatalog(RestCatalog):
        def _create_session(self):
            session = super()._create_session()
            send = session.send
            limit = timeout or _timeout()
            session.send = lambda request, **kw: send(request, **{**kw, "timeout": kw.get("timeout") or limit})
            return session

    return _TimedRestCatalog("flowfile", **catalog_properties(ref, keys=keys))


def _chain(exc: BaseException) -> list[BaseException]:
    """``exc`` and every exception it was raised from or during, outermost first."""
    seen: list[BaseException] = []
    while exc is not None and exc not in seen:
        seen.append(exc)
        exc = exc.__cause__ or exc.__context__
    return seen


def _in_chain(exc: BaseException, *kinds: type[BaseException]) -> BaseException | None:
    return next((link for link in _chain(exc) if isinstance(link, kinds)), None)


def explain_catalog_error(exc: BaseException, ref: IcebergTableRef | IcebergCatalogRef) -> str | None:
    """One sentence for a catalog's refusal a person can act on, ``None`` for any other error.

    No sentence quotes the catalog's own text, so none can repeat a sign-in the catalog echoed.
    """
    from pyiceberg.exceptions import (
        AuthorizationExpiredError,
        ForbiddenError,
        NoSuchNamespaceError,
        OAuthError,
        RESTError,
        UnauthorizedError,
    )
    from requests.exceptions import ConnectionError as RequestsConnectionError
    from requests.exceptions import Timeout

    uri = ref.catalog_uri
    oauth = _in_chain(exc, OAuthError)
    if oauth is not None:
        text = str(oauth)
        if "invalid_scope" in text:
            return (
                f"The catalog at {uri} refused the scope; Polaris and Snowflake Open Catalog take PRINCIPAL_ROLE:ALL."
            )
        if "unexpected JSON" in text or "<?xml" in text or "<html" in text.lower():
            return f"{uri} did not answer as an Iceberg REST catalog."
        return f"The catalog at {uri} refused the client id and secret."
    if _in_chain(exc, UnauthorizedError, AuthorizationExpiredError) is not None:
        if not ref.token:
            return f"The catalog at {uri} asks for a sign-in."
        return (
            f"The catalog at {uri} refused the sign-in: it is wrong or has run out. A token is not renewed;"
            " a client id and secret is."
        )
    if _in_chain(exc, ForbiddenError) is not None:
        return f"The catalog at {uri} knows this sign-in and does not allow it this."
    if _in_chain(exc, RequestsConnectionError, Timeout) is not None:
        return f"No catalog answers at {uri}."
    missing = _in_chain(exc, RESTError, NoSuchNamespaceError)
    if missing is not None and "warehouse" in str(missing).lower():
        return f"The catalog at {uri} has no warehouse {ref.warehouse}."
    if _in_chain(exc, RESTError) is not None:
        return f"{uri} did not answer as an Iceberg REST catalog."
    return None


def _refusal(exc: BaseException, ref: IcebergTableRef | IcebergCatalogRef) -> ValueError:
    """The sentence for ``exc`` as a ``ValueError``, naming only the error's class when there is no sentence."""
    from shared.cloud_credential_provider import CredentialDecryptionError

    if isinstance(exc, ValueError):
        return exc
    if isinstance(exc, CredentialDecryptionError):
        return ValueError(str(exc))
    sentence = explain_catalog_error(exc, ref)
    return ValueError(sentence or f"The catalog at {ref.catalog_uri} could not be asked ({type(exc).__name__}).")


def load_table(ref: IcebergTableRef) -> Table:
    """The table loaded from its catalog, which hands out the keys to its files with it."""
    from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

    try:
        return _catalog(ref).load_table(ref.table)
    except (NoSuchTableError, NoSuchNamespaceError):
        raise ValueError(f"The catalog at {ref.catalog_uri} has no table {ref.table}.") from None
    except Exception as exc:
        sentence = explain_catalog_error(exc, ref)
        if sentence is None:
            raise
        raise ValueError(sentence) from None


def list_schemas(ref: IcebergCatalogRef) -> tuple[list[str], int]:
    """The catalog's top-level namespaces Flowfile shows, sorted, and how many it leaves out.

    Left out: a name holding a dot and every nested namespace (Flowfile names a table ``catalog.schema.table``). The
    nested ones are counted one call per shown namespace; a catalog that refuses that call adds nothing to the count.
    No storage key is asked for. A refusal is a ``ValueError`` with one sentence.
    """
    try:
        catalog = _catalog(ref, keys=False, timeout=INTERACTIVE_TIMEOUT)
        names = catalog.list_namespaces()
    except Exception as exc:
        raise _refusal(exc, ref) from None
    shown = sorted(name[0] for name in names if len(name) == 1 and "." not in name[0])
    left_out = len(names) - len(shown)
    for name in shown:
        try:
            left_out += len(catalog.list_namespaces(name))
        except Exception:  # noqa: BLE001
            continue
    return shown, left_out


def list_table_names(ref: IcebergCatalogRef, schema: str) -> list[str]:
    """The names of the tables in one namespace, sorted, as the catalog lists them now; no storage key asked for."""
    from pyiceberg.exceptions import NoSuchNamespaceError

    try:
        identifiers = _catalog(ref, keys=False, timeout=INTERACTIVE_TIMEOUT).list_tables(schema)
    except NoSuchNamespaceError:
        raise ValueError(f"The catalog at {ref.catalog_uri} has no schema {schema}.") from None
    except Exception as exc:
        raise _refusal(exc, ref) from None
    return sorted(identifier[-1] for identifier in identifiers)


def describe_table(ref: IcebergTableRef) -> IcebergTableDescription:
    """A table's columns as Polars types and its metadata, from the catalog's description alone.

    The table is loaded without asking for storage keys and no file is read, so no ``scan_iceberg`` runs here. Nothing
    comes from a snapshot or its summary: a summary's row count is wrong after deletes.
    """
    from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

    try:
        table = _catalog(ref, keys=False, timeout=INTERACTIVE_TIMEOUT).load_table(ref.table)
    except (NoSuchTableError, NoSuchNamespaceError):
        raise ValueError(f"The catalog at {ref.catalog_uri} has no table {ref.table}.") from None
    except Exception as exc:
        raise _refusal(exc, ref) from None
    schema = table.schema()
    columns = dict(pl.from_arrow(schema.as_arrow().empty_table()).schema)
    partitioned = dict.fromkeys(schema.find_column_name(part.source_id) for part in table.spec().fields)
    return IcebergTableDescription(
        columns=columns,
        format_version=int(table.metadata.format_version),
        location=table.metadata.location,
        partitioned_by=tuple(name for name in partitioned if name),
        updated_at_ms=table.metadata.last_updated_ms,
    )


def table_schema(ref: IcebergTableRef) -> pl.Schema:
    """The table's columns as Polars types, from the catalog's description of the table and no storage read.

    The catalog may hand out a credential with that description (Lakekeeper always does); it is dropped with the table
    object when this returns.
    """
    table = load_table(ref)
    return pl.scan_iceberg(table, storage_options=storage_properties(table)).collect_schema()


def metadata_fingerprint(ref: IcebergTableRef) -> str:
    """The table's current metadata file as its catalog reports it: every commit, by any engine, writes a new one."""
    return f"{ref.catalog_uri}#{ref.table}@{load_table(ref).metadata_location}"


def credential_seconds_left(properties: Mapping[str, str], now: float | None = None) -> float | None:
    """Seconds until the handed-out credential expires; ``None`` when the catalog gave no expiry."""
    expires = properties.get(_EXPIRY_KEY)
    if not expires:
        return None
    return int(expires) / 1000 - (time.time() if now is None else now)


def refuse_short_credential(
    properties: Mapping[str, str], table: str, minimum_seconds: float, purpose: Literal["read", "write"]
) -> float | None:
    """The seconds left on the credential, refused when fewer than ``minimum_seconds``: it is not renewed mid-run."""
    left = credential_seconds_left(properties)
    if left is not None and left < minimum_seconds:
        raise ValueError(
            f"The catalog's credential for {table} runs out in {max(int(left), 0)} seconds, too soon to {purpose} it;"
            " run again in a moment."
        )
    return left


def _quiet_planner() -> None:
    """Silence Polars' warning that PyIceberg plans the scan while the ``polars_iceberg`` plugin is not installed.

    Without the plugin PyIceberg plans every scan anyway; the warning only suggests installing it. A warnings filter
    rather than ``POLARS_ICEBERG_PLANNER``, because a scan may be built on any thread and the environment must not be
    written off the main thread. Called once, when this module is imported.
    """
    if importlib.util.find_spec("polars_iceberg") is None:
        warnings.filterwarnings("ignore", message=_PLANNER_WARNING, category=pl.exceptions.PerformanceWarning)


_quiet_planner()


def _credential_line(verb: str, table: str, properties: Mapping[str, str], left: float | None) -> str:
    storage = (properties.get("s3.endpoint") or "").rstrip("/") or "its catalog's storage"
    valid = "" if left is None else f", valid for {int(left // 60)} more minutes"
    return f"{verb} {table} {'from' if verb == 'Reading' else 'to'} {storage} with the catalog's credential{valid}."


def _native_failure(exc: Exception) -> str | None:
    """Why Polars' own reader refused the table, when ``exc`` is that refusal; ``None`` for any other error."""
    first = str(exc).splitlines()[0] if str(exc) else ""
    if not isinstance(exc, pl.exceptions.ComputeError) or "reader_override='native' failed" not in first:
        return None
    reason = first.split("failed: ", 1)[-1]
    return re.sub(r":\s*\S+://\S*$", "", reason).strip()


def _equality_deletes(table: str, exc: BaseException) -> ValueError | None:
    if "equality delete" in str(exc).lower():
        return ValueError(
            f"{table} has equality deletes (rows deleted by value, as Flink writes them), which neither Polars nor"
            " PyIceberg reads yet."
        )
    return None


@dataclass(frozen=True)
class _TableScan:
    """The source of a ``scan_iceberg_table`` plan: the ref and the run's ids, and nothing that could hold a key.

    It is a module-level class so a plan pickles it by reference and its fields: the keys are asked for when it is
    called, in the process that reads.
    """

    ref: IcebergTableRef
    flow_id: int
    node_id: int | str
    minimum_seconds: float

    def __call__(
        self, with_columns: list[str] | None, predicate: pl.Expr | None, n_rows: int | None, batch_size: int | None
    ) -> Iterator[pl.DataFrame]:
        name = self.ref.table
        access = table_access(load_table(self.ref))
        left = refuse_short_credential(access.storage_properties, name, self.minimum_seconds, "read")
        _say(self.flow_id, self.node_id, _credential_line("Reading", name, access.storage_properties, left))
        yielded = False
        try:
            for batch in self._batches(access, "native", with_columns, predicate, n_rows, batch_size):
                yielded = True
                yield batch
            return
        except Exception as exc:
            refusal = _equality_deletes(name, exc)
            if refusal is not None:
                raise refusal from None
            reason = _native_failure(exc)
            if yielded or reason is None:
                raise
        _say(
            self.flow_id,
            self.node_id,
            f"Polars' own reader cannot read {name} ({reason}); reading it with PyIceberg, which is slower.",
        )
        try:
            yield from self._batches(access, "pyiceberg", with_columns, predicate, n_rows, batch_size)
        except Exception as exc:
            refusal = _equality_deletes(name, exc)
            if refusal is not None:
                raise refusal from None
            raise

    @staticmethod
    def _batches(
        access: IcebergTableAccess,
        reader: Literal["native", "pyiceberg"],
        with_columns: list[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None,
    ) -> Iterator[pl.DataFrame]:
        frame = scan_table(access, reader_override=reader)
        if with_columns is not None:
            frame = frame.select(with_columns)
        if predicate is not None:
            frame = frame.filter(predicate)
        if n_rows is not None:
            frame = frame.head(n_rows)
        yield from frame.collect_batches(chunk_size=batch_size)


def scan_iceberg_table(
    ref: IcebergTableRef,
    *,
    schema: SchemaDict | None = None,
    flow_id: int = -1,
    node_id: int | str = -1,
    minimum_seconds: float = MIN_CREDENTIAL_SECONDS,
) -> pl.LazyFrame:
    """A lazy scan of the table whose plan holds ``ref`` and nothing else, so it can be serialized and sent.

    Where the plan runs, the catalog is asked for the table and its keys, columns and filters are pushed into the
    Iceberg scan and rows stream in batches. Polars' own reader reads first; when it refuses the table (position
    delete files it cannot apply), PyIceberg reads it and the run log says so. Without ``schema`` the columns are read
    from the catalog here.
    """
    return pl.io.plugins.register_io_source(
        _TableScan(ref, flow_id, node_id, minimum_seconds),
        schema=dict(schema if schema is not None else table_schema(ref)),
        explain_name="ICEBERG",
        explain_detail=ref.table,
    )


def creatable_schema(schema: Mapping[str, pl.DataType]) -> dict[str, pl.DataType]:
    """The columns a new table gets for a frame of ``schema``: ``UInt32`` widened to ``Int64``, ``UInt64`` refused.

    Iceberg has no unsigned integers, and PyIceberg would map both to a signed type of the same width.
    """
    columns: dict[str, pl.DataType] = {}
    for name, dtype in schema.items():
        if dtype == pl.UInt64:
            raise ValueError(f"Column {name} is UInt64, which no Iceberg type holds in full; cast it first.")
        columns[name] = pl.Int64() if dtype == pl.UInt32 else dtype
    return columns


def _load_or_create(ref: IcebergTableRef, frame: pl.LazyFrame) -> Table:
    from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

    try:
        catalog = _catalog(ref)
        return catalog.load_table(ref.table)
    except (NoSuchTableError, NoSuchNamespaceError):
        pass
    except Exception as exc:
        sentence = explain_catalog_error(exc, ref)
        if sentence is None:
            raise
        raise ValueError(sentence) from None
    namespace = ref.table.rsplit(".", 1)[0]
    if not catalog.namespace_exists(namespace):
        raise ValueError(f"The catalog has no namespace {namespace}; create it there first.")
    empty = pl.DataFrame(schema=creatable_schema(frame.collect_schema()))
    catalog.create_table(ref.table, schema=empty.to_arrow(compat_level=pl.CompatLevel.oldest()).schema)
    return catalog.load_table(ref.table)


def refuse_unwritable(table: Table) -> None:
    """Refuse a table this Flowfile cannot write: Polars writes no manifest of an Iceberg version 3 table."""
    if table.metadata.format_version >= 3:
        name = ".".join(table.name())
        raise ValueError(f"{name} is an Iceberg version 3 table, which this Flowfile reads and cannot write yet.")


def _data_folders(table: Table) -> set[str] | None:
    """The immediate children of the table's ``data/`` folder, listed with the handed-out keys, or ``None``."""
    from urllib.parse import urlparse

    data = f"{table.metadata.location.rstrip('/')}/data"
    parsed = urlparse(data)
    fs_by_scheme = getattr(table.io, "fs_by_scheme", None)
    if fs_by_scheme is None:
        return None
    try:
        from pyarrow.fs import FileSelector

        fs = fs_by_scheme(parsed.scheme, parsed.netloc)
        infos = fs.get_file_info(FileSelector(f"{parsed.netloc}{parsed.path}", allow_not_found=True))
    except Exception:
        return None
    return {f"{parsed.scheme}://{info.path}" for info in infos}


def _sink(table: Table, frame: pl.LazyFrame, mode: Literal["append", "overwrite"]) -> None:
    """Write and commit; on a failure, name what the attempt left under the table's ``data/`` folder.

    An overwrite is one commit of two snapshots (a delete and an append), and the delete files earlier writes left stay
    listed. A table created for this write stays, empty, when it fails. The folders new under ``data/`` are named and
    not removed: a listing cannot tell this write's files from those of another write that ran at the same moment.
    """
    before = _data_folders(table)
    try:
        write_table(table, frame, mode)
    except Exception as exc:
        cause = _without_sign_in((str(exc).splitlines() or [type(exc).__name__])[0], table)
        name = ".".join(table.name())
        if _sign_in_refused(exc):
            raise RuntimeError(
                f"The write to {name} did not commit: the catalog refused the sign-in when the write finished, most"
                " likely a token that ran out meanwhile. A client id and secret is renewed during a run; a token is"
                f" not.{_left_behind(table, before)}"
            ) from None
        if _commit_state_unknown(exc):
            raise RuntimeError(
                f"The write to {name} may not have committed ({cause}): the catalog did not say. Load the table before"
                " removing anything."
            ) from exc
        raise RuntimeError(f"The write to {name} did not commit ({cause}).{_left_behind(table, before)}") from exc


def _left_behind(table: Table, before: set[str] | None) -> str:
    """What a failed write left under the table's ``data/`` folder, as the sentence that ends its error."""
    after = _data_folders(table)
    if before is None or after is None:
        return " The files it may have left could not be listed."
    new = sorted(after - before)
    if not new:
        return " It left no files under data/."
    shown = ", ".join(new[:5]) + (f" and {len(new) - 5} more" if len(new) > 5 else "")
    return (
        f" New under its data/ folder since this write began: {shown}. If no other write ran at the same time"
        " they are this write's files, which the table does not use; nothing removes them."
    )


def _without_sign_in(text: str, table: Table) -> str:
    """``text`` with the catalog's token, client secret and their pair masked, as they sit in the table's client."""
    properties = getattr(table.io, "properties", {}) or {}
    values = [str(properties.get(name) or "") for name in ("token", "credential")]
    values += [value.split(":", 1)[-1] for value in values if ":" in value]
    for value in sorted({value for value in values if len(value) >= 4}, key=len, reverse=True):
        text = text.replace(value, "***")
    return text


def _sign_in_refused(exc: BaseException) -> bool:
    """Whether the catalog refused the sign-in: it said so with a 401 or at its token endpoint, so nothing committed."""
    from pyiceberg.exceptions import AuthorizationExpiredError, OAuthError, UnauthorizedError

    return _in_chain(exc, UnauthorizedError, AuthorizationExpiredError, OAuthError) is not None


def _commit_state_unknown(exc: BaseException) -> bool:
    """Whether the commit may have gone through: the catalog answered 5xx, or the connection dropped mid-request."""
    from pyiceberg.exceptions import CommitStateUnknownException
    from requests.exceptions import ConnectionError as RequestsConnectionError
    from requests.exceptions import Timeout

    return _in_chain(exc, CommitStateUnknownException, RequestsConnectionError, Timeout) is not None


def write_iceberg_table(
    ref: IcebergTableRef,
    frame: pl.LazyFrame,
    mode: Literal["append", "overwrite"],
    *,
    flow_id: int = -1,
    node_id: int | str = -1,
    minimum_seconds: float = MIN_CREDENTIAL_SECONDS,
) -> None:
    """Append ``frame`` to the ref's table or replace its rows, creating the table when the catalog has none.

    The catalog is asked for the table and its keys here, in the process that writes. A missing table is created
    first and stays, empty, if a check then fails; no data file is written before every check passed: the table is
    writable, the keys are usable and have time left, and the frame has the table's column names. The frame is cast
    to the table's own types (strictly: a value that does not fit fails the write), and the table's columns and
    partitioning are never changed.
    """
    table = _load_or_create(ref, frame)
    refuse_unwritable(table)
    properties = _handed_out_keys(table)
    left = refuse_short_credential(properties, ref.table, minimum_seconds, "write")
    columns = pl.scan_iceberg(table, storage_options=properties).collect_schema()
    given = list(frame.collect_schema())
    if sorted(given) != sorted(columns):
        raise ValueError(
            f"The table has columns {list(columns)} and the data has {given}; a write here does not change a table's"
            " columns."
        )
    _say(flow_id, node_id, _credential_line("Writing", ref.table, properties, left))
    _sink(table, frame.cast(dict(columns)).select(list(columns)), mode)
