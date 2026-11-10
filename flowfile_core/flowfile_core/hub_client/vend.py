"""The credential a run over the hub's tables gets from the hub: asked once per run, held in memory, never kept.

A flow that reads or writes tables on the hub holds no credential for them. When it runs, ``request`` asks the hub for
one per storage root the run's namespaces live under (``POST /hub/vend``, which the hub records against the run id),
and the graph holds the answer for that run only. A read's plan carries the options without their secrets and an
``EncryptedCredentialProvider`` encrypted for the user; a write's payload carries a cloud connection whose secrets are
encrypted for the worker. Both travel as a local cloud connection's credentials do, and the worker decrypts them with
the master key it shares with core. Nothing here is cached, stored or logged beyond the hub's URL and the status.

A server that joined its hub with keys of its own has its own master key, so what it sends a server of the hub's
must be opened there instead: a run that places such a step on a server of the hub asks with ``targets``, the hub
names of the servers its steps run on (``node_name``), and the hub answers each credential with no options and a map
of node name to a token sealed to that node (``shared.hub_sealed``). A desktop connected to the hub shares its master
key with no server of the hub either, so a run that places a hub-table step on one asks the same way. Core hands the
map on unopened: a read's plan scans through a Python source that opens it where the plan runs, a write's connection
carries it, and the exchange store's crossings send it (``SealedCredential``). Such a vend also names the secrets the
run's steps on those servers use (``secrets``), and the hub answers each sealed the same way: a step carries
``sealed_secrets``' strings in place of its ``$ffsec$`` ciphertext, and the server that runs it opens them. It
names the stored connections those steps use as well (``connections``, a kind and a name each), which the hub keeps
for a server joined with keys and answers sealed the same way (``sealed_connections``).
"""

import uuid
from collections.abc import Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace

import polars as pl
from pydantic import SecretStr

from flowfile_core.catalog import table_sources
from flowfile_core.catalog.storage_backend import _is_cloud_uri
from flowfile_core.flowfile.execution.protocol import EncryptedCredential, SealedCredential
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import CloudStorageReader
from flowfile_core.hub_client import catalog, client
from flowfile_core.hub_client.catalog import HubNamespace
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused
from flowfile_core.schemas.catalog_schema import CatalogTableOut, NamespaceTree
from flowfile_core.schemas.cloud_storage_schemas import (
    FullCloudStorageConnection,
    FullCloudStorageConnectionWorkerInterface,
)
from shared import hub_sealed
from shared.cloud_storage.storage_options import ALLOW_INVALID_CERTIFICATES

REFUSED = "The hub did not give this run access to its tables"
_S3_FIELDS = ("aws_access_key_id", "aws_region", "endpoint_url")
_S3_SECRETS = ("aws_secret_access_key", "aws_session_token")


@dataclass(frozen=True)
class HubCredential:
    """One credential the hub vended to a run, for the tables under ``base_uri``.

    ``storage_options`` are what Polars and deltalake take, secrets included; ``repr`` never shows them. A vend with
    targets leaves them empty and maps each target node to a token sealed to it in ``sealed``. The hub's answer also
    carries the credential's expiry and the prefixes it may read and write; the hub enforces those, so nothing here
    keeps them.
    """

    base_uri: str
    storage_options: dict[str, str]
    sealed: dict[str, str] = field(default_factory=dict)

    def __repr__(self) -> str:
        return f"HubCredential(base_uri={self.base_uri!r}, sealed={bool(self.sealed)!r})"

    def covers(self, uri: str) -> bool:
        base = self.base_uri.rstrip("/")
        return uri.rstrip("/") == base or uri.startswith(f"{base}/")


@dataclass(frozen=True)
class RunVend:
    """What the hub vended to the run ``run_id``: its ``vend_id``, the credentials, and where each namespace lives.

    The run's steps name a hub namespace by its qualified ``catalog.schema``; ``bases`` is the storage root of each
    such name the run asked for and ``ids`` the hub's own id of it, both resolved on the hub for this run.
    ``targets`` are the hub servers the vend was sealed to; empty for a vend the run's own worker opens with core's
    master key. ``secrets`` maps each secret name the run asked for to its map of node name to sealed token,
    ``connections`` each ``(kind, name)`` of a stored connection to its map, and ``connection_fields`` each to the
    names of the secret fields the hub says it holds.

    ``own`` is the unsealed vend of the same run that a machine which is no server of the hub asks beside a sealed one,
    for the steps on the hub's tables that run on the worker beside its core: a table on the hub reads and writes on
    whichever machine its step runs (``here``).
    """

    run_id: str
    vend_id: int
    credentials: tuple[HubCredential, ...]
    bases: dict[str, str]
    ids: dict[str, int]
    targets: tuple[str, ...] = ()
    secrets: dict[str, dict[str, str]] = field(default_factory=dict, repr=False)
    connections: dict[tuple[str, str], dict[str, str]] = field(default_factory=dict, repr=False)
    connection_fields: dict[tuple[str, str], tuple[str, ...]] = field(default_factory=dict, repr=False)
    own: "RunVend | None" = field(default=None, repr=False)

    def sealed_secrets(self, names: Iterable[str]) -> dict[str, str]:
        """Each of ``names`` the hub sealed to this run, as the string a step carries in place of its ciphertext."""
        return {name: hub_sealed.sealed_secret(name, self.secrets[name]) for name in names if name in self.secrets}

    def sealed_connections(self, refs: Iterable[tuple[str, str]]) -> dict[tuple[str, str], dict[str, str]]:
        """The sealed map of each ``(kind, name)`` of ``refs`` the hub sealed to this run."""
        return {ref: dict(self.connections[ref]) for ref in refs if ref in self.connections}

    def secret_fields(self, refs: Iterable[tuple[str, str]]) -> dict[tuple[str, str], tuple[str, ...]]:
        """The names of the secret fields the hub says each ``(kind, name)`` of ``refs`` holds; never a value."""
        return {ref: self.connection_fields[ref] for ref in refs if ref in self.connection_fields}

    @property
    def sealed(self) -> bool:
        """Whether the hub sealed this vend to its target servers, so no credential holds options here."""
        return bool(self.targets)

    def sealed_for(self, backend_id: str) -> bool:
        """Whether a step on ``backend_id`` opens this run's credentials sealed: the vend is sealed, and the backend
        is not this machine's own worker with an unsealed vend of its own (``own``)."""
        return self.sealed and (self.own is None or node_name(backend_id) is not None)

    def here(self) -> "RunVend":
        """The vend the step running now opens: this one, or ``own`` when the step runs on this machine's own worker
        and the run holds an unsealed vend for it."""
        from flowfile_core.flowfile.execution.context import current_context

        if self.own is None or self.sealed_for(current_context().backend_id):
            return self
        return self.own

    def sealed_credential(self, uri: str) -> SealedCredential:
        """The sealed map of the credential for ``uri``, as an export, an import or a write's connection sends it.

        Raises:
            LookupError: No credential of this run covers ``uri``, or this vend was not sealed.
        """
        credential = self.for_uri(uri)
        if not credential.sealed:
            raise LookupError(f"The hub sealed this run no credential for {uri}")
        return SealedCredential(base_uri=credential.base_uri, sealed=dict(credential.sealed))

    def encrypted_credential(self, uri: str, user_id: int | None) -> EncryptedCredential:
        """The unsealed credential for ``uri`` as an export or an import on the worker beside this core takes it: its
        options without their secrets, and the secrets encrypted for ``user_id`` as a scan's are (``scan_kwargs``).

        Raises:
            LookupError: No credential of this run covers ``uri``.
        """
        credential = self.for_uri(uri)
        kwargs = self.scan_kwargs(uri, user_id)
        provider = kwargs.get("credential_provider")
        return EncryptedCredential(
            base_uri=credential.base_uri,
            options=dict(kwargs.get("storage_options") or {}),
            encrypted=provider.encrypted_credentials if provider is not None else "",
        )

    def exchange_credentials(
        self, uri: str, user_id: int | None
    ) -> tuple[SealedCredential | None, EncryptedCredential | None]:
        """The exchange store at ``uri`` as the run's backends open it: sealed to the run's servers when this vend is
        sealed, and, on a machine that is no server of the hub, encrypted for the worker beside its core."""
        from flowfile_core.flowfile.execution import registry

        sealed = self.sealed_credential(uri) if self.targets else None
        if node_name(registry.WORKER_BACKEND_ID) is not None:
            return sealed, None
        return sealed, (self.own or self).encrypted_credential(uri, user_id)

    def scan_delta(self, uri: str, user_id: int | None, schema: Mapping[str, pl.DataType]) -> pl.LazyFrame:
        """A lazy scan of the Delta table at ``uri`` with the run's credential, its plan holding no secret.

        Unsealed, the plan carries the options without their secrets and the secrets encrypted for ``user_id``
        (``scan_kwargs``). Sealed, core knows no option of the store, not even its endpoint, so the plan reads the table
        through a Python source that opens the map where the plan runs, with ``schema`` as its columns. Which of the
        two is the vend of the step running now (``here``).
        """
        here = self.here()
        if here.sealed:
            return hub_sealed.scan_delta_sealed(uri, here.sealed_credential(uri).sealed, schema)
        return pl.scan_delta(uri, **here.scan_kwargs(uri, user_id))

    def for_namespace(self, namespace: str) -> HubCredential:
        """The credential for the tables of the hub namespace ``namespace`` (``catalog.schema``).

        Raises:
            LookupError: The run did not ask for that namespace, or the hub gave no credential for its root.
        """
        if namespace not in self.bases:
            raise LookupError(f"This run did not ask the hub for namespace {namespace}")
        return self.for_uri(self.bases[namespace])

    def namespace_id(self, namespace: str) -> int:
        """The hub's own id of the namespace ``namespace`` (``catalog.schema``), as the hub answered it for this run.

        Raises:
            LookupError: The run did not ask for that namespace.
        """
        if namespace not in self.ids:
            raise LookupError(f"This run did not ask the hub for namespace {namespace}")
        return self.ids[namespace]

    def for_uri(self, uri: str) -> HubCredential:
        """The credential whose ``base_uri`` holds ``uri``.

        Raises:
            LookupError: No credential of this run covers ``uri``.
        """
        for credential in self.credentials:
            if credential.covers(uri):
                return credential
        raise LookupError(f"The hub gave this run no credential for {uri}")

    def scan_kwargs(self, uri: str, user_id: int | None) -> dict:
        """``storage_options`` and ``credential_provider`` for a scan of ``uri``: the secrets encrypted for ``user_id``.

        The options the plan carries hold no secret, so a serialized plan does not either.
        """
        return CloudStorageReader.get_secure_scan_kwargs(self.for_uri(uri).storage_options, user_id)

    def worker_connection(self, uri: str, user_id: int) -> FullCloudStorageConnectionWorkerInterface:
        """The credential for ``uri`` as an S3 access-key connection, its secrets encrypted for the worker.

        A sealed vend's connection carries the map instead and nothing else, opened by the server that writes.

        Raises:
            ValueError: The credential is for storage other than S3.
        """
        here = self.here()
        credential = here.for_uri(uri)
        scheme = catalog.scheme(credential.base_uri)
        if scheme != "s3":
            raise ValueError(catalog.other_scheme(scheme))
        if here.sealed:
            return FullCloudStorageConnectionWorkerInterface(
                storage_type="s3",
                auth_method="access_key",
                connection_name=None,
                sealed=here.sealed_credential(uri).sealed,
            )
        options = credential.storage_options
        fields: dict = {name: options[name] for name in _S3_FIELDS if options.get(name)}
        fields.update({name: SecretStr(options[name]) for name in _S3_SECRETS if options.get(name)})
        if "aws_allow_http" in options:
            fields["aws_allow_unsafe_html"] = str(options["aws_allow_http"]).lower() == "true"
        if ALLOW_INVALID_CERTIFICATES in options:
            fields["verify_ssl"] = str(options[ALLOW_INVALID_CERTIFICATES]).lower() != "true"
        connection = FullCloudStorageConnection(
            storage_type="s3", auth_method="access_key", connection_name=None, **fields
        )
        return connection.get_worker_interface(user_id)


_held: dict[str, RunVend] = {}


def hold(run_vend: RunVend) -> None:
    """Keep ``run_vend`` for its run, under the run's id; ``release`` forgets it when the run ends."""
    _held[run_vend.run_id] = run_vend


def held(run_id: str | None) -> RunVend | None:
    """What the hub vended to the run ``run_id``; ``None`` for a run that asked nothing, and once it ended."""
    return _held.get(run_id) if run_id else None


def release(run_id: str) -> None:
    """Forget what the hub vended to the run ``run_id``."""
    _held.pop(run_id, None)


@dataclass(frozen=True)
class VendedTarget:
    """A run's vend as the storage target of the tables under ``base``, as ``CatalogStorageTarget`` is for a local
    catalog's: what the engine's readers and writers take, whichever catalog keeps the table.

    Nothing is resolved until it is used, and then for the step running now (``RunVend.here``). A sealed vend has no
    options core can open, so its scan is the sealed source and its worker connection carries the map.
    """

    run_vend: RunVend = field(repr=False)
    base: str
    user_id: int | None = None
    is_cloud: bool = True
    connection_name: str | None = None

    @property
    def storage_options(self) -> dict[str, str]:
        here = self.run_vend.here()
        return {} if here.sealed else dict(here.for_uri(self.base).storage_options)

    @property
    def worker_interface(self) -> FullCloudStorageConnectionWorkerInterface:
        return self.run_vend.worker_connection(self.base, self.user_id)

    def to_worker_payload(self) -> dict:
        return {"base_uri": self.base, "connection": self.worker_interface.model_dump()}

    def scan_kwargs(self, user_id: int | None) -> dict:
        return self.run_vend.here().scan_kwargs(self.base, user_id)

    def scan_delta(self, path: str, user_id: int | None, columns: Mapping[str, pl.DataType], **kwargs) -> pl.LazyFrame:
        here = self.run_vend.here()
        if here.sealed:
            return hub_sealed.scan_delta_sealed(path, here.sealed_credential(path).sealed, columns)
        return pl.scan_delta(path, **kwargs, **here.scan_kwargs(path, user_id))


def _physical(table: dict, namespace_name) -> dict:
    """``table``, a row the hub answered, when this Flowfile can read it: a physical table on object storage."""
    if table.get("table_type", "physical") != "physical":
        raise ValueError("A virtual table on the hub cannot be read from this Flowfile")
    if not table.get("file_path") or not _is_cloud_uri(table["file_path"]):
        raise ValueError(catalog.on_own_disk(namespace_name()))
    return table


class _RunCatalog:
    """The hub's catalog as ``table_sources.RemoteCatalog``: a run's targets, answered from the vend held for the
    run, and the reads outside a run, each answered from a vend of its own."""

    def run_holds(self, run_id: str | None) -> bool:
        return held(run_id) is not None

    def run_starts(self, start: table_sources.RunStart) -> RunVend:
        """The run's one vend: its tables' credentials, the secrets and connections its steps use and the exchange
        store, sealed to ``start.servers``. A machine that is no server of the hub also asks an unsealed vend for the
        steps that run on the worker beside its core (``RunVend.own``). Held under the run's id."""
        from flowfile_core.flowfile.execution import registry

        def schemas(tables, writes: bool) -> set[str]:
            return {name for name, written, _ in tables if written is writes}

        run_vend = request(
            start.user_id,
            start.run_id,
            schemas(start.tables, False),
            schemas(start.tables, True),
            targets=start.servers,
            exchange=start.exchange,
            secrets=sorted(start.secrets),
            connections=sorted(start.connections),
        )
        beside = [table for table in start.tables if node_name(table[2]) is None]
        if start.servers and node_name(registry.WORKER_BACKEND_ID) is None and (beside or start.exchange):
            own = request(
                start.user_id, start.run_id, schemas(beside, False), schemas(beside, True), exchange=start.exchange
            )
            run_vend = replace(run_vend, own=own)
        hold(run_vend)
        return run_vend

    def run_grant(self, run_id: str | None) -> RunVend | None:
        return held(run_id)

    def run_ends(self, run_id: str) -> None:
        release(run_id)

    def table_named(self, user_id: int | None, qualified_name: str) -> dict:
        """The hub's table ``qualified_name`` (``catalog.schema.table``, as a reader step names it), which must be a
        physical table on object storage (``_physical``)."""
        return _physical(catalog.table_named(user_id, qualified_name), lambda: qualified_name.rpartition(".")[0])

    def table(self, user_id: int | None, table_id: int) -> CatalogTableOut:
        """The hub's table whose row in the catalog tree is ``table_id``, as that row, for a read outside a run."""
        found = catalog.table(user_id, -table_id)
        _physical(found, lambda: catalog.locate(user_id, found["namespace_id"]).full_name)
        return CatalogTableOut.model_validate(catalog.marked(found))

    def read_targets(self, user_id: int | None, tables: Sequence[CatalogTableOut], purpose: str) -> list[VendedTarget]:
        """The storage target of each of ``tables``, rows of the catalog tree, for one read outside a run: the
        Catalog page's preview and history, the SQL editor, a visual, a notebook cell.

        The hub vends one read of the tables' schemas to ``user_id`` now (``request_for_tables``) under a run id of
        its own that starts with ``purpose`` (``preview-…``, ``sql-…``, ``visual-…``, ``notebook-…``, which the hub's
        vend ledger keeps). The credential lives for the call, or as long as the plan built from the target carries
        it, encrypted.
        """
        vended = request_for_tables(user_id, f"{purpose}-{uuid.uuid4().hex}", tables)
        return [VendedTarget(vended, row.file_path.rstrip("/").rpartition("/")[0], user_id) for row in tables]

    def tree(self, user_id: int | None) -> list[NamespaceTree]:
        """The person's hub catalogs with their schemas and tables, read now; empty without a hub session, or when
        the hub does not answer."""
        return catalog.tree(user_id) if user_id is not None else []

    def table_target(self, run_id: str | None, user_id: int | None, file_path: str) -> VendedTarget:
        run_vend = held(run_id)
        if run_vend is None:
            raise ValueError("Run the flow to read a table on the hub")
        return VendedTarget(run_vend, file_path.rstrip("/").rpartition("/")[0], user_id)

    def schema_target(self, run_id: str | None, user_id: int | None, schema: str) -> VendedTarget:
        run_vend = held(run_id)
        if run_vend is None:
            raise ValueError("Run the flow to write a table on the hub")
        return VendedTarget(run_vend, run_vend.for_namespace(schema).base_uri, user_id)

    def schema_table(self, run_id: str | None, user_id: int | None, schema: str, name: str) -> dict | None:
        namespace_id = self.schema_target(run_id, user_id, schema).run_vend.namespace_id(schema)
        hub_url, access_token = catalog.signed_in(user_id)
        tables = client.list_tables(hub_url, access_token, namespace_id=namespace_id)
        return next(({"namespace_id": namespace_id, **table} for table in tables if table["name"] == name), None)

    def register_table(self, run_id: str | None, user_id: int | None, schema: str, table: dict) -> None:
        namespace_id = self.schema_target(run_id, user_id, schema).run_vend.namespace_id(schema)
        try:
            hub_url, access_token = catalog.signed_in(user_id)
            client.register_table_from_data(hub_url, access_token, {**table, "namespace_id": namespace_id})
        except (HubRefused, HubUnreachable, HubVersionRefused) as e:
            raise ValueError(
                f"The table was written to {table['table_path']} but could not be registered on the hub: {e}"
            ) from e

    def request(self, user_id: int | None, method: str, path: str, params: dict | None, body) -> tuple[int, object]:
        """A metadata request on a row of the hub's catalog, answered by the hub's same route (``catalog.relay``)."""
        return catalog.relay(user_id, method, path, params, body)


table_sources.register_remote("hub", _RunCatalog())


def refusal_sentence(detail: str, status: int) -> str:
    """The run's error for a vend the hub refused with ``detail`` (its HTTP ``status``).

    A hub that says what to do answers a sentence of its own, capitalised as one ("You can read main.default but not
    write to it; ask an admin or your team's manager for write access"), and the run's error is that sentence alone. A
    bare reason ("no manage grant on namespace 6"), or no text at all, reads after ``REFUSED``.
    """
    if detail[:1].isupper() and detail != client.answered(status):
        return detail
    return f"{REFUSED}: {detail}"


@contextmanager
def _hub_answers():
    """The hub's refusals and silences as the one ``LookupError`` sentence a caller shows the person."""
    try:
        yield
    except HubRefused as e:
        if e.status == 401:
            raise LookupError(catalog.NOT_CONNECTED) from e
        raise LookupError(refusal_sentence(e.detail, e.status)) from e
    except HubVersionRefused as e:
        raise LookupError(refusal_sentence(e.detail, 426)) from e
    except HubUnreachable as e:
        raise LookupError(str(e)) from e


def _usable(namespace: HubNamespace | None, missing: str) -> HubNamespace:
    """``namespace`` when this Flowfile can use its tables, else the sentence why: ``missing`` when there is none."""
    if namespace is None:
        raise LookupError(f"{REFUSED}: {missing}")
    refusal = catalog.unreachable(namespace.storage_uri, namespace.full_name)
    if refusal is not None:
        raise LookupError(refusal)
    return namespace


def request(
    user_id: int,
    run_id: str,
    readers: set[str],
    writers: set[str],
    *,
    targets: Iterable[str] = (),
    exchange: bool = False,
    secrets: Iterable[str] = (),
    connections: Iterable[tuple[str, str]] = (),
) -> RunVend:
    """Ask the hub, through ``user_id``'s hub session, for the credentials of a run over its tables.

    ``readers`` and ``writers`` are the qualified names (``catalog.schema``) of the hub namespaces the run reads and
    writes. Every namespace is found, and its hub id resolved, in one walk of the hub's catalogs; one the hub does not
    list for the account is refused, and so is one on the hub's own disk, or on storage other than S3, before the vend
    with the sentence the catalog tree marks it with (``catalog.unreachable``), which says what to do. A run with no
    namespace walks nothing. ``exchange`` also asks for the organisation's exchange store; ``targets``, the hub names
    of the servers the run's steps use, has the hub seal every credential to each of them, and ``secrets``, the names
    of the secrets the run's steps on those servers use, each such secret too, and ``connections``, the ``(kind,
    name)`` of the stored connections they use, each such connection.

    Raises:
        LookupError: A sentence for the user: not connected to the hub, a namespace the hub keeps on its own disk or
            on other storage, the hub's refusal with its reason, or a hub that does not answer.
    """
    secrets = tuple(dict.fromkeys(secrets))
    connections = tuple(dict.fromkeys(connections))
    targets = tuple(dict.fromkeys(targets))
    with _hub_answers():
        hub_url, access_token = catalog.signed_in(user_id)
        found = catalog.namespaces(hub_url, access_token) if readers | writers else {}
        by_name = {namespace.full_name: namespace for namespace in found.values()}
        ids: dict[str, int] = {}
        bases: dict[str, str] = {}
        for name in sorted(readers | writers):
            namespace = _usable(by_name.get(name), f"the hub lists no catalog schema {name} for this account")
            ids[name] = namespace.id
            bases[name] = namespace.storage_uri.rstrip("/")
        answer = client.vend(
            hub_url,
            access_token,
            run_id,
            sorted(ids[name] for name in readers),
            sorted(ids[name] for name in writers),
            targets=targets,
            exchange=exchange,
            secrets=secrets,
            connections=connections,
        )
    return _run_vend(run_id, answer, bases, ids, targets, secrets, connections)


def request_for_tables(user_id: int, run_id: str, tables: Iterable[CatalogTableOut]) -> RunVend:
    """Ask the hub, through ``user_id``'s hub session, for one read of ``tables`` outside a run: the Catalog page's
    preview, a notebook cell's read, the SQL editor and a visual.

    ``tables`` are rows of the catalog tree, each carrying the hub's id of its namespace, so the
    credential is for the schema each table lives in. A run names a namespace by its qualified name; this call does
    not, since namespace names are free text and ``a.b`` + ``c`` names the same ``catalog.schema`` as ``a`` + ``b.c``.

    Raises:
        LookupError: As ``request`` does, the namespace named by the table it holds.
    """
    tables = list(tables)
    with _hub_answers():
        hub_url, access_token = catalog.signed_in(user_id)
        found = catalog.namespaces(hub_url, access_token)
        for table in tables:
            _usable(found.get(table.hub_namespace_id), f"the hub lists no catalog schema of {table.name}")
        answer = client.vend(hub_url, access_token, run_id, sorted({t.hub_namespace_id for t in tables}), [])
    return _run_vend(run_id, answer, {}, {}, (), (), ())


def _run_vend(
    run_id: str,
    answer: dict,
    bases: dict[str, str],
    ids: dict[str, int],
    targets: tuple[str, ...],
    secrets: tuple[str, ...],
    connections: tuple[tuple[str, str], ...],
) -> RunVend:
    credentials = tuple(
        HubCredential(
            base_uri=credential["base_uri"].rstrip("/"),
            storage_options=dict(credential["storage_options"]),
            sealed=dict(credential.get("sealed") or {}) if targets else {},
        )
        for credential in answer["credentials"]
    )
    return RunVend(
        run_id=run_id,
        vend_id=answer["vend_id"],
        credentials=credentials,
        bases=bases,
        ids=ids,
        targets=targets,
        secrets={secret["name"]: dict(secret["sealed"]) for secret in answer.get("secrets", []) if secrets},
        connections={
            (stored["kind"], stored["name"]): dict(stored["sealed"])
            for stored in answer.get("connections", [])
            if connections
        },
        connection_fields={
            (stored["kind"], stored["name"]): tuple(stored.get("secret_fields") or ())
            for stored in answer.get("connections", [])
            if connections
        },
    )


def node_name(backend_id: str) -> str | None:
    """The hub's name of the server ``backend_id`` runs on, which a vend seals to; ``None`` when it is not one of the
    hub's servers.

    On a server joined to its hub with keys of its own, the embedded ``local`` and the built-in ``worker`` are this
    server, named in its ``hub_node.json``; a ``worker:<name>`` row filled from the hub is the hub's server ``<name>``.
    Elsewhere only such rows are named.
    """
    from flowfile_core.auth import issuer_accounts
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import ConfiguredBackend
    from flowfile_core.flowfile.execution import registry
    from shared import hub_node

    if backend_id in (registry.LOCAL_BACKEND_ID, registry.WORKER_BACKEND_ID):
        return hub_node.resolved().name if issuer_accounts.joined_with_keys() is not None else None
    if not backend_id.startswith(registry.CONFIGURED_WORKER_PREFIX):
        return None
    with get_db_context() as db:
        row = db.get(ConfiguredBackend, backend_id)
        filled_from_hub = row is not None and row.remote_id is not None
    return backend_id.removeprefix(registry.CONFIGURED_WORKER_PREFIX) if filled_from_hub else None
