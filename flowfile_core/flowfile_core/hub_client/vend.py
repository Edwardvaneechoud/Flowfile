"""The credentials a run over the hub's tables gets from the hub: asked once per run, held in memory, never kept.

A plan or payload never carries a secret: unsealed, the secrets travel encrypted for the worker beside core, which
shares core's master key. A joined server or a desktop shares no master key with the hub's servers, so for steps on
those the vend names ``targets`` and the hub seals every credential, secret and connection to each target node; core
hands the sealed maps on unopened and only the target opens them.
"""

import uuid
from collections.abc import Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace

import polars as pl
from pydantic import SecretStr

from flowfile_core.catalog import table_sources
from flowfile_core.catalog.storage_backend import _is_cloud_uri
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.protocol import EncryptedCredential, SealedCredential
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import CloudStorageReader
from flowfile_core.hub_client import catalog, client, session
from flowfile_core.hub_client.catalog import HubNamespace
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused
from flowfile_core.schemas.catalog_schema import CatalogTableOut, NamespaceTree
from flowfile_core.schemas.cloud_storage_schemas import (
    FullCloudStorageConnection,
    FullCloudStorageConnectionWorkerInterface,
)
from shared import sealed, server_extension
from shared.cloud_storage.storage_options import ALLOW_INVALID_CERTIFICATES

REFUSED = "The hub did not give this run access to its tables"
_S3_FIELDS = ("aws_access_key_id", "aws_region", "endpoint_url")
_S3_SECRETS = ("aws_secret_access_key", "aws_session_token")


@dataclass(frozen=True)
class HubCredential:
    """One vended credential for the tables under ``base_uri``; ``repr`` never shows ``storage_options``, which hold
    secrets. Expiry and prefixes are the hub's to enforce, so none are kept."""

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
    """What the hub vended to the run ``run_id``; ``bases`` and ``ids`` by ``catalog.schema``, ``targets`` empty for an
    unsealed vend.

    ``own`` is the unsealed vend a machine that is no hub server asks beside a sealed one, for the steps on the worker
    beside its core: a hub table reads and writes on whichever machine its step runs (``here``).
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
        return {name: sealed.sealed_secret(name, self.secrets[name]) for name in names if name in self.secrets}

    def sealed_connections(self, refs: Iterable[tuple[str, str]]) -> dict[tuple[str, str], dict[str, str]]:
        return {ref: dict(self.connections[ref]) for ref in refs if ref in self.connections}

    def secret_fields(self, refs: Iterable[tuple[str, str]]) -> dict[tuple[str, str], tuple[str, ...]]:
        return {ref: self.connection_fields[ref] for ref in refs if ref in self.connection_fields}

    @property
    def sealed(self) -> bool:
        return bool(self.targets)

    def sealed_for(self, backend_id: str) -> bool:
        return self.sealed and (self.own is None or registry.server_name(backend_id) is not None)

    def here(self) -> "RunVend":
        """The vend the step running now opens: ``own`` on this machine's own worker, else this one."""
        from flowfile_core.flowfile.execution.context import current_context

        if self.own is None or self.sealed_for(current_context().backend_id):
            return self
        return self.own

    def sealed_credential(self, uri: str) -> SealedCredential:
        credential = self.for_uri(uri)
        if not credential.sealed:
            raise LookupError(f"The hub sealed this run no credential for {uri}")
        return SealedCredential(base_uri=credential.base_uri, sealed=dict(credential.sealed))

    def encrypted_credential(self, uri: str, user_id: int | None) -> EncryptedCredential:
        """The unsealed credential for the worker beside this core: options without secrets, secrets encrypted."""
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
        sealed = self.sealed_credential(uri) if self.targets else None
        if registry.server_name(registry.WORKER_BACKEND_ID) is not None:
            return sealed, None
        return sealed, (self.own or self).encrypted_credential(uri, user_id)

    def scan_delta(self, uri: str, user_id: int | None, schema: Mapping[str, pl.DataType], **kwargs) -> pl.LazyFrame:
        """A scan whose plan holds no secret; sealed, core knows no option, so a Python source opens the map where the
        plan runs."""
        here = self.here()
        if here.sealed:
            return sealed.scan_delta_sealed(uri, here.sealed_credential(uri).sealed, schema)
        return pl.scan_delta(uri, **kwargs, **here.scan_kwargs(uri, user_id))

    def for_namespace(self, namespace: str) -> HubCredential:
        if namespace not in self.bases:
            raise LookupError(f"This run did not ask the hub for namespace {namespace}")
        return self.for_uri(self.bases[namespace])

    def namespace_id(self, namespace: str) -> int:
        if namespace not in self.ids:
            raise LookupError(f"This run did not ask the hub for namespace {namespace}")
        return self.ids[namespace]

    def for_uri(self, uri: str) -> HubCredential:
        for credential in self.credentials:
            if credential.covers(uri):
                return credential
        raise LookupError(f"The hub gave this run no credential for {uri}")

    def scan_kwargs(self, uri: str, user_id: int | None) -> dict:
        """Scan kwargs whose options hold no secret, so a serialized plan does not either."""
        return CloudStorageReader.get_secure_scan_kwargs(self.for_uri(uri).storage_options, user_id)

    def worker_connection(self, uri: str, user_id: int) -> FullCloudStorageConnectionWorkerInterface:
        """An S3 access-key connection, secrets encrypted for the worker; sealed, it carries only the map."""
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
    _held[run_vend.run_id] = run_vend


def held(run_id: str | None) -> RunVend | None:
    return _held.get(run_id) if run_id else None


@dataclass(frozen=True)
class VendedTarget:
    """A vend as ``CatalogStorageTarget`` for the tables under ``base``, resolved at use for the step running now."""

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
        return self.run_vend.scan_delta(path, user_id, columns, **kwargs)


def _physical(table: dict, namespace_name) -> dict:
    if table.get("table_type", "physical") != "physical":
        raise ValueError("A virtual table on the hub cannot be read from this Flowfile")
    if not table.get("file_path") or not _is_cloud_uri(table["file_path"]):
        raise ValueError(catalog.on_own_disk(namespace_name()))
    return table


class _RunCatalog:
    """The hub's catalog as ``table_sources.RemoteCatalog``."""

    def run_starts(self, start: table_sources.RunStart) -> RunVend:
        """The run's one vend, sealed to ``start.servers``, plus ``RunVend.own`` on a machine that is no hub server."""

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
        beside = [table for table in start.tables if registry.server_name(table[2]) is None]
        if start.servers and registry.server_name(registry.WORKER_BACKEND_ID) is None and (beside or start.exchange):
            own = request(
                start.user_id, start.run_id, schemas(beside, False), schemas(beside, True), exchange=start.exchange
            )
            run_vend = replace(run_vend, own=own)
        hold(run_vend)
        return run_vend

    run_grant = staticmethod(held)

    def run_ends(self, run_id: str) -> None:
        _held.pop(run_id, None)

    def table_named(self, user_id: int | None, qualified_name: str) -> dict:
        return _physical(catalog.table_named(user_id, qualified_name), lambda: qualified_name.rpartition(".")[0])

    def table(self, user_id: int | None, table_id: int) -> CatalogTableOut:
        found = catalog.table_named(user_id, -table_id)
        _physical(found, lambda: (found.get("qualified_name") or found["name"]).rpartition(".")[0])
        return CatalogTableOut.model_validate(catalog.marked(found))

    def row_targets(
        self, user_id: int | None, tables: Sequence[CatalogTableOut], purpose: str, writes: bool
    ) -> list[VendedTarget]:
        """A vend outside a run, under a run id starting with ``purpose`` for the hub's ledger."""
        vended = request_for_tables(user_id, f"{purpose}-{uuid.uuid4().hex}", tables, writes=writes)
        return [VendedTarget(vended, row.file_path.rstrip("/").rpartition("/")[0], user_id) for row in tables]

    def tree(self, user_id: int | None) -> list[NamespaceTree]:
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
        listed = catalog.schemas(*session.bearer(user_id, catalog.NOT_CONNECTED))
        tables = (table for namespace, tables in listed if namespace.id == namespace_id for table in tables)
        return next(({"namespace_id": namespace_id, **table} for table in tables if table["name"] == name), None)

    def register_table(self, run_id: str | None, user_id: int | None, schema: str, table: dict) -> None:
        namespace_id = self.schema_target(run_id, user_id, schema).run_vend.namespace_id(schema)
        try:
            hub_url, access_token = session.bearer(user_id, catalog.NOT_CONNECTED)
            client.register_table_from_data(hub_url, access_token, {**table, "namespace_id": namespace_id})
        except (HubRefused, HubUnreachable, HubVersionRefused) as e:
            raise ValueError(
                f"The table was written to {table['table_path']} but could not be registered on the hub: {e}"
            ) from e

    request = staticmethod(catalog.relay)

    def of_record(self) -> bool:
        """On a joined server the hub's catalog is the catalog of record; a desktop keeps its own beside it."""
        return server_extension.joined() is not None

    def save_flow(self, user_id: int | None, flow) -> str | None:
        """Keep a flow from this hub on the hub and return its note; ``None`` when it is not the hub's to keep."""
        origin = getattr(flow.flow_settings, "hub_origin", None)
        stored = session.load(user_id) if user_id is not None else None
        if origin is None or stored is None or origin.hub_url.rstrip("/") != stored["hub_url"].rstrip("/"):
            return None
        document = flow.get_flowfile_data()
        document.flowfile_settings.source_registration_id = None
        document.flowfile_settings.hub_origin = None
        body = {"base_version": origin.version, "definition": document.model_dump(mode="json")}
        with _hub_answers():
            hub_url, access_token = session.bearer(user_id, catalog.NOT_CONNECTED)
            try:
                saved = client.save_content(hub_url, access_token, origin.flow_uuid, body)
            except HubRefused as e:
                if e.status in (404, 405):
                    return None
                raise
        return saved.get("note") or None

    def flow_uuid(self, flow) -> str | None:
        origin = getattr(flow.flow_settings, "hub_origin", None)
        return origin.flow_uuid if origin is not None else None


table_sources.register_remote("hub", _RunCatalog())


def refusal_sentence(detail: str, status: int) -> str:
    """The hub's own capitalised sentence alone; a bare reason, or no text, after ``REFUSED``."""
    if detail[:1].isupper() and detail != client.answered(status):
        return detail
    return f"{REFUSED}: {detail}"


@contextmanager
def _hub_answers():
    """Hub failures as one ``LookupError`` sentence; a 403 as ``table_sources.Refused``."""
    try:
        yield
    except HubRefused as e:
        if e.status == 401:
            raise LookupError(catalog.NOT_CONNECTED) from e
        refused = table_sources.Refused if e.status == 403 else LookupError
        raise refused(refusal_sentence(e.detail, e.status)) from e
    except HubVersionRefused as e:
        raise LookupError(refusal_sentence(e.detail, 426)) from e
    except HubUnreachable as e:
        raise LookupError(str(e)) from e


def _usable(namespace: HubNamespace | None, missing: str) -> HubNamespace:
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
    """A run's vend for the namespaces ``readers`` and ``writers`` (``catalog.schema``); one this Flowfile cannot use
    is refused before anything is vended. Every failure is a ``LookupError`` sentence for the user."""
    secrets = tuple(dict.fromkeys(secrets))
    connections = tuple(dict.fromkeys(connections))
    targets = tuple(dict.fromkeys(targets))
    with _hub_answers():
        hub_url, access_token = session.bearer(user_id, catalog.NOT_CONNECTED)
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


def request_for_tables(
    user_id: int, run_id: str, tables: Iterable[CatalogTableOut], *, writes: bool = False
) -> RunVend:
    """A vend outside a run for tree rows, by their hub namespace id since ``a.b`` + ``c`` and ``a`` + ``b.c`` name the
    same ``catalog.schema``."""
    tables = list(tables)
    with _hub_answers():
        hub_url, access_token = session.bearer(user_id, catalog.NOT_CONNECTED)
        found = catalog.namespaces(hub_url, access_token)
        for table in tables:
            _usable(found.get(-table.namespace_id), f"the hub lists no catalog schema of {table.name}")
        namespace_ids = sorted({-t.namespace_id for t in tables})
        answer = client.vend(hub_url, access_token, run_id, namespace_ids, namespace_ids if writes else [])
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
