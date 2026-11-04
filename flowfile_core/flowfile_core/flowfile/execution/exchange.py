"""The exchange store: an object-storage prefix that a run's results cross between backends through.

``FLOWFILE_EXCHANGE_URI`` names its ``s3://`` root and ``FLOWFILE_EXCHANGE_CONNECTION`` the stored cloud connection
that opens it, else the store saved from the Backends page, else, on a team node of a hub, the store its hub sets
(``exchange_config.effective_store``). The connection is the operator's, never a node's user's, and reaches a backend
as a ``CredentialRef``. On a server joined to its hub with keys of its own the hub's store is opened instead by what
the hub vends the run sealed to each server its steps run on (``ExchangeRun.sealed``), a ``SealedCredential`` each
backend sends whole. A run writes under ``<uri>/<flow id>/<run id>/``, which ``run_graph`` removes when the run ends.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit

from flowfile_core.configs import exchange_config
from flowfile_core.configs.exchange_config import ExchangeConfig
from flowfile_core.flowfile.execution.protocol import Credential, CredentialRef, SealedCredential
from shared.cloud_credential_provider import credential_ref_kwargs, resolve_credential_ref
from shared.cloud_storage.storage_options import build_s3_client

NOT_VENDED = "the hub vended this run no credential for its exchange store; run the flow again"


@dataclass(frozen=True)
class ExchangeStore:
    """The exchange store's root URI, without a trailing slash, and the credential that opens it."""

    uri: str
    credential: Credential


def exchange_store(hub: ExchangeConfig | None = None) -> ExchangeStore | None:
    """The exchange store as configured, opened by the operator's connection; ``None`` when no URI is set.

    ``hub`` is the store this node's hub sets, which applies when neither the environment nor the saved store names
    one; its connection is looked up by name here, as a local store's is.

    In multi-user mode the connection is the lowest-id one of that name an administrator owns, so no regular user's
    connection of the name can shadow it. In single-user mode it is the lowest-id one of that name. Its
    ``CredentialRef`` names the connection and its owner.

    Raises:
        ValueError: The URI is set without a connection name, its scheme is not ``s3``, or no administrator has a
            cloud connection of that name.
    """
    configured = exchange_config.effective_store(hub=hub)
    uri = configured.uri
    if uri is None:
        return None
    name = configured.connection
    if name is None:
        raise ValueError(
            "FLOWFILE_EXCHANGE_URI is set but FLOWFILE_EXCHANGE_CONNECTION is not: name the stored cloud connection "
            "that opens the exchange store"
        )
    scheme = urlsplit(uri).scheme
    if scheme != "s3":
        raise ValueError(f"The exchange store must be on S3; FLOWFILE_EXCHANGE_URI {uri!r} has the scheme {scheme!r}")
    from flowfile_core.auth import sharing
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CloudStorageConnection, User

    with get_db_context() as db:
        rows = db.query(CloudStorageConnection).filter(CloudStorageConnection.connection_name == name)
        if sharing.sharing_enabled():
            rows = rows.join(User).filter(User.is_admin.is_(True))
        row = rows.order_by(CloudStorageConnection.id.asc()).first()
        if row is None:
            raise ValueError(
                f"No administrator has a cloud connection named {name!r}, which FLOWFILE_EXCHANGE_CONNECTION names"
            )
        return ExchangeStore(uri=uri.rstrip("/"), credential=CredentialRef(connection_id=row.id, user_id=row.user_id))


class ExchangeRun:
    """One run's prefix in the exchange store, ``<uri>/<flow id>/<run id>/``, and its removal when the run ends.

    The run looks the store up once, on the first ``prefix`` that finds one, and answers that store after; ``collect``
    removes the run's prefix from it. ``hub_store`` is the store the node's hub set for this run, if any, which
    applies, with its row limit, while neither the environment nor the saved store names one.
    """

    def __init__(self, flow_id: int, run_id: str, hub_store: ExchangeConfig | None = None):
        self.flow_id = flow_id
        self.run_id = run_id
        self.hub_store = hub_store
        self.sealed: SealedCredential | None = None
        self._store: ExchangeStore | None = None

    def vended(self) -> bool:
        """Whether the run's store is the one a joined server's hub sets, opened by what the hub vends this run."""
        if self.hub_store is None or self.hub_store.vended_for is None:
            return False
        return exchange_config.effective_store(hub=self.hub_store).uri == self.hub_store.uri

    def prefix(self) -> tuple[str, Credential] | None:
        """The run's prefix and the credential that opens it; ``None`` when no store is configured.

        Raises:
            ValueError: The store is misconfigured, as ``exchange_store`` says, or it is the hub's on a joined server
                and the hub vended this run no credential for it (``NOT_VENDED``).
        """
        if self._store is None and self.vended():
            if self.sealed is None:
                raise ValueError(NOT_VENDED)
            self._store = ExchangeStore(uri=self.hub_store.uri.rstrip("/"), credential=self.sealed)
        if self._store is None:
            self._store = exchange_store() if self.hub_store is None else exchange_store(self.hub_store)
        if self._store is None:
            return None
        return self._prefix(self._store), self._store.credential

    def max_rows(self) -> int | None:
        """The most rows an input may cross with in this run; ``None`` for no limit.

        Raises:
            ValueError: ``FLOWFILE_EXCHANGE_MAX_ROWS`` is set to something other than a whole number.
        """
        return exchange_config.effective_max_rows(hub=self.hub_store)

    def collect(self) -> None:
        """Remove the run's prefix from its store and forget the store; without one, call nothing."""
        store, self._store = self._store, None
        if store is not None:
            remove_prefix(store, self._prefix(store))

    def _prefix(self, store: ExchangeStore) -> str:
        return f"{store.uri}/{self.flow_id}/{self.run_id}/"


def remove_prefix(store: ExchangeStore, prefix: str) -> None:
    """Delete every object under *prefix*, an ``s3://`` URI in *store*, resolving its connection as core does.

    A sealed credential is opened with this server's own entry of the map: the vend always seals the exchange to the
    server core runs on, which removes the run's prefix. Each listed page is deleted in one request, so an empty prefix
    costs only the listing. Path-style addressing applies only to a connection with its own endpoint (MinIO and other
    S3-compatible stores).

    Raises:
        OSError: The store refused to delete an object; the message names how many and the first reason.
        CredentialResolutionError: A sealed credential this server cannot open.
    """
    if isinstance(store.credential, SealedCredential):
        from shared.hub_sealed import sealed_storage_options

        client = build_s3_client(sealed_storage_options(store.credential.sealed), path_style=True)
    else:
        connection_id, user_id = store.credential.connection_id, store.credential.user_id
        options = credential_ref_kwargs(connection_id, user_id).get("storage_options", {})
        keys, _ = resolve_credential_ref(connection_id, user_id)
        client = build_s3_client({**options, **keys}, path_style=True)
    bucket, _, key_prefix = prefix.removeprefix("s3://").partition("/")
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key_prefix):
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if not objects:
            continue
        errors = client.delete_objects(Bucket=bucket, Delete={"Objects": objects, "Quiet": True}).get("Errors", [])
        if errors:
            raise OSError(f"Could not delete {len(errors)} object(s) under {prefix}: {errors[0].get('Message')}")
