"""The exchange store: an object-storage prefix that a run's results cross between backends through.

``FLOWFILE_EXCHANGE_URI`` names its root, an ``s3://`` URI, and ``FLOWFILE_EXCHANGE_CONNECTION`` the stored cloud
connection that opens it, as a catalog on object storage is configured. The connection is the operator's, never a
node's user's: an administrator's connection of that name, looked up once per run. It is named to a backend by its
``CredentialRef``, which core resolves with its master key and a worker against the shared catalog. A run writes under
``<uri>/<flow id>/<run id>/``, and ``run_graph`` removes that prefix when the run ends.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit

from flowfile_core.configs import settings
from flowfile_core.flowfile.execution.protocol import CredentialRef
from shared.cloud_credential_provider import credential_ref_kwargs, resolve_credential_ref
from shared.cloud_storage.storage_options import build_s3_client


@dataclass(frozen=True)
class ExchangeStore:
    """The exchange store's root URI, without a trailing slash, and the reference to the connection that opens it."""

    uri: str
    credential: CredentialRef


def exchange_store() -> ExchangeStore | None:
    """The exchange store as configured, opened by the operator's connection; ``None`` when no URI is set.

    The connection is the deployment's, never a node's user's: in multi-user mode the lowest-id cloud connection of
    that name an administrator owns, so no user's own connection of the name shadows it and none a regular user owns
    is ever the store's; in single-user mode the lowest-id one of that name, the one user's. Its ``CredentialRef``
    names the connection's owner.

    Raises:
        ValueError: ``FLOWFILE_EXCHANGE_URI`` is set without ``FLOWFILE_EXCHANGE_CONNECTION``, its scheme is not
            ``s3``, or no administrator has a cloud connection of that name.
    """
    uri = settings.get_exchange_uri()
    if uri is None:
        return None
    name = settings.get_exchange_connection()
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
    removes the run's prefix from it.
    """

    def __init__(self, flow_id: int, run_id: str):
        self.flow_id = flow_id
        self.run_id = run_id
        self._store: ExchangeStore | None = None

    def prefix(self) -> tuple[str, CredentialRef] | None:
        """The run's prefix and the reference that opens it; ``None`` when no store is configured.

        Raises:
            ValueError: The store is misconfigured, as ``exchange_store`` says.
        """
        if self._store is None:
            self._store = exchange_store()
        if self._store is None:
            return None
        return self._prefix(self._store), self._store.credential

    def collect(self) -> None:
        """Remove the run's prefix from its store and forget the store; without one, call nothing."""
        store, self._store = self._store, None
        if store is not None:
            remove_prefix(store, self._prefix(store))

    def _prefix(self, store: ExchangeStore) -> str:
        return f"{store.uri}/{self.flow_id}/{self.run_id}/"


def remove_prefix(store: ExchangeStore, prefix: str) -> None:
    """Delete every object under *prefix*, an ``s3://`` URI in *store*, with core's own resolution of its connection.

    The objects are listed a page at a time and each page is deleted in one request, so an empty prefix costs only the
    listing. ``build_s3_client`` addresses the store path-style when the connection names its own endpoint (MinIO and
    other S3-compatible stores) and by boto3's default, virtual-hosted, when it names none (a bucket on AWS).

    Raises:
        OSError: The store refused to delete an object; the message names how many and the first reason.
    """
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
