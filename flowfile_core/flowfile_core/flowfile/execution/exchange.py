"""The exchange store: an object-storage prefix that a run's results cross between backends through.

``FLOWFILE_EXCHANGE_URI`` names its root, an ``s3://`` URI, and ``FLOWFILE_EXCHANGE_CONNECTION`` the stored cloud
connection that opens it, as a catalog on object storage is configured. The connection is named to a backend by its
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


def exchange_store(user_id: int) -> ExchangeStore | None:
    """The exchange store as configured, its connection looked up for *user_id*; ``None`` when no URI is set.

    The connection is found by name as a cloud read's is, the user's own first, then the lowest-id group grant.

    Raises:
        ValueError: ``FLOWFILE_EXCHANGE_URI`` is set without ``FLOWFILE_EXCHANGE_CONNECTION``, its scheme is not
            ``s3``, or *user_id* has no cloud connection of that name.
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
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.flowfile.database_connection_manager.db_connections import get_cloud_connection

    with get_db_context() as db:
        row = get_cloud_connection(db, name, user_id)
        if row is None:
            raise ValueError(
                f"User {user_id} has no cloud connection named {name!r}, which FLOWFILE_EXCHANGE_CONNECTION names"
            )
        return ExchangeStore(uri=uri.rstrip("/"), credential=CredentialRef(connection_id=row.id, user_id=user_id))


class ExchangeRun:
    """One run's prefix in the exchange store, ``<uri>/<flow id>/<run id>/``, and its removal when the run ends.

    The store's connection is looked up per user, so the run remembers every store it answered a prefix for, and
    ``collect`` removes the run's prefix from each of them.
    """

    def __init__(self, flow_id: int, run_id: str):
        self.flow_id = flow_id
        self.run_id = run_id
        self._stores: set[ExchangeStore] = set()

    def prefix(self, user_id: int) -> tuple[str, CredentialRef] | None:
        """The run's prefix for *user_id* and the reference that opens it; ``None`` when no store is configured.

        Raises:
            ValueError: The store is misconfigured, as ``exchange_store`` says.
        """
        store = exchange_store(user_id)
        if store is None:
            return None
        self._stores.add(store)
        return self._prefix(store), store.credential

    def collect(self) -> None:
        """Remove the run's prefix from every store it answered one for, and forget them; without one, call nothing."""
        stores, self._stores = self._stores, set()
        for store in stores:
            remove_prefix(store, self._prefix(store))

    def _prefix(self, store: ExchangeStore) -> str:
        return f"{store.uri}/{self.flow_id}/{self.run_id}/"


def remove_prefix(store: ExchangeStore, prefix: str) -> None:
    """Delete every object under *prefix*, an ``s3://`` URI in *store*, with core's own resolution of its connection.

    The objects are listed a page at a time and each page is deleted in one request, so an empty prefix costs only the
    listing.

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
