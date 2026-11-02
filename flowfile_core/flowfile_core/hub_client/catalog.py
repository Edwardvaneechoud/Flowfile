"""The hub's catalogs and tables as rows of the local catalog tree.

A connected desktop lists the hub's namespaces and tables beside its own, in the one tree the Catalog tab reads. Each
hub row carries ``source = "hub"``, the negative of the hub's id as its ``id`` (local ids are positive
autoincrements, so the two never meet) and the hub's own id as ``hub_id``. The hub is read on every listing and
nothing is kept; a hub that does not answer, or no longer accepts the session, leaves its catalogs out of the tree
with one warning in the log.

A run that reads or writes the hub's tables finds their namespaces and tables here too (``locate``, ``table``), each
lookup a fresh request through the user's hub session.
"""

from dataclasses import dataclass

from pydantic import ValidationError

from flowfile_core.configs import logger
from flowfile_core.hub_client import client, session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused
from flowfile_core.hub_client.node_tokens import RECONNECT
from flowfile_core.schemas.catalog_schema import CatalogTableOut, NamespaceTree

HUB = "hub"
LOCAL = "local"
_NAMESPACE_FIELDS = (
    "name",
    "level",
    "description",
    "owner_id",
    "created_at",
    "updated_at",
    "storage_uri",
    "storage_connection_name",
    "access",
)


NOT_CONNECTED = (
    f"This flow uses tables on the hub, but this Flowfile is not connected to it; {RECONNECT} (Compute → Hub)"
)


@dataclass(frozen=True)
class HubNamespace:
    """A namespace on the hub: its hub id, its ``catalog.schema`` name and the storage root of its catalog.

    ``storage_uri`` is ``None`` when the hub keeps the catalog's tables on its own disk.
    """

    id: int
    full_name: str
    storage_uri: str | None


def row_id(hub_id: int) -> int:
    """The id a hub row carries in the local tree: the negative of the hub's own id."""
    return -hub_id


def tree(user_id: int) -> list[NamespaceTree]:
    """The hub's catalogs, their schemas and their tables, for ``user_id``'s hub connection; ``[]`` without one.

    One walk per call: the hub's root namespaces, the children of each root (a catalog nests catalog → schema, so
    no deeper), and every table the account can see in one request, grouped under their namespaces. The hub's
    order is kept. A hub that cannot be reached, refuses (an expired session included) or refuses this client's
    version answers ``[]`` after one warning naming the hub and the reason.
    """
    stored = session.load(user_id)
    if stored is None:
        return []
    hub_url = stored["hub_url"]
    try:
        access_token = session.access_token(user_id)
        if access_token is None:
            return []
        return _walk(hub_url, access_token)
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        logger.warning("Leaving the catalogs of the hub at %s out of the catalog tree: %s", hub_url, e)
        return []


def signed_in(user_id: int) -> tuple[str, str]:
    """The hub URL of ``user_id``'s hub session and an access token from it.

    Raises:
        LookupError: ``user_id`` has no hub session, or the hub no longer accepts it (``NOT_CONNECTED``).
        HubUnreachable, HubRefused, HubVersionRefused: the refresh failed for another reason.
    """
    stored = session.load(user_id)
    if stored is None:
        raise LookupError(NOT_CONNECTED)
    try:
        access_token = session.access_token(user_id)
    except HubRefused as e:
        if e.status == 401:
            raise LookupError(NOT_CONNECTED) from e
        raise
    if access_token is None:
        raise LookupError(NOT_CONNECTED)
    return stored["hub_url"], access_token


def namespaces(hub_url: str, access_token: str) -> dict[int, HubNamespace]:
    """Every namespace the account sees on the hub, by hub id, from the walk ``tree`` makes (roots, then children)."""
    roots, children = _namespace_rows(hub_url, access_token)
    found: dict[int, HubNamespace] = {}
    for root in roots:
        storage_uri = root.get("storage_uri") or None
        found[root["id"]] = HubNamespace(root["id"], root["name"], storage_uri)
        for schema in children[root["id"]]:
            found[schema["id"]] = HubNamespace(schema["id"], f"{root['name']}.{schema['name']}", storage_uri)
    return found


def locate(user_id: int, hub_namespace_id: int) -> HubNamespace:
    """The hub's namespace ``hub_namespace_id``, read through ``user_id``'s hub session.

    Raises:
        LookupError: The hub lists no such namespace for the account, does not answer, or the session is gone.
    """
    try:
        found = namespaces(*signed_in(user_id))
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        raise LookupError(f"The hub did not answer for its catalog namespaces: {e}") from e
    if hub_namespace_id not in found:
        raise LookupError(f"The hub lists no catalog namespace {hub_namespace_id} for this account")
    return found[hub_namespace_id]


def table(user_id: int, hub_table_id: int) -> dict:
    """The hub's catalog table ``hub_table_id`` as the hub answers it (core's ``CatalogTableOut``).

    Raises:
        LookupError: The hub has no such table for the account, does not answer, or the session is gone.
    """
    try:
        return client.get_table(*signed_in(user_id), hub_table_id)
    except HubRefused as e:
        if e.status == 404:
            raise LookupError(f"The hub has no catalog table {hub_table_id} for this account") from e
        if e.status == 401:
            raise LookupError(NOT_CONNECTED) from e
        raise LookupError(f"The hub did not answer for its catalog table {hub_table_id}: {e.detail}") from e
    except (HubUnreachable, HubVersionRefused) as e:
        raise LookupError(f"The hub did not answer for its catalog table {hub_table_id}: {e}") from e


def _namespace_rows(hub_url: str, access_token: str) -> tuple[list[dict], dict[int, list[dict]]]:
    roots = _namespaces(hub_url, client.list_namespaces(hub_url, access_token))
    children = {
        root["id"]: _namespaces(hub_url, client.list_namespaces(hub_url, access_token, parent_id=root["id"]))
        for root in roots
    }
    return roots, children


def _walk(hub_url: str, access_token: str) -> list[NamespaceTree]:
    roots, children = _namespace_rows(hub_url, access_token)
    tables: dict[int, list[CatalogTableOut]] = {}
    for table in client.list_tables(hub_url, access_token):
        if table.get("namespace_id") is not None:
            row = _table(hub_url, table)
            tables.setdefault(row.hub_namespace_id, []).append(row)
    return [
        _namespace(
            hub_url,
            root,
            tables,
            [_namespace(hub_url, schema, tables) for schema in children[root["id"]]],
        )
        for root in roots
    ]


def _namespaces(hub_url: str, rows: list) -> list[dict]:
    """``rows`` as the hub answered them, refused unless each is a namespace with an integer ``id`` and a ``name``."""
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("id"), int) or not row.get("name"):
            raise HubRefused(200, f"{hub_url} did not answer /catalog/namespaces as a Flowfile hub does.")
    return rows


def _namespace(
    hub_url: str, namespace: dict, tables: dict[int, list[CatalogTableOut]], children: list | None = None
) -> NamespaceTree:
    try:
        return NamespaceTree(
            id=row_id(namespace["id"]),
            parent_id=None if namespace.get("parent_id") is None else row_id(namespace["parent_id"]),
            source=HUB,
            hub_id=namespace["id"],
            **{field: namespace.get(field) for field in _NAMESPACE_FIELDS},
            children=children or [],
            tables=tables.get(namespace["id"], []),
        )
    except (ValidationError, KeyError, TypeError) as e:
        raise HubRefused(200, f"{hub_url} did not answer /catalog/namespaces as a Flowfile hub does.") from e


def _table(hub_url: str, table: dict) -> CatalogTableOut:
    try:
        return CatalogTableOut.model_validate(
            {
                **table,
                "id": row_id(table["id"]),
                "namespace_id": row_id(table["namespace_id"]),
                "source": HUB,
                "hub_id": table["id"],
                "hub_namespace_id": table["namespace_id"],
            }
        )
    except (ValidationError, TypeError) as e:
        raise HubRefused(200, f"{hub_url} did not answer /catalog/tables as a Flowfile hub does.") from e
