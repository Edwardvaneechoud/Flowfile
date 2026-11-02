"""The hub's catalogs and tables as rows of the local catalog tree.

A connected desktop lists the hub's namespaces and tables beside its own, in the one tree the Catalog tab reads. Each
hub row carries ``source = "hub"``, the negative of the hub's id as its ``id`` (local ids are positive
autoincrements, so the two never meet) and the hub's own id as ``hub_id``. The hub is read on every listing and
nothing is kept; a hub that does not answer, or no longer accepts the session, leaves its catalogs out of the tree
with one warning in the log.
"""

from pydantic import ValidationError

from flowfile_core.configs import logger
from flowfile_core.hub_client import client, session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused
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


def _walk(hub_url: str, access_token: str) -> list[NamespaceTree]:
    roots = _namespaces(hub_url, client.list_namespaces(hub_url, access_token))
    children = {
        root["id"]: _namespaces(hub_url, client.list_namespaces(hub_url, access_token, parent_id=root["id"]))
        for root in roots
    }
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
