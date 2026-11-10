"""The hub's catalogs, tables and flows as rows of the local catalog tree.

A connected desktop lists the hub's namespaces, tables and published flows beside its own, in the one tree the Catalog
tab reads. Each hub row carries ``source = "hub"``, the negative of the hub's id as its ``id`` (local ids are positive
autoincrements, so the two never meet) and the hub's own id as ``hub_id``. The hub is read on every listing and
nothing is kept; a hub that does not answer, or no longer accepts the session, leaves its catalogs out of the tree
with one warning in the log.

The tree is the hub's own ``GET /catalog/namespaces/tree``, and any other metadata route of a catalog on the hub is
the hub's same route (``relay``): core asks as the signed-in person and hands the answer back as the hub gave it,
marking only what makes a row of the hub recognisable here (``marked``). A hub flow's row carries ``flow_uuid``,
``latest_version`` and ``author_name`` as the hub lists them, so the Catalog tab can open the version a person picks
as Open Flow → Hub does; a flow with no published version has nothing to open and is left out.

Each hub row also says where it lives and whether this Flowfile can use it: the hub's name as its ``place`` and, for
a catalog on the hub's own disk or on storage other than S3, the sentence a run over it would be refused with as its
``unavailable_reason``; ``table_sources.merged_tree`` puts them after this machine's. A hub row's ``access`` is what
the hub sent, ``owner`` on a row the person owns.

A run that reads or writes the hub's tables finds their namespaces and tables here too (``schemas``, ``table``,
``table_named``), each lookup a fresh request through the user's hub session. A saved flow names a hub table by its
qualified name (``catalog.schema.table``) and a hub schema by ``catalog.schema``, never by an id of the hub's database;
the ids are resolved on the hub when the flow runs (``table_named``, ``namespaces``).
"""

import re
from dataclasses import dataclass

from pydantic import ValidationError

from flowfile_core.configs import logger
from flowfile_core.hub_client import client, session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused
from flowfile_core.hub_client.node_tokens import RECONNECT
from flowfile_core.schemas.catalog_schema import NamespaceTree

HUB = "hub"
_ROW_REFERENCES = ("id", "namespace_id", "parent_id", "table_id", "registration_id", "source_registration_id")
_THEIR_ID = re.compile(r"(?<=/)-(\d+)(?=/|$)")
_THEIR_VALUE = re.compile(r"-\d+")


NOT_CONNECTED = (
    f"This flow uses tables on the hub, but this Flowfile is not connected to it; {RECONNECT} (Compute → Hub)"
)
_WHAT_TO_DO = "Pick a catalog on the organisation's storage, or ask the hub's admin to put this one there."


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


def scheme(uri: str) -> str:
    """The scheme of a storage URI, ``file`` for a path without one."""
    return uri.split("://", 1)[0] if "://" in uri else "file"


def on_own_disk(full_name: str) -> str:
    """Why a run cannot use the tables of the hub namespace ``full_name``, which the hub keeps on its own disk."""
    return f"The hub keeps the tables of {full_name} on its own disk; this Flowfile cannot read them. {_WHAT_TO_DO}"


def other_scheme(found: str) -> str:
    """Why a run cannot use the tables of a hub catalog on ``found://`` storage: only S3 is read and written."""
    return (
        f"Flowfile reads and writes hub tables on S3 storage; this catalog is on {found}:// storage. "
        "Pick a catalog on the organisation's S3 storage, or ask the hub's admin to put this one there."
    )


def unreachable(storage_uri: str | None, full_name: str) -> str | None:
    """Why this Flowfile cannot use the tables of the hub namespace ``full_name`` under ``storage_uri``; ``None`` if
    it can. No storage, or a path on the hub's filesystem, is the hub's own disk; any scheme but S3 is storage this
    Flowfile does not write. The tree marks such a catalog with it and a run over it is refused with it, before
    anything is vended."""
    found = None if storage_uri is None else scheme(storage_uri)
    if found in (None, "file"):
        return on_own_disk(full_name)
    if found != "s3":
        return other_scheme(found)
    return None


def tree(user_id: int) -> list[NamespaceTree]:
    """The hub's catalogs, their schemas, their tables and their flows, for ``user_id``'s hub connection; ``[]``
    without one.

    One request per call: the hub's own ``GET /catalog/namespaces/tree``, relayed (``marked``). The hub's order is
    kept. A hub that cannot be reached, refuses (an expired session included), refuses this client's version or
    answers what is not core's tree answers ``[]`` after one warning naming the hub and the reason.

    Every namespace row carries the hub's name as its ``place`` (its organisation as the stored connection names it,
    else its URL) and, when this Flowfile cannot use the catalog's tables, the reason as its
    ``unavailable_reason``: with the catalog's name on the catalog's row and ``catalog.schema`` on each schema's. A
    flow with no published version has nothing to open and is left out.
    """
    stored = session.load(user_id)
    if stored is None:
        return []
    hub_url = stored["hub_url"]
    path = "/catalog/namespaces/tree"
    try:
        access_token = session.access_token(user_id)
        if access_token is None:
            return []
        _, answer = _relayed(hub_url, access_token, "GET", path)
        try:
            roots = [NamespaceTree.model_validate(row) for row in answer]
        except (ValidationError, TypeError) as e:
            raise HubRefused(200, f"{hub_url} did not answer {path} as a Flowfile hub does.") from e
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        logger.warning("Leaving the catalogs of the hub at %s out of the catalog tree: %s", hub_url, e)
        return []
    place = stored.get("org_name") or hub_url
    return [_placed(root, place, root.storage_uri or None) for root in roots]


def _placed(node: NamespaceTree, place: str, storage_uri: str | None, parents: str = "") -> NamespaceTree:
    """``node`` and the namespaces under it saying where they live and whether this Flowfile can use their tables."""
    full_name = f"{parents}{node.name}"
    return node.model_copy(
        update={
            "place": place,
            "unavailable_reason": unreachable(storage_uri, full_name),
            "children": [_placed(child, place, storage_uri, f"{full_name}.") for child in node.children],
            "flows": [flow for flow in node.flows if flow.latest_version is not None],
        }
    )


def relay(user_id: int | None, method: str, path: str, params: dict | None = None, body=None) -> tuple[int, object]:
    """A metadata request on a catalog that lives on ``user_id``'s hub, answered by the hub's same route
    (``table_sources.RemoteCatalog.request``).

    ``path`` is core's own catalog route (``/catalog/tables/-12``). An id this machine shows negative, in the path,
    the query or the body, goes to the hub as the hub's own; the answer comes back as the hub gave it, with its
    status, changed only by ``marked``. Core adds no logic: what the hub refuses, the hub refuses, with its status
    and detail (a 401 answers 400, as does a person with no hub session). A hub that does not answer is a 502, one
    that refuses this client's version a 426.
    """
    try:
        return _relayed(*signed_in(user_id), method, path, params, body)
    except LookupError as e:
        return 400, {"detail": str(e)}
    except HubRefused as e:
        if 400 <= e.status < 500:
            return (400 if e.status == 401 else e.status), {"detail": e.payload or e.detail}
        return 502, {"detail": f"The hub failed to answer: {e}"}
    except HubVersionRefused as e:
        return 426, {"detail": e.detail}
    except HubUnreachable as e:
        return 502, {"detail": str(e)}


def _relayed(hub_url: str, access_token: str, method: str, path: str, params=None, body=None) -> tuple[int, object]:
    status, answer = client.catalog_request(
        hub_url, access_token, method, _THEIR_ID.sub(r"\1", path), params=_theirs(params), body=_theirs(body)
    )
    return status, marked(answer)


def _is_id(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def marked(answer):
    """``answer`` of a catalog route on the hub as this machine shows it: every row that has an id says
    ``source: "hub"`` and keeps the hub's id as ``hub_id`` (its namespace's as ``hub_namespace_id``), and every
    reference to a row of the hub (``_ROW_REFERENCES``) is negated, so it never meets a local id. Nothing else
    changes, whatever the shape."""
    if isinstance(answer, list):
        return [marked(item) for item in answer]
    if not isinstance(answer, dict):
        return answer
    row = {key: marked(value) for key, value in answer.items()}
    if _is_id(answer.get("id")):
        row["hub_id"] = answer["id"]
        if answer.get("source", "local") == "local":
            row["source"] = HUB
        if _is_id(answer.get("namespace_id")):
            row["hub_namespace_id"] = answer["namespace_id"]
    for key in _ROW_REFERENCES:
        if _is_id(row.get(key)) and row[key] > 0:
            row[key] = -row[key]
    return row


def _theirs(sent):
    """A request's query or body with this machine's negative row references back as the hub's own ids."""
    if isinstance(sent, list):
        return [_theirs(item) for item in sent]
    if not isinstance(sent, dict):
        return sent
    return {key: _their_id(value) if key in _ROW_REFERENCES else _theirs(value) for key, value in sent.items()}


def _their_id(value):
    if _is_id(value):
        return abs(value)
    return value.lstrip("-") if isinstance(value, str) and _THEIR_VALUE.fullmatch(value) else value


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


def schemas(hub_url: str, access_token: str) -> list[tuple[HubNamespace, list[dict]]]:
    """Every namespace the account sees on the hub, each with its tables' rows, from the hub's tree: one request."""
    path = "/catalog/namespaces/tree"
    _, roots = client.catalog_request(hub_url, access_token, "GET", path)
    try:
        return [
            (HubNamespace(node["id"], prefix + node["name"], root.get("storage_uri") or None), node.get("tables") or [])
            for root in roots
            for prefix, node in [("", root), *((f"{root['name']}.", child) for child in root.get("children") or [])]
        ]
    except (TypeError, KeyError, AttributeError) as e:
        raise HubRefused(200, f"{hub_url} did not answer {path} as a Flowfile hub does.") from e


def namespaces(hub_url: str, access_token: str) -> dict[int, HubNamespace]:
    """Every namespace the account sees on the hub, by hub id (``schemas``)."""
    return {namespace.id: namespace for namespace, _ in schemas(hub_url, access_token)}


def table(user_id: int, hub_table_id: int) -> dict:
    """The hub's catalog table ``hub_table_id`` as the hub answers it (core's ``CatalogTableOut``).

    Raises:
        LookupError: The hub has no such table for the account, does not answer, or the session is gone.
    """
    return _answered_table(
        user_id, hub_table_id, lambda hub_url, access_token: client.get_table(hub_url, access_token, hub_table_id)
    )


def table_named(user_id: int, qualified_name: str) -> dict:
    """The hub's catalog table ``qualified_name`` (``catalog.schema.table``) as the hub answers it (core's
    ``CatalogTableOut``), resolved on the hub by that name: a saved flow names a hub table by name, never by an id of
    the hub's database.

    Raises:
        LookupError: The hub has no table of that name for the account, does not answer, or the session is gone.
    """
    return _answered_table(
        user_id,
        qualified_name,
        lambda hub_url, access_token: client.resolve_table(hub_url, access_token, qualified_name),
    )


def _answered_table(user_id: int, named: int | str, read) -> dict:
    try:
        return read(*signed_in(user_id))
    except HubRefused as e:
        if e.status == 404:
            raise LookupError(f"The hub has no catalog table {named} for this account") from e
        if e.status == 401:
            raise LookupError(NOT_CONNECTED) from e
        raise LookupError(f"The hub did not answer for its catalog table {named}: {e.detail}") from e
    except (HubUnreachable, HubVersionRefused) as e:
        raise LookupError(f"The hub did not answer for its catalog table {named}: {e}") from e
