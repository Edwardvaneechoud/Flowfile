"""The hub's catalogs, tables and flows as rows of the local catalog tree.

A hub row carries ``source = "hub"``, its id negated (local ids are positive) and the hub's own as ``hub_id``. Nothing
is kept; every metadata route of a hub catalog is the hub's same route (``relay``). A saved flow names a hub table by
its qualified name, never by a hub database id, which is resolved when the flow runs.
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
SCHEDULERS = "/catalog/schedulers"
_ROW_REFERENCES = (
    "id",
    "namespace_id",
    "parent_id",
    "table_id",
    "registration_id",
    "source_registration_id",
    "schedule_id",
    "trigger_table_id",
    "trigger_namespace_id",
    "resource_id",
    "group_id",
)
_THEIR_ID = re.compile(r"(?<=/)-(\d+)(?=/|$)")
_THEIR_VALUE = re.compile(r"-\d+")


NOT_CONNECTED = (
    f"This flow uses tables on the hub, but this Flowfile is not connected to it; {RECONNECT} (Compute → Hub)"
)
_WHAT_TO_DO = "Pick a catalog on the organisation's storage, or ask the hub's admin to put this one there."


@dataclass(frozen=True)
class HubNamespace:
    """A hub namespace; ``storage_uri`` is ``None`` when the hub keeps its tables on its own disk."""

    id: int
    full_name: str
    storage_uri: str | None


def scheme(uri: str) -> str:
    return uri.split("://", 1)[0] if "://" in uri else "file"


def on_own_disk(full_name: str) -> str:
    return f"The hub keeps the tables of {full_name} on its own disk; this Flowfile cannot read them. {_WHAT_TO_DO}"


def other_scheme(found: str) -> str:
    return (
        f"Flowfile reads and writes hub tables on S3 storage; this catalog is on {found}:// storage. "
        "Pick a catalog on the organisation's S3 storage, or ask the hub's admin to put this one there."
    )


def unreachable(storage_uri: str | None, full_name: str) -> str | None:
    """Why this Flowfile cannot use the namespace's tables (only S3 is read and written); ``None`` if it can."""
    found = None if storage_uri is None else scheme(storage_uri)
    if found in (None, "file"):
        return on_own_disk(full_name)
    if found != "s3":
        return other_scheme(found)
    return None


def tree(user_id: int) -> list[NamespaceTree]:
    """The hub's catalog tree for ``user_id``, in one request; ``[]`` without a connection or, with one warning, when
    the hub cannot be read."""
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
    """Mark where each namespace lives and why it cannot be used; flows with no published version are left out."""
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
    """A metadata request on a hub catalog answered by the hub's same route, negative ids sent as the hub's own.

    A hub 401 answers 400 (the frontend reads 401 as an expired local session); a hub that does not answer is 502. The
    hub's schedulers say they are the hub's, by the name its catalogs are placed under.
    """
    try:
        status, answer = _relayed(*session.bearer(user_id, NOT_CONNECTED), method, path, params, body)
        if path == SCHEDULERS and isinstance(answer, list):
            stored = session.load(user_id) or {}
            place = stored.get("org_name") or stored.get("hub_url")
            answer = [{**row, "place": row.get("place") or place} for row in answer if isinstance(row, dict)]
        return status, answer
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
    """A hub catalog answer as this machine shows it: rows with an id get ``source: "hub"``, ``hub_id`` and
    ``hub_namespace_id``, and every reference in ``_ROW_REFERENCES`` is negated."""
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
    if isinstance(sent, list):
        return [_theirs(item) for item in sent]
    if not isinstance(sent, dict):
        return sent
    return {key: _their_id(value) if key in _ROW_REFERENCES else _theirs(value) for key, value in sent.items()}


def _their_id(value):
    if _is_id(value):
        return abs(value)
    return value.lstrip("-") if isinstance(value, str) and _THEIR_VALUE.fullmatch(value) else value


def schemas(hub_url: str, access_token: str) -> list[tuple[HubNamespace, list[dict]]]:
    """Every namespace the account sees on the hub with its tables' rows, from one request for the tree."""
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
    return {namespace.id: namespace for namespace, _ in schemas(hub_url, access_token)}


def table_named(user_id: int, named: int | str) -> dict:
    """The hub's table by its hub id or qualified name; any failure is a ``LookupError`` sentence."""
    read = client.get_table if isinstance(named, int) else client.resolve_table
    try:
        return read(*session.bearer(user_id, NOT_CONNECTED), named)
    except HubRefused as e:
        if e.status == 404:
            raise LookupError(f"The hub has no catalog table {named} for this account") from e
        if e.status == 401:
            raise LookupError(NOT_CONNECTED) from e
        raise LookupError(f"The hub did not answer for its catalog table {named}: {e.detail}") from e
    except (HubUnreachable, HubVersionRefused) as e:
        raise LookupError(f"The hub did not answer for its catalog table {named}: {e}") from e
