"""This node's identity on a hub: the join, and the small file that remembers it.

A worker, or a core, becomes a node on a hub by joining it once: it posts a single-use join token from the hub's
Compute page and its own description (its name, the address desktops reach it at) to ``<hub>/hub/nodes/join``, and
keeps what the hub answers in ``hub_node.json`` in the storage dir: the hub's URL, the issuer the hub signs with, the
name the hub recorded and the node's id there, and when it joined. The file holds nothing secret. The join token is
spent by the join and written nowhere, and the hub's keys, the only thing a node verifies with, stay public.

``shared.hub_tokens`` reads the node's settings through ``resolved()``: the environment first, field by field
(``FLOWFILE_HUB_URL``, ``FLOWFILE_NODE_NAME``, ``FLOWFILE_HUB_ISSUER``), the file second, and the file only for the hub
it names, so a node set up by hand behaves as before whatever the file says. A node joins once: while the file is
readable a start makes no request, even with a join token on its command line. To join again, delete the file.
"""

import json
import logging
import os
import socket
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import httpx

logger = logging.getLogger("flowfile.hub_node")
# The join's lines are start-up lines for the operator, so they show where only warnings would.
logger.setLevel(logging.INFO)

FILE_NAME = "hub_node.json"
JOIN_PATH = "/hub/nodes/join"

_FIELDS = {"hub_url": str, "issuer": str, "name": str, "node_id": int, "joined_at": str}
_warned_unreadable = False


class JoinRefused(Exception):
    """A join that did not happen. ``reason`` is the text to print; it never carries the token."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class HubNode:
    """What a node keeps after joining a hub: where the hub is, whose tokens to take, its own name and id there.

    ``joined_at`` is ISO-8601 in UTC. Whether the node is enabled is not kept: that changes on the hub.
    """

    hub_url: str
    issuer: str
    name: str
    node_id: int
    joined_at: str


@dataclass(frozen=True)
class NodeSettings:
    """The hub this node takes tokens from, its name there and the issuer to expect; each ``None`` when unset."""

    hub_url: str | None
    name: str | None
    issuer: str | None


def path() -> Path:
    """``hub_node.json`` in the storage dir, where this node keeps its identity on a hub."""
    from shared.storage_config import storage

    return storage.base_directory / FILE_NAME


def _fits(value, kind: type) -> bool:
    if kind is int:
        return isinstance(value, int) and not isinstance(value, bool)
    return isinstance(value, kind) and bool(value)


def read() -> HubNode | None:
    """The node's identity on a hub, read from the file on every call; ``None`` when it never joined one.

    A file that does not read as one (not JSON, a field missing or of the wrong type) is ``None`` as well, with one
    warning per process naming the file.
    """
    global _warned_unreadable
    target = path()
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError):
        data = None
    if isinstance(data, dict) and all(_fits(data.get(field), kind) for field, kind in _FIELDS.items()):
        return HubNode(**{field: data[field] for field in _FIELDS})
    if not _warned_unreadable:
        logger.warning("%s is not a node's identity on a hub and is ignored", target)
        _warned_unreadable = True
    return None


def _env(name: str) -> str | None:
    return os.environ.get(name, "").strip() or None


def resolved() -> NodeSettings:
    """Each setting from its variable when set, else from the joined file; the issuer falls back to the hub's URL.

    The file counts only while ``FLOWFILE_HUB_URL`` is unset or names the file's hub (a trailing ``/`` aside), so a
    file from another hub is never mixed with this one's URL.
    """
    env_url = (_env("FLOWFILE_HUB_URL") or "").rstrip("/") or None
    joined = read()
    if joined is not None and env_url is not None and joined.hub_url.rstrip("/") != env_url:
        joined = None
    hub_url = env_url or (joined.hub_url.rstrip("/") if joined else None)
    name = _env("FLOWFILE_NODE_NAME") or (joined.name if joined else None)
    issuer = _env("FLOWFILE_HUB_ISSUER") or (joined.issuer if joined else None) or hub_url
    return NodeSettings(hub_url=hub_url, name=name, issuer=issuer)


def _payload(resp: "httpx.Response"):
    try:
        return resp.json()
    except ValueError:
        return None


def _detail(resp: "httpx.Response") -> str:
    payload = _payload(resp)
    detail = payload.get("detail") if isinstance(payload, dict) else None
    return detail if isinstance(detail, str) else ""


def _first_error(resp: "httpx.Response") -> str:
    """FastAPI's first validation error as ``<msg> (<field>)``, never its ``input``."""
    payload = _payload(resp)
    errors = payload.get("detail") if isinstance(payload, dict) else None
    if not (isinstance(errors, list) and errors and isinstance(errors[0], dict)):
        return _detail(resp) or "no reason given"
    loc = errors[0].get("loc")
    field = ".".join(str(part) for part in loc if part != "body") if isinstance(loc, list) else ""
    msg = str(errors[0].get("msg") or "invalid")
    return f"{msg} ({field})" if field else msg


def _is_joined(answer) -> bool:
    return (
        isinstance(answer, dict)
        and _fits(answer.get("node_id"), int)
        and _fits(answer.get("name"), str)
        and _fits(answer.get("issuer"), str)
        and isinstance(answer.get("enabled"), bool)
    )


def _write(node: HubNode) -> None:
    target = path()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".tmp")
    temporary.write_text(json.dumps(asdict(node), indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, target)


def _join(
    hub_url: str,
    token: str,
    *,
    name: str,
    worker_url: str,
    core_url: str | None,
    http: "httpx.Client | None" = None,
) -> tuple[HubNode, bool]:
    """``join``, and whether the hub enabled the node at once."""
    import httpx

    hub_url = hub_url.strip().rstrip("/")
    body = {"token": token, "name": name, "worker_url": worker_url, "core_url": core_url}
    client = http or httpx.Client(
        timeout=httpx.Timeout(connect=5.0, read=15.0, write=15.0, pool=5.0), follow_redirects=False
    )
    try:
        resp = client.post(f"{hub_url}{JOIN_PATH}", json=body)
    except (httpx.HTTPError, httpx.InvalidURL) as exc:
        raise JoinRefused(f"{hub_url} did not answer ({type(exc).__name__})") from None
    finally:
        if http is None:
            client.close()
    logger.info("hub POST %s -> %s", JOIN_PATH, resp.status_code)
    if resp.status_code == 401:
        raise JoinRefused(_detail(resp) or f"{hub_url} answered 401")
    if resp.status_code == 422:
        raise JoinRefused(f"The hub refused this node's name or address: {_first_error(resp)}")
    if resp.status_code != 200:
        raise JoinRefused(f"{hub_url} answered {resp.status_code}: {_detail(resp) or resp.reason_phrase}")
    answer = _payload(resp)
    if not _is_joined(answer):
        raise JoinRefused(f"{hub_url} did not answer as a Flowfile hub does")
    node = HubNode(
        hub_url=hub_url,
        issuer=answer["issuer"],
        name=answer["name"],
        node_id=answer["node_id"],
        joined_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )
    _write(node)
    return node, answer["enabled"]


def join(
    hub_url: str,
    token: str,
    *,
    name: str,
    worker_url: str,
    core_url: str | None = None,
    http: "httpx.Client | None" = None,
) -> HubNode:
    """Join the hub at ``hub_url`` with a join token and keep its answer in the file; ``JoinRefused`` when it says no.

    ``name`` is the name this node asks for, ``worker_url`` the address desktops reach its worker at and ``core_url``
    its core's, when it has one. The file is written whole or not at all, with the name and issuer the hub answered.
    Logs the method, the path and the status, never the token or a body.
    """
    return _join(hub_url, token, name=name, worker_url=worker_url, core_url=core_url, http=http)[0]


def join_at_start(hub_url: str, token: str, *, name: str, worker_url: str, core_url: str | None = None) -> HubNode:
    """Join before serving, once: a node that joined already makes no request; a refused join exits with code 2.

    A file naming another hub than ``hub_url`` is kept and warned about, not replaced: the node stays on the hub it
    joined until the file is deleted.
    """
    hub_url = hub_url.strip().rstrip("/")
    joined = read()
    if joined is not None:
        if joined.hub_url.rstrip("/") != hub_url:
            logger.warning(
                "This node joined %s already, not %s: the join token is not used; delete %s to join %s",
                joined.hub_url,
                hub_url,
                path(),
                hub_url,
            )
        else:
            logger.info("Already joined %s as %s; the join token is not used", joined.hub_url, joined.name)
        return joined
    try:
        node, enabled = _join(hub_url, token, name=name, worker_url=worker_url, core_url=core_url)
    except JoinRefused as refused:
        logger.error("Could not join %s: %s", hub_url, refused.reason)
        raise SystemExit(2) from None
    state = "it is enabled" if enabled else "an admin enables it on the hub's Compute page"
    logger.info("Joined %s as %s (node %s); %s", node.hub_url, node.name, node.node_id, state)
    return node


def default_name() -> str:
    """The name a node asks for when the operator gives none: this machine's host name up to its first dot."""
    return socket.gethostname().split(".")[0]


def default_url(port: int, *, tls: bool) -> str:
    """The address a node gives for itself when the operator gives none: this machine's full name and ``port``.

    Where the full name is only a reverse-lookup name (``*.arpa``, as macOS can answer for its own address), which no
    desktop reaches, the host name stands in for it.
    """
    host = socket.getfqdn()
    if host.endswith(".arpa"):
        host = socket.gethostname()
    return f"{'https' if tls else 'http'}://{host}:{port}"
