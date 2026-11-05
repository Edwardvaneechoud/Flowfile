"""This node's identity on a hub: the join, and the small file that remembers it.

A worker, or a core, becomes a node on a hub by joining it once: it posts a single-use join token from the hub's
Compute page and its own description (its name, the address desktops reach it at) to ``<hub>/hub/nodes/join``, and
keeps what the hub answers in ``hub_node.json`` in the storage dir: the hub's URL, the issuer the hub signs with, the
name the hub recorded and the node's id there. The file holds nothing secret. The join token is spent by the join and
written nowhere, and the hub's keys, the only thing a node verifies with, stay public. The node's own keys are another
file: at its first join it makes two key pairs, keeps them in ``hub_node_key.json`` beside this one
(``shared.hub_node_key``) and sends the hub their public halves with every join. ``joined()`` is the one question
"did this storage dir join a hub, with keys of its own": the file and the key file both there.

A joined node tells its hub it is up, since the hub may not reach it (behind a NAT, say) while it can always reach the
hub: ``heartbeat`` posts one beat signed with the node's own key, and ``start_heartbeat`` beats at once and then every
``HEARTBEAT_SECONDS`` from a daemon thread. One process speaks for the node: core when one joined for itself and its
worker (the file's ``has_core``), else the worker.

``shared.hub_tokens`` reads the node's settings through ``resolved()``: the environment first (``FLOWFILE_HUB_URL``,
``FLOWFILE_NODE_NAME``), the file second, and the file only for the hub it names, so a node set up by hand behaves as
before whatever the file says; the issuer to expect is the joined file's, else the hub's URL. The file also keeps a
short hash of the join token it was written with: a start with that same token makes no request, so a restart with an
unchanged command is quiet, and a start with any other join token joins again and replaces the file. One storage dir
holds one node: every process reading it takes the name the latest join wrote.

A node that joins keeps its files in a storage dir of its own, never in Flowfile desktop's ``~/.flowfile``: the hub's
install line sets ``FLOWFILE_STORAGE_DIR`` (in docker mode the storage dir is the container's), and a start with a join
token outside docker mode and without one is refused before anything reads the storage dir
(``require_own_storage_dir``). It serves on the port it is given, ``--port`` or the default, and binds that socket
before it joins (``serving_socket``): a node whose port is in use exits with code 1 saying so, before it spends its
token. The address it gives the hub is ``--url``, which the install line computes, else this machine's full name and
that port (``default_url``). A core joins for itself and the worker beside it the same way.
"""

import hashlib
import ipaddress
import json
import logging
import os
import signal
import socket
import threading
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    import httpx

logger = logging.getLogger("flowfile.hub_node")
# The join's lines are start-up lines for the operator, so they show where only warnings would.
logger.setLevel(logging.INFO)

FILE_NAME = "hub_node.json"
JOIN_PATH = "/hub/nodes/join"
HEARTBEAT_PATH = "/hub/nodes/heartbeat"
HEARTBEAT_SECONDS = 60
"""How often a joined node tells its hub it is up, in seconds; not a setting. The hub shows a node Online while its last
beat is under three minutes old."""

_BACKLOG = 2048

_FIELDS = {"hub_url": str, "issuer": str, "name": str, "node_id": int, "join_token_hash": str}
_OPTIONAL_FIELDS = {"has_core": bool}
_warned_unreadable = False
_warned_beats: set[str] = set()
_removed = False


class JoinRefused(Exception):
    """A join that did not happen. ``reason`` is the text to print; it never carries the token."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class HubNode:
    """What a node keeps after joining a hub: where the hub is, whose tokens to take, its own name and id there.

    Whether the node is enabled is not kept: that changes on the hub. ``join_token_hash`` tells the join token it was
    written with from another one. ``has_core`` is ``True`` when a core joined for itself and its worker, ``False``
    when a worker joined alone: it decides which of the two beats.
    """

    hub_url: str
    issuer: str
    name: str
    node_id: int
    join_token_hash: str
    has_core: bool = False


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
    if kind is bool:
        return isinstance(value, bool)
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
        optional = {field: data[field] for field, kind in _OPTIONAL_FIELDS.items() if _fits(data.get(field), kind)}
        return HubNode(**{field: data[field] for field in _FIELDS}, **optional)
    if not _warned_unreadable:
        logger.warning("%s is not a node's identity on a hub and is ignored", target)
        _warned_unreadable = True
    return None


def joined() -> HubNode | None:
    """This storage dir's node when it joined a hub and has keys of its own (``hub_node_key.json`` beside the file);
    ``None`` otherwise. Read from the files on every call."""
    from shared import hub_node_key

    node = read()
    return node if node is not None and hub_node_key.path().is_file() else None


def _env(name: str) -> str | None:
    return os.environ.get(name, "").strip() or None


def resolved() -> NodeSettings:
    """Each setting from its variable when set, else from the joined file; the issuer is the file's, else the hub's URL.

    The file counts only while ``FLOWFILE_HUB_URL`` is unset or names the file's hub (a trailing ``/`` aside), so a
    file from another hub is never mixed with this one's URL.
    """
    env_url = (_env("FLOWFILE_HUB_URL") or "").rstrip("/") or None
    node = read()
    if node is not None and env_url is not None and node.hub_url.rstrip("/") != env_url:
        node = None
    hub_url = env_url or (node.hub_url.rstrip("/") if node else None)
    name = _env("FLOWFILE_NODE_NAME") or (node.name if node else None)
    issuer = (node.issuer if node else None) or hub_url
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


def token_hash(token: str) -> str:
    """The first 16 hex digits of the join token's SHA-256: enough to tell two tokens apart, nothing to join with."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]


def _write(node: HubNode) -> None:
    target = path()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".tmp")
    temporary.write_text(json.dumps(asdict(node), indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, target)


def join(
    hub_url: str,
    token: str,
    *,
    name: str,
    worker_url: str,
    core_url: str | None = None,
    ui_url: str | None = None,
    http: "httpx.Client | None" = None,
) -> tuple[HubNode, bool]:
    """Join the hub at ``hub_url`` with a join token and keep its answer in the file: the node, and whether the hub
    enabled it at once. ``JoinRefused`` when the hub says no.

    ``name`` is the name this node asks for, ``worker_url`` the address desktops reach its worker at, ``core_url``
    its core's, when it has one, and ``ui_url`` the browser editor's, when its core serves one; without it the join
    says nothing about an editor. The join also sends the public halves of this node's own keys, made at its first
    join and reused after; a storage dir that cannot keep them refuses the join before any request. The file is
    written whole or not at all, with the name and issuer the hub answered and ``has_core`` set when ``core_url`` is
    given (a core joining for itself and its worker).
    Logs the method, the path and the status, never the token or a body.
    """
    import httpx

    from shared import hub_node_key

    hub_url = hub_url.strip().rstrip("/")
    try:
        keys = hub_node_key.load_or_create()
    except (OSError, ValueError) as exc:
        raise JoinRefused(f"could not keep this node's keys at {hub_node_key.path()}: {type(exc).__name__}") from None
    body = {"token": token, "name": name, "worker_url": worker_url, "core_url": core_url}
    if ui_url:
        body["ui_url"] = ui_url
    body["keys"] = keys.public_jwks()
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
        join_token_hash=token_hash(token),
        has_core=core_url is not None,
    )
    _write(node)
    return node, answer["enabled"]


def already_joined(token: str) -> HubNode | None:
    """The node this storage dir joined with ``token``, so a start with it makes no request; else ``None``."""
    node = read()
    return node if node is not None and node.join_token_hash == token_hash(token) else None


def join_at_start(
    hub_url: str,
    token: str,
    *,
    name: str,
    worker_url: str,
    core_url: str | None = None,
    ui_url: str | None = None,
) -> HubNode:
    """Join before serving; a refused join exits with code 2 and leaves the file as it was.

    The join token this node joined with makes no request a second time, so a restart with the same command keeps
    the node as it is. Any other join token joins ``hub_url`` and replaces the file, whichever hub and name it held:
    a new token on the command line is the operator asking for a new join.
    """
    hub_url = hub_url.strip().rstrip("/")
    node = already_joined(token)
    if node is not None:
        logger.info("Already joined %s as %s; the join token is not used", node.hub_url, node.name)
        return node
    node = read()
    if node is not None:
        logger.warning(
            "This node was %s on %s; joining %s with the new join token replaces that, for every process using %s. "
            "A second node on this machine needs its own FLOWFILE_STORAGE_DIR",
            node.name,
            node.hub_url,
            hub_url,
            path().parent,
        )
    try:
        node, enabled = join(hub_url, token, name=name, worker_url=worker_url, core_url=core_url, ui_url=ui_url)
    except JoinRefused as refused:
        logger.error("Could not join %s: %s", hub_url, refused.reason)
        raise SystemExit(2) from None
    state = "it is enabled" if enabled else "an admin enables it on the hub's Compute page"
    logger.info("Joined %s as %s (node %s); %s", node.hub_url, node.name, node.node_id, state)
    return node


def _not_taken(reason: str) -> bool:
    if reason not in _warned_beats:
        _warned_beats.add(reason)
        logger.warning("The hub did not take this node's heartbeat: %s", reason)
    return False


def _removed_from_hub(node: HubNode, resp: "httpx.Response") -> bool:
    global _removed
    _removed = True
    logger.error("%s; this node stops and stays stopped", _detail(resp) or f"{node.hub_url} removed this node")
    return False


def _stop_this_process() -> None:
    """Stop the process this node runs in as an operator's SIGTERM does: uvicorn shuts it down cleanly and it exits 0,
    and the join script's service restarts a node only after a failure, so a removed node stays stopped."""
    os.kill(os.getpid(), signal.SIGTERM)


def heartbeat(process: Literal["core", "worker"], http: "httpx.Client | None" = None) -> bool:
    """Tell the hub this storage dir's node joined that the node is up, as ``process``; whether the hub took the beat.

    Posts ``{version, process}`` to the joined hub's ``/hub/nodes/heartbeat`` with a node assertion signed with this
    node's own key, its audience the joined issuer and that path. A beat the hub does not take, or one that cannot be
    sent (no joined node, no keys, a hub that does not answer), logs one warning per distinct reason until a beat is
    taken again, and never the assertion. Never makes keys. A 410 is a hub that removed this node: said once at ERROR
    with the hub's sentence, and the beat loop stops the process (``_stop_this_process``).
    """
    global _removed
    import httpx

    from shared import hub_node_key
    from shared._version import get_version

    _removed = False
    node = read()
    if node is None:
        return _not_taken(f"no node has joined a hub from {path().parent}")
    try:
        keys = hub_node_key.load()
    except (OSError, ValueError) as exc:
        return _not_taken(f"could not read this node's keys at {hub_node_key.path()}: {type(exc).__name__}")
    if keys is None:
        return _not_taken(f"this node has no keys of its own at {hub_node_key.path()}")
    assertion = keys.assertion(node.name, node.issuer.rstrip("/") + HEARTBEAT_PATH)
    client = http or httpx.Client(timeout=httpx.Timeout(10.0), follow_redirects=False)
    try:
        resp = client.post(
            f"{node.hub_url.rstrip('/')}{HEARTBEAT_PATH}",
            json={"version": get_version(), "process": process},
            headers={"Authorization": f"Bearer {assertion}"},
        )
    except (httpx.HTTPError, httpx.InvalidURL) as exc:
        return _not_taken(f"{node.hub_url} did not answer ({type(exc).__name__})")
    finally:
        if http is None:
            client.close()
    if resp.status_code == 410:
        return _removed_from_hub(node, resp)
    if resp.status_code != 204:
        return _not_taken(f"{node.hub_url} answered {resp.status_code}: {_detail(resp) or resp.reason_phrase}")
    _warned_beats.clear()
    return True


def _speaks_for(node: HubNode | None, process: str) -> bool:
    return node is not None and node.has_core == (process == "core")


def _beat(process: Literal["core", "worker"], stop: threading.Event) -> None:
    """Beat at once and every ``HEARTBEAT_SECONDS`` until ``stop`` is set, each time ``process`` speaks for the node."""
    while True:
        if _speaks_for(read(), process):
            try:
                heartbeat(process)
            except Exception as exc:
                _not_taken(f"the beat failed ({type(exc).__name__})")
            if _removed:
                _stop_this_process()
                return
        if stop.wait(HEARTBEAT_SECONDS):
            return


def start_heartbeat(process: Literal["core", "worker"]) -> threading.Event | None:
    """Beat for this storage dir's node from a daemon thread, at start and every minute; set the event to stop it.

    ``None``, and no thread, when ``process`` does not speak for a node: no node joined a hub from this storage dir with
    keys of its own (``joined``), ``process`` is a worker beside a core that joined for both, or a core on the storage
    dir of a worker that joined alone. The thread checks the file before every beat, so a worker that beats alone goes
    quiet once a core joins beside it.
    """
    node = joined()
    if not _speaks_for(node, process):
        return None
    stop = threading.Event()
    threading.Thread(target=_beat, args=(process, stop), name="hub-heartbeat", daemon=True).start()
    logger.info("Telling %s that %s is up, every %s seconds", node.hub_url, node.name, HEARTBEAT_SECONDS)
    return stop


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


def require_own_storage_dir(token: str | None) -> None:
    """Refuse a start with a join token that has no storage dir of its own, before anything reads the storage dir.

    Outside docker mode, whose storage dir is the container's, a node that joins needs ``FLOWFILE_STORAGE_DIR``, which
    the hub's install line sets: without it the node's files would land in Flowfile desktop's ``~/.flowfile``, and the
    desktop would then act as the joined node. Exits with code 2 and one sentence, writing nothing. Without a join
    token, nothing.
    """
    if not token or os.environ.get("FLOWFILE_STORAGE_DIR") or os.environ.get("FLOWFILE_MODE") == "docker":
        return
    logger.error(
        "Cannot join a hub without a storage dir of this node's own: set FLOWFILE_STORAGE_DIR to a folder for it, as "
        "the hub's install line does, so it stays out of Flowfile desktop's ~/.flowfile"
    )
    raise SystemExit(2)


def _host_port(host: str, port: int) -> str:
    host = host.strip("[]")
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def _bind(host: str, port: int) -> socket.socket:
    """A socket listening on ``host`` and ``port`` as the server's own bind makes it: the address reusable on POSIX, so
    a restart is not refused for the last run's closing connections, and an IPv6 address bound for IPv6 alone."""
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    try:
        if os.name == "posix":
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if family == socket.AF_INET6:
            sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        sock.bind((host.strip("[]"), port))
        sock.listen(_BACKLOG)
    except BaseException:
        sock.close()
        raise
    sock.set_inheritable(True)
    return sock


def serving_socket(host: str, port: int) -> socket.socket:
    """The socket a node that joins a hub serves on, bound and listening on ``host`` and ``port`` before it joins, so
    one that cannot serve never spends a token.

    The server is handed this socket, so nothing can take the port between this call and serving. The node serves on
    ``port`` or not at all: when it cannot be bound, one line names the address and the OS's reason, ``Cannot serve on
    127.0.0.1:63578: Address already in use``, and the process exits with code 1.
    """
    try:
        return _bind(host, port)
    except OSError as exc:
        logger.error("Cannot serve on %s: %s", _host_port(host, port), exc.strerror or type(exc).__name__)
        raise SystemExit(1) from None


def is_loopback_host(host: str) -> bool:
    """Whether ``host``, a bind address, is one only this machine reaches: ``localhost`` or a loopback IP."""
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host.strip("[]")).is_loopback
    except ValueError:
        return False
