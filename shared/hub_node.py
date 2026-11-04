"""This node's identity on a hub: the join, and the small file that remembers it.

A worker, or a core, becomes a node on a hub by joining it once: it posts a single-use join token from the hub's
Compute page and its own description (its name, the address desktops reach it at) to ``<hub>/hub/nodes/join``, and
keeps what the hub answers in ``hub_node.json`` in the storage dir: the hub's URL, the issuer the hub signs with, the
name the hub recorded and the node's id there, and when it joined. The file holds nothing secret. The join token is
spent by the join and written nowhere, and the hub's keys, the only thing a node verifies with, stay public. The node's
own keys are another file: at its first join it makes two key pairs, keeps them in ``hub_node_key.json`` beside this
one (``shared.hub_node_key``) and sends the hub their public halves with every join.

A joined node with keys tells its hub it is up, since the hub may not reach it (behind a NAT, say) while it can always
reach the hub: ``heartbeat`` posts one beat signed with the node's own key, and ``start_heartbeat`` beats at once and
then every ``HEARTBEAT_SECONDS`` from a daemon thread. One process speaks for the node: core when one joined for itself
and its worker (the file's ``has_core``), else the worker. A node without keys, joined by an older Flowfile or set up
by hand, never beats. A hub that answers a beat 404 or 405 was built before it took heartbeats: that is said once, and
the node asks it again every ``HEARTBEAT_ABSENT_SECONDS`` instead of every minute.

``shared.hub_tokens`` reads the node's settings through ``resolved()``: the environment first, field by field
(``FLOWFILE_HUB_URL``, ``FLOWFILE_NODE_NAME``, ``FLOWFILE_HUB_ISSUER``), the file second, and the file only for the hub
it names, so a node set up by hand behaves as before whatever the file says. The file also keeps a short hash of the
join token it was written with: a start with that same token makes no request, so a restart with an unchanged command
is quiet, and a start with any other join token joins again and replaces the file. A file from before the hash was
kept cannot tell its own token from a new one, so for its own hub it keeps the old rule: no request until it is deleted.
One storage dir holds one node: every process reading it takes the name the latest join wrote.

A node that joins beside Flowfile desktop keeps out of the desktop's way. Started with a join token and no
``FLOWFILE_STORAGE_DIR``, it keeps its files in ``~/.flowfile-node/<name>`` (``node_storage``), never in the desktop's
``~/.flowfile``. It binds the socket it serves on before it joins (``listen``), so a node that cannot serve never spends
its token, and when the default port is in use it takes the port it joined with, else a free one, and joins with that.
Without an address of its own (``--url``) it gives the hub this machine's address on the interface that reaches the hub
(``node_url``), not a name the hub may not resolve. A core joins for itself and the worker beside it the same way, and
keeps the port it gave for that worker (``beside_port``); the worker, started with the node's name and no join token,
finds the core's folder by that name and serves on that port.
"""

import errno
import hashlib
import ipaddress
import json
import logging
import os
import re
import selectors
import signal
import socket
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlsplit

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
HEARTBEAT_ABSENT_SECONDS = 15 * 60
"""How long a node waits after a beat its hub answered 404 or 405, a hub built before it took heartbeats; not a
setting."""

NODE_FILES_DIR = ".flowfile-node"
"""The folder under the home directory where a node that joins keeps its files when no storage dir is set."""
HUB_PORT = 63580
"""A hub's own default port, which a node looking for a free port leaves alone."""
PORT_SEARCH = 50
"""How far above its default port a node looks for a free one before the OS picks."""
NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$")
"""The names a hub takes for a node, which are also the folder names ``node_storage`` makes."""
_ROUTE_PROBES = {socket.AF_INET: "192.0.2.1", socket.AF_INET6: "2001:db8::1"}
_BACKLOG = 2048

_FIELDS = {"hub_url": str, "issuer": str, "name": str, "node_id": int, "joined_at": str}
_OPTIONAL_FIELDS = {"join_token_hash": str, "has_core": bool, "core_port": int, "worker_port": int}
_PORT_FIELDS = ("core_port", "worker_port")
_warned_unreadable = False
_warned_beats: set[str] = set()
_hub_without_beats = False
_removed = False
_said_without_beats = False


class JoinRefused(Exception):
    """A join that did not happen. ``reason`` is the text to print; it never carries the token."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class HubNode:
    """What a node keeps after joining a hub: where the hub is, whose tokens to take, its own name and id there.

    ``joined_at`` is ISO-8601 in UTC. Whether the node is enabled is not kept: that changes on the hub.
    ``join_token_hash`` tells the join token it was written with from another one; empty in a file from before it
    was kept, or one written by hand. ``has_core`` is ``True`` when a core joined for itself and its worker, ``False``
    when a worker joined alone (and in a file from before it was kept): it decides which of the two beats.
    ``core_port`` is the port a core that joined served on when it joined, and ``worker_port`` the port the worker
    served on, or, for a core, the port it gave for the worker beside it: the ports the hub has. Each ``None`` where no
    such port was given, and in a file from before it was kept.
    """

    hub_url: str
    issuer: str
    name: str
    node_id: int
    joined_at: str
    join_token_hash: str = ""
    has_core: bool = False
    core_port: int | None = None
    worker_port: int | None = None


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
        for field in _PORT_FIELDS:
            if not 0 < optional.get(field, 1) < 65536:
                del optional[field]
        return HubNode(**{field: data[field] for field in _FIELDS}, **optional)
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


def token_hash(token: str) -> str:
    """The first 16 hex digits of the join token's SHA-256: enough to tell two tokens apart, nothing to join with."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]


def _write(node: HubNode) -> None:
    target = path()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".tmp")
    kept = {field: value for field, value in asdict(node).items() if value is not None}
    temporary.write_text(json.dumps(kept, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, target)


def _join(
    hub_url: str,
    token: str,
    *,
    name: str,
    worker_url: str,
    core_url: str | None,
    ui_url: str | None = None,
    core_port: int | None = None,
    worker_port: int | None = None,
    http: "httpx.Client | None" = None,
) -> tuple[HubNode, bool]:
    """``join``, and whether the hub enabled the node at once."""
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
        joined_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        join_token_hash=token_hash(token),
        has_core=core_url is not None,
        core_port=core_port,
        worker_port=worker_port,
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
    ui_url: str | None = None,
    core_port: int | None = None,
    worker_port: int | None = None,
    http: "httpx.Client | None" = None,
) -> HubNode:
    """Join the hub at ``hub_url`` with a join token and keep its answer in the file; ``JoinRefused`` when it says no.

    ``name`` is the name this node asks for, ``worker_url`` the address desktops reach its worker at, ``core_url``
    its core's, when it has one, and ``ui_url`` the browser editor's, when its core serves one; without it the join
    says nothing about an editor. The join also sends the public halves of this node's own keys, made at its first
    join and reused after; a storage dir that cannot keep them refuses the join before any request. The file is
    written whole or not at all, with the name and issuer the hub answered, ``has_core`` set when ``core_url`` is
    given (a core joining for itself and its worker), and ``core_port`` and ``worker_port``, the ports core and the
    worker serve on, when given.
    Logs the method, the path and the status, never the token or a body.
    """
    return _join(
        hub_url,
        token,
        name=name,
        worker_url=worker_url,
        core_url=core_url,
        ui_url=ui_url,
        core_port=core_port,
        worker_port=worker_port,
        http=http,
    )[0]


def already_joined(hub_url: str, token: str) -> HubNode | None:
    """The node this storage dir joined, when a start with ``token`` for ``hub_url`` makes no request; else ``None``.

    That is a file written with this same token, or one from before the token's hash was kept that names this hub.
    """
    joined = read()
    if joined is None:
        return None
    if joined.join_token_hash == token_hash(token):
        return joined
    if not joined.join_token_hash and joined.hub_url.rstrip("/") == hub_url.strip().rstrip("/"):
        return joined
    return None


def join_at_start(
    hub_url: str,
    token: str,
    *,
    name: str,
    worker_url: str,
    core_url: str | None = None,
    ui_url: str | None = None,
    core_port: int | None = None,
    worker_port: int | None = None,
) -> HubNode:
    """Join before serving; a refused join exits with code 2 and leaves the file as it was.

    The join token this node joined with makes no request a second time, so a restart with the same command keeps
    the node as it is. Any other join token joins ``hub_url`` and replaces the file, whichever hub and name it held:
    a new token on the command line is the operator asking for a new join. A file without the token's hash, from
    before it was kept, makes no request for its own hub, since its own spent token would be refused at every start.
    """
    hub_url = hub_url.strip().rstrip("/")
    joined = already_joined(hub_url, token)
    if joined is not None and joined.join_token_hash:
        logger.info("Already joined %s as %s; the join token is not used", joined.hub_url, joined.name)
        return joined
    if joined is not None:
        logger.warning(
            "Already joined %s as %s; the join token is not used, since %s cannot tell it from the one it joined "
            "with. Delete that file to join with this token",
            joined.hub_url,
            joined.name,
            path(),
        )
        return joined
    joined = read()
    if joined is not None:
        logger.warning(
            "This node was %s on %s; joining %s with the new join token replaces that, for every process using %s. "
            "A second node on this machine needs its own FLOWFILE_STORAGE_DIR",
            joined.name,
            joined.hub_url,
            hub_url,
            path().parent,
        )
    try:
        node, enabled = _join(
            hub_url,
            token,
            name=name,
            worker_url=worker_url,
            core_url=core_url,
            ui_url=ui_url,
            core_port=core_port,
            worker_port=worker_port,
        )
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


def _without_beats(hub_url: str) -> bool:
    global _hub_without_beats, _said_without_beats
    _hub_without_beats = True
    if not _said_without_beats:
        _said_without_beats = True
        minutes = HEARTBEAT_ABSENT_SECONDS // 60
        logger.info("%s does not take heartbeats yet; this node asks again every %s minutes", hub_url, minutes)
    return False


def heartbeat(process: Literal["core", "worker"], http: "httpx.Client | None" = None) -> bool:
    """Tell the hub this storage dir's node joined that the node is up, as ``process``; whether the hub took the beat.

    Posts ``{version, process}`` to the joined hub's ``/hub/nodes/heartbeat`` with a node assertion signed with this
    node's own key, its audience the joined issuer and that path. A beat the hub does not take, or one that cannot be
    sent (no joined node, no keys, a hub that does not answer), logs one warning per distinct reason until a beat is
    taken again, and never the assertion. Never makes keys. A 404 or 405 is a hub built before it took heartbeats,
    not a refusal: said once per process at INFO, never warned, and the beat loop waits ``HEARTBEAT_ABSENT_SECONDS``
    before it asks again. A 410 is a hub that removed this node: said once at ERROR with the hub's sentence, and the
    beat loop stops the process (``_stop_this_process``).
    """
    global _hub_without_beats, _removed
    import httpx

    from shared import hub_node_key
    from shared._version import get_version

    _hub_without_beats = False
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
    if resp.status_code in (404, 405):
        return _without_beats(node.hub_url)
    if resp.status_code == 410:
        return _removed_from_hub(node, resp)
    if resp.status_code != 204:
        return _not_taken(f"{node.hub_url} answered {resp.status_code}: {_detail(resp) or resp.reason_phrase}")
    _warned_beats.clear()
    return True


def _speaks_for(node: HubNode | None, process: str) -> bool:
    return node is not None and node.has_core == (process == "core")


def _beat(process: Literal["core", "worker"], stop: threading.Event) -> None:
    """Beat at once and every ``HEARTBEAT_SECONDS`` until ``stop`` is set, each time ``process`` speaks for the node;
    after a beat the hub answered 404 or 405 the next one comes ``HEARTBEAT_ABSENT_SECONDS`` later."""
    while True:
        absent = False
        if _speaks_for(read(), process):
            try:
                heartbeat(process)
            except Exception as exc:
                _not_taken(f"the beat failed ({type(exc).__name__})")
            absent = _hub_without_beats
            if _removed:
                _stop_this_process()
                return
        if stop.wait(HEARTBEAT_ABSENT_SECONDS if absent else HEARTBEAT_SECONDS):
            return


def start_heartbeat(process: Literal["core", "worker"]) -> threading.Event | None:
    """Beat for this storage dir's node from a daemon thread, at start and every minute (every 15 minutes while the hub
    answers that it takes no heartbeats); set the event to stop it.

    ``None``, and no thread, when ``process`` does not speak for a node: no node joined a hub from this storage dir, it
    has no keys of its own (joined by an older Flowfile, or set up by hand; said once), ``process`` is a worker beside a
    core that joined for both, or a core on the storage dir of a worker that joined alone. The thread checks the file
    before every beat, so a worker that beats alone goes quiet once a core joins beside it.
    """
    from shared import hub_node_key

    node = read()
    if not _speaks_for(node, process):
        return None
    if not hub_node_key.path().is_file():
        logger.warning(
            "%s joined %s without keys of its own, so it does not tell the hub it is up; join again with a new join "
            "token to make them",
            node.name,
            node.hub_url,
        )
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


def node_storage(token: str | None, name: str | None, *, by_name: bool = False) -> Path | None:
    """Give a node that joins a hub a folder of its own, before anything in this process reads the storage dir.

    With a join token, no ``FLOWFILE_STORAGE_DIR`` and outside Docker (whose storage dir is the container's own), the
    node's files go to ``~/.flowfile-node/<name>``, made with mode ``0o700``, and its secure storage to the ``secure``
    folder in it: ``FLOWFILE_STORAGE_DIR`` is set to the folder and ``SECURE_STORAGE_PATH`` and
    ``FLOWFILE_SECURE_STORAGE_PATH``, each when unset, to ``secure``, in this process's environment, so every process it
    starts uses them too. Returns the folder, or ``None`` when nothing changed. On a machine that runs Flowfile desktop
    this keeps the node out of the desktop's ``~/.flowfile``. ``name`` is the node's name on the hub, the machine's when
    ``None``; one a hub would refuse, which could also name a folder elsewhere, exits with code 2.

    With ``by_name`` and no join token, a process given the ``name`` of a node that joined on this machine
    (``~/.flowfile-node/<name>/hub_node.json`` exists) takes that node's folder the same way, so the worker beside a
    core lands in the core's folder; without that file, or without a name, nothing changes.
    """
    if os.environ.get("FLOWFILE_STORAGE_DIR") or os.environ.get("FLOWFILE_MODE") == "docker":
        return None
    if not token:
        if not (by_name and name and NAME_PATTERN.match(name)):
            return None
        folder = Path.home() / NODE_FILES_DIR / name
        if not (folder / FILE_NAME).is_file():
            return None
    else:
        name = name or default_name()
        if not NAME_PATTERN.match(name):
            logger.error(
                "Cannot keep this node's files under ~/%s: %r is not a name a hub takes (letters, digits, '.', '_' "
                "and '-', starting with a letter or digit); give one with --name",
                NODE_FILES_DIR,
                name,
            )
            raise SystemExit(2)
        folder = Path.home() / NODE_FILES_DIR / name
        folder.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        folder.mkdir(mode=0o700, exist_ok=True)
    os.environ["FLOWFILE_STORAGE_DIR"] = str(folder)
    for variable in ("SECURE_STORAGE_PATH", "FLOWFILE_SECURE_STORAGE_PATH"):
        if not os.environ.get(variable):
            os.environ[variable] = str(folder / "secure")
    return folder


def home_relative(folder: Path) -> str:
    """``folder`` written from ``~`` when it is under the home directory, as the start-up lines name it."""
    try:
        return f"~/{folder.relative_to(Path.home()).as_posix()}"
    except ValueError:
        return str(folder)


def log_storage(folder: Path | None) -> None:
    """Say at start where a node that joins keeps its files, when ``node_storage`` gave it ``folder``."""
    if folder is not None:
        logger.info("Keeping this node's files in %s, apart from Flowfile desktop's", home_relative(folder))


class CannotServe(Exception):
    """A port a node cannot listen on: ``reason`` is the sentence to print, ``in_use`` whether something holds it."""

    def __init__(self, reason: str, *, in_use: bool):
        super().__init__(reason)
        self.reason = reason
        self.in_use = in_use


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


def _is_any_address(host: str) -> bool:
    host = host.strip("[]")
    if not host:
        return True
    try:
        return ipaddress.ip_address(host).is_unspecified
    except ValueError:
        return False


def _shadowed(sock: socket.socket) -> bool:
    """Whether another socket listens on the loopback address of the port ``sock`` holds on every address.

    Linux refuses such a bind; macOS and the BSDs take it and give the loopback address's connections to the other
    socket, so a port Flowfile desktop's worker holds on 127.0.0.1 would be shared. A connection to the loopback address
    that ``sock`` does not receive went to the other socket.
    """
    loopback = "::1" if sock.family == socket.AF_INET6 else "127.0.0.1"
    try:
        probe = socket.create_connection((loopback, sock.getsockname()[1]), timeout=1.0)
    except OSError:
        return False
    with probe, selectors.DefaultSelector() as waiting:
        waiting.register(sock, selectors.EVENT_READ)
        if not waiting.select(0.2):
            return True
        sock.accept()[0].close()
        return False


def _answers(host: str, port: int) -> bool:
    """Whether a server answers on the loopback address ``host`` at ``port``, asked before binding it.

    macOS and the BSDs let a socket bind one address of a port another socket holds on every address, and give it that
    address's connections, so a core bound to 127.0.0.1 would take Flowfile desktop's own connections to its core.
    """
    try:
        socket.create_connection((host.strip("[]"), port), timeout=1.0).close()
    except OSError:
        return False
    return True


def _held_elsewhere(host: str, port: int) -> bool:
    return is_loopback_host(host) and _answers(host, port)


def _holder(port: int) -> str | None:
    """The process listening on ``port``, as ``<name> (pid <pid>)``, when the OS says; ``None`` when it does not."""
    try:
        import psutil
    except ImportError:
        return None
    for process in psutil.process_iter(["pid", "name"]):
        try:
            connections = (getattr(process, "net_connections", None) or process.connections)(kind="inet")
        except (psutil.Error, OSError):
            continue
        for connection in connections:
            if connection.status == psutil.CONN_LISTEN and connection.laddr and connection.laddr.port == port:
                return f"{process.info['name']} (pid {process.info['pid']})"
    return None


def _refusal(host: str, port: int, exc: OSError) -> CannotServe:
    in_use = exc.errno == errno.EADDRINUSE
    holder = _holder(port) if in_use else None
    why = f"in use by {holder}" if holder else (exc.strerror or type(exc).__name__)
    return CannotServe(f"Cannot serve on {_host_port(host, port)}: {why}", in_use=in_use)


def _other_ports(port: int, kept: int | None) -> list[int]:
    """Where a node goes when ``port`` is in use: ``kept``, the next ports above ``port`` but ``HUB_PORT``, then 0."""
    ports = [kept] if kept and kept != port else []
    ports += [other for other in range(port + 1, min(port + PORT_SEARCH, 65535) + 1) if other not in (HUB_PORT, kept)]
    return [*ports, 0]


def listen(host: str, port: int, *, move: bool = False, kept: int | None = None) -> socket.socket:
    """The socket a node serves on, bound and listening on ``host`` and ``port``; ``CannotServe`` when it cannot be.

    The server is handed this socket, so nothing can take the port between this call and serving. Without ``move`` the
    node serves on ``port`` or not at all. With ``move``, a ``port`` in use gives way to ``kept``, the port the node
    last joined with, when it is free; else to the next free port above ``port`` within ``PORT_SEARCH``, ``HUB_PORT``
    left alone; else to one the OS assigns, and one line says so. When moving, a port that another socket holds on the
    loopback address counts as in use for a node bound to every address (``_shadowed``), and a port a server answers on
    counts as in use for a node bound to loopback (``_answers``).
    """
    sock = None
    if not (move and _held_elsewhere(host, port)):
        try:
            sock = _bind(host, port)
        except OSError as exc:
            if not (move and exc.errno == errno.EADDRINUSE):
                raise _refusal(host, port, exc) from None
    if sock is not None:
        if not (move and _is_any_address(host) and _shadowed(sock)):
            return sock
        sock.close()
    for other in _other_ports(port, kept):
        if other and _held_elsewhere(host, other):
            continue
        try:
            sock = _bind(host, other)
        except OSError as exc:
            if exc.errno == errno.EADDRINUSE:
                continue
            raise _refusal(host, other, exc) from None
        if other and _is_any_address(host) and _shadowed(sock):
            sock.close()
            continue
        logger.info("Port %s is in use; serving on %s", port, sock.getsockname()[1])
        return sock
    raise CannotServe(f"Cannot serve on {_host_port(host, port)} or any other port", in_use=True)


def serving_socket(
    process: Literal["core", "worker"],
    host: str,
    port: int,
    *,
    given: bool,
    hub_url: str | None = None,
    token: str | None = None,
    found: bool = False,
) -> socket.socket:
    """The socket ``process`` serves on, bound before it joins a hub, so one that cannot serve never spends a token.

    A port the operator ``given``, and any port of a process that neither joins nor was ``found`` by name: that port.
    A process that already joined with ``token``: the port the file keeps for it (``core_port`` or ``worker_port``),
    which the hub has, else ``port`` for a file from before it was kept. A worker ``found`` by its name beside its core
    (``node_storage``): the ``worker_port`` that core's join kept, else ``port``. A process about to join: ``port``,
    else the port it last joined with, else a free one (``listen``), which its join then reports. One that cannot bind
    its port exits with code 1 and one line, which says so when the hub has the node at that port.
    """
    field = f"{process}_port"
    move, kept, hubs_port = False, None, False
    if token and not given:
        joined = already_joined(hub_url or "", token)
        if joined is None:
            last = read()
            move, kept = True, getattr(last, field) if last else None
        elif getattr(joined, field):
            port, hubs_port = getattr(joined, field), True
    elif found and not given:
        joined = read()
        if joined is not None and getattr(joined, field):
            port, hubs_port = getattr(joined, field), True
    try:
        return listen(host, port, move=move, kept=kept)
    except CannotServe as refused:
        reason = refused.reason
        if refused.in_use and hubs_port:
            reason += "; the hub has this node at that port, so free it or join again with a new join token"
        logger.error(reason)
        raise SystemExit(1) from None


def _free(port: int) -> int | None:
    """``port`` when no server holds it on any address, as the worker beside a core binds it; for 0, one the OS has."""
    try:
        sock = _bind("0.0.0.0", port)
    except OSError:
        return None
    with sock:
        if port and _shadowed(sock):
            return None
        return sock.getsockname()[1]


def beside_port(port: int, *, kept: int | None = None) -> int:
    """The port a core that joins a hub gives for the worker beside it, which that worker then serves on.

    ``kept``, the port the node joined with before, whether a server answers there or not: that server is the worker
    beside, serving already. Else ``port`` when no server holds it on any address; else the next free port above it
    within ``PORT_SEARCH``, ``HUB_PORT`` left alone; else one the OS has free, and one line says so. The worker binds it
    itself, later, so it is checked here by binding it and closing it again; ``port`` when even the OS has none.
    """
    if kept:
        return kept
    for other in (port, *_other_ports(port, None)):
        free = _free(other)
        if free is not None:
            if free != port:
                logger.info("Port %s is in use; the worker beside this core is to serve on %s", port, free)
            return free
    return port


def is_loopback_host(host: str) -> bool:
    """Whether ``host``, a bind address, is one only this machine reaches: ``localhost`` or a loopback IP."""
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host.strip("[]")).is_loopback
    except ValueError:
        return False


def _route_address(host: str, port: int, family: int) -> str | None:
    """This machine's ``family`` address on the interface that reaches ``host``, asked of the OS with a UDP socket
    "connected" there, which sends nothing; ``None`` when there is no route or only a loopback or link-local one."""
    try:
        targets = socket.getaddrinfo(host, port, family=family, type=socket.SOCK_DGRAM)
    except (OSError, UnicodeError):
        return None
    for found, _, _, _, target in targets:
        try:
            with socket.socket(found, socket.SOCK_DGRAM) as probe:
                probe.connect(target)
                address = probe.getsockname()[0]
        except OSError:
            continue
        try:
            ip = ipaddress.ip_address(address)
        except ValueError:
            continue
        if not (ip.is_loopback or ip.is_unspecified or ip.is_link_local):
            return address
    return None


def address_for(hub_url: str, host: str) -> str | None:
    """The address a node bound to ``host`` gives the hub at ``hub_url``; ``None`` when there is none but its name.

    A node bound to one address gives that address, loopback included. One bound to every address (``0.0.0.0``,
    ``::``) gives this machine's address of that family on the interface that reaches the hub, or, for a hub on this
    machine, the one that reaches other machines (the route to a documentation address, which sends nothing).
    """
    if not _is_any_address(host):
        return host.strip("[]")
    parts = urlsplit(hub_url)
    try:
        hub_port = parts.port
    except ValueError:
        hub_port = None
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    targets = [(_ROUTE_PROBES[family], 80)]
    if parts.hostname and not is_loopback_host(parts.hostname):
        targets.insert(0, (parts.hostname, hub_port or (443 if parts.scheme == "https" else 80)))
    for target in targets:
        address = _route_address(*target, family)
        if address is not None:
            return address
    return None


def node_url(hub_url: str, host: str, port: int, *, tls: bool) -> str:
    """The address a node bound to ``host`` and ``port`` gives the hub at ``hub_url``, when the operator gives none.

    ``address_for``'s address, else this machine's name as ``default_url`` gives it.
    """
    address = address_for(hub_url, host)
    if address is None:
        return default_url(port, tls=tls)
    return f"{'https' if tls else 'http'}://{_host_port(address, port)}"
