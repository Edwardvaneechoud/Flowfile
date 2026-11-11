"""This node's identity on a hub: the join, ``hub_node.json`` that remembers it, and the heartbeat.

The file holds nothing secret: the join token is spent by the join and written nowhere, only a short hash of it is
kept so a restart with the same token makes no request. A joined node beats to its hub because the hub may not reach
it (behind a NAT) while it can always reach the hub. A node keeps its files and keys in a storage dir and secure store
of its own, never the desktop's, or the desktop would act as the joined node.
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
from urllib.parse import urlsplit

from shared.net import is_loopback_host

if TYPE_CHECKING:
    import httpx

logger = logging.getLogger("flowfile.hub_node")
# The join's lines are start-up lines for the operator, so they show where only warnings would.
logger.setLevel(logging.INFO)

FILE_NAME = "hub_node.json"
JOIN_PATH = "/hub/nodes/join"
HEARTBEAT_PATH = "/hub/nodes/heartbeat"
HEARTBEAT_SECONDS = 60

_BACKLOG = 2048

_FIELDS = {"hub_url": str, "issuer": str, "name": str, "node_id": int, "join_token_hash": str}
_OPTIONAL_FIELDS = {"has_core": bool}
_warned_unreadable = False
_warned_beats: set[str] = set()
_removed = False


class JoinRefused(Exception):
    """A join that did not happen; ``reason`` never carries the token."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class HubNode:
    """What a node keeps after joining; ``has_core`` (a core joined for itself and its worker) decides who beats."""

    hub_url: str
    issuer: str
    name: str
    node_id: int
    join_token_hash: str
    has_core: bool = False


@dataclass(frozen=True)
class NodeSettings:
    hub_url: str | None
    name: str | None
    issuer: str | None


def path() -> Path:
    from shared.storage_config import storage

    return storage.base_directory / FILE_NAME


def _fits(value, kind: type) -> bool:
    if kind is int:
        return isinstance(value, int) and not isinstance(value, bool)
    if kind is bool:
        return isinstance(value, bool)
    return isinstance(value, kind) and bool(value)


def read() -> HubNode | None:
    """The joined identity, read on every call; ``None`` when absent or unreadable (one warning per process)."""
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
    """This storage dir's node when it joined a hub and has keys of its own, else ``None``."""
    from shared import hub_node_key

    node = read()
    return node if node is not None and hub_node_key.path().is_file() else None


def _env(name: str) -> str | None:
    return os.environ.get(name, "").strip() or None


def resolved() -> NodeSettings:
    """Each setting from its variable, else the joined file; the file counts only for the hub ``FLOWFILE_HUB_URL``
    names, so a file from another hub is never mixed with this one's URL."""
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
    """Join with a join token and keep the answer; ``(node, enabled)``. Keys that cannot be kept refuse the join
    before any request. Logs method, path and status, never the token or a body."""
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
    """Join before serving; a refused join exits 2. The same token joins once; a new one is the operator asking for a
    new join and replaces the file."""
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
    """SIGTERM ourselves: a clean exit 0, which the join script's service does not restart, so a removed node stays
    stopped."""
    os.kill(os.getpid(), signal.SIGTERM)


def heartbeat(
    process: Literal["core", "worker"], http: "httpx.Client | None" = None, *, addresses: list[str] | None = None
) -> bool:
    """One beat signed with the node's own key; whether the hub took it. Never makes keys and never logs the
    assertion; a 410 means the hub removed this node and the beat loop stops the process."""
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
    body: dict = {"version": get_version(), "process": process}
    if addresses is not None:
        body["addresses"] = addresses
    client = http or httpx.Client(timeout=httpx.Timeout(10.0), follow_redirects=False)
    try:
        resp = client.post(
            f"{node.hub_url.rstrip('/')}{HEARTBEAT_PATH}",
            json=body,
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


def _beat(
    process: Literal["core", "worker"], stop: threading.Event, *, port: int | None = None, tls: bool = False
) -> None:
    """Beat now and every ``HEARTBEAT_SECONDS``, addresses worked out afresh since the network can change."""
    while True:
        node = read()
        if _speaks_for(node, process):
            try:
                addresses = reach_addresses(port, tls=tls, hub_url=node.hub_url) if port is not None else None
                heartbeat(process, addresses=addresses)
            except Exception as exc:
                _not_taken(f"the beat failed ({type(exc).__name__})")
            if _removed:
                _stop_this_process()
                return
        if stop.wait(HEARTBEAT_SECONDS):
            return


def start_heartbeat(
    process: Literal["core", "worker"], *, port: int | None = None, tls: bool = False
) -> threading.Event | None:
    """Beat from a daemon thread; set the returned event to stop it. ``None`` when ``process`` does not speak for the
    node (one process beats: core when it joined for both, else the worker)."""
    node = joined()
    if not _speaks_for(node, process):
        return None
    stop = threading.Event()
    threading.Thread(
        target=_beat, args=(process, stop), kwargs={"port": port, "tls": tls}, name="hub-heartbeat", daemon=True
    ).start()
    logger.info("Telling %s that %s is up, every %s seconds", node.hub_url, node.name, HEARTBEAT_SECONDS)
    return stop


def reach_addresses(port: int, *, tls: bool, hub_url: str) -> list[str]:
    """Addresses the hub may try when the one it holds stops answering, likeliest first, at most 8; IPv4 only, since a
    joined node binds ``0.0.0.0``."""
    hosts: list[str] = []
    if is_loopback_host(urlsplit(hub_url).hostname or ""):
        hosts.append("localhost")
    toward_hub = _address_toward(hub_url)
    if toward_hub is not None:
        hosts.append(toward_hub)
    hosts.append(_default_host())
    hosts.extend(_interface_addresses())
    scheme = "https" if tls else "http"
    return [f"{scheme}://{host}:{port}" for host in list(dict.fromkeys(hosts))[:8]]


def _reaches_others(address: str) -> bool:
    try:
        ip = ipaddress.ip_address(address)
    except ValueError:
        return False
    return ip.version == 4 and not (ip.is_loopback or ip.is_unspecified or ip.is_link_local)


def _address_toward(hub_url: str) -> str | None:
    """The local address toward the hub, from a "connected" UDP socket, which sends nothing."""
    parts = urlsplit(hub_url)
    try:
        hub_port = parts.port
    except ValueError:
        hub_port = None
    hub_port = hub_port or (443 if parts.scheme == "https" else 80)
    try:
        targets = socket.getaddrinfo(parts.hostname or "", hub_port, family=socket.AF_INET, type=socket.SOCK_DGRAM)
    except (OSError, UnicodeError):
        return None
    for family, _, _, _, target in targets:
        try:
            with socket.socket(family, socket.SOCK_DGRAM) as probe:
                probe.connect(target)
                address = probe.getsockname()[0]
        except OSError:
            continue
        if _reaches_others(address):
            return address
    return None


def _interface_addresses() -> list[str]:
    import psutil

    up = {name for name, stats in psutil.net_if_stats().items() if stats.isup}
    return [
        entry.address
        for name, entries in psutil.net_if_addrs().items()
        if name in up
        for entry in entries
        if entry.family == socket.AF_INET and _reaches_others(entry.address)
    ]


def default_name() -> str:
    return socket.gethostname().split(".")[0]


def _default_host() -> str:
    """The full name, or the host name where that is a ``*.arpa`` reverse-lookup name, which no desktop reaches."""
    host = socket.getfqdn()
    if host.endswith(".arpa"):
        host = socket.gethostname()
    return host


def default_url(port: int, *, tls: bool) -> str:
    return f"{'https' if tls else 'http'}://{_default_host()}:{port}"


def require_own_storage_dir(token: str | None) -> None:
    """Exit 2 on a join outside docker mode without ``FLOWFILE_STORAGE_DIR`` and ``FLOWFILE_SECURE_STORAGE_PATH``,
    before anything reads the storage dir: else the desktop would act as the joined node."""
    if not token or os.environ.get("FLOWFILE_MODE") == "docker":
        return
    missing = [name for name in ("FLOWFILE_STORAGE_DIR", "FLOWFILE_SECURE_STORAGE_PATH") if not os.environ.get(name)]
    if not missing:
        return
    logger.error(
        "Cannot join a hub without a storage dir and a secure store of this node's own: set %s to folders for it, as "
        "the hub's install line does, so it stays out of Flowfile desktop's ~/.flowfile and ~/.config/flowfile",
        " and ".join(missing),
    )
    raise SystemExit(2)


def _host_port(host: str, port: int) -> str:
    host = host.strip("[]")
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def _bind(host: str, port: int) -> socket.socket:
    """Bind as the server does: reusable on POSIX so a restart is not refused for closing connections, IPv6 only for
    an IPv6 address."""
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
    """Bind before joining, so a node that cannot serve never spends a token; the server is handed this socket, so
    nothing takes the port in between. Exits 1 when it cannot be bound."""
    try:
        return _bind(host, port)
    except OSError as exc:
        logger.error("Cannot serve on %s: %s", _host_port(host, port), exc.strerror or type(exc).__name__)
        raise SystemExit(1) from None
