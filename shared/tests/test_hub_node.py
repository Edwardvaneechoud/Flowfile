"""A node's identity on a hub: how the variables and the joined file combine, the join itself, joining at start, the
storage dir a node that joins must have, the port it binds, the address it gives, and the heartbeat that tells the hub
the node is up."""

import json
import logging
import os
import signal
import socket
import stat
import threading
import uuid
from http import HTTPStatus
from types import SimpleNamespace

import jwt
import pytest

from shared import hub_node, hub_node_key
from shared._version import get_version
from shared.hub_node import HubNode, JoinRefused, NodeSettings
from shared.tests import hub_join
from shared.tests.hub_join import EXPIRED, HEARTBEAT_PATH, NOT_VALID, USED, FakeJoinHub
from shared.tests.hub_jwks import HubSigner

join_hub = hub_join.join_hub

HUB = "http://hub.example:63580"
VARIABLES = ("FLOWFILE_HUB_URL", "FLOWFILE_NODE_NAME")


@pytest.fixture(autouse=True)
def node_file(tmp_path, monkeypatch):
    """Where the node keeps its file in these tests, absent until a test writes it, and none of the variables set."""
    target = tmp_path / "storage" / hub_node.FILE_NAME
    monkeypatch.setattr(hub_node, "path", lambda: target)
    monkeypatch.setattr(hub_node, "_warned_unreadable", False)
    monkeypatch.setattr(hub_node, "_warned_beats", set())
    for name in VARIABLES:
        monkeypatch.delenv(name, raising=False)
    return target


def write_node(target, drop: tuple[str, ...] = (), **fields) -> dict:
    node = {
        "hub_url": HUB,
        "issuer": "http://localhost:63580",
        "name": "team",
        "node_id": 3,
        "join_token_hash": "0" * 16,
        **fields,
    }
    for field in drop:
        del node[field]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(node), encoding="utf-8")
    return node


def join_with(hub: FakeJoinHub, token: str, name: str = "worker-1") -> HubNode:
    return hub_node.join(hub.url, token, name=name, worker_url="http://worker-1.example:63579")[0]


def test_nothing_set_and_no_file_is_no_hub():
    assert hub_node.resolved() == NodeSettings(hub_url=None, name=None, issuer=None)


def test_the_variables_alone_are_the_settings_with_the_url_as_issuer(monkeypatch):
    monkeypatch.setenv("FLOWFILE_HUB_URL", HUB + "/")
    monkeypatch.setenv("FLOWFILE_NODE_NAME", "team")

    assert hub_node.resolved() == NodeSettings(hub_url=HUB, name="team", issuer=HUB)


def test_the_file_alone_is_the_settings(node_file):
    write_node(node_file)

    assert hub_node.resolved() == NodeSettings(hub_url=HUB, name="team", issuer="http://localhost:63580")


def test_the_file_fills_in_for_the_hub_the_url_names(node_file, monkeypatch):
    write_node(node_file)
    monkeypatch.setenv("FLOWFILE_HUB_URL", HUB + "/")

    assert hub_node.resolved() == NodeSettings(hub_url=HUB, name="team", issuer="http://localhost:63580")


def test_a_file_from_another_hub_is_ignored(node_file, monkeypatch):
    write_node(node_file)
    monkeypatch.setenv("FLOWFILE_HUB_URL", "http://other-hub.example:63580")

    assert hub_node.resolved() == NodeSettings(
        hub_url="http://other-hub.example:63580", name=None, issuer="http://other-hub.example:63580"
    )


def test_the_name_variable_wins_over_the_file_and_the_issuer_stays_the_files(node_file, monkeypatch):
    write_node(node_file)
    monkeypatch.setenv("FLOWFILE_NODE_NAME", "by-hand")

    assert hub_node.resolved() == NodeSettings(hub_url=HUB, name="by-hand", issuer="http://localhost:63580")


def test_a_missing_file_is_none_and_says_nothing(caplog):
    with caplog.at_level(logging.WARNING, logger="flowfile.hub_node"):
        assert hub_node.read() is None

    assert caplog.records == []


def test_a_file_that_is_not_json_is_none_and_warns_once_naming_it(node_file, caplog):
    node_file.parent.mkdir(parents=True)
    node_file.write_text("{not json", encoding="utf-8")

    with caplog.at_level(logging.WARNING, logger="flowfile.hub_node"):
        assert hub_node.read() is None
        assert hub_node.read() is None

    warnings = [record for record in caplog.records if record.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert str(node_file) in warnings[0].getMessage()


@pytest.mark.parametrize(
    ("drop", "fields"),
    [
        (("name",), {}),
        (("join_token_hash",), {}),
        ((), {"node_id": "3"}),
        ((), {"node_id": True}),
        ((), {"hub_url": 7}),
    ],
    ids=["no name", "no token hash", "node id as text", "node id as bool", "url as number"],
)
def test_a_file_missing_a_field_or_with_one_of_the_wrong_type_is_none(node_file, drop, fields):
    write_node(node_file, drop=drop, **fields)

    assert hub_node.read() is None


def test_a_join_sends_the_contract_body_and_keeps_the_answer_but_not_the_token(join_hub, node_file):
    token = join_hub.mint()

    node = join_with(join_hub, token)

    assert join_hub.calls == [
        (
            "/hub/nodes/join",
            {
                "token": token,
                "name": "worker-1",
                "worker_url": "http://worker-1.example:63579",
                "core_url": None,
                "keys": hub_node_key.load_or_create().public_jwks(),
            },
        )
    ]
    kept = json.loads(node_file.read_text(encoding="utf-8"))
    assert set(kept) == {"hub_url", "issuer", "name", "node_id", "join_token_hash", "has_core"}
    assert (kept["hub_url"], kept["issuer"], kept["name"], kept["node_id"]) == (
        join_hub.url,
        join_hub.url,
        "worker-1",
        1,
    )
    assert token not in node_file.read_text(encoding="utf-8")
    assert hub_node.read() == node
    assert not node_file.with_name(node_file.name + ".tmp").exists()


def test_a_join_keeps_the_hub_url_without_its_trailing_slash(join_hub, node_file):
    hub_node.join(join_hub.url + "/", join_hub.mint(), name="worker-1", worker_url="http://w:1")

    assert hub_node.read().hub_url == join_hub.url
    assert join_hub.calls[0][0] == "/hub/nodes/join"


def test_a_join_token_used_twice_is_refused_and_the_first_join_stays(join_hub, node_file):
    token = join_hub.mint()
    join_with(join_hub, token)
    first = node_file.read_bytes()

    with pytest.raises(JoinRefused) as refused:
        join_with(join_hub, token, name="worker-2")

    assert "has been used" in refused.value.reason
    assert refused.value.reason == USED
    assert node_file.read_bytes() == first


@pytest.mark.parametrize(
    ("make_token", "sentence"),
    [(lambda hub: "join-never-minted", "is not valid"), (lambda hub: hub.mint(expired=True), "has expired")],
    ids=["unknown", "expired"],
)
def test_a_refused_join_token_names_the_hubs_reason_and_writes_no_file(join_hub, node_file, make_token, sentence):
    token = make_token(join_hub)

    with pytest.raises(JoinRefused) as refused:
        join_with(join_hub, token)

    assert sentence in refused.value.reason
    assert token not in refused.value.reason
    assert not node_file.exists()


def test_a_name_the_hub_does_not_take_is_refused_with_the_field(join_hub, node_file):
    with pytest.raises(JoinRefused) as refused:
        join_with(join_hub, join_hub.mint(), name="bad name")

    assert "refused this node's name or address" in refused.value.reason
    assert "(name)" in refused.value.reason
    assert "bad name" not in refused.value.reason
    assert not node_file.exists()


def test_a_hub_that_does_not_answer_is_refused_naming_its_url(node_file):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        closed = f"http://127.0.0.1:{probe.getsockname()[1]}"

    with pytest.raises(JoinRefused) as refused:
        hub_node.join(closed, "join-token", name="worker-1", worker_url="http://w:1")

    assert refused.value.reason.startswith(f"{closed} did not answer (")
    assert not node_file.exists()


def test_an_answer_without_an_issuer_is_not_a_hub(node_file):
    class HubWithoutIssuer(FakeJoinHub):
        def joined(self, body, grant, node_id):
            answer = super().joined(body, grant, node_id)
            del answer["issuer"]
            return answer

    hub = HubWithoutIssuer()
    try:
        with pytest.raises(JoinRefused) as refused:
            join_with(hub, hub.mint())
    finally:
        hub.close()

    assert refused.value.reason == f"{hub.url} did not answer as a Flowfile hub does"
    assert not node_file.exists()


def key_file_of(node_file):
    return node_file.with_name(hub_node_key.FILE_NAME)


def test_a_first_join_keeps_the_nodes_keys_and_sends_their_public_halves(join_hub, node_file):
    join_with(join_hub, join_hub.mint())

    sent = join_hub.calls[0][1]["keys"]
    assert sent == hub_node_key.load_or_create().public_jwks()
    assert (sent["sig"]["kty"], sent["enc"]["kty"]) == ("EC", "OKP")
    assert "d" not in sent["sig"] and "d" not in sent["enc"]
    if os.name != "nt":
        assert stat.S_IMODE(key_file_of(node_file).stat().st_mode) == 0o600


def test_a_second_join_from_the_same_storage_dir_sends_the_same_keys(join_hub, node_file):
    join_with(join_hub, join_hub.mint())
    kept = key_file_of(node_file).read_bytes()

    join_with(join_hub, join_hub.mint(), name="worker-2")

    first, second = (body["keys"] for _, body in join_hub.calls)
    assert first == second
    assert key_file_of(node_file).read_bytes() == kept


def test_an_assertion_the_node_signs_verifies_with_the_key_it_sent(join_hub):
    join_with(join_hub, join_hub.mint())
    audience = join_hub.url + "/hub/nodes/heartbeat"

    token = hub_node_key.load_or_create().assertion("worker-1", audience)

    sent = jwt.PyJWK(join_hub.calls[0][1]["keys"]["sig"])
    assert jwt.decode(token, sent, algorithms=["ES256"], audience=audience)["sub"] == "node:worker-1"


def test_a_join_sends_and_logs_no_private_key(join_hub, node_file, caplog):
    with caplog.at_level(logging.DEBUG):
        hub_node.join_at_start(join_hub.url, join_hub.mint(), name="worker-1", worker_url="http://w:1")

    kept = json.loads(key_file_of(node_file).read_text(encoding="utf-8"))
    sent = json.dumps(join_hub.calls)
    for secret in (*kept["sig"].splitlines()[1:-1], kept["enc"]):
        assert secret not in sent and secret not in caplog.text


def test_a_restart_that_makes_no_request_makes_no_keys(join_hub, node_file):
    token = join_hub.mint()
    write_node(node_file, hub_url=join_hub.url, join_token_hash=hub_node.token_hash(token))

    hub_node.join_at_start(join_hub.url, token, name="team", worker_url="http://w:1")

    assert join_hub.calls == []
    assert not key_file_of(node_file).exists()


@pytest.mark.skipif(os.name == "nt", reason="Windows keeps no POSIX file modes")
@pytest.mark.skipif(hasattr(os, "geteuid") and os.geteuid() == 0, reason="root writes into a read-only directory")
def test_a_storage_dir_that_cannot_keep_the_keys_refuses_the_join_and_sends_nothing(join_hub, node_file):
    node_file.parent.mkdir()
    node_file.parent.chmod(0o500)
    try:
        with pytest.raises(JoinRefused) as refused:
            join_with(join_hub, join_hub.mint())
    finally:
        node_file.parent.chmod(0o700)

    assert refused.value.reason == f"could not keep this node's keys at {key_file_of(node_file)}: PermissionError"
    assert join_hub.calls == []
    assert os.listdir(node_file.parent) == []


def test_a_join_at_start_whose_keys_cannot_be_kept_exits_2_before_any_request(join_hub, node_file, caplog):
    node_file.parent.write_text("a file where the storage dir should be", encoding="utf-8")

    with caplog.at_level(logging.INFO, logger="flowfile.hub_node"):
        with pytest.raises(SystemExit) as exited:
            hub_node.join_at_start(join_hub.url, join_hub.mint(), name="worker-1", worker_url="http://w:1")

    assert exited.value.code == 2
    sentence = f"could not keep this node's keys at {key_file_of(node_file)}: "
    assert f"Could not join {join_hub.url}: {sentence}" in caplog.text
    assert join_hub.calls == []


def test_joining_at_start_joins_once_and_says_whether_the_node_is_enabled(join_hub, node_file, caplog):
    with caplog.at_level(logging.INFO, logger="flowfile.hub_node"):
        node = hub_node.join_at_start(
            join_hub.url, join_hub.mint(enable_on_join=True), name="worker-1", worker_url="http://w:1"
        )

    assert node == hub_node.read()
    assert f"Joined {join_hub.url} as worker-1 (node 1); it is enabled" in caplog.text


def test_a_node_that_waits_for_an_admin_is_told_so(join_hub, caplog):
    with caplog.at_level(logging.INFO, logger="flowfile.hub_node"):
        hub_node.join_at_start(join_hub.url, join_hub.mint(), name="worker-1", worker_url="http://w:1")

    assert "an admin enables it on the hub's Compute page" in caplog.text


def test_a_restart_with_the_same_join_token_makes_no_request(join_hub, node_file, caplog):
    token = join_hub.mint()
    first = hub_node.join_at_start(join_hub.url, token, name="worker-1", worker_url="http://w:1")

    with caplog.at_level(logging.INFO, logger="flowfile.hub_node"):
        node = hub_node.join_at_start(join_hub.url + "/", token, name="worker-1", worker_url="http://w:1")

    assert len(join_hub.calls) == 1
    assert node == first and node.join_token_hash == hub_node.token_hash(token)
    assert f"Already joined {join_hub.url} as worker-1; the join token is not used" in caplog.text
    assert token not in node_file.read_text()


def test_a_new_join_token_joins_again_under_its_name(join_hub, node_file, caplog):
    hub_node.join_at_start(join_hub.url, join_hub.mint(), name="worker-1", worker_url="http://w:1")

    with caplog.at_level(logging.INFO, logger="flowfile.hub_node"):
        node = hub_node.join_at_start(join_hub.url, join_hub.mint(), name="worker-2", worker_url="http://w:1")

    assert len(join_hub.calls) == 2
    assert node.name == "worker-2" and hub_node.read() == node
    warnings = [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert warnings[0].startswith(
        f"This node was worker-1 on {join_hub.url}; joining {join_hub.url} with the new join token replaces that"
    )
    assert "needs its own FLOWFILE_STORAGE_DIR" in warnings[0]


def test_a_new_join_token_moves_a_node_from_another_hub(join_hub, node_file):
    write_node(node_file)

    node = hub_node.join_at_start(join_hub.url, join_hub.mint(), name="worker-1", worker_url="http://w:1")

    assert len(join_hub.calls) == 1
    assert node.hub_url == join_hub.url and hub_node.read() == node


def test_a_refused_new_join_token_keeps_the_node_as_it_was(join_hub, node_file, caplog):
    kept = write_node(node_file, hub_url=join_hub.url, join_token_hash="0" * 16)

    with caplog.at_level(logging.INFO, logger="flowfile.hub_node"):
        with pytest.raises(SystemExit) as exited:
            hub_node.join_at_start(join_hub.url, join_hub.mint(expired=True), name="worker-2", worker_url="http://w:1")

    assert exited.value.code == 2
    assert f"Could not join {join_hub.url}: {EXPIRED}" in caplog.text
    assert json.loads(node_file.read_text()) == kept


@pytest.mark.parametrize(
    ("refusal", "sentence"),
    [
        ("not valid", NOT_VALID),
        ("expired", EXPIRED),
        ("used", USED),
        ("bad name", "The hub refused this node's name or address: String should match pattern"),
    ],
)
def test_a_refused_join_at_start_exits_2_with_the_hubs_sentence(join_hub, node_file, caplog, refusal, sentence):
    token, name = join_hub.mint(), "worker-1"
    if refusal == "not valid":
        token = "join-never-minted"
    elif refusal == "expired":
        token = join_hub.mint(expired=True)
    elif refusal == "used":
        join_with(join_hub, token)
        node_file.unlink()
    else:
        name = "bad name"

    with caplog.at_level(logging.INFO, logger="flowfile.hub_node"):
        with pytest.raises(SystemExit) as exited:
            hub_node.join_at_start(join_hub.url, token, name=name, worker_url="http://w:1")

    assert exited.value.code == 2
    assert f"Could not join {join_hub.url}: {sentence}" in caplog.text
    assert token not in caplog.text
    assert not node_file.exists()


def test_a_node_names_itself_after_the_machine_when_the_operator_does_not(monkeypatch):
    monkeypatch.setattr(hub_node.socket, "gethostname", lambda: "build-7.example.com")
    monkeypatch.setattr(hub_node.socket, "getfqdn", lambda: "build-7.example.com")

    assert hub_node.default_name() == "build-7"
    assert hub_node.default_url(63579, tls=False) == "http://build-7.example.com:63579"
    assert hub_node.default_url(63579, tls=True) == "https://build-7.example.com:63579"


def test_a_reverse_lookup_name_is_not_an_address_a_desktop_reaches(monkeypatch):
    monkeypatch.setattr(hub_node.socket, "gethostname", lambda: "build-7.local")
    monkeypatch.setattr(hub_node.socket, "getfqdn", lambda: "1.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.ip6.arpa")

    assert hub_node.default_url(63579, tls=False) == "http://build-7.local:63579"


class Probe:
    """A UDP socket as ``reach_addresses`` uses one: "connected" to the hub's address, it answers the local address the
    OS would route from, and it sends nothing (``sent`` would hold anything written)."""

    toward: dict[str, str] = {}
    sent: list = []

    def __init__(self, family, kind):
        assert (family, kind) == (socket.AF_INET, socket.SOCK_DGRAM)
        self.local = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def connect(self, target):
        self.local = Probe.toward[target[0]]

    def getsockname(self):
        return (self.local, 50000)

    def send(self, data):
        Probe.sent.append(data)

    sendto = send


def interface(address: str, *, up: bool = True, family=socket.AF_INET) -> tuple:
    return (SimpleNamespace(isup=up), [SimpleNamespace(family=family, address=address)])


@pytest.fixture
def machine(monkeypatch):
    """A machine named ``build-7.example.com`` whose hub is reached from 10.0.0.5; its interfaces are set per test as
    ``{name: interface(...)}``, and the hub's host resolves to the address its URL names."""
    import psutil

    monkeypatch.setattr(hub_node.socket, "gethostname", lambda: "build-7.example.com")
    monkeypatch.setattr(hub_node.socket, "getfqdn", lambda: "build-7.example.com")
    monkeypatch.setattr(hub_node.socket, "socket", Probe)
    monkeypatch.setattr(Probe, "toward", {"192.0.2.10": "10.0.0.5", "127.0.0.1": "127.0.0.1"})
    monkeypatch.setattr(Probe, "sent", [])
    names = {"hub.example.test": "192.0.2.10"}
    monkeypatch.setattr(
        hub_node.socket,
        "getaddrinfo",
        lambda host, port, **kw: [(socket.AF_INET, socket.SOCK_DGRAM, 17, "", (names.get(host, host), port))],
    )
    interfaces: dict[str, tuple] = {}
    monkeypatch.setattr(psutil, "net_if_stats", lambda: {name: stats for name, (stats, _) in interfaces.items()})
    monkeypatch.setattr(psutil, "net_if_addrs", lambda: {name: addrs for name, (_, addrs) in interfaces.items()})
    return interfaces


def test_reach_addresses_lead_with_the_address_toward_the_hub_then_the_name_then_every_other_interface(machine):
    machine["en0"] = interface("10.0.0.5")
    machine["en1"] = interface("192.168.1.179")
    machine["utun0"] = interface("172.16.0.9")

    assert hub_node.reach_addresses(63610, tls=False, hub_url="http://192.0.2.10:8000") == [
        "http://10.0.0.5:63610",
        "http://build-7.example.com:63610",
        "http://192.168.1.179:63610",
        "http://172.16.0.9:63610",
    ]
    assert Probe.sent == []


def test_reach_addresses_name_localhost_only_for_a_hub_on_this_machine(machine):
    machine["en0"] = interface("10.0.0.5")

    local = hub_node.reach_addresses(63610, tls=True, hub_url="https://127.0.0.1:8000")
    remote = hub_node.reach_addresses(63610, tls=True, hub_url="https://192.0.2.10")

    assert local == ["https://localhost:63610", "https://build-7.example.com:63610", "https://10.0.0.5:63610"]
    assert remote == ["https://10.0.0.5:63610", "https://build-7.example.com:63610"]


def test_reach_addresses_leave_out_loopback_link_local_down_and_ipv6_interfaces(machine):
    machine["lo0"] = interface("127.0.0.1")
    machine["en0"] = interface("169.254.7.7")
    machine["en1"] = interface("192.168.1.179", up=False)
    machine["en2"] = interface("fe80::1", family=socket.AF_INET6)
    machine["en3"] = interface("10.0.0.5")

    assert hub_node.reach_addresses(63610, tls=False, hub_url="http://hub.example.test") == [
        "http://10.0.0.5:63610",
        "http://build-7.example.com:63610",
    ]


def test_reach_addresses_are_at_most_eight(machine):
    for n in range(12):
        machine[f"en{n}"] = interface(f"10.0.{n}.5")

    addresses = hub_node.reach_addresses(63610, tls=False, hub_url="http://192.0.2.10")

    assert len(addresses) == 8
    assert addresses[:2] == ["http://10.0.0.5:63610", "http://build-7.example.com:63610"]


def test_reach_addresses_without_a_route_to_the_hub_start_with_the_name(machine, monkeypatch):
    def unresolvable(host, port, **kw):
        raise socket.gaierror(8, "nodename nor servname provided")

    monkeypatch.setattr(hub_node.socket, "getaddrinfo", unresolvable)
    machine["en0"] = interface("10.0.0.5")

    assert hub_node.reach_addresses(63610, tls=False, hub_url="http://no-such-hub.invalid") == [
        "http://build-7.example.com:63610",
        "http://10.0.0.5:63610",
    ]


def test_a_start_with_the_token_the_node_joined_with_is_already_joined(join_hub, node_file):
    token = join_hub.mint()
    join_with(join_hub, token)

    assert hub_node.already_joined(token).name == "worker-1"
    assert hub_node.already_joined(join_hub.mint()) is None


@pytest.fixture
def home(tmp_path, monkeypatch):
    """A home directory of the test's own holding Flowfile desktop's empty ``~/.flowfile``, with neither
    ``FLOWFILE_STORAGE_DIR`` nor ``FLOWFILE_MODE`` set."""
    target = tmp_path / "home"
    (target / ".flowfile").mkdir(parents=True)
    monkeypatch.setenv("HOME", str(target))
    for name in ("FLOWFILE_STORAGE_DIR", "FLOWFILE_SECURE_STORAGE_PATH", "FLOWFILE_MODE"):
        monkeypatch.delenv(name, raising=False)
    return target


def refusal(*missing: str) -> str:
    return (
        f"Cannot join a hub without a storage dir and a secure store of this node's own: set {' and '.join(missing)} "
        "to folders for it, as the hub's install line does, so it stays out of Flowfile desktop's ~/.flowfile and "
        "~/.config/flowfile"
    )


@pytest.mark.skipif(os.name == "nt", reason="the home directory comes from HOME on POSIX only")
@pytest.mark.parametrize(
    "given,missing",
    [
        ({}, ("FLOWFILE_STORAGE_DIR", "FLOWFILE_SECURE_STORAGE_PATH")),
        ({"FLOWFILE_STORAGE_DIR": "/srv/flowfile"}, ("FLOWFILE_SECURE_STORAGE_PATH",)),
        ({"FLOWFILE_SECURE_STORAGE_PATH": "/srv/flowfile/secure"}, ("FLOWFILE_STORAGE_DIR",)),
    ],
    ids=["neither", "no secure store", "no storage dir"],
)
def test_a_join_without_a_storage_dir_and_secure_store_of_its_own_exits_2_naming_what_is_missing(
    home, join_hub, caplog, monkeypatch, given, missing
):
    for name, value in given.items():
        monkeypatch.setenv(name, value)

    with caplog.at_level(logging.ERROR, logger="flowfile.hub_node"), pytest.raises(SystemExit) as exited:
        hub_node.require_own_storage_dir(join_hub.mint())

    assert exited.value.code == 2
    assert [record.getMessage() for record in caplog.records] == [refusal(*missing)]
    assert join_hub.calls == []
    assert [path.name for path in home.iterdir()] == [".flowfile"]
    assert list((home / ".flowfile").iterdir()) == []
    for name in missing:
        assert name not in os.environ


OWN = {"FLOWFILE_STORAGE_DIR": "/srv/flowfile", "FLOWFILE_SECURE_STORAGE_PATH": "/srv/flowfile/secure"}


@pytest.mark.parametrize(
    "token,variables",
    [("join-token", OWN), ("join-token", {"FLOWFILE_MODE": "docker"}), (None, {}), ("", {})],
    ids=["both set", "docker", "no token", "empty token"],
)
def test_a_storage_dir_and_secure_store_of_its_own_docker_mode_or_no_join_token_is_let_through(
    home, monkeypatch, token, variables
):
    for name, value in variables.items():
        monkeypatch.setenv(name, value)

    hub_node.require_own_storage_dir(token)

    assert os.environ.get("FLOWFILE_STORAGE_DIR") == variables.get("FLOWFILE_STORAGE_DIR")
    assert [path.name for path in home.iterdir()] == [".flowfile"]


@pytest.fixture
def held():
    """Listening sockets that hold ports on loopback the way another server would; ``held()`` gives one's port."""
    sockets = []

    def hold() -> int:
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        sock.listen()
        sockets.append(sock)
        return sock.getsockname()[1]

    yield hold
    for sock in sockets:
        sock.close()


def in_use_reason(port: int) -> str:
    """The OS's own reason a second bind of the held ``port`` on 127.0.0.1 is refused (its wording differs per OS)."""
    with socket.socket() as sock, pytest.raises(OSError) as refused:
        sock.bind(("127.0.0.1", port))
    return refused.value.strerror


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_a_node_that_joins_listens_on_the_port_it_is_given():
    port = free_port()

    with hub_node.serving_socket("127.0.0.1", port) as sock:
        assert sock.getsockname() == ("127.0.0.1", port)
        socket.create_connection(("127.0.0.1", port), timeout=2).close()


def test_an_address_this_machine_does_not_have_exits_1_with_the_os_reason(caplog):
    port = free_port()

    with caplog.at_level(logging.ERROR, logger="flowfile.hub_node"), pytest.raises(SystemExit) as exited:
        hub_node.serving_socket("192.0.2.1", port)

    assert exited.value.code == 1
    (line,) = [record.getMessage() for record in caplog.records]
    assert line.startswith(f"Cannot serve on 192.0.2.1:{port}: ")
    assert "in use" not in line


def test_a_node_whose_port_is_in_use_exits_1_with_the_sentence_before_any_request(join_hub, held, caplog):
    port = held()

    with caplog.at_level(logging.ERROR, logger="flowfile.hub_node"), pytest.raises(SystemExit) as exited:
        hub_node.serving_socket("127.0.0.1", port)

    assert exited.value.code == 1
    assert [record.getMessage() for record in caplog.records] == [
        f"Cannot serve on 127.0.0.1:{port}: {in_use_reason(port)}"
    ]
    assert join_hub.calls == []


def joined_with_keys(hub: FakeJoinHub, *, enabled: bool = True, core_url: str | None = None) -> HubNode:
    """``worker-1`` joined to ``hub`` with keys of its own, by a core for both when ``core_url`` is given."""
    token = hub.mint(enable_on_join=enabled)
    return hub_node.join(hub.url, token, name="worker-1", worker_url="http://w:1", core_url=core_url)[0]


def beat_warnings(caplog) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == "flowfile.hub_node" and record.levelno == logging.WARNING
    ]


def test_a_core_that_joins_for_itself_and_its_worker_keeps_has_core(join_hub, node_file):
    joined_with_keys(join_hub, core_url="http://team-1.example:63578")

    assert json.loads(node_file.read_text(encoding="utf-8"))["has_core"] is True
    assert hub_node.read().has_core is True


def test_a_worker_that_joins_alone_keeps_has_core_false(join_hub, node_file):
    joined_with_keys(join_hub)

    assert json.loads(node_file.read_text(encoding="utf-8"))["has_core"] is False
    assert hub_node.read().has_core is False


@pytest.mark.parametrize("fields", [{}, {"has_core": "true"}, {"has_core": 1}], ids=["missing", "text", "number"])
def test_a_file_without_a_has_core_of_its_own_reads_as_a_worker_alone(node_file, fields):
    write_node(node_file, **fields)

    assert hub_node.read().has_core is False


def test_a_beat_carries_an_assertion_the_hub_verifies_against_the_key_the_node_joined_with(join_hub):
    joined_with_keys(join_hub)
    sent = join_hub.calls[0][1]["keys"]["sig"]

    assert hub_node.heartbeat("worker") is True

    [beat] = join_hub.beats
    assert beat["header"] == {"alg": "ES256", "kid": sent["kid"], "typ": "JWT"}
    claims = beat["claims"]
    assert (claims["iss"], claims["sub"]) == ("node:worker-1", "node:worker-1")
    assert claims["aud"] == join_hub.url + HEARTBEAT_PATH
    assert claims["exp"] - claims["iat"] == 60
    assert uuid.UUID(claims["jti"]).version == 4
    assert beat["body"] == {"version": get_version(), "process": "worker"}
    assert jwt.PyJWK(sent).key_id == hub_node_key.load().sig_kid


def test_a_beat_names_the_issuer_the_hub_answered_as_its_audience(node_file):
    signer = HubSigner()
    hub = FakeJoinHub(signer)
    try:
        joined_with_keys(hub)
        taken = hub_node.heartbeat("core")
    finally:
        hub.close()
        signer.close()

    assert taken is True
    assert signer.url != hub.url
    assert hub.beats[0]["claims"]["aud"] == signer.url + HEARTBEAT_PATH
    assert hub.beats[0]["body"]["process"] == "core"


def test_a_beat_carries_the_addresses_it_is_given_and_no_key_without_them(join_hub):
    joined_with_keys(join_hub)

    assert hub_node.heartbeat("worker", addresses=["http://10.0.0.5:63610", "http://build-7:63610"]) is True
    assert hub_node.heartbeat("worker") is True

    with_addresses, without = (beat["body"] for beat in join_hub.beats)
    assert with_addresses == {
        "version": get_version(),
        "process": "worker",
        "addresses": ["http://10.0.0.5:63610", "http://build-7:63610"],
    }
    assert without == {"version": get_version(), "process": "worker"}


def test_the_beat_loop_works_out_the_addresses_on_its_port_again_before_every_beat(join_hub, monkeypatch):
    joined_with_keys(join_hub)
    asked: list[tuple] = []

    def addresses_now(port, *, tls, hub_url):
        asked.append((port, tls, hub_url))
        return [f"http://10.0.{len(asked)}.5:{port}"]

    monkeypatch.setattr(hub_node, "reach_addresses", addresses_now)

    hub_node._beat("worker", Waits(join_hub, stop_at=2), port=63610, tls=True)

    assert asked == [(63610, True, join_hub.url)] * 2
    assert [beat["body"]["addresses"] for beat in join_hub.beats] == [
        ["http://10.0.1.5:63610"],
        ["http://10.0.2.5:63610"],
    ]


def test_the_beat_loop_names_no_addresses_without_a_port(join_hub):
    joined_with_keys(join_hub)

    hub_node._beat("worker", Waits(join_hub, stop_at=1))

    assert "addresses" not in join_hub.beats[0]["body"]


def test_every_beat_carries_a_fresh_assertion(join_hub):
    joined_with_keys(join_hub)

    assert hub_node.heartbeat("worker") is True
    assert hub_node.heartbeat("worker") is True

    first, second = (beat["claims"]["jti"] for beat in join_hub.beats)
    assert first != second


def test_a_beat_the_hub_refuses_warns_once_with_its_sentence_and_never_the_assertion(join_hub, caplog):
    joined_with_keys(join_hub, enabled=False)

    with caplog.at_level(logging.DEBUG):
        assert hub_node.heartbeat("worker") is False
        assert hub_node.heartbeat("worker") is False

    assert len(join_hub.sent_beats()) == 2 and join_hub.beats == []
    assert beat_warnings(caplog) == [
        f"The hub did not take this node's heartbeat: {join_hub.url} answered 401: this node is disabled on the hub"
    ]
    assert "eyJ" not in caplog.text


def test_a_beat_taken_again_lets_the_next_refusal_warn_again(join_hub, caplog):
    joined_with_keys(join_hub, enabled=False)

    hub_node.heartbeat("worker")
    join_hub.set_enabled("worker-1", True)
    assert hub_node.heartbeat("worker") is True
    join_hub.set_enabled("worker-1", False)
    hub_node.heartbeat("worker")
    hub_node.heartbeat("worker")

    assert len(beat_warnings(caplog)) == 2


def test_a_hub_that_does_not_answer_a_beat_warns_once_naming_its_url(node_file, caplog):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        closed = f"http://127.0.0.1:{probe.getsockname()[1]}"
    hub_node_key.load_or_create()
    write_node(node_file, hub_url=closed, issuer=closed)

    assert hub_node.heartbeat("core") is False
    assert hub_node.heartbeat("core") is False

    [warning] = beat_warnings(caplog)
    assert warning.startswith(f"The hub did not take this node's heartbeat: {closed} did not answer (")


def test_a_node_without_keys_sends_no_beat_and_makes_none(join_hub, node_file, caplog):
    write_node(node_file, hub_url=join_hub.url, issuer=join_hub.url)

    assert hub_node.heartbeat("core") is False

    assert join_hub.calls == []
    assert not key_file_of(node_file).exists()
    assert beat_warnings(caplog) == [
        f"The hub did not take this node's heartbeat: this node has no keys of its own at {key_file_of(node_file)}"
    ]


def test_a_storage_dir_that_never_joined_sends_no_beat(join_hub, node_file):
    assert hub_node.heartbeat("core") is False

    assert join_hub.calls == []
    assert not key_file_of(node_file).exists()


def test_no_beat_starts_where_no_node_joined(node_file):
    assert hub_node.start_heartbeat("core") is None
    assert hub_node.start_heartbeat("worker") is None


@pytest.mark.parametrize(
    ("core_url", "process", "speaks"),
    [
        ("http://team-1.example:63578", "core", True),
        ("http://team-1.example:63578", "worker", False),
        (None, "worker", True),
        (None, "core", False),
    ],
    ids=["core of a core's node", "worker beside a joined core", "worker alone", "core beside a worker alone"],
)
def test_one_process_speaks_for_a_node(join_hub, core_url, process, speaks):
    joined_with_keys(join_hub, core_url=core_url)

    stop = hub_node.start_heartbeat(process)
    try:
        if speaks:
            assert stop is not None
            assert join_hub.wait_for_beats(1)[0]["process"] == process
        else:
            assert stop is None
    finally:
        if stop is not None:
            stop.set()


class Waits(threading.Event):
    """A stop event that never sleeps: each ``wait`` records its timeout and the beats sent so far, runs ``then``, and
    stops the loop at the ``stop_at``-th wait."""

    def __init__(self, hub: FakeJoinHub, stop_at: int, then=None):
        super().__init__()
        self.hub, self.stop_at, self.then = hub, stop_at, then
        self.seen: list[tuple[float, int]] = []

    def wait(self, timeout=None):
        self.seen.append((timeout, len(self.hub.sent_beats())))
        if self.then is not None:
            self.then(len(self.seen))
        return len(self.seen) >= self.stop_at


def test_the_beat_comes_at_once_and_then_every_sixty_seconds(join_hub):
    joined_with_keys(join_hub)
    stop = Waits(join_hub, stop_at=3)

    hub_node._beat("worker", stop)

    assert stop.seen == [(60, 1), (60, 2), (60, 3)]
    assert len(join_hub.beats) == 3


def test_a_worker_that_beats_alone_goes_quiet_once_a_core_joins_beside_it(join_hub):
    joined_with_keys(join_hub)

    def core_joins(waits: int) -> None:
        if waits == 1:
            joined_with_keys(join_hub, core_url="http://team-1.example:63578")

    stop = Waits(join_hub, stop_at=3, then=core_joins)

    hub_node._beat("worker", stop)

    assert [sent for _, sent in stop.seen] == [1, 1, 1]


def test_a_beat_that_fails_unexpectedly_warns_once_and_the_beats_go_on(join_hub, monkeypatch, caplog):
    joined_with_keys(join_hub)
    tries = []

    def broken(process, *, addresses=None):
        tries.append(process)
        raise RuntimeError("boom")

    monkeypatch.setattr(hub_node, "heartbeat", broken)

    hub_node._beat("worker", Waits(join_hub, stop_at=2))

    assert tries == ["worker", "worker"]
    assert beat_warnings(caplog) == ["The hub did not take this node's heartbeat: the beat failed (RuntimeError)"]


class HubAnswering(FakeJoinHub):
    """A hub whose heartbeat route answers each beat with the next of ``statuses``, the last one again once they run
    out; 204 is a beat checked as the contract says."""

    def __init__(self, *statuses: int):
        super().__init__()
        self.statuses = list(statuses)

    def beat(self, authorization, body):
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        if status == 204:
            return super().beat(authorization, body)
        return status, {"detail": HTTPStatus(status).phrase}


@pytest.fixture
def hub_answering():
    """Starts a ``HubAnswering`` with the given statuses, joins ``worker-1`` to it with keys, and closes it after."""
    hubs: list[HubAnswering] = []

    def start(*statuses: int) -> HubAnswering:
        hubs.append(HubAnswering(*statuses))
        joined_with_keys(hubs[-1])
        return hubs[-1]

    yield start
    for hub in hubs:
        hub.close()


@pytest.mark.parametrize("status", [404, 405, 500])
def test_a_hub_that_does_not_take_a_beat_is_warned_about_once_and_asked_every_minute(hub_answering, caplog, status):
    hub = hub_answering(status)
    stop = Waits(hub, stop_at=3)

    hub_node._beat("worker", stop)

    assert stop.seen == [(60, 1), (60, 2), (60, 3)]
    assert beat_warnings(caplog) == [
        f"The hub did not take this node's heartbeat: {hub.url} answered {status}: {HTTPStatus(status).phrase}"
    ]


def test_a_hub_that_removed_the_node_stops_the_process_and_beats_no_more(hub_answering, monkeypatch, caplog):
    hub = hub_answering(410)
    stopped: list[int] = []
    monkeypatch.setattr(hub_node, "_stop_this_process", lambda: stopped.append(1))
    stop = Waits(hub, stop_at=4)

    with caplog.at_level(logging.DEBUG):
        hub_node._beat("worker", stop)

    assert stopped == [1]
    assert stop.seen == []
    [record] = [r for r in caplog.records if r.name == "flowfile.hub_node" and r.levelno == logging.ERROR]
    assert record.getMessage().endswith("; this node stops and stays stopped")
    assert beat_warnings(caplog) == []


def test_a_hub_that_answers_503_keeps_the_node_beating(hub_answering, monkeypatch):
    hub = hub_answering(503)
    stopped: list[int] = []
    monkeypatch.setattr(hub_node, "_stop_this_process", lambda: stopped.append(1))
    stop = Waits(hub, stop_at=3)

    hub_node._beat("worker", stop)

    assert stopped == []
    assert [timeout for timeout, _ in stop.seen] == [60, 60, 60]


def test_stopping_the_process_sends_it_sigterm(monkeypatch):
    sent: list[tuple[int, int]] = []
    monkeypatch.setattr(hub_node.os, "kill", lambda pid, sig: sent.append((pid, sig)))

    hub_node._stop_this_process()

    assert sent == [(os.getpid(), signal.SIGTERM)]
