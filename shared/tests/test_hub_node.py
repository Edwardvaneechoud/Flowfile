"""A node's identity on a hub: how the variables and the joined file combine, the join itself, joining at start, the
folder, port and address a node that joins takes, and the heartbeat that tells the hub the node is up."""

import json
import logging
import os
import socket
import stat
import threading
import uuid
from http import HTTPStatus

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
VARIABLES = ("FLOWFILE_HUB_URL", "FLOWFILE_NODE_NAME", "FLOWFILE_HUB_ISSUER")


@pytest.fixture(autouse=True)
def node_file(tmp_path, monkeypatch):
    """Where the node keeps its file in these tests, absent until a test writes it, and none of the variables set."""
    target = tmp_path / "storage" / hub_node.FILE_NAME
    monkeypatch.setattr(hub_node, "path", lambda: target)
    monkeypatch.setattr(hub_node, "_warned_unreadable", False)
    monkeypatch.setattr(hub_node, "_warned_beats", set())
    monkeypatch.setattr(hub_node, "_hub_without_beats", False)
    monkeypatch.setattr(hub_node, "_said_without_beats", False)
    for name in VARIABLES:
        monkeypatch.delenv(name, raising=False)
    return target


def write_node(target, drop: tuple[str, ...] = (), **fields) -> dict:
    node = {
        "hub_url": HUB,
        "issuer": "http://localhost:63580",
        "name": "team",
        "node_id": 3,
        "joined_at": "2026-11-03T09:00:00+00:00",
        **fields,
    }
    for field in drop:
        del node[field]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(node), encoding="utf-8")
    return node


def join_with(hub: FakeJoinHub, token: str, name: str = "worker-1") -> HubNode:
    return hub_node.join(hub.url, token, name=name, worker_url="http://worker-1.example:63579")


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


def test_the_name_and_issuer_variables_win_over_the_file(node_file, monkeypatch):
    write_node(node_file)
    monkeypatch.setenv("FLOWFILE_NODE_NAME", "by-hand")
    monkeypatch.setenv("FLOWFILE_HUB_ISSUER", "http://hub:63580")

    assert hub_node.resolved() == NodeSettings(hub_url=HUB, name="by-hand", issuer="http://hub:63580")


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
    [(("name",), {}), ((), {"node_id": "3"}), ((), {"node_id": True}), ((), {"hub_url": 7})],
    ids=["no name", "node id as text", "node id as bool", "url as number"],
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
    assert set(kept) == {"hub_url", "issuer", "name", "node_id", "joined_at", "join_token_hash", "has_core"}
    assert (kept["hub_url"], kept["issuer"], kept["name"], kept["node_id"]) == (
        join_hub.url,
        join_hub.url,
        "worker-1",
        1,
    )
    assert kept["joined_at"].endswith("+00:00")
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


def test_a_file_from_before_the_token_hash_still_reads(node_file):
    write_node(node_file)

    assert hub_node.read().join_token_hash == ""


def test_a_file_from_before_the_token_hash_restarts_on_its_own_hub_without_a_request(join_hub, node_file, caplog):
    write_node(node_file, hub_url=join_hub.url)

    with caplog.at_level(logging.INFO, logger="flowfile.hub_node"):
        node = hub_node.join_at_start(join_hub.url, join_hub.mint(), name="worker-2", worker_url="http://w:1")

    assert join_hub.calls == []
    assert node.name == "team"
    assert "Delete that file to join with this token" in caplog.text and str(node_file) in caplog.text


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


def test_a_join_keeps_the_port_the_worker_serves_on_and_a_cores_join_keeps_none(join_hub, node_file):
    hub_node.join(join_hub.url, join_hub.mint(), name="worker-1", worker_url="http://w:64001", worker_port=64001)

    assert json.loads(node_file.read_text(encoding="utf-8"))["worker_port"] == 64001
    assert hub_node.read().worker_port == 64001

    hub_node.join(join_hub.url, join_hub.mint(), name="team", worker_url="http://w:1", core_url="http://c:2")

    assert "worker_port" not in json.loads(node_file.read_text(encoding="utf-8"))
    assert hub_node.read().worker_port is None


def test_a_cores_join_keeps_the_port_it_serves_on_and_the_one_it_gives_its_worker(join_hub, node_file):
    hub_node.join(
        join_hub.url,
        join_hub.mint(),
        name="team",
        worker_url="http://w:64002",
        core_url="http://c:64001",
        core_port=64001,
        worker_port=64002,
    )

    kept = json.loads(node_file.read_text(encoding="utf-8"))
    assert (kept["core_port"], kept["worker_port"]) == (64001, 64002)
    assert (hub_node.read().core_port, hub_node.read().worker_port) == (64001, 64002)


@pytest.mark.parametrize("field", ["core_port", "worker_port"])
@pytest.mark.parametrize("port", [0, 70000, "63579", True], ids=["zero", "too high", "text", "bool"])
def test_a_kept_port_that_is_no_port_reads_as_none(node_file, port, field):
    write_node(node_file, **{field: port})

    assert getattr(hub_node.read(), field) is None


def test_a_start_with_the_token_the_node_joined_with_is_already_joined(join_hub, node_file):
    token = join_hub.mint()
    join_with(join_hub, token)

    assert hub_node.already_joined(join_hub.url + "/", token).name == "worker-1"
    assert hub_node.already_joined(join_hub.url, join_hub.mint()) is None


def test_a_file_from_before_the_token_hash_is_already_joined_for_its_own_hub_only(node_file):
    write_node(node_file)

    assert hub_node.already_joined(HUB, "any-token").name == "team"
    assert hub_node.already_joined("http://other.example:63580", "any-token") is None


@pytest.fixture
def home(tmp_path, monkeypatch):
    """A home directory of the test's own, and none of the storage variables set."""
    target = tmp_path / "home"
    target.mkdir()
    monkeypatch.setenv("HOME", str(target))
    for name in ("FLOWFILE_STORAGE_DIR", "SECURE_STORAGE_PATH", "FLOWFILE_SECURE_STORAGE_PATH", "FLOWFILE_MODE"):
        monkeypatch.delenv(name, raising=False)
    return target


@pytest.mark.skipif(os.name == "nt", reason="the home directory comes from HOME on POSIX only")
def test_a_node_that_joins_keeps_its_files_in_a_folder_of_its_own(home):
    folder = hub_node.node_storage("join-token", "card-node")

    assert folder == home / ".flowfile-node" / "card-node"
    assert stat.S_IMODE(folder.stat().st_mode) == 0o700
    assert stat.S_IMODE(folder.parent.stat().st_mode) == 0o700
    assert os.environ["FLOWFILE_STORAGE_DIR"] == str(folder)
    assert os.environ["SECURE_STORAGE_PATH"] == os.environ["FLOWFILE_SECURE_STORAGE_PATH"] == str(folder / "secure")
    assert sorted(path.name for path in home.iterdir()) == [".flowfile-node"]


@pytest.mark.skipif(os.name == "nt", reason="the home directory comes from HOME on POSIX only")
def test_a_node_given_no_name_keeps_its_files_under_the_machines_name(home, monkeypatch):
    monkeypatch.setattr(hub_node.socket, "gethostname", lambda: "build-7.example.com")

    assert hub_node.node_storage("join-token", None) == home / ".flowfile-node" / "build-7"


@pytest.mark.skipif(os.name == "nt", reason="the home directory comes from HOME on POSIX only")
def test_a_secure_storage_path_already_set_is_kept(home, monkeypatch):
    monkeypatch.setenv("FLOWFILE_SECURE_STORAGE_PATH", "/vault/flowfile")

    folder = hub_node.node_storage("join-token", "card-node")

    assert os.environ["FLOWFILE_SECURE_STORAGE_PATH"] == "/vault/flowfile"
    assert os.environ["SECURE_STORAGE_PATH"] == str(folder / "secure")


@pytest.mark.parametrize(
    "token,variables",
    [
        ("join-token", {"FLOWFILE_STORAGE_DIR": "/srv/flowfile"}),
        (None, {}),
        ("", {}),
        ("join-token", {"FLOWFILE_MODE": "docker"}),
    ],
    ids=["storage dir set", "no token", "empty token", "docker"],
)
def test_the_storage_dir_is_left_alone_without_a_token_or_with_one_set(home, monkeypatch, token, variables):
    for name, value in variables.items():
        monkeypatch.setenv(name, value)

    assert hub_node.node_storage(token, "card-node") is None
    assert os.environ.get("FLOWFILE_STORAGE_DIR") == variables.get("FLOWFILE_STORAGE_DIR")
    assert "FLOWFILE_SECURE_STORAGE_PATH" not in os.environ
    assert list(home.iterdir()) == []


def joined_on_this_machine(home, name: str = "card-node") -> "os.PathLike":
    """The folder a core that joined as ``name`` left under the home directory, with its ``hub_node.json``."""
    folder = home / ".flowfile-node" / name
    write_node(folder / hub_node.FILE_NAME, name=name, has_core=True, worker_port=64002)
    return folder


@pytest.mark.skipif(os.name == "nt", reason="the home directory comes from HOME on POSIX only")
def test_a_process_named_after_a_node_that_joined_here_takes_its_folder(home):
    folder = joined_on_this_machine(home)

    assert hub_node.node_storage(None, "card-node", by_name=True) == folder
    assert os.environ["FLOWFILE_STORAGE_DIR"] == str(folder)
    assert os.environ["SECURE_STORAGE_PATH"] == os.environ["FLOWFILE_SECURE_STORAGE_PATH"] == str(folder / "secure")
    assert sorted(path.name for path in folder.iterdir()) == [hub_node.FILE_NAME]


@pytest.mark.skipif(os.name == "nt", reason="the home directory comes from HOME on POSIX only")
@pytest.mark.parametrize(
    "name,by_name,variables",
    [
        ("other-node", True, {}),
        (None, True, {}),
        ("../card-node", True, {}),
        ("card-node", False, {}),
        ("card-node", True, {"FLOWFILE_STORAGE_DIR": "/srv/flowfile"}),
        ("card-node", True, {"FLOWFILE_MODE": "docker"}),
    ],
    ids=["no such node", "no name", "a name no hub takes", "not looked up", "storage dir set", "docker"],
)
def test_without_a_node_of_that_name_or_with_a_storage_dir_a_process_with_no_token_is_unchanged(
    home, monkeypatch, name, by_name, variables
):
    joined_on_this_machine(home)
    for variable, value in variables.items():
        monkeypatch.setenv(variable, value)

    assert hub_node.node_storage(None, name, by_name=by_name) is None
    assert os.environ.get("FLOWFILE_STORAGE_DIR") == variables.get("FLOWFILE_STORAGE_DIR")
    assert "FLOWFILE_SECURE_STORAGE_PATH" not in os.environ


@pytest.mark.parametrize("name", ["../desktop", "a/b", ".hidden", "with space"])
def test_a_name_a_hub_would_refuse_is_no_folder_and_exits_2(home, caplog, name):
    with caplog.at_level(logging.ERROR, logger="flowfile.hub_node"), pytest.raises(SystemExit) as exited:
        hub_node.node_storage("join-token", name)

    assert exited.value.code == 2
    assert "give one with --name" in caplog.text
    assert "FLOWFILE_STORAGE_DIR" not in os.environ
    assert list(home.iterdir()) == []


@pytest.fixture
def held():
    """Listening sockets that hold ports the way another server would; ``held(host)`` gives one and its port."""
    sockets = []

    def hold(host: str = "127.0.0.1", port: int = 0) -> int:
        sock = socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET)
        sock.bind((host, port))
        sock.listen()
        sockets.append(sock)
        return sock.getsockname()[1]

    yield hold
    for sock in sockets:
        sock.close()


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_a_node_listens_on_the_port_it_is_given(held):
    port = free_port()

    with hub_node.listen("127.0.0.1", port) as sock:
        assert sock.getsockname() == ("127.0.0.1", port)
        socket.create_connection(("127.0.0.1", port), timeout=2).close()


def test_a_given_port_in_use_is_refused_naming_the_host_the_port_and_what_holds_it(held):
    port = held()

    with pytest.raises(hub_node.CannotServe) as refused:
        hub_node.listen("127.0.0.1", port)

    assert refused.value.in_use
    assert refused.value.reason.startswith(f"Cannot serve on 127.0.0.1:{port}: in use by ")
    assert f"(pid {os.getpid()})" in refused.value.reason


def test_an_address_this_machine_does_not_have_is_refused_with_the_os_reason_even_when_moving():
    port = free_port()

    with pytest.raises(hub_node.CannotServe) as refused:
        hub_node.listen("192.0.2.1", port, move=True)

    assert not refused.value.in_use
    assert refused.value.reason.startswith(f"Cannot serve on 192.0.2.1:{port}: ")
    assert "in use" not in refused.value.reason


def test_a_port_in_use_gives_way_to_the_next_free_one_and_says_so(held, caplog):
    port = held()

    with caplog.at_level(logging.INFO, logger="flowfile.hub_node"):
        with hub_node.listen("127.0.0.1", port, move=True) as sock:
            moved = sock.getsockname()[1]

    assert moved != port
    assert f"Port {port} is in use; serving on {moved}" in caplog.text


def test_a_port_in_use_gives_way_to_the_kept_port_first(held):
    port, kept = held(), free_port()

    with hub_node.listen("127.0.0.1", port, move=True, kept=kept) as sock:
        assert sock.getsockname()[1] == kept


def test_a_kept_port_in_use_too_gives_way_to_another(held):
    port, kept = held(), held()

    with hub_node.listen("127.0.0.1", port, move=True, kept=kept) as sock:
        assert sock.getsockname()[1] not in (port, kept)


def test_the_ports_a_node_moves_to_skip_a_hubs_and_end_with_one_the_os_picks():
    ports = hub_node._other_ports(63579, None)

    assert ports[:3] == [63581, 63582, 63583]
    assert hub_node.HUB_PORT not in ports
    assert ports[-2:] == [63629, 0]
    assert hub_node._other_ports(63579, 64000)[:2] == [64000, 63581]
    assert hub_node._other_ports(63579, 63582).count(63582) == 1
    assert hub_node._other_ports(65533, None) == [65534, 65535, 0]


def test_a_port_another_server_holds_on_loopback_counts_as_in_use_for_every_address(held):
    port = held("127.0.0.1")

    with hub_node.listen("0.0.0.0", port, move=True) as sock:
        assert sock.getsockname()[1] != port


def test_a_port_no_other_server_holds_is_not_shadowed():
    with hub_node._bind("0.0.0.0", 0) as sock:
        assert not hub_node._shadowed(sock)


@pytest.mark.parametrize("holder", ["0.0.0.0", "127.0.0.1"], ids=["every address", "loopback"])
def test_a_port_a_server_answers_on_counts_as_in_use_for_a_node_on_loopback(held, holder):
    port = held(holder)

    with hub_node.listen("127.0.0.1", port, move=True) as sock:
        assert sock.getsockname()[1] != port
        socket.create_connection(("127.0.0.1", sock.getsockname()[1]), timeout=2).close()


def test_a_kept_port_a_server_answers_on_gives_way_on_loopback_too(held):
    port, kept = held("0.0.0.0"), held("0.0.0.0")

    with hub_node.listen("127.0.0.1", port, move=True, kept=kept) as sock:
        assert sock.getsockname()[1] not in (port, kept)


def serving_on(process, port, **kwargs) -> int:
    with hub_node.serving_socket(process, "127.0.0.1", port, **kwargs) as sock:
        return sock.getsockname()[1]


def test_a_core_about_to_join_moves_off_its_port_in_use_and_a_given_one_stays(join_hub, held, caplog):
    port, token = held(), join_hub.mint()

    assert serving_on("core", port, given=False, hub_url=join_hub.url, token=token) != port
    with caplog.at_level(logging.ERROR, logger="flowfile.hub_node"), pytest.raises(SystemExit) as exited:
        serving_on("core", port, given=True, hub_url=join_hub.url, token=token)

    assert exited.value.code == 1
    assert f"Cannot serve on 127.0.0.1:{port}: in use by " in caplog.text
    assert join_hub.calls == []


def test_a_core_about_to_join_prefers_the_port_it_joined_with_to_another(join_hub, node_file, held):
    kept = free_port()
    hub_node.join(
        join_hub.url,
        join_hub.mint(),
        name="team",
        worker_url="http://w:1",
        core_url="http://c:2",
        core_port=kept,
        worker_port=held(),
    )

    assert serving_on("core", held(), given=False, hub_url=join_hub.url, token=join_hub.mint()) == kept


def test_a_core_that_joined_with_this_token_serves_on_its_kept_port_or_says_the_hub_has_it(
    join_hub, node_file, held, caplog
):
    token, kept = join_hub.mint(), free_port()
    hub_node.join(join_hub.url, token, name="team", worker_url="http://w:1", core_url="http://c:2", core_port=kept)

    assert serving_on("core", free_port(), given=False, hub_url=join_hub.url, token=token) == kept

    token, kept = join_hub.mint(), held()
    hub_node.join(join_hub.url, token, name="team", worker_url="http://w:1", core_url="http://c:2", core_port=kept)
    with caplog.at_level(logging.ERROR, logger="flowfile.hub_node"), pytest.raises(SystemExit) as exited:
        serving_on("core", free_port(), given=False, hub_url=join_hub.url, token=token)

    assert exited.value.code == 1
    assert "the hub has this node at that port, so free it or join again with a new join token" in caplog.text


def test_the_worker_found_by_name_serves_on_the_port_its_cores_join_gave_it(node_file, held, caplog):
    kept = free_port()
    write_node(node_file, has_core=True, core_port=free_port(), worker_port=kept)

    assert serving_on("worker", free_port(), given=False, found=True) == kept
    given = free_port()
    assert serving_on("worker", given, given=True, found=True) == given

    write_node(node_file, has_core=True, worker_port=held())
    with caplog.at_level(logging.ERROR, logger="flowfile.hub_node"), pytest.raises(SystemExit) as exited:
        serving_on("worker", free_port(), given=False, found=True)

    assert exited.value.code == 1
    assert "the hub has this node at that port" in caplog.text


def test_the_worker_found_by_name_in_a_file_without_its_port_serves_on_the_one_it_is_given(node_file):
    write_node(node_file, has_core=True)
    port = free_port()

    assert serving_on("worker", port, given=False, found=True) == port


def test_the_worker_beside_a_core_keeps_the_port_the_node_joined_with_even_while_a_server_answers_there(held):
    kept = held()

    assert hub_node.beside_port(held(), kept=kept) == kept


def test_the_worker_beside_a_core_gets_the_default_port_when_no_server_holds_it(caplog):
    port = free_port()

    with caplog.at_level(logging.INFO, logger="flowfile.hub_node"):
        assert hub_node.beside_port(port) == port

    assert "is in use" not in caplog.text


@pytest.mark.parametrize("holder", ["0.0.0.0", "127.0.0.1"], ids=["every address", "loopback"])
def test_the_worker_beside_a_core_gets_a_free_port_when_a_server_holds_the_default_and_one_line_says_so(
    held, caplog, holder
):
    port = held(holder)

    with caplog.at_level(logging.INFO, logger="flowfile.hub_node"):
        moved = hub_node.beside_port(port)

    assert moved != port
    with hub_node._bind("0.0.0.0", moved):
        pass
    assert f"Port {port} is in use; the worker beside this core is to serve on {moved}" in caplog.text


@pytest.fixture
def routes(monkeypatch):
    """The routes asked of the OS, each answered from ``answers`` (``None`` when absent): no network needed."""
    asked, answers = [], {}

    def route(host, port, family):
        asked.append((host, port, family))
        return answers.get(host)

    monkeypatch.setattr(hub_node, "_route_address", route)
    return asked, answers


@pytest.mark.parametrize("host", ["10.1.2.3", "127.0.0.1", "localhost", "::1", "[::1]"])
def test_a_node_bound_to_one_address_gives_that_address(routes, host):
    asked, _ = routes

    assert hub_node.address_for(HUB, host) == host.strip("[]")
    assert asked == []


def test_a_node_on_every_address_gives_its_address_towards_the_hub(routes):
    asked, answers = routes
    answers["hub.example"] = "10.0.0.7"

    assert hub_node.node_url(HUB, "0.0.0.0", 63581, tls=False) == "http://10.0.0.7:63581"
    assert asked == [("hub.example", 63580, socket.AF_INET)]


@pytest.mark.parametrize("hub_url", ["http://localhost:63580", "http://127.0.0.1:63580", "http://[::1]:63580"])
def test_a_node_beside_its_hub_gives_its_address_towards_other_machines(routes, hub_url):
    asked, answers = routes
    answers["192.0.2.1"] = "192.168.1.20"

    assert hub_node.node_url(hub_url, "0.0.0.0", 63579, tls=True) == "https://192.168.1.20:63579"
    assert asked == [("192.0.2.1", 80, socket.AF_INET)]


def test_a_hub_with_no_route_falls_back_to_the_route_towards_other_machines(routes):
    asked, answers = routes
    answers["192.0.2.1"] = "192.168.1.20"

    assert hub_node.address_for("https://hub.example", "0.0.0.0") == "192.168.1.20"
    assert asked == [("hub.example", 443, socket.AF_INET), ("192.0.2.1", 80, socket.AF_INET)]


def test_with_no_route_at_all_a_node_gives_the_machines_name(routes, monkeypatch):
    monkeypatch.setattr(hub_node.socket, "getfqdn", lambda: "build-7.example.com")

    assert hub_node.node_url(HUB, "0.0.0.0", 63579, tls=False) == "http://build-7.example.com:63579"


def test_a_node_on_every_ipv6_address_gives_an_ipv6_address_in_brackets(routes):
    asked, answers = routes
    answers["2001:db8::1"] = "2001:db8::5"

    assert hub_node.node_url("http://localhost:63580", "::", 63579, tls=False) == "http://[2001:db8::5]:63579"
    assert asked == [("2001:db8::1", 80, socket.AF_INET6)]


def test_the_os_route_to_loopback_is_no_address_for_a_hub():
    assert hub_node._route_address("127.0.0.1", 9, socket.AF_INET) is None


def joined_with_keys(hub: FakeJoinHub, *, enabled: bool = True, core_url: str | None = None) -> HubNode:
    """``worker-1`` joined to ``hub`` with keys of its own, by a core for both when ``core_url`` is given."""
    token = hub.mint(enable_on_join=enabled)
    return hub_node.join(hub.url, token, name="worker-1", worker_url="http://w:1", core_url=core_url)


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


def test_a_node_joined_without_keys_starts_no_beat_and_says_so_once(join_hub, node_file, caplog):
    write_node(node_file, hub_url=join_hub.url, has_core=True)

    assert hub_node.start_heartbeat("core") is None

    [warning] = beat_warnings(caplog)
    assert warning.startswith(f"team joined {join_hub.url} without keys of its own, so it does not tell the hub")
    assert "join again with a new join token" in warning
    assert join_hub.calls == []
    assert not key_file_of(node_file).exists()


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

    def broken(process):
        tries.append(process)
        raise RuntimeError("boom")

    monkeypatch.setattr(hub_node, "heartbeat", broken)

    hub_node._beat("worker", Waits(join_hub, stop_at=2))

    assert tries == ["worker", "worker"]
    assert beat_warnings(caplog) == ["The hub did not take this node's heartbeat: the beat failed (RuntimeError)"]


class HubAnswering(FakeJoinHub):
    """A hub whose heartbeat route answers each beat with the next of ``statuses``, the last one again once they run
    out: 404 and 405 as a hub built before the route answers, 204 a beat checked as the contract says."""

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


def absent_lines(caplog) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == "flowfile.hub_node" and "does not take heartbeats" in record.getMessage()
    ]


@pytest.mark.parametrize("status", [404, 405])
def test_a_hub_without_the_heartbeat_route_is_said_once_and_asked_every_fifteen_minutes(hub_answering, caplog, status):
    hub = hub_answering(status)
    stop = Waits(hub, stop_at=4)
    caplog.clear()

    with caplog.at_level(logging.DEBUG):
        hub_node._beat("worker", stop)

    assert stop.seen == [(900, 1), (900, 2), (900, 3), (900, 4)]
    [record] = [record for record in caplog.records if record.name == "flowfile.hub_node"]
    assert record.levelno == logging.INFO
    assert record.getMessage() == f"{hub.url} does not take heartbeats yet; this node asks again every 15 minutes"
    assert hub_node.HEARTBEAT_ABSENT_SECONDS == 15 * 60


def test_a_beat_answered_404_is_not_taken(hub_answering, caplog):
    hub_answering(404)

    assert hub_node.heartbeat("worker") is False
    assert hub_node.heartbeat("worker") is False

    assert beat_warnings(caplog) == []
    assert len(absent_lines(caplog)) == 1


def test_a_beat_taken_brings_back_the_minute_and_the_line_is_said_once_per_process(hub_answering, caplog):
    hub = hub_answering(404, 204, 204, 404)
    stop = Waits(hub, stop_at=4)

    hub_node._beat("worker", stop)

    assert stop.seen == [(900, 1), (60, 2), (60, 3), (900, 4)]
    assert len(hub.beats) == 2
    assert len(absent_lines(caplog)) == 1
    assert beat_warnings(caplog) == []


def test_another_answer_after_a_hub_without_the_route_warns_as_today_every_minute(hub_answering, caplog):
    hub = hub_answering(404, 500)
    stop = Waits(hub, stop_at=3)

    hub_node._beat("worker", stop)

    assert stop.seen == [(900, 1), (60, 2), (60, 3)]
    assert beat_warnings(caplog) == [
        f"The hub did not take this node's heartbeat: {hub.url} answered 500: Internal Server Error"
    ]


def test_a_hub_that_answers_500_warns_once_and_is_asked_every_minute(hub_answering, caplog):
    hub = hub_answering(500)
    stop = Waits(hub, stop_at=3)

    hub_node._beat("worker", stop)

    assert stop.seen == [(60, 1), (60, 2), (60, 3)]
    assert len(beat_warnings(caplog)) == 1
    assert absent_lines(caplog) == []
