"""A node's identity on a hub: how the variables and the joined file combine, the join itself, and joining at start."""

import json
import logging
import socket

import pytest

from shared import hub_node
from shared.hub_node import HubNode, JoinRefused, NodeSettings
from shared.tests import hub_join
from shared.tests.hub_join import EXPIRED, NOT_VALID, USED, FakeJoinHub

join_hub = hub_join.join_hub

HUB = "http://hub.example:63580"
VARIABLES = ("FLOWFILE_HUB_URL", "FLOWFILE_NODE_NAME", "FLOWFILE_HUB_ISSUER")


@pytest.fixture(autouse=True)
def node_file(tmp_path, monkeypatch):
    """Where the node keeps its file in these tests, absent until a test writes it, and none of the variables set."""
    target = tmp_path / "storage" / hub_node.FILE_NAME
    monkeypatch.setattr(hub_node, "path", lambda: target)
    monkeypatch.setattr(hub_node, "_warned_unreadable", False)
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
            {"token": token, "name": "worker-1", "worker_url": "http://worker-1.example:63579", "core_url": None},
        )
    ]
    kept = json.loads(node_file.read_text(encoding="utf-8"))
    assert set(kept) == {"hub_url", "issuer", "name", "node_id", "joined_at", "join_token_hash"}
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
