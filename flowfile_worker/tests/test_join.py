"""A worker joins a hub with ``--join`` and ``--hub`` before it serves, once, and then takes that hub's tokens."""

import sys

import pytest
from fastapi.testclient import TestClient

from flowfile_worker import configs, main
from shared import hub_node
from shared.tests import hub_join, hub_jwks
from shared.tests.hub_join import FakeJoinHub
from tests.test_hub_tokens import v1_headers

pytestmark = pytest.mark.worker

join_hub = hub_join.join_hub
hub_signer = hub_jwks.hub_signer

client = TestClient(main.app)

VARIABLES = ("FLOWFILE_HUB_JOIN_TOKEN", "FLOWFILE_HUB_URL", "FLOWFILE_NODE_NAME", "FLOWFILE_HUB_ISSUER")


@pytest.fixture
def node_file(tmp_path, monkeypatch):
    """The worker's ``hub_node.json`` in a storage dir of the test's own, absent until a join writes it."""
    target = tmp_path / "storage" / hub_node.FILE_NAME
    monkeypatch.setattr(hub_node, "path", lambda: target)
    return target


@pytest.fixture
def no_variables(monkeypatch):
    for name in VARIABLES:
        monkeypatch.delenv(name, raising=False)


def parse(monkeypatch, *flags):
    monkeypatch.setattr(sys, "argv", ["flowfile_worker", *flags])
    return configs.parse_args()


def started_with(monkeypatch, hub_url, token, *, name="worker-1", url="http://worker-1.example:63579"):
    """The module values a worker's command line leaves behind, as ``configs`` sets them in the main process."""
    monkeypatch.setattr(main, "JOIN_TOKEN", token)
    monkeypatch.setattr(main, "HUB_URL", hub_url)
    monkeypatch.setattr(main, "NODE_NAME", name)
    monkeypatch.setattr(main, "NODE_URL", url)


def test_join_without_a_hub_is_refused_naming_the_flag(monkeypatch, no_variables):
    with pytest.raises(ValueError, match="--join needs --hub"):
        parse(monkeypatch, "--join", "t")


def test_the_join_flags_are_read(monkeypatch, no_variables):
    args = parse(monkeypatch, "--join", "t", "--hub", "http://h", "--name", "n", "--url", "http://w:1")

    assert (args.join, args.hub, args.name, args.url) == ("t", "http://h", "n", "http://w:1")


def test_the_join_token_and_hub_come_from_the_variables_as_from_the_flags(monkeypatch, no_variables):
    monkeypatch.setenv("FLOWFILE_HUB_JOIN_TOKEN", "t")
    monkeypatch.setenv("FLOWFILE_HUB_URL", "http://h")
    monkeypatch.setenv("FLOWFILE_NODE_NAME", "n")

    args = parse(monkeypatch)

    assert (args.join, args.hub, args.name, args.url) == ("t", "http://h", "n", None)


def test_without_flags_or_variables_there_is_nothing_to_join(monkeypatch, no_variables):
    args = parse(monkeypatch)

    assert (args.join, args.hub, args.name, args.url) == (None, None, None, None)


def test_a_worker_joins_before_it_serves_and_a_restart_makes_no_request(monkeypatch, join_hub, node_file):
    started_with(monkeypatch, join_hub.url, join_hub.mint())

    main.join_hub()

    assert [path for path, _ in join_hub.calls] == ["/hub/nodes/join"]
    assert join_hub.calls[0][1]["name"] == "worker-1"
    assert join_hub.calls[0][1]["worker_url"] == "http://worker-1.example:63579"
    assert (hub_node.read().name, hub_node.read().has_core) == ("worker-1", False)

    main.join_hub()

    assert len(join_hub.calls) == 1


def test_a_worker_without_a_join_token_asks_nothing(monkeypatch, join_hub, node_file):
    started_with(monkeypatch, join_hub.url, None)

    main.join_hub()

    assert join_hub.calls == []
    assert not node_file.exists()


def test_a_refused_join_stops_the_worker_with_exit_code_2(monkeypatch, join_hub, node_file):
    started_with(monkeypatch, join_hub.url, "join-never-minted")

    with pytest.raises(SystemExit) as exited:
        main.join_hub()

    assert exited.value.code == 2
    assert not node_file.exists()


def test_a_worker_given_no_name_or_address_gives_the_machines_and_https_when_it_serves_tls(
    monkeypatch, join_hub, node_file
):
    monkeypatch.setattr(hub_node.socket, "gethostname", lambda: "build-7.example.com")
    monkeypatch.setattr(hub_node.socket, "getfqdn", lambda: "build-7.example.com")
    monkeypatch.setenv("FLOWFILE_WORKER_TLS_CERT", "/certs/worker.pem")
    started_with(monkeypatch, join_hub.url, join_hub.mint(), name=None, url=None)

    main.join_hub(64000)

    body = join_hub.calls[0][1]
    assert (body["name"], body["worker_url"]) == ("build-7", "https://build-7.example.com:64000")


def test_a_joined_worker_takes_the_hubs_tokens_for_its_name_with_no_variable_set(hub_signer, monkeypatch, node_file):
    monkeypatch.delenv("FLOWFILE_NODE_NAME")
    monkeypatch.delenv("FLOWFILE_HUB_URL")
    hub = FakeJoinHub(signer=hub_signer)
    try:
        started_with(monkeypatch, hub.url, hub.mint(enable_on_join=True), name="joined-worker")
        main.join_hub()
        accepted = client.get("/v1/capabilities", headers=v1_headers(hub_signer.token(aud="joined-worker")))
        refused = client.get("/v1/capabilities", headers=v1_headers(hub_signer.token(aud=hub_jwks.NODE_NAME)))
    finally:
        hub.close()

    assert hub_node.read().issuer == hub_signer.url
    assert accepted.status_code == 200
    assert refused.status_code == 401
    assert "wrong audience" in refused.json()["detail"]
