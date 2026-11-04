"""A vend with targets: what core asks, what it keeps of the hub's sealed answer, and what it hands a worker.

The hub is ``SealingHubCatalog`` behind the hub client's seam, sealing with a ``HubSigner`` to node keys made here.
Core never opens a token for another node: the tests open them as each target node would, with that node's key.
"""

import polars as pl
import pytest

from flowfile_core.hub_client import client, session, vend
from flowfile_core.hub_client.client import HubRefused
from shared import hub_node, hub_node_key, hub_sealed, hub_tokens
from shared.tests.hub_jwks import HubSigner
from tests.hub_client import conftest as hub_conftest
from tests.hub_client.conftest import HUB_URL, SealingHubCatalog

store = hub_conftest.store

OPTIONS = {
    "aws_region": "eu-west-1",
    "aws_access_key_id": "AKIAHUBVENDED",
    "aws_secret_access_key": "vended-secret-key",
    "endpoint_url": "http://minio.example.test:9000",
    "aws_allow_http": "true",
}
COLUMNS = {"id": pl.Int64, "name": pl.String}


@pytest.fixture
def signer():
    signer = HubSigner()
    yield signer
    signer.close()


@pytest.fixture
def node_keys(tmp_path, monkeypatch):
    """Keys of the nodes ``team`` and ``worker``, each in a storage dir of its own; ``as_node(name)`` makes this
    process that node of ``signer``'s hub, as the node opening a token is."""
    made = {}
    for name in ("team", "worker"):
        target = tmp_path / name / hub_node.FILE_NAME
        monkeypatch.setattr(hub_node, "path", lambda target=target: target)
        made[name] = (target, hub_node_key.load_or_create())
    return made


@pytest.fixture
def hub(signer, node_keys, monkeypatch, store) -> SealingHubCatalog:
    fake = SealingHubCatalog(signer, issuer=signer.url)
    monkeypatch.setattr(client, "_module_http", fake.http())
    sales = fake.seed_catalog("sales", storage_uri="s3://acme/sales")
    raw = fake.seed_schema(sales, "raw")
    fake.vend_options["s3://acme/sales"] = dict(OPTIONS)
    fake.vend_options["s3://acme/exchange"] = dict(OPTIONS)
    fake.exchange_store = {"uri": "s3://acme/exchange", "connection": "org-exchange", "max_rows": None}
    fake.grants[raw["id"]] = "manage"
    fake.raw = raw["id"]
    fake.enc_keys = {name: keys.public_jwks()["enc"] for name, (_, keys) in node_keys.items()}
    tokens = client.sign_in(HUB_URL, "ana", "s3cret")
    session.save(1, {"hub_url": HUB_URL, "refresh_token": tokens["refresh_token"]})
    return fake


def open_as(name: str, node_keys, signer, monkeypatch, sealed: dict[str, str]) -> hub_sealed.OpenedCredential:
    target, _ = node_keys[name]
    monkeypatch.setattr(hub_node, "path", lambda: target)
    monkeypatch.setenv("FLOWFILE_HUB_URL", signer.url)
    monkeypatch.setenv("FLOWFILE_NODE_NAME", name)
    monkeypatch.setattr(hub_tokens, "_client", None)
    return hub_sealed.open_sealed(sealed)


def test_the_vend_body_names_targets_and_the_exchange_only_when_asked(hub):
    vend.request(1, "run-a", set(), {hub.raw})
    vend.request(1, "run-b", set(), {hub.raw}, targets=["worker", "team", "worker"], exchange=True)

    first, second = hub.vend_calls
    assert "targets" not in first and "exchange" not in first
    assert (second["targets"], second["exchange"]) == (["worker", "team"], True)


def test_a_vend_without_targets_keeps_its_options_and_no_seal(hub):
    run_vend = vend.request(1, "run-a", set(), {hub.raw})

    assert not run_vend.sealed and run_vend.targets == ()
    [credential] = run_vend.credentials
    assert credential.storage_options == OPTIONS and credential.sealed == {}
    with pytest.raises(LookupError, match="sealed this run no credential"):
        run_vend.sealed_credential("s3://acme/sales/raw/t")


def test_a_sealed_vend_holds_no_option_and_each_target_opens_its_own_token(hub, signer, node_keys, monkeypatch):
    run_vend = vend.request(1, "run-s", set(), {hub.raw}, targets=["team", "worker"], exchange=True)

    assert run_vend.sealed and run_vend.targets == ("team", "worker")
    assert all(credential.storage_options == {} for credential in run_vend.credentials)
    table = run_vend.sealed_credential("s3://acme/sales/raw/orders")
    exchange = run_vend.sealed_credential("s3://acme/exchange/7/run-s/1-output-0-2.parquet")
    assert (table.base_uri, exchange.base_uri) == ("s3://acme/sales", "s3://acme/exchange")
    assert "vended-secret-key" not in repr(table) + repr(run_vend)
    for name in ("team", "worker"):
        opened = open_as(name, node_keys, signer, monkeypatch, exchange.sealed)
        assert (opened.base_uri, opened.storage_options, opened.run_id) == ("s3://acme/exchange", OPTIONS, "run-s")
        assert opened.vend_id == run_vend.vend_id


def test_a_sealed_writes_connection_and_reads_plan_carry_the_map_and_no_secret(hub):
    run_vend = vend.request(1, "run-s", {hub.raw}, {hub.raw}, targets=["worker"])

    connection = run_vend.worker_connection("s3://acme/sales/raw/orders_1", user_id=1)
    plan = run_vend.scan_delta("s3://acme/sales/raw/orders_1", 1, COLUMNS).serialize()

    assert (connection.storage_type, connection.auth_method) == ("s3", "access_key")
    assert set(connection.sealed) == {"worker"}
    assert connection.aws_access_key_id is None and connection.aws_secret_access_key is None
    assert b"vended-secret-key" not in plan and b"AKIAHUBVENDED" not in plan


def test_a_hub_that_answers_targets_without_sealing_to_them_is_refused(hub, monkeypatch):
    def unsealed(body):
        response = SealingHubCatalog._vend(hub, {**body, "targets": []})
        return response

    monkeypatch.setattr(hub, "_vend", unsealed)

    with pytest.raises(HubRefused, match="did not answer /hub/vend as a Flowfile hub does"):
        client.vend(
            HUB_URL, client.sign_in(HUB_URL, "ana", "s3cret")["access_token"], "r", [], [hub.raw], targets=["x"]
        )


@pytest.mark.parametrize(
    ("refused", "status", "sentence"),
    [
        (None, 422, "The server nowhere has no keys on this hub"),
        ("worker", 403, "You may not run on the server worker"),
    ],
    ids=["no keys", "may not run"],
)
def test_a_target_the_hub_refuses_ends_the_vend_with_its_sentence(hub, refused, status, sentence):
    if refused:
        hub.refused_nodes.add(refused)

    with pytest.raises(LookupError) as raised:
        vend.request(1, "run-r", set(), {hub.raw}, targets=[refused or "nowhere"])

    assert str(raised.value) == sentence
    assert raised.value.__cause__.status == status
