"""The credential a run over the hub's tables asks the hub for, and how it travels to the compute.

One ``POST /hub/vend`` per request names the run and the namespaces it reads and writes. The answer is held as a
``RunVend``: a scan gets the options without their secrets and an ``EncryptedCredentialProvider``, a Delta write an S3
connection whose secrets are ``$ffsec$`` ciphertext. Every refusal is a sentence, raised before the hub is asked to vend.
"""

import pytest

from flowfile_core.hub_client import client, session, vend
from flowfile_core.secret_manager.secret_manager import decrypt_secret
from shared.cloud_credential_provider import PROVIDER_CREDENTIAL_KEYS, EncryptedCredentialProvider
from tests.hub_client.conftest import HUB_URL, FakeHubWithCatalog

RECONNECT = "This flow uses tables on the hub, but this Flowfile is not connected to it; connect to the hub again"
OPTIONS = {
    "aws_region": "eu-west-1",
    "aws_access_key_id": "AKIAHUBVENDED",
    "aws_secret_access_key": "vended-secret-key",
    "aws_session_token": "vended-session-token",
    "endpoint_url": "http://minio.example.test:9000",
    "aws_allow_http": "true",
    "allow_invalid_certificates": "true",
}
SECRETS = ("vended-secret-key", "vended-session-token")
USER = 1


@pytest.fixture
def hub(monkeypatch) -> FakeHubWithCatalog:
    fake = FakeHubWithCatalog()
    monkeypatch.setattr(client, "_module_http", fake.http())
    return fake


@pytest.fixture
def catalog(hub) -> dict:
    sales = hub.seed_catalog("sales", storage_uri="s3://acme/sales/", storage_connection_name="acme-s3")
    raw = hub.seed_schema(sales, "raw")
    curated = hub.seed_schema(sales, "curated")
    local = hub.seed_catalog("scratch")
    notes = hub.seed_schema(local, "notes")
    lake = hub.seed_catalog("lake", storage_uri="az://lake/data", storage_connection_name="acme-adls")
    landing = hub.seed_schema(lake, "landing")
    hub.vend_options["s3://acme/sales/"] = dict(OPTIONS)
    hub.grants.update({raw["id"]: "use", curated["id"]: "manage", notes["id"]: "manage", landing["id"]: "manage"})
    return {"raw": raw["id"], "curated": curated["id"], "notes": notes["id"], "landing": landing["id"]}


@pytest.fixture
def connected(store, hub):
    tokens = client.sign_in(HUB_URL, "ana", "s3cret")
    session.save(USER, {"hub_url": HUB_URL, "refresh_token": tokens["refresh_token"], "device_id": 1})
    hub.calls.clear()
    return hub


def test_a_run_asks_the_hub_once_naming_the_run_and_its_namespaces(connected, catalog):
    run_vend = vend.request(USER, "run-abc", {catalog["raw"]}, {catalog["curated"]})

    assert connected.vend_calls == [
        {"run_id": "run-abc", "read_namespace_ids": [catalog["raw"]], "write_namespace_ids": [catalog["curated"]]}
    ]
    assert [path for method, path in connected.calls if path == "/hub/vend"] == ["/hub/vend"]
    assert run_vend.run_id == "run-abc" and run_vend.vend_id == 1
    assert run_vend.bases == {catalog["raw"]: "s3://acme/sales", catalog["curated"]: "s3://acme/sales"}
    assert run_vend.names == {catalog["raw"]: "sales.raw", catalog["curated"]: "sales.curated"}
    [credential] = run_vend.credentials
    assert credential.base_uri == "s3://acme/sales" and credential.mode == "static"
    assert credential.write_prefixes == ("s3://acme/sales",)
    assert run_vend.for_namespace(catalog["curated"]) is credential
    assert run_vend.for_uri("s3://acme/sales/orders_1a2b3c4d") is credential
    with pytest.raises(LookupError):
        run_vend.for_uri("s3://acme/sales2/orders")


@pytest.mark.parametrize("state", ["no session", "expired session"])
def test_without_a_hub_session_the_run_is_told_to_connect_again_and_the_hub_vends_nothing(store, hub, catalog, state):
    if state == "expired session":
        tokens = client.sign_in(HUB_URL, "ana", "s3cret")
        session.save(USER, {"hub_url": HUB_URL, "refresh_token": tokens["refresh_token"], "device_id": 1})
        hub.live_refresh.clear()
    hub.calls.clear()

    with pytest.raises(LookupError) as refused:
        vend.request(USER, "run-abc", {catalog["raw"]}, set())

    assert str(refused.value) == f"{RECONNECT} (Compute → Hub)"
    assert hub.vend_calls == []
    assert all(path != "/hub/vend" and not path.startswith("/catalog") for _, path in hub.calls)


def test_a_hub_refusal_is_a_sentence_carrying_the_hub_s_reason(connected, catalog):
    connected.grants.pop(catalog["curated"])

    with pytest.raises(LookupError) as refused:
        vend.request(USER, "run-abc", {catalog["raw"]}, {catalog["curated"]})

    assert str(refused.value) == (
        f"The hub did not give this run access to its tables: no manage grant on namespace {catalog['curated']}"
    )


def test_a_hub_that_vends_nothing_says_why(connected, catalog):
    connected.vend_failure = 501

    with pytest.raises(LookupError) as refused:
        vend.request(USER, "run-abc", {catalog["raw"]}, set())

    assert (
        str(refused.value)
        == "The hub did not give this run access to its tables: vending is not configured on this hub"
    )


def test_a_namespace_on_the_hub_s_own_disk_is_refused_before_the_vend(connected, catalog):
    with pytest.raises(LookupError) as refused:
        vend.request(USER, "run-abc", {catalog["raw"], catalog["notes"]}, set())

    assert str(refused.value) == (
        "The hub keeps the tables of scratch.notes on its own disk; this Flowfile cannot read them. "
        "Pick a catalog on the organisation's storage, or ask the hub's admin to put this one there."
    )
    assert connected.vend_calls == []


def test_a_namespace_on_storage_other_than_s3_is_refused_before_the_vend(connected, catalog):
    with pytest.raises(LookupError) as refused:
        vend.request(USER, "run-abc", set(), {catalog["landing"]})

    assert str(refused.value) == (
        "Flowfile reads and writes hub tables on S3 storage; this catalog is on az:// storage. "
        "Pick a catalog on the organisation's S3 storage, or ask the hub's admin to put this one there."
    )
    assert connected.vend_calls == []


def test_a_hub_that_does_not_answer_is_a_sentence(connected, catalog):
    connected.down = True

    with pytest.raises(LookupError) as refused:
        vend.request(USER, "run-abc", {catalog["raw"]}, set())

    assert str(refused.value) == f"Could not reach the hub at {HUB_URL}."


def test_a_scan_carries_the_credential_encrypted_and_options_without_a_secret(connected, catalog):
    run_vend = vend.request(USER, "run-abc", {catalog["raw"]}, set())

    kwargs = run_vend.scan_kwargs("s3://acme/sales/orders_1a2b3c4d", USER)

    assert not PROVIDER_CREDENTIAL_KEYS & kwargs["storage_options"].keys()
    assert kwargs["storage_options"]["endpoint_url"] == OPTIONS["endpoint_url"]
    assert not any(secret in repr(kwargs["storage_options"]) for secret in SECRETS)
    provider = kwargs["credential_provider"]
    assert isinstance(provider, EncryptedCredentialProvider)
    assert provider.encrypted_credentials.startswith(f"$ffsec$1${USER}$")
    assert "vended-secret-key" not in provider.encrypted_credentials


def test_a_write_gets_an_s3_connection_with_its_secrets_encrypted_for_the_worker(connected, catalog):
    run_vend = vend.request(USER, "run-abc", set(), {catalog["curated"]})

    connection = run_vend.worker_connection("s3://acme/sales/orders_1a2b3c4d", USER)

    assert connection.storage_type == "s3" and connection.auth_method == "access_key"
    assert connection.connection_name is None
    assert connection.aws_access_key_id == "AKIAHUBVENDED"
    assert connection.aws_region == "eu-west-1"
    assert connection.endpoint_url == "http://minio.example.test:9000"
    assert connection.aws_allow_unsafe_html is True
    assert connection.verify_ssl is False
    for field, secret in (("aws_secret_access_key", SECRETS[0]), ("aws_session_token", SECRETS[1])):
        ciphertext = getattr(connection, field)
        assert ciphertext.startswith(f"$ffsec$1${USER}$")
        assert decrypt_secret(ciphertext).get_secret_value() == secret


def test_a_credential_for_other_storage_is_refused_by_scheme():
    run_vend = vend.RunVend(
        run_id="run-abc",
        vend_id=7,
        credentials=(vend.HubCredential("az://lake/data", {"account_key": "k"}, "soon", "static", (), ()),),
        bases={3: "az://lake/data"},
        names={3: "lake.landing"},
    )

    with pytest.raises(ValueError, match=r"this catalog is on az:// storage"):
        run_vend.worker_connection("az://lake/data/orders", USER)


def test_no_repr_shows_a_secret(connected, catalog):
    run_vend = vend.request(USER, "run-abc", {catalog["raw"]}, {catalog["curated"]})

    for shown in (repr(run_vend), repr(run_vend.credentials[0]), str(run_vend)):
        assert not any(secret in shown for secret in (*SECRETS, "AKIAHUBVENDED"))
    assert repr(run_vend.credentials[0]) == "HubCredential(base_uri='s3://acme/sales', mode='static')"
