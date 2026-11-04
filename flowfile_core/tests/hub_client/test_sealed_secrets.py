"""Secrets by name in a sealed vend: which names a step uses, what core asks and keeps of the hub's answer, and what a
step on another server of the hub carries in place of its ``$ffsec$`` ciphertext.

The hub is ``SealingHubCatalog`` behind the hub client's seam (``test_sealed_vend``'s fixtures), sealing each secret
with a ``HubSigner`` to node keys made here. Core never opens a token: the tests open each as the target node would,
with that node's key, from what core's worker backend would send.
"""

import json
import uuid

import polars as pl
import pytest
from pydantic import SecretStr

from flowfile_core.auth.models import SecretInput
from flowfile_core.configs.node_store import add_to_custom_node_store
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import store_database_connection
from flowfile_core.flowfile.execution import run_secrets
from flowfile_core.flowfile.execution.backends import worker as worker_backend
from flowfile_core.flowfile.execution.payloads import DatabaseReadPayload, KafkaReadPayload, RestApiReadPayload
from flowfile_core.flowfile.flow_graph.builders.connectors import _step_database_credentials, _step_kafka_settings
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings, SecretSelector, Section
from flowfile_core.flowfile.sources.external_sources.rest_api_source import resolve_auth_secret_encrypted
from flowfile_core.flowfile.user_defined.dispatch import resolve_secret_payload
from flowfile_core.hub_client import client, vend
from flowfile_core.hub_client.client import HubRefused
from flowfile_core.kafka.connection_manager import store_kafka_connection
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.kafka_schemas import KafkaConnectionCreate
from flowfile_core.secret_manager.secret_manager import store_secret
from shared import hub_node, hub_sealed, hub_tokens
from shared.kafka.models import KafkaReadSettings
from tests.hub_client import test_sealed_vend
from tests.hub_client.conftest import HUB_URL, SealingHubCatalog

store = test_sealed_vend.store
signer = test_sealed_vend.signer
node_keys = test_sealed_vend.node_keys
hub = test_sealed_vend.hub

PASSWORD = "hub-only-password"


def open_secret_as(name: str, node_keys, signer, monkeypatch, value: str) -> str:
    target, _ = node_keys[name]
    monkeypatch.setattr(hub_node, "path", lambda: target)
    monkeypatch.setenv("FLOWFILE_HUB_URL", signer.url)
    monkeypatch.setenv("FLOWFILE_NODE_NAME", name)
    monkeypatch.setattr(hub_tokens, "_client", None)
    return hub_sealed.open_secret(value)


@pytest.fixture
def user_id() -> int:
    with get_db_context() as db:
        return db.query(db_models.User).filter(db_models.User.username == "local_user").one().id


@pytest.fixture
def local_secret(user_id):
    """A secret of ``local_user``'s on this server only; removed afterwards."""
    name = f"local_{uuid.uuid4().hex[:8]}"
    with get_db_context() as db:
        store_secret(db, SecretInput(name=name, value=SecretStr("local-value")), user_id)
    yield name
    with get_db_context() as db:
        db.query(db_models.Secret).filter(db_models.Secret.name == name).delete()
        db.commit()


@pytest.fixture
def stored_connections(user_id):
    """A stored database connection and a Kafka connection with SASL and SSL secrets; removed afterwards."""
    suffix = uuid.uuid4().hex[:8]
    database, kafka = f"warehouse_{suffix}", f"events_{suffix}"
    with get_db_context() as db:
        store_database_connection(
            db,
            input_schema.FullDatabaseConnection(
                connection_name=database, username="u", password=SecretStr("pw"), host="db", port=5432, database="d"
            ),
            user_id,
        )
        row = store_kafka_connection(
            db,
            KafkaConnectionCreate(
                connection_name=kafka,
                bootstrap_servers="broker:9092",
                security_protocol="SASL_SSL",
                sasl_mechanism="PLAIN",
                sasl_username="svc",
                sasl_password=SecretStr("sasl-pw"),
                ssl_key_pem=SecretStr("-----KEY-----"),
            ),
            user_id,
        )
    yield database, kafka, row.id
    with get_db_context() as db:
        db.query(db_models.DatabaseConnection).filter(db_models.DatabaseConnection.connection_name == database).delete()
        db.query(db_models.KafkaConnection).filter(db_models.KafkaConnection.connection_name == kafka).delete()
        db.query(db_models.Secret).filter(db_models.Secret.name.in_([database, *kafka_names(kafka)])).delete()
        db.commit()


def kafka_names(connection: str) -> tuple[str, str]:
    return f"kafka_{connection}_sasl", f"kafka_{connection}_ssl_key"


class Step:
    """A node as ``run_secrets.names_for`` reads it: its settings and its type."""

    def __init__(self, setting_input, node_type: str = ""):
        self.setting_input = setting_input
        self.node_type = node_type


def inline_reader(user_id: int, password_ref: str) -> input_schema.NodeDatabaseReader:
    connection = input_schema.DatabaseConnection(
        database_type="postgresql",
        username="testuser",
        host="db",
        port=5433,
        database="testdb",
        password_ref=password_ref,
    )
    return input_schema.NodeDatabaseReader(
        flow_id=1,
        node_id=1,
        user_id=user_id,
        database_settings=input_schema.DatabaseSettings(
            database_connection=connection, schema_name="public", table_name="movies"
        ),
    )


def rest_reader(user_id: int, secret_name: str) -> input_schema.NodeRestApiReader:
    return input_schema.NodeRestApiReader(
        flow_id=1,
        node_id=3,
        user_id=user_id,
        rest_api_settings=input_schema.RestApiSettings(
            url="http://api.example/people",
            auth=input_schema.RestApiAuthSettings(auth_type="bearer", secret_name=secret_name),
        ),
    )


class SecretNode(CustomNodeBase):
    node_name: str = "Sealed secret user"
    number_of_inputs: int = 1
    number_of_outputs: int = 1
    settings_schema: NodeSettings = NodeSettings(config=Section(title="Auth", api_key=SecretSelector(label="Key")))

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]


def test_each_kind_names_its_secrets(user_id, local_secret, stored_connections):
    database, kafka, kafka_id = stored_connections
    add_to_custom_node_store(SecretNode)
    reference = input_schema.NodeDatabaseReader(
        flow_id=1,
        node_id=2,
        user_id=user_id,
        database_settings=input_schema.DatabaseSettings(
            connection_mode="reference", database_connection_name=database, table_name="movies"
        ),
    )
    source = input_schema.NodeKafkaSource(
        flow_id=1,
        node_id=4,
        user_id=user_id,
        kafka_settings=input_schema.KafkaSourceSettings(kafka_connection_id=kafka_id),
    )
    custom = input_schema.UserDefinedNode(
        flow_id=1, node_id=5, user_id=user_id, settings={"config": {"api_key": "hub_api_key"}}, is_user_defined=True
    )

    assert run_secrets.names_for(Step(inline_reader(user_id, local_secret))) == [local_secret]
    assert run_secrets.names_for(Step(inline_reader(user_id, "on_the_hub"))) == ["on_the_hub"]
    assert run_secrets.names_for(Step(reference)) == [database]
    assert run_secrets.names_for(Step(source)) == list(kafka_names(kafka))
    assert run_secrets.names_for(Step(rest_reader(user_id, local_secret))) == [local_secret]
    assert run_secrets.names_for(Step(custom, SecretNode().item)) == ["hub_api_key"]
    assert run_secrets.names_for(Step(input_schema.NodeRecordCount(flow_id=1, node_id=6, user_id=user_id))) == []


def test_the_vend_asks_the_names_and_each_target_opens_its_own_secret(hub, signer, node_keys, monkeypatch):
    hub.secret_values = {"warehouse": PASSWORD, "api": "tok-1"}

    run_vend = vend.request(1, "run-s", set(), set(), targets=["team", "worker"], secrets=["warehouse", "api"])

    [body] = hub.vend_calls
    assert (body["targets"], body["secrets"]) == (["team", "worker"], ["warehouse", "api"])
    assert PASSWORD not in repr(run_vend)
    sealed = run_vend.sealed_secrets(["warehouse", "unknown"])
    assert set(sealed) == {"warehouse"} and PASSWORD not in sealed["warehouse"]
    for name in ("team", "worker"):
        assert open_secret_as(name, node_keys, signer, monkeypatch, sealed["warehouse"]) == PASSWORD


def test_a_vend_without_names_asks_none(hub):
    run_vend = vend.request(1, "run-a", set(), set(), targets=["worker"])

    assert "secrets" not in hub.vend_calls[0] and run_vend.secrets == {}


def test_names_without_targets_are_the_hubs_422(hub):
    with pytest.raises(LookupError, match="Secrets are vended only sealed to the servers that run the step"):
        vend.request(1, "run-n", set(), set(), secrets=["warehouse"])


def test_a_name_the_hub_refuses_ends_the_vend_with_its_sentence(hub):
    with pytest.raises(LookupError) as refused:
        vend.request(1, "run-r", set(), set(), targets=["worker"], secrets=["nowhere"])

    assert str(refused.value).startswith("You have no secret named nowhere on the hub")


def test_a_hub_that_answers_names_without_their_secrets_is_refused(hub, monkeypatch):
    def forgetful(body):
        return SealingHubCatalog._vend(hub, {**body, "secrets": []})

    hub.secret_values = {"warehouse": PASSWORD}
    monkeypatch.setattr(hub, "_vend", forgetful)

    with pytest.raises(HubRefused, match="did not answer /hub/vend as a Flowfile hub does"):
        client.vend(
            HUB_URL,
            client.sign_in(HUB_URL, "ana", "s3cret")["access_token"],
            "r",
            [],
            [],
            targets=["worker"],
            secrets=["warehouse"],
        )


@pytest.fixture
def sealed_for_worker(hub):
    """The run's vend of the secrets the tests name, sealed to ``worker``."""

    def seal(**values: str) -> dict[str, str]:
        hub.secret_values = dict(values)
        return vend.request(1, "run-w", set(), set(), targets=["worker"], secrets=list(values)).sealed_secrets(values)

    return seal


def test_a_database_step_carries_the_sealed_password_which_opens_where_it_runs(
    user_id, sealed_for_worker, node_keys, signer, monkeypatch
):
    settings = inline_reader(user_id, "on_the_hub")
    sealed = sealed_for_worker(on_the_hub=PASSWORD)

    def resolved():
        raise AssertionError("a password the hub sealed is not looked up here")

    with run_secrets.use_sealed(sealed):
        connection, password = _step_database_credentials(settings.database_settings, user_id, resolved)
    read = DatabaseReadPayload(
        flow_id=1, node_id=1, settings=settings, connection=connection, encrypted_password=password, cancel_check=bool
    )
    wire = worker_backend._database_read_settings(read).model_dump(mode="json")

    assert "$ffsec$" not in json.dumps(wire) and PASSWORD not in json.dumps(wire)
    assert connection is settings.database_settings.database_connection
    assert open_secret_as("worker", node_keys, signer, monkeypatch, wire["connection"]["password"]) == PASSWORD


def test_a_kafka_step_carries_its_connections_secrets_sealed_and_core_keeps_its_own_to_commit(
    user_id, stored_connections, sealed_for_worker, node_keys, signer, monkeypatch
):
    _, kafka, kafka_id = stored_connections
    sasl, ssl_key = kafka_names(kafka)
    sealed = sealed_for_worker(**{sasl: "hub-sasl", ssl_key: "hub-key"})
    source = input_schema.KafkaSourceSettings(kafka_connection_id=kafka_id)
    stored = KafkaReadSettings(
        bootstrap_servers="broker:9092", topic="t", sasl_password="$ffsec$1$1$core", ssl_key_pem="$ffsec$1$1$key"
    )

    assert _step_kafka_settings(stored, source, user_id) is stored
    with run_secrets.use_sealed(sealed):
        carried = _step_kafka_settings(stored, source, user_id)
    read = KafkaReadPayload(flow_id=1, node_id=4, settings=carried, read_kafka_source=lambda *a, **k: None)
    wire = worker_backend._kafka_read_settings(read).model_dump(mode="json")

    assert "$ffsec$" not in json.dumps(wire) and stored.sasl_password == "$ffsec$1$1$core"
    sent = KafkaReadSettings.model_validate(wire)
    opened = [
        open_secret_as("worker", node_keys, signer, monkeypatch, v) for v in (sent.sasl_password, sent.ssl_key_pem)
    ]
    assert opened == ["hub-sasl", "hub-key"]


def test_a_rest_step_carries_its_sealed_auth_secret_which_opens_where_it_runs(
    user_id, local_secret, sealed_for_worker, node_keys, signer, monkeypatch
):
    node = rest_reader(user_id, local_secret)
    auth = node.rest_api_settings.auth
    sealed = sealed_for_worker(**{local_secret: "hub-token"})

    assert resolve_auth_secret_encrypted(auth, user_id).startswith("$ffsec$")
    with run_secrets.use_sealed(sealed):
        carried = run_secrets.in_place_of(auth.secret_name, resolve_auth_secret_encrypted(auth, user_id))
    wire = worker_backend._rest_api_read_settings(
        RestApiReadPayload(settings=node, encrypted_secret=carried, flow_id=1, node_id=3)
    )

    assert "$ffsec$" not in json.dumps(wire.model_dump(mode="json"))
    assert open_secret_as("worker", node_keys, signer, monkeypatch, wire.auth.bearer_token_encrypted) == "hub-token"


def test_a_custom_step_carries_its_selected_secrets_sealed_and_needs_none_here(
    user_id, local_secret, sealed_for_worker, node_keys, signer, monkeypatch
):
    node = SecretNode.from_settings({"config": {"api_key": "hub_api_key"}})
    sealed = sealed_for_worker(hub_api_key="hub-key")

    with pytest.raises(ValueError, match="not found for this user"):
        resolve_secret_payload(node, user_id)
    with run_secrets.use_sealed(sealed):
        carried = resolve_secret_payload(node, user_id)

    assert set(carried) == {"hub_api_key"} and "$ffsec$" not in carried["hub_api_key"]
    assert open_secret_as("worker", node_keys, signer, monkeypatch, carried["hub_api_key"]) == "hub-key"
    mine = SecretNode.from_settings({"config": {"api_key": local_secret}})
    assert resolve_secret_payload(mine, user_id)[local_secret].startswith("$ffsec$")
