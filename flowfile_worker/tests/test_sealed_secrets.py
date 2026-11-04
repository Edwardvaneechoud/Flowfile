"""A worker on a hub with keys of its own opens a secret the hub sealed to it wherever it decrypts a ``$ffsec$`` today:
a database connection's password, a REST source's auth, a Kafka connection's secrets and a custom node's secrets.

The hub is ``HubSigner``; the node ``team`` has its keys under the test's storage dir. Each secret is sealed as the
hub's vend seals one and carried as ``hub_sealed.sealed_secret``'s string, which is what core puts where the
ciphertext was.
"""

import pytest
from cryptography.fernet import Fernet, InvalidToken

from flowfile_worker import secrets
from flowfile_worker.custom_node_runner import WorkerSecretResolver
from flowfile_worker.external_sources.kafka_source.main import _decrypt_fn
from flowfile_worker.external_sources.rest_api_source.models import AuthConfig, RestApiReadSettings
from flowfile_worker.external_sources.sql_source.models import DataBaseConnection
from shared import hub_node_key, hub_sealed
from shared.cloud_credential_provider import CredentialResolutionError
from shared.kafka.models import KafkaReadSettings
from shared.tests import hub_jwks
from shared.tests.hub_jwks import NODE_NAME

hub_signer = hub_jwks.hub_signer


@pytest.fixture
def node(hub_signer):
    """This worker's keys as node ``team``, made in the test's storage dir."""
    return hub_node_key.load_or_create()


def sealed(signer, keys, name: str, value: str) -> str:
    token = signer.sealed_secret(keys.public_jwks()["enc"], name=name, value=value)
    return hub_sealed.sealed_secret(name, {NODE_NAME: token, "team-1": "sealed.to.another.node.key"})


def other_servers_ciphertext(value: str) -> str:
    return f"{secrets.SECRET_FORMAT_PREFIX}1${Fernet(Fernet.generate_key()).encrypt(value.encode()).decode()}"


def test_a_sealed_secret_decrypts_where_a_ciphertext_did(hub_signer, node):
    assert secrets.decrypt_secret(sealed(hub_signer, node, "pw", "s3cr3t")).get_secret_value() == "s3cr3t"


def test_a_database_password_sealed_to_this_server_signs_in(hub_signer, node):
    connection = DataBaseConnection(
        username="testuser",
        password=sealed(hub_signer, node, "warehouse", "testpass"),
        host="db",
        port=5433,
        database="testdb",
    )

    assert "testuser:testpass@db:5433/testdb" in connection.create_uri()
    assert connection.get_decrypted_secret().get_secret_value() == "testpass"


@pytest.mark.parametrize(
    ("auth", "read"),
    [
        ({"auth_type": "api_key", "api_key_encrypted": "{v}"}, RestApiReadSettings.get_api_key),
        ({"auth_type": "bearer", "bearer_token_encrypted": "{v}"}, RestApiReadSettings.get_bearer_token),
        (
            {"auth_type": "basic", "basic_username": "u", "basic_password_encrypted": "{v}"},
            RestApiReadSettings.get_basic_password,
        ),
    ],
    ids=["api key", "bearer", "basic"],
)
def test_a_rest_sources_auth_sealed_to_this_server_opens(hub_signer, node, auth, read):
    value = sealed(hub_signer, node, "api", "tok-1")
    settings = RestApiReadSettings(
        url="http://api.example/x", auth=AuthConfig(**{k: v.format(v=value) for k, v in auth.items()})
    )

    assert read(settings) == "tok-1"


def test_a_kafka_connections_secrets_sealed_to_this_server_reach_the_consumer(hub_signer, node):
    settings = KafkaReadSettings(
        bootstrap_servers="broker:9092",
        topic="orders",
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_username="svc",
        sasl_password=sealed(hub_signer, node, "kafka_events_sasl", "sasl-pw"),
        ssl_key_pem=sealed(hub_signer, node, "kafka_events_ssl_key", "-----KEY-----"),
    )

    config = settings.to_consumer_config(decrypt_fn=_decrypt_fn)

    assert (config["sasl.password"], config["ssl.key.pem"]) == ("sasl-pw", "-----KEY-----")


def test_a_custom_nodes_secrets_sealed_to_this_server_resolve(hub_signer, node):
    resolver = WorkerSecretResolver({"warehouse": sealed(hub_signer, node, "warehouse", "s3cr3t")})

    assert resolver.resolve("warehouse").get_secret_value() == "s3cr3t"


def test_a_secret_sealed_to_another_server_or_under_another_name_is_refused(hub_signer, node):
    token = hub_signer.sealed_secret(node.public_jwks()["enc"], name="warehouse", value="s3cr3t")

    with pytest.raises(CredentialResolutionError, match=r"sealed it to team-1, not to this server \(team\)"):
        secrets.decrypt_secret(hub_sealed.sealed_secret("warehouse", {"team-1": token}))
    with pytest.raises(CredentialResolutionError, match='does not hold the secret "other"'):
        secrets.decrypt_secret(hub_sealed.sealed_secret("other", {NODE_NAME: token}))


def test_a_server_that_never_joined_a_hub_fails_another_servers_ciphertext_as_before(hub_signer):
    assert not hub_sealed.can_open()
    with pytest.raises(InvalidToken):
        secrets.decrypt_secret(other_servers_ciphertext("s3cr3t"))
    assert secrets.decrypt_secret(secrets.encrypt_secret("mine", user_id=1)).get_secret_value() == "mine"
