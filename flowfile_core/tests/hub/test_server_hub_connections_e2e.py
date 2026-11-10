"""A server joined to its hub with keys of its own runs steps that use the stored connections the hub keeps on a
compute-only server of the hub, end to end.

The setup is ``test_server_sealed_e2e``'s: core here as ``team-1``, joined with its keys and its own master key; the
compute-only server ``worker`` a second ``flowfile_worker`` joined with keys of its own, started and stopped by the
test on a free port above 64980; the organisation's exchange store on the suite's MinIO. The hub is
``tests/hub/hub_connections.ConnectionsHub``, which keeps each person's cloud storage, Kafka and database connections
and seals one a run names to every server the run uses. The data is on the suite's MinIO, Redpanda and Postgres; the
module skips without MinIO, a test without its broker or database.

Done when a cloud storage read and a Kafka read placed on the compute-only server open the connection the hub sealed
to it, a database connection the hub keeps is used on the compute-only server by a team member it is shared with, who
never sees its password, and no request or socket frame core sent that server carries a secret or a ``$ffsec$``.
"""

import time
import uuid
from collections.abc import Iterator

import polars as pl
import pytest
from cryptography.fernet import Fernet

from flowfile_core.hub_client import node_tokens
from flowfile_core.schemas import input_schema, transform_schema
from shared.tests.hub_jwks import HubSigner
from test_utils.kafka import fixtures as kafka_fixtures
from test_utils.postgres import fixtures as pg_fixtures
from tests import test_hub_sign_in
from tests.flowfile.execution.graph_helpers import add_promise, connect
from tests.flowfile.execution.test_hub_tables_e2e import MINIO_OPTIONS
from tests.hub import test_server_run_report as run_report
from tests.hub import test_server_sealed_e2e as sealed_e2e
from tests.hub.hub_connections import ConnectionsHub
from tests.hub.test_server_hub_secrets import drop_accounts
from tests.test_catalog_cloud_virtual import _BUCKET
from tests.test_hub_sign_in import NODE, bearer, join

node_file = test_hub_sign_in.node_file
username = test_hub_sign_in.username
compute_server = sealed_e2e.compute_server
store = sealed_e2e.store
person = sealed_e2e.person
sent = sealed_e2e.sent
clean_catalog = sealed_e2e.clean_catalog
open_flow = run_report.open_flow
client = run_report.client
UID = run_report.UID

pytestmark = sealed_e2e.pytestmark

MEMBER = UID + 1
KAFKA_SECRET = "kafka-secret-only-the-hub-holds"
PEOPLE = {"id": [1, 2, 3], "name": ["ada", "grace", "dee"]}


@pytest.fixture
def hub(node_file, monkeypatch) -> Iterator[ConnectionsHub]:
    """A ``ConnectionsHub`` this server joined as ``team-1`` with its own keys and master key; its people removed
    after."""
    monkeypatch.setenv("FLOWFILE_MASTER_KEY", Fernet.generate_key().decode())
    signer = HubSigner()
    fake = ConnectionsHub(signer)
    join(fake)
    try:
        yield fake
    finally:
        fake.close()
        signer.close()
        drop_accounts(fake.issuer)


@pytest.fixture
def name() -> str:
    return f"conn_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def topic() -> Iterator[str]:
    """A topic of its own on the suite's Redpanda, holding two messages; deleted after."""
    from confluent_kafka.admin import AdminClient

    if not kafka_fixtures.wait_for_redpanda(max_retries=2, interval=0.5):
        pytest.skip("the test Redpanda broker is not running")
    name = f"hub_{uuid.uuid4().hex[:8]}"
    kafka_fixtures.create_topic(name)
    try:
        kafka_fixtures.produce_json_messages(name, [{"id": 1, "name": "ada"}, {"id": 2, "name": "grace"}])
        yield name
    finally:
        admin = AdminClient({"bootstrap.servers": kafka_fixtures.BOOTSTRAP_SERVERS})
        for deleted in admin.delete_topics([name]).values():
            deleted.result(timeout=10)


@pytest.fixture
def postgres():
    if not pg_fixtures.can_connect_to_db():
        pytest.skip("the test PostgreSQL database is not running")


def run_vend(hub: ConnectionsHub) -> dict:
    [asked] = hub.catalog.vend_calls
    return asked


def nothing_secret(sent: list[tuple[str, bytes]], *secrets: bytes) -> bool:
    return not [route for route, body in sent if b"$ffsec$" in body or any(secret in body for secret in secrets)]


def add_select(flow, node_id: int, depending_on: int, column: str) -> None:
    add_promise(flow, node_id, "select")
    flow.add_select(
        input_schema.NodeSelect(
            flow_id=flow.flow_id,
            node_id=node_id,
            depending_on_id=depending_on,
            select_input=[transform_schema.SelectInput(old_name=column)],
            keep_missing=False,
        )
    )
    connect(flow, depending_on, node_id)


def test_a_cloud_read_on_the_compute_only_server_opens_the_connection_the_hub_sealed_to_it(
    hub, store, person, sent, open_flow, name
):
    person_id, access_token = person
    path = f"s3://{_BUCKET}/{store}/people.parquet"
    pl.DataFrame(PEOPLE).write_parquet(path, storage_options=MINIO_OPTIONS)
    hub.add(
        "cloud",
        UID,
        {
            "connection_name": name,
            "storage_type": "s3",
            "auth_method": "access_key",
            "aws_access_key_id": MINIO_OPTIONS["aws_access_key_id"],
            "aws_secret_access_key": MINIO_OPTIONS["aws_secret_access_key"],
            "aws_region": "us-east-1",
            "endpoint_url": MINIO_OPTIONS["endpoint_url"],
            "aws_allow_unsafe_html": True,
        },
    )
    flow = open_flow(person_id, "cloud_read_on_the_compute_server")
    add_promise(flow, 1, "cloud_storage_reader")
    flow.add_cloud_storage_reader(
        input_schema.NodeCloudStorageReader(
            flow_id=flow.flow_id,
            node_id=1,
            user_id=person_id,
            cloud_storage_settings=input_schema.CloudStorageReadSettings(
                resource_path=path, file_format="parquet", connection_name=name
            ),
        )
    )
    sealed_e2e.on(flow, 1, sealed_e2e.BACKEND)
    add_select(flow, 2, 1, "name")

    run_info = sealed_e2e.run(flow, access_token)

    assert (1, 2) in [tuple(boundary) for boundary in run_info.boundaries]
    assert flow.get_node(2).results.result_ref.backend_id == "local"
    assert sorted(flow.get_node(2).get_resulting_data().data_frame.collect()["name"]) == ["ada", "dee", "grace"]
    asked = run_vend(hub)
    assert asked["connections"] == [{"kind": "cloud", "name": name}]
    assert set(asked["targets"]) == {sealed_e2e.SERVER, NODE}
    assert sent and nothing_secret(sent, sealed_e2e.SECRET)
    assert any(b"SealedConnectionProvider" in body or b"hub_sealed" in body for _, body in sent)


def test_a_kafka_read_on_the_compute_only_server_opens_the_connection_the_hub_sealed_to_it(
    hub, store, person, sent, open_flow, name, topic
):
    person_id, access_token = person
    hub.add(
        "kafka",
        UID,
        {
            "connection_name": name,
            "bootstrap_servers": kafka_fixtures.BOOTSTRAP_SERVERS,
            "security_protocol": "PLAINTEXT",
            "sasl_username": "ada",
            "sasl_password": KAFKA_SECRET,
        },
    )
    flow = open_flow(person_id, "kafka_read_on_the_compute_server")
    add_promise(flow, 1, "kafka_source")
    flow.add_kafka_source(
        input_schema.NodeKafkaSource(
            flow_id=flow.flow_id,
            node_id=1,
            user_id=person_id,
            kafka_settings=input_schema.KafkaSourceSettings(
                kafka_connection_name=name,
                topic_name=topic,
                start_offset="earliest",
                max_messages=10,
                poll_timeout_seconds=10,
            ),
        )
    )
    sealed_e2e.on(flow, 1, sealed_e2e.BACKEND)
    add_select(flow, 2, 1, "name")

    run_info = sealed_e2e.run(flow, access_token)

    assert (1, 2) in [tuple(boundary) for boundary in run_info.boundaries]
    assert sorted(flow.get_node(2).get_resulting_data().data_frame.collect()["name"]) == ["ada", "grace"]
    asked = run_vend(hub)
    assert asked["connections"] == [{"kind": "kafka", "name": name}]
    assert set(asked["targets"]) == {sealed_e2e.SERVER, NODE}
    assert sent and nothing_secret(sent, KAFKA_SECRET.encode(), sealed_e2e.SECRET)
    assert any("/v1/ops/kafka_read" in route and b"$ffseal$" in body for route, body in sent)


def test_a_database_connection_the_hub_keeps_is_used_by_a_member_it_is_shared_with_on_the_compute_only_server(
    hub, store, person, sent, open_flow, name, postgres
):
    hub.add(
        "database",
        UID,
        {
            "connection_name": name,
            "database_type": "postgresql",
            "username": pg_fixtures.POSTGRES_USER,
            "password": pg_fixtures.POSTGRES_PASSWORD,
            "host": pg_fixtures.POSTGRES_HOST,
            "port": pg_fixtures.POSTGRES_PORT,
            "database": pg_fixtures.POSTGRES_DB,
        },
    )
    hub.people[MEMBER] = {"uid": MEMBER, "username": "dee", "full_name": None, "node_admin": False}
    hub.share("database", name, UID, MEMBER)
    signed_in = client.post("/auth/hub/signin", json={"token": hub.sign_in_token(NODE, uid=MEMBER, username="dee")})
    assert signed_in.status_code == 200, signed_in.text
    member_token = signed_in.json()["access_token"]
    member_id = client.get("/auth/users/me", headers=bearer(member_token)).json()["id"]
    run_token = hub.signer.token(uid=member_id, aud=sealed_e2e.SERVER, iss=hub.issuer)
    node_tokens._tokens[(member_id, hub._node_ids[sealed_e2e.SERVER])] = (run_token, time.time() + 3600)
    listed = client.get("/db_connection_lib", headers=bearer(member_token))
    assert [(row["connection_name"], row["source"], row["access"]["access_level"]) for row in listed.json()] == [
        (name, "hub", "use")
    ]
    assert pg_fixtures.POSTGRES_PASSWORD not in listed.text
    flow = open_flow(member_id, "a_members_database_read_on_the_compute_server")
    add_promise(flow, 1, "database_reader")
    flow.add_database_reader(
        input_schema.NodeDatabaseReader(
            flow_id=flow.flow_id,
            node_id=1,
            user_id=member_id,
            database_settings=input_schema.DatabaseSettings(
                connection_mode="reference",
                database_connection_name=name,
                schema_name="public",
                table_name="movies",
            ),
        )
    )
    sealed_e2e.on(flow, 1, sealed_e2e.BACKEND)
    add_select(flow, 2, 1, "title")

    run_info = sealed_e2e.run(flow, member_token)

    assert (1, 2) in [tuple(boundary) for boundary in run_info.boundaries]
    assert flow.get_node(2).get_resulting_data().count() > 0
    asked = run_vend(hub)
    assert asked["connections"] == [{"kind": "database", "name": name}] and not asked.get("secrets")
    assert set(asked["targets"]) == {sealed_e2e.SERVER, NODE}
    assert {uid for uid, _ in hub.connection_vends} == {MEMBER}
    assert sent and nothing_secret(sent, pg_fixtures.POSTGRES_PASSWORD.encode())
    assert any("/v1/ops/database_read" in route and b"$ffseal$" in body for route, body in sent)
