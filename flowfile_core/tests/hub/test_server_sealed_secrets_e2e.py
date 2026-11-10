"""A server joined to its hub with keys of its own reads a database on a compute-only server of the hub, end to end,
with the password the hub keeps for the person and seals to that server.

The setup is ``test_server_sealed_e2e``'s: core here as ``team-1``, joined with its keys and its own master key; the
compute-only server ``worker`` a second ``flowfile_worker`` joined with keys of its own, started and stopped by the
test on a free port above 64980; a thread-served hub that seals what it vends, now also each secret a run names by
name, to every server the run uses; the organisation's exchange store on the suite's MinIO. The database is the
suite's Postgres (``test_utils.postgres``); the module skips without it or MinIO.

Done when a database read placed on the compute-only server runs with a password only the hub holds, its rows cross
back to this server, and no request or socket frame core sent that server carries the password or a ``$ffsec$``.
"""

import uuid

import pytest

from flowfile_core.schemas import input_schema, transform_schema
from test_utils.postgres import fixtures as pg_fixtures
from tests import test_hub_sign_in
from tests.flowfile.execution.graph_helpers import add_promise, connect
from tests.hub import test_server_sealed_e2e as sealed_e2e
from tests.hub.test_server_run_report import open_flow  # noqa: F401

node_file = test_hub_sign_in.node_file
username = test_hub_sign_in.username
hub = sealed_e2e.hub
compute_server = sealed_e2e.compute_server
store = sealed_e2e.store
person = sealed_e2e.person
sent = sealed_e2e.sent
clean_catalog = sealed_e2e.clean_catalog

pytestmark = [*sealed_e2e.pytestmark, pytest.mark.usefixtures("postgres")]

PASSWORD = pg_fixtures.POSTGRES_PASSWORD


@pytest.fixture
def postgres():
    """The suite's Postgres, which the session starts when Docker runs."""
    if not pg_fixtures.can_connect_to_db():
        pytest.skip("the test PostgreSQL database is not running")


@pytest.fixture
def secret_name() -> str:
    return f"warehouse_{uuid.uuid4().hex[:8]}"


def add_reader(flow, person_id: int, secret: str) -> None:
    add_promise(flow, 1, "database_reader")
    connection = input_schema.DatabaseConnection(
        database_type="postgresql",
        username=pg_fixtures.POSTGRES_USER,
        password_ref=secret,
        host=pg_fixtures.POSTGRES_HOST,
        port=pg_fixtures.POSTGRES_PORT,
        database=pg_fixtures.POSTGRES_DB,
    )
    flow.add_database_reader(
        input_schema.NodeDatabaseReader(
            flow_id=flow.flow_id,
            node_id=1,
            user_id=person_id,
            database_settings=input_schema.DatabaseSettings(
                database_connection=connection, schema_name="public", table_name="movies"
            ),
        )
    )
    sealed_e2e.on(flow, 1, sealed_e2e.BACKEND)
    add_promise(flow, 2, "select")
    flow.add_select(
        input_schema.NodeSelect(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            select_input=[transform_schema.SelectInput(old_name="title")],
            keep_missing=False,
        )
    )
    connect(flow, 1, 2)


def test_a_database_read_on_the_compute_only_server_signs_in_with_the_password_the_hub_seals_to_it(
    hub, store, person, sent, open_flow, secret_name
):
    person_id, access_token = person
    hub.catalog.secret_values[secret_name] = PASSWORD
    flow = open_flow(person_id, "read_on_the_compute_server")
    add_reader(flow, person_id, secret_name)

    run_info = sealed_e2e.run(flow, access_token)

    assert (1, 2) in [tuple(boundary) for boundary in run_info.boundaries]
    assert flow.get_node(2).results.result_ref.backend_id == "local"
    assert flow.get_node(2).get_resulting_data().count() > 0
    [asked] = hub.catalog.vend_calls
    assert asked["secrets"] == [secret_name]
    assert set(asked["targets"]) == {sealed_e2e.SERVER, test_hub_sign_in.NODE}
    assert sent and not [
        route for route, body in sent if PASSWORD.encode() in body or b"$ffsec$" in body or sealed_e2e.SECRET in body
    ]
    assert any("/v1/ops/database_read" in route for route, _ in sent)
