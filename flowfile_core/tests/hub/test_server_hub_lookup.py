"""On a server joined to its hub with keys of its own a lookup by name asks the hub, wherever the secret is used.

The setup is ``test_server_hub_secrets``'s. A lookup asks the hub's vend for the person's secret sealed to this server
itself, and this server opens it with its own key: a database reader's schema callback signs in with a password only
the hub holds, and a secret only in this server's database is not found.
"""

import uuid

import pytest
from pydantic import SecretStr

from flowfile_core.auth.models import SecretInput
from flowfile_core.database.connection import get_db_context
from flowfile_core.schemas import input_schema
from flowfile_core.secret_manager.secret_manager import (
    decrypt_secret,
    get_encrypted_secret,
    store_secret,
    stored_encrypted_secret,
)
from shared import hub_sealed
from test_utils.postgres import fixtures as pg_fixtures
from tests import test_hub_sign_in
from tests.flowfile.conftest import create_test_graph
from tests.flowfile.execution.graph_helpers import add_promise
from tests.hub import test_server_hub_secrets as page

node_file = test_hub_sign_in.node_file
username = test_hub_sign_in.username
hub = page.hub
person = page.person
name = page.name
rows_named = page.rows_named


def test_a_lookup_asks_the_hub_for_the_secret_sealed_to_this_server_which_opens_it(hub, person, name):
    person_id, _ = person
    hub.values[name] = "from-the-hub"

    encrypted = get_encrypted_secret(current_user_id=person_id, secret_name=name)

    assert hub_sealed.is_sealed_secret(encrypted) and "from-the-hub" not in encrypted
    assert decrypt_secret(encrypted).get_secret_value() == "from-the-hub"
    [asked] = hub.catalog.vend_calls
    assert (asked["targets"], asked["secrets"]) == ([test_hub_sign_in.NODE], [name])
    assert asked["run_id"].startswith("core-lookup-") and not asked["read_namespace_ids"]


def test_a_secret_only_in_this_servers_database_is_not_found_by_a_lookup(hub, person, name):
    person_id, _ = person
    with get_db_context() as db:
        store_secret(db, SecretInput(name=name, value=SecretStr("only-here")), person_id)

    assert get_encrypted_secret(current_user_id=person_id, secret_name=name) is None
    assert stored_encrypted_secret(current_user_id=person_id, secret_name=name) is not None


@pytest.fixture
def postgres():
    if not pg_fixtures.can_connect_to_db():
        pytest.skip("the test PostgreSQL database is not running")


def test_a_database_readers_schema_callback_signs_in_with_the_secret_the_hub_holds(
    hub, person, name, postgres, tmp_path
):
    person_id, _ = person
    hub.values[name] = pg_fixtures.POSTGRES_PASSWORD
    flow = create_test_graph(flow_id=uuid.uuid4().int % 100_000, execution_location="local")
    add_promise(flow, 1, "database_reader")
    connection = input_schema.DatabaseConnection(
        database_type="postgresql",
        username=pg_fixtures.POSTGRES_USER,
        password_ref=name,
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

    columns = flow.get_node(1).schema_callback()

    assert "title" in [column.column_name for column in columns]
    assert rows_named(name) == 0
    assert any(asked["secrets"] == [name] for asked in hub.catalog.vend_calls)
