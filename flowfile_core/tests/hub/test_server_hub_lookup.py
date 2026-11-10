"""On a server joined to its hub with keys of its own a lookup by name finds this server's own secret, and a secret
only the hub holds has no value here.

The setup is ``test_server_hub_secrets``'s. Core asks the hub no vend for a lookup and opens nothing the hub sealed: a
secret in this server's database is found as its ciphertext and wins over the hub's of that name; a secret only the
hub holds is no ciphertext here, a step that names it outside a run is refused with one sentence, and a database
reader's schema callback gets no columns with it.
"""

import uuid

import pytest
from pydantic import SecretStr

from flowfile_core.auth.models import SecretInput
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.execution import run_secrets
from flowfile_core.hub_client import vend
from flowfile_core.schemas import input_schema
from flowfile_core.secret_manager.secret_manager import decrypt_secret, get_encrypted_secret, store_secret
from shared import hub_sealed
from tests import test_hub_sign_in
from tests.flowfile.conftest import create_test_graph
from tests.flowfile.execution.graph_helpers import add_promise
from tests.hub import test_server_hub_secrets as page
from tests.hub.test_server_run_report import UID

node_file = test_hub_sign_in.node_file
username = test_hub_sign_in.username
hub = page.hub
person = page.person
name = page.name
rows_named = page.rows_named


def refusal(secret_name: str) -> str:
    return f"{secret_name} is a hub secret, which only a hub server opens: run this step on a hub server"


def test_a_lookup_of_a_secret_only_the_hub_holds_finds_no_value_here_and_asks_the_hub_no_vend(hub, person, name):
    person_id, _ = person
    hub.keep(UID, name, "from-the-hub")

    assert get_encrypted_secret(current_user_id=person_id, secret_name=name) is None
    assert run_secrets.is_hub_secret(person_id, name)
    with pytest.raises(ValueError, match=refusal(name)):
        run_secrets.encrypted_secret(person_id, name)
    assert hub.catalog.vend_calls == [] and rows_named(name) == 0


def test_a_secret_in_this_servers_database_is_found_by_a_lookup_and_wins_over_the_hubs_of_that_name(hub, person, name):
    person_id, _ = person
    hub.keep(UID, name, "from-the-hub")
    with get_db_context() as db:
        store_secret(db, SecretInput(name=name, value=SecretStr("only-here")), person_id)

    encrypted = run_secrets.encrypted_secret(person_id, name)

    assert encrypted == get_encrypted_secret(current_user_id=person_id, secret_name=name)
    assert (
        encrypted.startswith(f"$ffsec$1${person_id}$") and decrypt_secret(encrypted).get_secret_value() == "only-here"
    )
    assert not run_secrets.is_hub_secret(person_id, name)
    assert hub.catalog.vend_calls == []


def test_core_opens_no_secret_the_hub_sealed_to_this_server(hub, person, name):
    person_id, _ = person
    hub.keep(UID, name, "from-the-hub")

    sealed = vend.request(person_id, "run-1", set(), set(), targets=[test_hub_sign_in.NODE], secrets=[name])
    value = sealed.sealed_secrets([name])[name]

    assert hub_sealed.is_sealed_secret(value) and "from-the-hub" not in value
    with pytest.raises(ValueError, match="A hub secret is opened only by the hub server that runs the step"):
        decrypt_secret(value)


def test_a_database_reader_whose_password_only_the_hub_holds_gives_core_no_columns_and_asks_no_vend(hub, person, name):
    person_id, _ = person
    hub.keep(UID, name, "from-the-hub")
    flow = create_test_graph(flow_id=uuid.uuid4().int % 100_000, execution_location="local")
    add_promise(flow, 1, "database_reader")
    connection = input_schema.DatabaseConnection(
        database_type="postgresql", username="ada", password_ref=name, host="localhost", port=5433, database="db"
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

    node = flow.get_node(1)

    with pytest.raises(ValueError, match=refusal(name)):
        node.user_provided_schema_callback()
    assert node.schema_callback() == [] and node.node_settings.setup_errors
    assert hub.catalog.vend_calls == [] and rows_named(name) == 0
