"""On a server joined to its hub with keys of its own, the same step finds the same secret on the hub wherever it runs.

The setup is ``test_server_sealed_secrets_e2e``'s (this server ``team-1``, a separately joined compute-only server
``worker``, the organisation's exchange store on the suite's MinIO, the suite's Postgres), with the hub that keeps the
servers' secrets (``tests/hub/hub_secrets.SecretsHub``). A database reader whose password only the hub holds runs on
the compute-only server, then on this server's own worker: both read the table with the password the hub sealed to the
server that ran the step, and this server's database never holds it.
"""

from collections.abc import Iterator

import pytest
from cryptography.fernet import Fernet

from shared.tests.hub_jwks import HubSigner
from tests import test_hub_sign_in
from tests.hub import test_server_sealed_e2e as sealed_e2e
from tests.hub import test_server_sealed_secrets_e2e as secrets_e2e
from tests.hub.hub_secrets import SecretsHub
from tests.hub.test_server_hub_secrets import drop_accounts, rows_named
from tests.hub.test_server_run_report import open_flow  # noqa: F401
from tests.test_hub_sign_in import join

node_file = test_hub_sign_in.node_file
username = test_hub_sign_in.username
compute_server = sealed_e2e.compute_server
store = sealed_e2e.store
person = sealed_e2e.person
clean_catalog = sealed_e2e.clean_catalog
postgres = secrets_e2e.postgres
secret_name = secrets_e2e.secret_name

pytestmark = [*sealed_e2e.pytestmark, pytest.mark.usefixtures("postgres")]


@pytest.fixture
def hub(node_file, monkeypatch) -> Iterator[SecretsHub]:
    monkeypatch.setenv("FLOWFILE_MASTER_KEY", Fernet.generate_key().decode())
    signer = HubSigner()
    fake = SecretsHub(signer)
    join(fake)
    try:
        yield fake
    finally:
        fake.close()
        signer.close()
        drop_accounts(fake.issuer)


def test_the_same_reader_finds_the_same_secret_on_the_compute_server_and_on_this_servers_worker(
    hub, store, person, open_flow, secret_name
):
    person_id, access_token = person
    hub.values[secret_name] = secrets_e2e.PASSWORD
    flow = open_flow(person_id, "one_secret_two_servers")
    secrets_e2e.add_reader(flow, person_id, secret_name)

    sealed_e2e.run(flow, access_token)
    on_the_compute_server = flow.get_node(2).get_resulting_data().count()
    sealed_e2e.on(flow, 1, "local")
    sealed_e2e.run(flow, access_token)
    on_this_server = flow.get_node(2).get_resulting_data().count()

    assert on_the_compute_server == on_this_server > 0
    assert flow.get_node(1).results.result_ref.backend_id == "local"
    asked = [(set(body["targets"]), body["secrets"]) for body in hub.catalog.vend_calls]
    assert ({sealed_e2e.SERVER, test_hub_sign_in.NODE}, [secret_name]) in asked
    assert ({test_hub_sign_in.NODE}, [secret_name]) in asked
    assert rows_named(secret_name) == 0
