"""On a server joined to its hub with keys of its own, a reader whose password only the hub holds reads on a server of
the hub wherever its step was set, or is refused before the run.

The setup is ``test_server_sealed_secrets_e2e``'s (this server ``team-1``, a separately joined compute-only server
``worker``, the organisation's exchange store on the suite's MinIO, the suite's Postgres), with the hub that keeps the
person's secrets (``tests/hub/hub_secrets.SecretsHub``) and lists them its servers. Only a hub server's worker opens
what the hub seals: a database reader set to this server's own ``local`` is moved to the hub's server by itself and
reads the table there with the password the hub sealed to that server, and with no server of the hub to run on it is
refused before a node starts with one sentence. This server's database never holds the password.
"""

from collections.abc import Iterator

import pytest
from cryptography.fernet import Fernet

from shared.tests.hub_jwks import HubSigner
from tests import test_hub_sign_in
from tests.hub import test_server_run_report as run_report
from tests.hub import test_server_sealed_e2e as sealed_e2e
from tests.hub import test_server_sealed_secrets_e2e as secrets_e2e
from tests.hub.hub_secrets import SecretsHub
from tests.hub.test_server_hub_secrets import drop_accounts, rows_named
from tests.hub_client.conftest import compute_node
from tests.test_hub_sign_in import bearer, join

node_file = test_hub_sign_in.node_file
username = test_hub_sign_in.username
compute_server = sealed_e2e.compute_server
store = sealed_e2e.store
person = sealed_e2e.person
clean_catalog = sealed_e2e.clean_catalog
postgres = secrets_e2e.postgres
secret_name = secrets_e2e.secret_name
open_flow = run_report.open_flow
client = run_report.client
UID = run_report.UID

pytestmark = [*sealed_e2e.pytestmark, pytest.mark.usefixtures("postgres")]


class ListingHub(SecretsHub):
    """``SecretsHub`` that answers ``GET /hub/compute-nodes`` with ``nodes`` for an access token it issued."""

    def __init__(self, signer):
        super().__init__(signer)
        self.nodes: list[dict] = []
        hub = self

        class Handler(self._server.RequestHandlerClass):
            def do_GET(self):
                if self.path != "/hub/compute-nodes":
                    super().do_GET()
                    return
                _, _, token = (self.headers.get("Authorization") or "").partition(" ")
                self._answer(200, hub.nodes) if token in hub.issued else self._answer(401, {"detail": "Not signed in"})

        self._server.RequestHandlerClass = Handler


@pytest.fixture
def hub(node_file, monkeypatch) -> Iterator[ListingHub]:
    monkeypatch.setenv("FLOWFILE_MASTER_KEY", Fernet.generate_key().decode())
    signer = HubSigner()
    fake = ListingHub(signer)
    join(fake)
    try:
        yield fake
    finally:
        fake.close()
        signer.close()
        drop_accounts(fake.issuer)


def reader_set_to_this_server(hub, open_one, person_id: int, secret_name: str):
    """A flow whose database reader, its password the hub's secret ``secret_name``, is set to this server's
    ``local``."""
    hub.keep(UID, secret_name, secrets_e2e.PASSWORD)
    flow = open_one(person_id, "one_secret_set_to_this_server")
    secrets_e2e.add_reader(flow, person_id, secret_name)
    sealed_e2e.on(flow, 1, "local")
    return flow


def test_a_reader_set_to_this_server_whose_password_only_the_hub_holds_moves_to_the_hubs_server_and_reads_there(
    hub, store, person, compute_server, open_flow, secret_name
):
    person_id, access_token = person
    hub.nodes = [compute_node(hub._node_ids[sealed_e2e.SERVER], sealed_e2e.SERVER, worker_url=compute_server)]
    flow = reader_set_to_this_server(hub, open_flow, person_id, secret_name)

    run_info = sealed_e2e.run(flow, access_token)

    assert {result.node_id: result.backend_id for result in run_info.node_step_result}[1] == sealed_e2e.BACKEND
    assert (1, 2) in [tuple(boundary) for boundary in run_info.boundaries]
    assert flow.get_node(2).get_resulting_data().count() > 0
    [asked] = hub.catalog.vend_calls
    assert asked["secrets"] == [secret_name]
    assert set(asked["targets"]) == {sealed_e2e.SERVER, test_hub_sign_in.NODE}
    assert rows_named(secret_name) == 0


def test_with_no_server_of_the_hub_to_run_on_the_reader_is_refused_before_a_node_starts_with_one_sentence(
    hub, store, person, open_flow, secret_name
):
    person_id, access_token = person
    flow = reader_set_to_this_server(hub, open_flow, person_id, secret_name)

    ran = client.post("/flow/run/", params={"flow_id": flow.flow_id}, headers=bearer(access_token))

    assert ran.status_code == 200, ran.text
    errors = {result.node_id: result.error for result in flow.get_run_info().node_step_result if not result.success}
    sentence = (
        f"{secret_name} is a hub secret, which only a hub server opens, and you may run on none of the hub's servers"
    )
    assert errors[1] == sentence
    assert hub.catalog.vend_calls == [] and rows_named(secret_name) == 0
