"""On a server that joined its hub with keys of its own, the hub connection says who signed in and through which hub.

``GET /hub_client/connection`` answers a person signed in through the hub as connected to it, with their full name,
else their username, as ``account``: what the editor's header shows. It says the server is ``joined``, names the hub
for a person without a hub session too, and carries the organisation's storage root, read from the hub's ``main`` on
``<root>/catalogs/main``, for the cloud connections page. The hub is ``OrganisationHub`` (``test_server_run_report``),
which answers no ``/hub/info``, so the hub is named by its URL.
"""

from types import SimpleNamespace

from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.routes import hub_client
from tests import test_hub_sign_in
from tests.hub import test_server_run_report
from tests.test_hub_sign_in import NODE, bearer

node_file = test_hub_sign_in.node_file
username = test_hub_sign_in.username
hub = test_server_run_report.hub
sign_in = test_server_run_report.sign_in
clean_catalog = test_server_run_report.clean_catalog

client = TestClient(main.app)


def test_the_hub_connection_says_who_is_signed_in_through_which_hub(hub, username):
    token = hub.sign_in_token(NODE, uid=test_server_run_report.UID, username=username, full_name="Ana Lima")
    signed_in = client.post("/auth/hub/signin", json={"token": token})
    assert signed_in.status_code == 200, signed_in.text

    body = client.get("/hub_client/connection", headers=bearer(signed_in.json()["access_token"])).json()

    assert (body["connected"], body["hub_url"], body["account"]) == (True, hub.url, "Ana Lima")


def test_the_hub_connection_names_a_person_without_a_full_name_by_their_username(hub, username):
    _, access_token = sign_in(hub, username)

    body = client.get("/hub_client/connection", headers=bearer(access_token)).json()

    assert (body["connected"], body["account"]) == (True, username)


def test_a_joined_server_says_it_belongs_to_its_hub_with_the_organisations_storage(hub, username):
    hub.catalog.seed_catalog("main", storage_uri="s3://acme/flowfile/catalogs/main")
    _, access_token = sign_in(hub, username)

    body = client.get("/hub_client/connection", headers=bearer(access_token)).json()

    assert (body["joined"], body["hub_url"], body["organisation_storage"]) == (True, hub.url, "s3://acme/flowfile")


def test_a_main_off_the_organisations_storage_names_no_storage(hub, username):
    hub.catalog.seed_catalog("main", storage_uri="s3://acme/main")
    hub.catalog.seed_catalog("scratch", storage_uri="s3://acme/flowfile/catalogs/main")
    _, access_token = sign_in(hub, username)

    body = client.get("/hub_client/connection", headers=bearer(access_token)).json()

    assert (body["joined"], body["organisation_storage"]) == (True, None)


def test_a_person_without_a_hub_session_still_sees_which_hub_the_server_belongs_to(hub):
    nobody = SimpleNamespace(id=987654, username="nobody", full_name=None)

    answer = hub_client.get_connection(current_user=nobody)

    assert (answer.connected, answer.joined, answer.hub_url, answer.organisation_storage) == (False, True, hub.url, None)
    assert hub.received == []
