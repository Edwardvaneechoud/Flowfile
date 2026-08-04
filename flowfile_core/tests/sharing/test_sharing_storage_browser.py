"""Authorization for /storage_browser/cloud in multi-user (docker) mode.

Browsing is a `use`-level operation: whoever can read data through a connection can
also enumerate it. What must hold is that a non-grantee cannot tell a connection
they lack access to apart from one that does not exist.
"""

import pytest

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.routes import storage_browser
from shared.cloud_storage.browse import BrowseResult

BROWSE_URL = "/storage_browser/cloud"


@pytest.fixture
def team(users, group_factory):
    """alice (owner) shares with bob (member); carol is not in the group."""
    return group_factory("browse-team", users["admin"].id, {users["alice"].id: "owner", users["bob"].id: "member"})


@pytest.fixture
def stub_listing(monkeypatch):
    """Stub the provider call so only authorization is under test."""
    calls = []

    def _fake(storage_type, uri, storage_options, **kwargs):
        calls.append({"storage_type": storage_type, "uri": uri, "storage_options": storage_options})
        return BrowseResult(path=uri or "s3://")

    monkeypatch.setattr(storage_browser, "list_cloud_uri", _fake)
    return calls


def _share(client, resource_type, resource_id, group_id, permission="use"):
    response = client.post(
        "/shares",
        json={
            "resource_type": resource_type,
            "resource_id": resource_id,
            "group_id": group_id,
            "permission": permission,
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


def _cloud_body(name, secret="s3secret"):
    return {
        "connection_name": name,
        "storage_type": "s3",
        "auth_method": "access_key",
        "aws_region": "eu-west-1",
        "aws_access_key_id": "AKIAEXAMPLE",
        "aws_secret_access_key": secret,
        "endpoint_url": "https://minio.internal",
        "verify_ssl": True,
    }


@pytest.fixture
def alice_connection(users, client_for):
    alice = client_for("alice")
    assert alice.post("/cloud_connections/cloud_connection", json=_cloud_body("browse_s3")).status_code == 200
    with get_db_context() as db:
        row = (
            db.query(db_models.CloudStorageConnection)
            .filter_by(connection_name="browse_s3", user_id=users["alice"].id)
            .first()
        )
        return row.id


def test_owner_can_browse_their_own_connection(client_for, alice_connection, stub_listing):
    response = client_for("alice").get(BROWSE_URL, params={"connection_name": "browse_s3"})
    assert response.status_code == 200
    assert len(stub_listing) == 1


def test_use_grantee_can_browse_a_shared_connection(client_for, alice_connection, team, stub_listing):
    _share(client_for("alice"), "cloud_connection", alice_connection, team, permission="use")
    response = client_for("bob").get(BROWSE_URL, params={"connection_name": "browse_s3"})
    assert response.status_code == 200


def test_grantee_browses_with_the_owners_credentials(client_for, alice_connection, team, stub_listing):
    # Secrets stay encrypted under the owner, so a grantee reaches exactly what the
    # connection reaches — no more, and nothing they could not already read.
    _share(client_for("alice"), "cloud_connection", alice_connection, team, permission="use")
    client_for("bob").get(BROWSE_URL, params={"connection_name": "browse_s3"})
    assert stub_listing[-1]["storage_options"]["aws_access_key_id"] == "AKIAEXAMPLE"


def test_non_grantee_gets_404_not_403(client_for, alice_connection, stub_listing):
    # 403 would confirm the connection exists — the same no-oracle rule the
    # connection mutation routes follow.
    response = client_for("carol").get(BROWSE_URL, params={"connection_name": "browse_s3"})
    assert response.status_code == 404
    assert response.json()["detail"]["error_code"] == "CONNECTION_NOT_FOUND"
    assert stub_listing == []


def test_unknown_connection_is_indistinguishable_from_a_forbidden_one(client_for, alice_connection):
    carol = client_for("carol")
    forbidden = carol.get(BROWSE_URL, params={"connection_name": "browse_s3"})
    missing = carol.get(BROWSE_URL, params={"connection_name": "no_such_connection"})
    assert forbidden.status_code == missing.status_code == 404
    assert forbidden.json() == missing.json()


def test_ambient_credentials_are_refused_in_multi_user_mode(client_for, users, stub_listing):
    # Ambient credentials belong to the server process, so any authenticated user
    # could otherwise enumerate every bucket the deployment can reach.
    response = client_for("bob").get(BROWSE_URL)
    assert response.status_code == 400
    assert response.json()["detail"]["error_code"] == "CONNECTION_REQUIRED"
    assert stub_listing == []


def test_revoking_the_grant_removes_browse_access(client_for, alice_connection, team, stub_listing):
    alice = client_for("alice")
    share = _share(alice, "cloud_connection", alice_connection, team, permission="use")
    bob = client_for("bob")
    assert bob.get(BROWSE_URL, params={"connection_name": "browse_s3"}).status_code == 200

    assert alice.delete(f"/shares/{share['id']}").status_code in (200, 204)
    assert bob.get(BROWSE_URL, params={"connection_name": "browse_s3"}).status_code == 404
