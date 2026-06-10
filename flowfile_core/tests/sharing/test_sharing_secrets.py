"""Secrets sharing: resolution semantics, format invariants, and no-leak guarantees."""

import pytest

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.secret_manager.secret_manager import decrypt_secret, get_encrypted_secret


@pytest.fixture
def team(users, group_factory):
    """alice (owner) shares with bob (member); carol is not in the group."""
    return group_factory("secrets-team", users["admin"].id, {users["alice"].id: "owner", users["bob"].id: "member"})


def _create_secret(client, name, value):
    response = client.post("/secrets/secrets", json={"name": name, "value": value})
    assert response.status_code == 200, response.text
    return response.json()


def _share_secret(client, secret_id, group_id):
    response = client.post(
        "/shares", json={"resource_type": "secret", "resource_id": secret_id, "group_id": group_id, "permission": "use"}
    )
    assert response.status_code == 200, response.text
    return response.json()


def test_grantee_resolves_shared_secret_owner_keyed(users, client_for, team):
    alice = client_for("alice")
    created = _create_secret(alice, "shared_db_password", "hunter2")
    _share_secret(alice, created["id"], team)

    ciphertext = get_encrypted_secret(current_user_id=users["bob"].id, secret_name="shared_db_password")
    assert ciphertext is not None
    # Ciphertext stays keyed to the OWNER: format pinned, never the caller's id.
    assert ciphertext.startswith(f"$ffsec$1${users['alice'].id}$")
    assert decrypt_secret(ciphertext).get_secret_value() == "hunter2"

    # Non-members resolve nothing.
    assert get_encrypted_secret(current_user_id=users["carol"].id, secret_name="shared_db_password") is None


def test_own_secret_shadows_shared(users, client_for, team):
    alice = client_for("alice")
    bob = client_for("bob")
    shared = _create_secret(alice, "collide_name", "alice-value")
    _share_secret(alice, shared["id"], team)
    _create_secret(bob, "collide_name", "bob-value")

    ciphertext = get_encrypted_secret(current_user_id=users["bob"].id, secret_name="collide_name")
    assert ciphertext.startswith(f"$ffsec$1${users['bob'].id}$")
    assert decrypt_secret(ciphertext).get_secret_value() == "bob-value"


def test_shared_collision_lowest_id_wins(users, client_for, team, group_factory):
    alice = client_for("alice")
    carol = client_for("carol")
    first = _create_secret(alice, "dup_shared", "first-value")
    second = _create_secret(carol, "dup_shared", "second-value")
    assert first["id"] < second["id"]
    _share_secret(alice, first["id"], team)
    carol_team = group_factory("secrets-carol-team", users["admin"].id, {users["carol"].id: "owner", users["bob"].id: "member"})
    _share_secret(carol, second["id"], carol_team)

    ciphertext = get_encrypted_secret(current_user_id=users["bob"].id, secret_name="dup_shared")
    assert decrypt_secret(ciphertext).get_secret_value() == "first-value"


def test_no_value_or_ciphertext_leaks_to_non_owner(users, client_for, team):
    alice = client_for("alice")
    bob = client_for("bob")
    created = _create_secret(alice, "leak_check", "topsecret")
    _share_secret(alice, created["id"], team)

    listed = bob.get("/secrets/secrets").json()
    shared_rows = [row for row in listed if row["name"] == "leak_check"]
    assert len(shared_rows) == 1
    row = shared_rows[0]
    assert row["value"] is None
    assert row["access"] == {"is_owner": False, "access_level": "use", "shared_by": "share_alice"}

    by_name = bob.get("/secrets/secrets/leak_check")
    assert by_name.status_code == 200
    assert by_name.json()["value"] is None
    assert "topsecret" not in by_name.text and "$ffsec$" not in by_name.text

    # Owner still sees the masked SecretStr, never plaintext or raw ciphertext on the wire.
    own = alice.get("/secrets/secrets/leak_check").json()
    assert own["value"] == "**********"
    assert own["access"]["is_owner"] is True


def test_revoked_grant_loses_access(users, client_for, team):
    alice = client_for("alice")
    created = _create_secret(alice, "revoke_me", "v")
    share = _share_secret(alice, created["id"], team)

    assert get_encrypted_secret(current_user_id=users["bob"].id, secret_name="revoke_me") is not None
    assert alice.delete(f"/shares/{share['id']}").status_code == 204
    assert get_encrypted_secret(current_user_id=users["bob"].id, secret_name="revoke_me") is None


def test_secret_delete_cleans_grants(users, client_for, team):
    alice = client_for("alice")
    created = _create_secret(alice, "delete_me", "v")
    _share_secret(alice, created["id"], team)

    assert alice.delete("/secrets/secrets/delete_me").status_code == 204
    with get_db_context() as db:
        grants = (
            db.query(db_models.ResourceGrant)
            .filter_by(resource_type="secret", resource_id=created["id"])
            .count()
        )
    assert grants == 0


def test_shared_secret_not_deletable_by_grantee(users, client_for, team):
    alice = client_for("alice")
    bob = client_for("bob")
    created = _create_secret(alice, "keep_me", "v")
    _share_secret(alice, created["id"], team)

    assert bob.delete("/secrets/secrets/keep_me").status_code == 404
    assert alice.get("/secrets/secrets/keep_me").status_code == 200
