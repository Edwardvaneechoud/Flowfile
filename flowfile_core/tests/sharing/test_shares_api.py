"""HTTP tests for the generic /shares router: re-share policy, oracle uniformity, cleanup."""

import pytest

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context


@pytest.fixture
def team(users, client_for, group_factory):
    """A group containing alice (owner), bob and carol (members); admin not a member."""
    gid = group_factory(
        "shares-team",
        users["admin"].id,
        {users["alice"].id: "owner", users["bob"].id: "member", users["carol"].id: "member"},
    )
    return gid


@pytest.fixture
def other_group(users, group_factory):
    """A group bob does NOT belong to."""
    return group_factory("shares-other", users["admin"].id, {users["carol"].id: "owner"})


def _make_secret(resource_factory, owner_id, name="shares_secret"):
    return resource_factory(db_models.Secret, name=name, encrypted_value="x", iv="", user_id=owner_id)


def _make_connection(resource_factory, owner_id, name="shares_conn"):
    return resource_factory(
        db_models.DatabaseConnection, connection_name=name, database_type="postgresql", user_id=owner_id
    )


def test_owner_shares_and_lists(users, client_for, resource_factory, team):
    alice = client_for("alice")
    secret_id = _make_secret(resource_factory, users["alice"].id)
    response = alice.post(
        "/shares",
        json={"resource_type": "secret", "resource_id": secret_id, "group_id": team, "permission": "use"},
    )
    assert response.status_code == 200, response.text
    share = response.json()
    assert share["group_name"] == "shares-team"
    assert share["granted_by_username"] == "share_alice"

    listed = alice.get("/shares", params={"resource_type": "secret", "resource_id": secret_id}).json()
    assert [s["id"] for s in listed] == [share["id"]]


def test_secret_manage_grant_rejected(users, client_for, resource_factory, team):
    alice = client_for("alice")
    secret_id = _make_secret(resource_factory, users["alice"].id, name="shares_secret_manage")
    response = alice.post(
        "/shares",
        json={"resource_type": "secret", "resource_id": secret_id, "group_id": team, "permission": "manage"},
    )
    assert response.status_code == 422


def test_uniform_404_no_enumeration_oracle(users, client_for, resource_factory, team):
    bob = client_for("bob")
    real_but_foreign = _make_secret(resource_factory, users["alice"].id, name="shares_foreign")

    missing = bob.get("/shares", params={"resource_type": "secret", "resource_id": 999999999})
    foreign = bob.get("/shares", params={"resource_type": "secret", "resource_id": real_but_foreign})
    assert missing.status_code == foreign.status_code == 404
    assert missing.json() == foreign.json()

    missing_post = bob.post(
        "/shares", json={"resource_type": "secret", "resource_id": 999999999, "group_id": team, "permission": "use"}
    )
    foreign_post = bob.post(
        "/shares",
        json={"resource_type": "secret", "resource_id": real_but_foreign, "group_id": team, "permission": "use"},
    )
    assert missing_post.status_code == foreign_post.status_code == 404
    assert missing_post.json() == foreign_post.json()


def test_reshare_policy_for_manage_grantees(users, client_for, resource_factory, team, other_group):
    alice = client_for("alice")
    bob = client_for("bob")
    conn_id = _make_connection(resource_factory, users["alice"].id)

    # Owner mints a manage grant to the team.
    response = alice.post(
        "/shares",
        json={"resource_type": "database_connection", "resource_id": conn_id, "group_id": team, "permission": "manage"},
    )
    assert response.status_code == 200

    # Manage-grantee bob: may re-share at use level to a group he belongs to...
    with get_db_context() as db:
        bob_group = db_models.UserGroup(name="shares-bobs-group", created_by=users["admin"].id)
        db.add(bob_group)
        db.commit()
        db.refresh(bob_group)
        db.add(db_models.UserGroupMembership(group_id=bob_group.id, user_id=users["bob"].id, role="owner"))
        db.commit()
        bob_gid = bob_group.id
    ok = bob.post(
        "/shares",
        json={"resource_type": "database_connection", "resource_id": conn_id, "group_id": bob_gid, "permission": "use"},
    )
    assert ok.status_code == 200

    # ...but not mint manage grants, and not share to groups he is not in.
    manage_attempt = bob.post(
        "/shares",
        json={
            "resource_type": "database_connection",
            "resource_id": conn_id,
            "group_id": bob_gid,
            "permission": "manage",
        },
    )
    assert manage_attempt.status_code == 403
    foreign_group_attempt = bob.post(
        "/shares",
        json={
            "resource_type": "database_connection",
            "resource_id": conn_id,
            "group_id": other_group,
            "permission": "use",
        },
    )
    assert foreign_group_attempt.status_code == 403

    # Carol only has use via the team grant -> cannot touch /shares for it at all.
    carol_attempt = client_for("carol").get(
        "/shares", params={"resource_type": "database_connection", "resource_id": conn_id}
    )
    # Carol has manage? No: the team grant is manage-level, carol is a member, so she CAN manage.
    assert carol_attempt.status_code == 200


def test_use_grantee_cannot_touch_shares(users, client_for, resource_factory, team):
    alice = client_for("alice")
    bob = client_for("bob")
    conn_id = _make_connection(resource_factory, users["alice"].id, name="shares_conn_use")
    alice.post(
        "/shares",
        json={"resource_type": "database_connection", "resource_id": conn_id, "group_id": team, "permission": "use"},
    )
    assert bob.get("/shares", params={"resource_type": "database_connection", "resource_id": conn_id}).status_code == 404
    assert (
        bob.post(
            "/shares",
            json={"resource_type": "database_connection", "resource_id": conn_id, "group_id": team, "permission": "use"},
        ).status_code
        == 404
    )


def test_duplicate_share_conflicts(users, client_for, resource_factory, team):
    alice = client_for("alice")
    secret_id = _make_secret(resource_factory, users["alice"].id, name="shares_dup")
    body = {"resource_type": "secret", "resource_id": secret_id, "group_id": team, "permission": "use"}
    assert alice.post("/shares", json=body).status_code == 200
    assert alice.post("/shares", json=body).status_code == 409


def test_update_share_permission_rules(users, client_for, resource_factory, team):
    alice = client_for("alice")
    bob = client_for("bob")
    conn_id = _make_connection(resource_factory, users["alice"].id, name="shares_conn_patch")
    share = alice.post(
        "/shares",
        json={"resource_type": "database_connection", "resource_id": conn_id, "group_id": team, "permission": "manage"},
    ).json()

    # Manage-grantee may downgrade to use, but not (re-)grant manage.
    assert bob.patch(f"/shares/{share['id']}", json={"permission": "use"}).status_code == 200
    assert bob.patch(f"/shares/{share['id']}", json={"permission": "manage"}).status_code == 404
    # (bob lost manage with the downgrade -> uniform 404, not 403: no oracle.)

    # Owner can re-upgrade.
    assert alice.patch(f"/shares/{share['id']}", json={"permission": "manage"}).status_code == 200
    assert bob.patch(f"/shares/{share['id']}", json={"permission": "manage"}).status_code == 403


def test_delete_share_revokes_access(users, client_for, resource_factory, team):
    alice = client_for("alice")
    secret_id = _make_secret(resource_factory, users["alice"].id, name="shares_revoke")
    share = alice.post(
        "/shares", json={"resource_type": "secret", "resource_id": secret_id, "group_id": team, "permission": "use"}
    ).json()
    assert alice.delete(f"/shares/{share['id']}").status_code == 204
    with get_db_context() as db:
        assert db.get(db_models.ResourceGrant, share["id"]) is None
    assert alice.delete(f"/shares/{share['id']}").status_code == 404


def test_admin_can_manage_any_share(users, client_for, resource_factory, team):
    admin = client_for("admin")
    secret_id = _make_secret(resource_factory, users["alice"].id, name="shares_admin")
    response = admin.post(
        "/shares", json={"resource_type": "secret", "resource_id": secret_id, "group_id": team, "permission": "use"}
    )
    assert response.status_code == 200
    assert admin.get("/shares", params={"resource_type": "secret", "resource_id": secret_id}).status_code == 200
