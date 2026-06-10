"""HTTP tests for the /user-groups router (multi-user mode)."""

from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context


def _create_group(client, name, description=None):
    response = client.post("/user-groups", json={"name": name, "description": description})
    assert response.status_code == 200, response.text
    return response.json()


def test_group_creation_is_admin_only(users, client_for):
    response = client_for("alice").post("/user-groups", json={"name": "nope"})
    assert response.status_code == 403

    group = _create_group(client_for("admin"), "ug-team")
    assert group["name"] == "ug-team"
    assert group["my_role"] == "owner"
    assert group["member_count"] == 1


def test_duplicate_group_name_rejected(users, client_for):
    admin = client_for("admin")
    _create_group(admin, "ug-dup")
    response = admin.post("/user-groups", json={"name": "ug-dup"})
    assert response.status_code == 400


def test_list_groups_mine_vs_all(users, client_for):
    admin = client_for("admin")
    alice = client_for("alice")
    group = _create_group(admin, "ug-list")
    admin.post(f"/user-groups/{group['id']}/members", json={"user_id": users["alice"].id, "role": "member"})

    mine = alice.get("/user-groups").json()
    assert [g["name"] for g in mine] == ["ug-list"]
    assert mine[0]["my_role"] == "member"

    assert client_for("bob").get("/user-groups").json() == []
    assert alice.get("/user-groups", params={"all": True}).status_code == 403
    all_groups = admin.get("/user-groups", params={"all": True}).json()
    assert "ug-list" in [g["name"] for g in all_groups]


def test_group_detail_membership_gated(users, client_for):
    admin = client_for("admin")
    group = _create_group(admin, "ug-detail")
    admin.post(f"/user-groups/{group['id']}/members", json={"user_id": users["alice"].id, "role": "member"})

    detail = client_for("alice").get(f"/user-groups/{group['id']}").json()
    assert {m["username"] for m in detail["members"]} == {"share_admin", "share_alice"}

    # Non-members get the same 404 as a missing group (no membership probing).
    assert client_for("bob").get(f"/user-groups/{group['id']}").status_code == 404
    assert client_for("bob").get("/user-groups/999999").status_code == 404


def test_member_role_rules(users, client_for):
    admin = client_for("admin")
    group = _create_group(admin, "ug-roles")
    gid = group["id"]
    admin.post(f"/user-groups/{gid}/members", json={"user_id": users["alice"].id, "role": "owner"})
    admin.post(f"/user-groups/{gid}/members", json={"user_id": users["bob"].id, "role": "manager"})

    bob = client_for("bob")
    # Managers can add members but cannot mint owners.
    assert bob.post(f"/user-groups/{gid}/members", json={"user_id": users["carol"].id, "role": "member"}).status_code == 200
    bob.delete(f"/user-groups/{gid}/members/{users['carol'].id}")
    assert bob.post(f"/user-groups/{gid}/members", json={"user_id": users["carol"].id, "role": "owner"}).status_code == 403

    # Plain members cannot manage membership at all.
    admin.post(f"/user-groups/{gid}/members", json={"user_id": users["carol"].id, "role": "member"})
    carol = client_for("carol")
    assert carol.patch(f"/user-groups/{gid}/members/{users['bob'].id}", json={"role": "member"}).status_code == 403
    assert carol.delete(f"/user-groups/{gid}/members/{users['bob'].id}").status_code == 403
    # But anyone may leave a group themselves.
    assert carol.delete(f"/user-groups/{gid}/members/{users['carol'].id}").status_code == 204


def test_last_owner_guard(users, client_for):
    admin = client_for("admin")
    group = _create_group(admin, "ug-last-owner")
    gid = group["id"]

    assert admin.patch(f"/user-groups/{gid}/members/{users['admin'].id}", json={"role": "member"}).status_code == 400
    assert admin.delete(f"/user-groups/{gid}/members/{users['admin'].id}").status_code == 400

    admin.post(f"/user-groups/{gid}/members", json={"user_id": users["alice"].id, "role": "owner"})
    assert admin.patch(f"/user-groups/{gid}/members/{users['admin'].id}", json={"role": "member"}).status_code == 200


def test_group_update_owner_only(users, client_for):
    admin = client_for("admin")
    group = _create_group(admin, "ug-update")
    gid = group["id"]
    admin.post(f"/user-groups/{gid}/members", json={"user_id": users["alice"].id, "role": "manager"})

    assert client_for("alice").patch(f"/user-groups/{gid}", json={"description": "x"}).status_code == 403
    assert admin.patch(f"/user-groups/{gid}", json={"description": "updated"}).status_code == 200


def test_group_delete_cascades_grants_and_memberships(users, client_for, resource_factory, grant_factory):
    admin = client_for("admin")
    group = _create_group(admin, "ug-cascade")
    gid = group["id"]
    admin.post(f"/user-groups/{gid}/members", json={"user_id": users["alice"].id, "role": "member"})
    secret_id = resource_factory(db_models.Secret, name="ug_s", encrypted_value="x", iv="", user_id=users["bob"].id)
    grant_factory("secret", secret_id, gid, granted_by=users["bob"].id)

    assert admin.delete(f"/user-groups/{gid}").status_code == 204
    with get_db_context() as db:
        assert db.query(db_models.ResourceGrant).filter_by(group_id=gid).count() == 0
        assert db.query(db_models.UserGroupMembership).filter_by(group_id=gid).count() == 0


def test_electron_mode_returns_404(monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    with TestClient(main.app) as client:
        token = client.post("/auth/token").json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}
        assert client.get("/user-groups", headers=headers).status_code == 404
        assert client.post("/user-groups", json={"name": "x"}, headers=headers).status_code == 404
        assert client.get("/shares", params={"resource_type": "secret", "resource_id": 1}, headers=headers).status_code == 404
