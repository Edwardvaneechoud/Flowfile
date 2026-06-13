"""Unit tests for the auth/sharing.py authorization layer (no HTTP)."""

from types import SimpleNamespace

from flowfile_core.auth import sharing
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context


def _principal(user, username=None):
    return SimpleNamespace(id=user.id, is_admin=user.is_admin, username=username or user.username)


def test_sharing_disabled_in_electron(monkeypatch, users):
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    assert sharing.sharing_enabled() is False
    with get_db_context() as db:
        assert sharing.user_group_ids(db, users["alice"].id) == []
        assert sharing.granted_resource_ids(db, users["alice"].id, "secret") == set()
        assert sharing.expand_namespace_grants(db, users["alice"].id) == set()


def test_sharing_enabled_in_docker():
    assert sharing.sharing_enabled() is True


def test_group_membership_helpers(users, group_factory):
    gid = group_factory(
        "authz-team",
        users["admin"].id,
        {users["alice"].id: "owner", users["bob"].id: "member", users["carol"].id: "manager"},
    )
    with get_db_context() as db:
        assert sharing.user_group_ids(db, users["alice"].id) == [gid]
        assert sharing.user_group_ids(db, users["bob"].id) == [gid]
        assert sharing.manageable_group_ids(db, users["alice"].id) == [gid]
        assert sharing.manageable_group_ids(db, users["carol"].id) == [gid]
        assert sharing.manageable_group_ids(db, users["bob"].id) == []
        assert sharing.group_role(db, gid, users["alice"].id) == "owner"
        assert sharing.group_role(db, gid, users["bob"].id) == "member"
        assert sharing.group_role(db, gid, users["admin"].id) is None


def test_granted_resource_ids_use_vs_manage(users, group_factory, grant_factory, resource_factory):
    gid = group_factory("authz-grants", users["admin"].id, {users["bob"].id: "member"})
    secret_id = resource_factory(db_models.Secret, name="authz_s1", encrypted_value="x", iv="", user_id=users["alice"].id)
    conn_id = resource_factory(
        db_models.DatabaseConnection, connection_name="authz_c1", database_type="postgresql", user_id=users["alice"].id
    )
    grant_factory("secret", secret_id, gid, permission="use", granted_by=users["alice"].id)
    grant_factory("database_connection", conn_id, gid, permission="manage", granted_by=users["alice"].id)

    with get_db_context() as db:
        assert sharing.granted_resource_ids(db, users["bob"].id, "secret", "use") == {secret_id}
        assert sharing.granted_resource_ids(db, users["bob"].id, "secret", "manage") == set()
        # "use" matches both grant levels; "manage" only manage-level grants.
        assert sharing.granted_resource_ids(db, users["bob"].id, "database_connection", "use") == {conn_id}
        assert sharing.granted_resource_ids(db, users["bob"].id, "database_connection", "manage") == {conn_id}
        assert sharing.granted_resource_ids(db, users["carol"].id, "secret", "use") == set()


def test_namespace_inheritance(users, group_factory, grant_factory, resource_factory):
    gid = group_factory("authz-ns", users["admin"].id, {users["bob"].id: "member"})
    catalog_id = resource_factory(
        db_models.CatalogNamespace, name="authz_catalog", parent_id=None, level=0, owner_id=users["alice"].id
    )
    schema_id = resource_factory(
        db_models.CatalogNamespace, name="authz_schema", parent_id=catalog_id, level=1, owner_id=users["alice"].id
    )
    table_id = resource_factory(
        db_models.CatalogTable, name="authz_table", namespace_id=schema_id, owner_id=users["alice"].id
    )
    flow_id = resource_factory(
        db_models.FlowRegistration,
        name="authz_flow",
        flow_path="/tmp/authz_flow.flowfile",
        namespace_id=schema_id,
        owner_id=users["alice"].id,
    )
    grant_factory("catalog_namespace", catalog_id, gid, permission="use", granted_by=users["alice"].id)

    with get_db_context() as db:
        assert sharing.expand_namespace_grants(db, users["bob"].id) == {catalog_id, schema_id}
        assert table_id in sharing.granted_resource_ids(db, users["bob"].id, "catalog_table")
        assert flow_id in sharing.granted_resource_ids(db, users["bob"].id, "flow")
        # Inheritance carries the grant's permission level: use grant gives no manage.
        assert sharing.granted_resource_ids(db, users["bob"].id, "catalog_table", "manage") == set()
        details = sharing.granted_access_details(db, users["bob"].id, "catalog_table")
        assert details[table_id] == ("use", None)


def test_can_use_can_manage(users, group_factory, grant_factory, resource_factory):
    gid = group_factory("authz-access", users["admin"].id, {users["bob"].id: "member", users["carol"].id: "member"})
    use_secret = resource_factory(db_models.Secret, name="authz_s2", encrypted_value="x", iv="", user_id=users["alice"].id)
    manage_conn = resource_factory(
        db_models.DatabaseConnection, connection_name="authz_c2", database_type="postgresql", user_id=users["alice"].id
    )
    grant_factory("secret", use_secret, gid, permission="use", granted_by=users["alice"].id)
    grant_factory("database_connection", manage_conn, gid, permission="manage", granted_by=users["alice"].id)

    alice = _principal(users["alice"])
    bob = _principal(users["bob"])
    admin = _principal(users["admin"])
    with get_db_context() as db:
        assert sharing.can_use(db, alice, "secret", use_secret)
        assert sharing.can_manage(db, alice, "secret", use_secret)
        assert sharing.can_use(db, admin, "secret", use_secret)
        assert sharing.can_manage(db, admin, "secret", use_secret)
        assert sharing.can_use(db, bob, "secret", use_secret)
        assert not sharing.can_manage(db, bob, "secret", use_secret)
        assert sharing.can_use(db, bob, "database_connection", manage_conn)
        assert sharing.can_manage(db, bob, "database_connection", manage_conn)
        # Nonexistent resource: uniformly inaccessible.
        assert not sharing.can_use(db, bob, "secret", 999999999)
        assert not sharing.can_manage(db, alice, "secret", 999999999)


def test_synthetic_principal_gets_nothing(users, group_factory, grant_factory, resource_factory):
    gid = group_factory("authz-synth", users["admin"].id, {users["alice"].id: "member"})
    secret_id = resource_factory(db_models.Secret, name="authz_s3", encrypted_value="x", iv="", user_id=users["alice"].id)
    grant_factory("secret", secret_id, gid, permission="use", granted_by=users["alice"].id)

    # The synthetic internal-service user defaults to id=1 (a real user). Even when its
    # id collides with the resource owner, the username sentinel must deny owner-match.
    synthetic = SimpleNamespace(id=users["alice"].id, is_admin=False, username="_internal_service")
    with get_db_context() as db:
        assert sharing.is_synthetic_principal(synthetic)
        assert not sharing.can_use(db, synthetic, "secret", secret_id)
        assert not sharing.can_manage(db, synthetic, "secret", secret_id)


def test_accessible_filter(users, group_factory, grant_factory, resource_factory):
    gid = group_factory("authz-filter", users["admin"].id, {users["bob"].id: "member"})
    own = resource_factory(db_models.Secret, name="authz_own", encrypted_value="x", iv="", user_id=users["bob"].id)
    shared = resource_factory(db_models.Secret, name="authz_shared", encrypted_value="x", iv="", user_id=users["alice"].id)
    hidden = resource_factory(db_models.Secret, name="authz_hidden", encrypted_value="x", iv="", user_id=users["alice"].id)
    grant_factory("secret", shared, gid, permission="use", granted_by=users["alice"].id)

    with get_db_context() as db:
        criterion = sharing.accessible_filter(db, users["bob"].id, "secret")
        ids = {row.id for row in db.query(db_models.Secret).filter(criterion)}
    assert own in ids
    assert shared in ids
    assert hidden not in ids


def test_delete_helpers(users, group_factory, grant_factory, resource_factory):
    gid_a = group_factory("authz-del-a", users["admin"].id, {users["bob"].id: "member"})
    gid_b = group_factory("authz-del-b", users["admin"].id, {users["bob"].id: "owner", users["carol"].id: "member"})
    s1 = resource_factory(db_models.Secret, name="authz_d1", encrypted_value="x", iv="", user_id=users["alice"].id)
    s2 = resource_factory(db_models.Secret, name="authz_d2", encrypted_value="x", iv="", user_id=users["alice"].id)
    grant_factory("secret", s1, gid_a, granted_by=users["alice"].id)
    grant_factory("secret", s1, gid_b, granted_by=users["alice"].id)
    grant_factory("secret", s2, gid_a, granted_by=users["alice"].id)

    with get_db_context() as db:
        sharing.delete_grants_for_resource(db, "secret", s1)
        db.commit()
        assert sharing.granted_resource_ids(db, users["bob"].id, "secret") == {s2}

        sharing.delete_grants_for_group(db, gid_a)
        db.commit()
        assert sharing.granted_resource_ids(db, users["bob"].id, "secret") == set()

        sharing.delete_memberships_for_user(db, users["bob"].id)
        db.commit()
        assert sharing.user_group_ids(db, users["bob"].id) == []
        assert sharing.user_group_ids(db, users["carol"].id) == [gid_b]
