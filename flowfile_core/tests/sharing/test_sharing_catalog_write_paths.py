"""Reader-only (use-grant) principals must not write into catalog namespaces
through the sibling save paths: editor flow-saves (C1), destructive table
maintenance (C2), the catalog-writer node (C3, save-time route gate + run-time
``_authorize_catalog_write``), SQL save-as-flow (C4), Kafka sync creation (C5),
and the ghost-registration name reclaim (C6).
"""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from flowfile_core import flow_file_handler
from flowfile_core.catalog.exceptions import NotAuthorizedError
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.catalog_helpers import register_flow_in_namespace
from flowfile_core.flowfile.flow_graph import _authorize_catalog_write
from shared.storage_config import storage

_TMP_FLOW_DIR = Path.cwd() / "flowfile_core/tests/support_files/flows/tmp"


@pytest.fixture
def alice_ns(users, resource_factory):
    """A private catalog + schema owned by alice."""
    catalog_id = resource_factory(
        db_models.CatalogNamespace, name="WritePrivate", parent_id=None, level=0, owner_id=users["alice"].id
    )
    schema_id = resource_factory(
        db_models.CatalogNamespace, name="write_schema", parent_id=catalog_id, level=1, owner_id=users["alice"].id
    )
    return {"catalog": catalog_id, "schema": schema_id}


@pytest.fixture
def team(users, group_factory):
    return group_factory("write-team", users["admin"].id, {users["alice"].id: "owner", users["bob"].id: "member"})


@pytest.fixture
def registration_cleanup():
    """Remove FlowRegistration rows created during the test."""
    with get_db_context() as db:
        before = {r[0] for r in db.query(db_models.FlowRegistration.id)}
    yield
    with get_db_context() as db:
        for row in db.query(db_models.FlowRegistration).all():
            if row.id not in before:
                db.delete(row)
        db.commit()


@pytest.fixture
def flow_session_cleanup():
    """Close in-memory flow sessions created during the test."""
    before = {f.flow_id for f in flow_file_handler.flowfile_flows}
    yield
    for f in list(flow_file_handler.flowfile_flows):
        if f.flow_id not in before:
            flow_file_handler.delete_flow(f.flow_id)


@pytest.fixture
def tmp_flow_path():
    """Unique flow path under the cwd sandbox; unlinked on teardown."""
    _TMP_FLOW_DIR.mkdir(parents=True, exist_ok=True)
    created: list[Path] = []

    def _make(stem: str) -> str:
        path = _TMP_FLOW_DIR / f"{stem}.yaml"
        path.unlink(missing_ok=True)
        created.append(path)
        return str(path)

    yield _make
    for path in created:
        path.unlink(missing_ok=True)


def _own_flow(client, tmp_flow_path, stem: str) -> int:
    resp = client.post("/editor/create_flow/", params={"flow_path": tmp_flow_path(stem)})
    assert resp.status_code == 200, resp.text
    return resp.json()


def _registrations_in(namespace_id: int) -> list[str]:
    with get_db_context() as db:
        rows = db.query(db_models.FlowRegistration).filter_by(namespace_id=namespace_id).all()
        return [r.name for r in rows]


# ---------- C1: editor flow-save routes ----------


def test_create_flow_into_foreign_namespace_denied(
    users, client_for, alice_ns, registration_cleanup, flow_session_cleanup, tmp_flow_path
):
    bob = client_for("bob")
    path = tmp_flow_path("bob_steal")
    resp = bob.post("/editor/create_flow/", params={"flow_path": path, "namespace_id": alice_ns["schema"]})
    assert resp.status_code == 403, resp.text
    assert _registrations_in(alice_ns["schema"]) == []
    # The pre-check fires before any session/YAML is created (capture the path
    # once — tmp_flow_path() unlinks on every call).
    assert not Path(path).exists()


def test_create_flow_use_grant_still_denied_manage_allows(
    users, client_for, alice_ns, team, grant_factory, registration_cleanup, flow_session_cleanup, tmp_flow_path
):
    bob = client_for("bob")
    grant_factory("catalog_namespace", alice_ns["schema"], team, permission="use", granted_by=users["alice"].id)
    denied = bob.post(
        "/editor/create_flow/",
        params={"flow_path": tmp_flow_path("bob_use"), "namespace_id": alice_ns["schema"]},
    )
    assert denied.status_code == 403, denied.text

    with get_db_context() as db:
        db.query(db_models.ResourceGrant).filter_by(
            resource_type="catalog_namespace", resource_id=alice_ns["schema"]
        ).update({"permission": "manage"})
        db.commit()
    allowed = bob.post(
        "/editor/create_flow/",
        params={"flow_path": tmp_flow_path("bob_manage"), "namespace_id": alice_ns["schema"]},
    )
    assert allowed.status_code == 200, allowed.text
    assert "bob_manage" in _registrations_in(alice_ns["schema"])


def test_create_flow_nonexistent_namespace_still_404(client_for, flow_session_cleanup, tmp_flow_path):
    bob = client_for("bob")
    resp = bob.post(
        "/editor/create_flow/",
        params={"flow_path": tmp_flow_path("bob_404"), "namespace_id": 10_000_000},
    )
    assert resp.status_code == 404, resp.text


def test_save_flow_into_foreign_namespace_denied(
    users, client_for, alice_ns, registration_cleanup, flow_session_cleanup, tmp_flow_path
):
    bob = client_for("bob")
    flow_id = _own_flow(bob, tmp_flow_path, "bob_own_save")
    # Save-As into alice's private schema (a new registration → namespace-create gate).
    new_path = tmp_flow_path("bob_saveas_foreign")
    resp = bob.post(
        "/save_flow", params={"flow_id": flow_id, "flow_path": new_path, "namespace_id": alice_ns["schema"]}
    )
    assert resp.status_code == 403, resp.text
    assert _registrations_in(alice_ns["schema"]) == []


def test_save_flow_to_catalog_denied_then_manage_allows(
    users, client_for, alice_ns, team, grant_factory, registration_cleanup, flow_session_cleanup, tmp_flow_path
):
    bob = client_for("bob")
    flow_id = _own_flow(bob, tmp_flow_path, "bob_own_cat")
    denied = bob.post(
        "/save_flow_to_catalog",
        params={"flow_id": flow_id, "flow_name": "stolen spot", "namespace_id": alice_ns["schema"]},
    )
    assert denied.status_code == 403, denied.text
    assert _registrations_in(alice_ns["schema"]) == []
    assert not list(Path(storage.flows_directory).glob("*stolen*"))

    grant_factory("catalog_namespace", alice_ns["schema"], team, permission="manage", granted_by=users["alice"].id)
    allowed = bob.post(
        "/save_flow_to_catalog",
        params={"flow_id": flow_id, "flow_name": "stolen spot", "namespace_id": alice_ns["schema"]},
    )
    assert allowed.status_code == 200, allowed.text
    assert "stolen spot" in _registrations_in(alice_ns["schema"])


def test_admin_bypasses_namespace_write_gate(
    users, client_for, alice_ns, registration_cleanup, flow_session_cleanup, tmp_flow_path
):
    admin = client_for("admin")
    resp = admin.post(
        "/editor/create_flow/",
        params={"flow_path": tmp_flow_path("admin_flow"), "namespace_id": alice_ns["schema"]},
    )
    assert resp.status_code == 200, resp.text


def test_register_helper_enforces_requesting_user(users, alice_ns, registration_cleanup, tmp_flow_path):
    path = tmp_flow_path("helper_flow")
    Path(path).write_text("flowfile_version: 1\n")
    bob = SimpleNamespace(id=users["bob"].id, username=users["bob"].username, is_admin=False)
    with pytest.raises(NotAuthorizedError):
        register_flow_in_namespace(path, "helper_flow", users["bob"].id, alice_ns["schema"], requesting_user=bob)
    assert _registrations_in(alice_ns["schema"]) == []
    # Internal-caller contract: no requesting_user keeps the historic unrestricted behavior.
    register_flow_in_namespace(path, "helper_flow", users["bob"].id, alice_ns["schema"])
    assert "helper_flow" in _registrations_in(alice_ns["schema"])


# ---------- C2: optimize / vacuum ----------


@pytest.fixture
def alice_delta_table(users, alice_ns, resource_factory, tmp_path):
    """A table row of alice's; no data on disk — the tests stop at the auth gate."""
    return resource_factory(
        db_models.CatalogTable,
        name="alice_delta",
        namespace_id=alice_ns["schema"],
        owner_id=users["alice"].id,
        file_path=str(tmp_path / "alice_delta"),
    )


@pytest.fixture
def alice_scd2_table(users, alice_ns, resource_factory, tmp_path):
    """An SCD2-tracked table row of alice's; no data on disk — the tests stop at the auth gate."""
    return resource_factory(
        db_models.CatalogTable,
        name="alice_scd2",
        namespace_id=alice_ns["schema"],
        owner_id=users["alice"].id,
        file_path=str(tmp_path / "alice_scd2"),
        scd2_config=json.dumps(
            {
                "business_keys": ["id"],
                "surrogate_key_column": "sk",
                "valid_from_column": "valid_from",
                "valid_to_column": "valid_to",
                "is_current_column": "is_current",
                "compare_columns": ["val"],
                "full_snapshot": False,
            }
        ),
    )


def test_optimize_and_vacuum_denied_for_reader(users, client_for, alice_delta_table, team, grant_factory):
    bob = client_for("bob")
    grant_factory("catalog_table", alice_delta_table, team, permission="use", granted_by=users["alice"].id)

    optimize = bob.post(f"/catalog/tables/{alice_delta_table}/optimize", json={"z_order_columns": None})
    assert optimize.status_code == 403, optimize.text
    assert "User " not in optimize.json()["detail"]

    vacuum = bob.post(
        f"/catalog/tables/{alice_delta_table}/vacuum", json={"retention_hours": 168, "dry_run": True}
    )
    assert vacuum.status_code == 403, vacuum.text


def test_optimize_and_vacuum_allowed_with_manage(users, alice_delta_table, team, grant_factory):
    """The auth gate passes for a manage grantee; the delta layer is stubbed so the
    test does not depend on real table data or a running worker."""
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.catalog.access import AccessResolver

    grant_factory("catalog_table", alice_delta_table, team, permission="manage", granted_by=users["alice"].id)
    calls = []
    with get_db_context() as db:
        bob = db.get(db_models.User, users["bob"].id)
        svc = CatalogService(SQLAlchemyCatalogRepository(db), access=AccessResolver(db, bob))
        svc._tables = SimpleNamespace(
            optimize_table=lambda table_id, z_order_columns=None: calls.append(("optimize", table_id)),
            vacuum_table=lambda table_id, retention_hours=168, dry_run=True: calls.append(("vacuum", table_id)),
        )
        svc.optimize_table(alice_delta_table)
        svc.vacuum_table(alice_delta_table)
    assert calls == [("optimize", alice_delta_table), ("vacuum", alice_delta_table)]


# ---------- C3 save-time: catalog-writer node settings ----------


def _writer_payload(flow_id: int, namespace_id: int, table_name: str = "bob_target") -> dict:
    return {
        "flow_id": flow_id,
        "node_id": 1,
        "catalog_write_settings": {"table_name": table_name, "namespace_id": namespace_id},
    }


def test_catalog_writer_settings_denied_for_reader(
    users, client_for, alice_ns, team, grant_factory, registration_cleanup, flow_session_cleanup, tmp_flow_path
):
    bob = client_for("bob")
    grant_factory("catalog_namespace", alice_ns["schema"], team, permission="use", granted_by=users["alice"].id)
    flow_id = _own_flow(bob, tmp_flow_path, "bob_writer_flow")
    bob.post(
        "/editor/add_node",
        params={"flow_id": flow_id, "node_id": 1, "node_type": "catalog_writer", "pos_x": 0, "pos_y": 0},
    )
    resp = bob.post(
        "/update_settings/",
        json=_writer_payload(flow_id, alice_ns["schema"]),
        params={"node_type": "catalog_writer"},
    )
    assert resp.status_code == 403, resp.text


def test_catalog_writer_scd2_settings_denied_for_reader(
    users, client_for, alice_ns, team, grant_factory, registration_cleanup, flow_session_cleanup, tmp_flow_path
):
    """The save-time gate keys on the write *target*, so an SCD2-mode payload is refused
    for a use-grantee exactly like any other physical mode."""
    bob = client_for("bob")
    grant_factory("catalog_namespace", alice_ns["schema"], team, permission="use", granted_by=users["alice"].id)
    flow_id = _own_flow(bob, tmp_flow_path, "bob_scd2_flow")
    bob.post(
        "/editor/add_node",
        params={"flow_id": flow_id, "node_id": 1, "node_type": "catalog_writer", "pos_x": 0, "pos_y": 0},
    )
    payload = _writer_payload(flow_id, alice_ns["schema"], table_name="bob_scd2_target")
    payload["catalog_write_settings"].update(
        {
            "write_mode": "scd2",
            "merge_keys": ["id"],
            "scd2": {"compare_columns": [], "full_snapshot": False},
        }
    )
    resp = bob.post("/update_settings/", json=payload, params={"node_type": "catalog_writer"})
    assert resp.status_code == 403, resp.text


def test_catalog_writer_settings_allowed_with_manage(
    users, client_for, alice_ns, team, grant_factory, registration_cleanup, flow_session_cleanup, tmp_flow_path
):
    bob = client_for("bob")
    grant_factory("catalog_namespace", alice_ns["schema"], team, permission="manage", granted_by=users["alice"].id)
    flow_id = _own_flow(bob, tmp_flow_path, "bob_writer_ok")
    bob.post(
        "/editor/add_node",
        params={"flow_id": flow_id, "node_id": 1, "node_type": "catalog_writer", "pos_x": 0, "pos_y": 0},
    )
    resp = bob.post(
        "/update_settings/",
        json=_writer_payload(flow_id, alice_ns["schema"]),
        params={"node_type": "catalog_writer"},
    )
    assert resp.status_code == 200, resp.text


def test_catalog_writer_incomplete_settings_pass(client_for, registration_cleanup, flow_session_cleanup, tmp_flow_path):
    bob = client_for("bob")
    flow_id = _own_flow(bob, tmp_flow_path, "bob_writer_empty")
    bob.post(
        "/editor/add_node",
        params={"flow_id": flow_id, "node_id": 1, "node_type": "catalog_writer", "pos_x": 0, "pos_y": 0},
    )
    resp = bob.post(
        "/update_settings/",
        json={"flow_id": flow_id, "node_id": 1, "catalog_write_settings": {"table_name": ""}},
        params={"node_type": "catalog_writer"},
    )
    assert resp.status_code == 200, resp.text


def test_catalog_writer_existing_table_needs_table_manage(
    users,
    client_for,
    alice_ns,
    alice_delta_table,
    team,
    grant_factory,
    registration_cleanup,
    flow_session_cleanup,
    tmp_flow_path,
):
    bob = client_for("bob")
    grant_factory("catalog_table", alice_delta_table, team, permission="use", granted_by=users["alice"].id)
    flow_id = _own_flow(bob, tmp_flow_path, "bob_overwrite")
    bob.post(
        "/editor/add_node",
        params={"flow_id": flow_id, "node_id": 1, "node_type": "catalog_writer", "pos_x": 0, "pos_y": 0},
    )
    denied = bob.post(
        "/update_settings/",
        json=_writer_payload(flow_id, alice_ns["schema"], table_name="alice_delta"),
        params={"node_type": "catalog_writer"},
    )
    assert denied.status_code == 403, denied.text

    with get_db_context() as db:
        db.query(db_models.ResourceGrant).filter_by(
            resource_type="catalog_table", resource_id=alice_delta_table
        ).update({"permission": "manage"})
        db.commit()
    allowed = bob.post(
        "/update_settings/",
        json=_writer_payload(flow_id, alice_ns["schema"], table_name="alice_delta"),
        params={"node_type": "catalog_writer"},
    )
    assert allowed.status_code == 200, allowed.text


# ---------- C3 run-time: _authorize_catalog_write ----------


def test_runtime_write_denied_for_non_member(users, alice_ns):
    with get_db_context() as db:
        with pytest.raises(PermissionError):
            _authorize_catalog_write(db, users["bob"].id, existing=None, namespace_id=alice_ns["schema"])


def test_runtime_write_allowed_for_owner_and_admin(users, alice_ns):
    with get_db_context() as db:
        _authorize_catalog_write(db, users["alice"].id, existing=None, namespace_id=alice_ns["schema"])
        _authorize_catalog_write(db, users["admin"].id, existing=None, namespace_id=alice_ns["schema"])


def test_runtime_write_user_id_none_fails_open(users, alice_ns):
    """Internal/scheduler/CLI runs carry no user id — unrestricted, matching the reader gate."""
    with get_db_context() as db:
        _authorize_catalog_write(db, None, existing=None, namespace_id=alice_ns["schema"])


def test_runtime_write_stale_user_fails_closed(users, alice_ns):
    with get_db_context() as db:
        with pytest.raises(PermissionError):
            _authorize_catalog_write(db, 10_000_000, existing=None, namespace_id=alice_ns["schema"])


def test_runtime_write_namespace_manage_grant_allows(users, alice_ns, team, grant_factory):
    grant_factory("catalog_namespace", alice_ns["schema"], team, permission="manage", granted_by=users["alice"].id)
    with get_db_context() as db:
        _authorize_catalog_write(db, users["bob"].id, existing=None, namespace_id=alice_ns["schema"])


def test_runtime_write_use_grant_still_denied(users, alice_ns, team, grant_factory):
    grant_factory("catalog_namespace", alice_ns["schema"], team, permission="use", granted_by=users["alice"].id)
    with get_db_context() as db:
        with pytest.raises(PermissionError):
            _authorize_catalog_write(db, users["bob"].id, existing=None, namespace_id=alice_ns["schema"])


def test_runtime_overwrite_existing_table_needs_manage(users, alice_ns, alice_delta_table, team, grant_factory):
    grant_factory("catalog_table", alice_delta_table, team, permission="use", granted_by=users["alice"].id)
    with get_db_context() as db:
        existing = db.get(db_models.CatalogTable, alice_delta_table)
        with pytest.raises(PermissionError):
            _authorize_catalog_write(db, users["bob"].id, existing=existing, namespace_id=alice_ns["schema"])
        db.query(db_models.ResourceGrant).filter_by(
            resource_type="catalog_table", resource_id=alice_delta_table
        ).update({"permission": "manage"})
        db.commit()
        _authorize_catalog_write(db, users["bob"].id, existing=existing, namespace_id=alice_ns["schema"])


def test_runtime_overwrite_existing_scd2_table_needs_manage(
    users, alice_ns, alice_scd2_table, team, grant_factory
):
    """An SCD2-tracked table is still just a catalog table to the authorization gate:
    a use-grantee may not rewrite its history, a manage-grantee may."""
    grant_factory("catalog_table", alice_scd2_table, team, permission="use", granted_by=users["alice"].id)
    with get_db_context() as db:
        existing = db.get(db_models.CatalogTable, alice_scd2_table)
        assert existing.scd2_config
        with pytest.raises(PermissionError):
            _authorize_catalog_write(db, users["bob"].id, existing=existing, namespace_id=alice_ns["schema"])
        db.query(db_models.ResourceGrant).filter_by(
            resource_type="catalog_table", resource_id=alice_scd2_table
        ).update({"permission": "manage"})
        db.commit()
        _authorize_catalog_write(db, users["bob"].id, existing=existing, namespace_id=alice_ns["schema"])


def test_runtime_write_public_namespace_allowed(users, resource_factory):
    catalog_id = resource_factory(
        db_models.CatalogNamespace,
        name="PubCat",
        parent_id=None,
        level=0,
        owner_id=users["alice"].id,
        is_public=True,
    )
    schema_id = resource_factory(
        db_models.CatalogNamespace,
        name="pub_schema",
        parent_id=catalog_id,
        level=1,
        owner_id=users["alice"].id,
        is_public=True,
    )
    with get_db_context() as db:
        _authorize_catalog_write(db, users["bob"].id, existing=None, namespace_id=schema_id)


# ---------- C4: SQL save-as-flow ----------


def test_sql_save_as_flow_denied_then_manage_allows(
    users, client_for, alice_ns, team, grant_factory, registration_cleanup
):
    bob = client_for("bob")
    denied = bob.post(
        "/catalog/sql/save-as-flow",
        json={"query": "SELECT 1", "name": "bob_query", "namespace_id": alice_ns["schema"]},
    )
    assert denied.status_code == 403, denied.text
    assert _registrations_in(alice_ns["schema"]) == []

    grant_factory("catalog_namespace", alice_ns["schema"], team, permission="manage", granted_by=users["alice"].id)
    allowed = bob.post(
        "/catalog/sql/save-as-flow",
        json={"query": "SELECT 1", "name": "bob_query", "namespace_id": alice_ns["schema"]},
    )
    assert allowed.status_code == 200, allowed.text
    assert "bob_query" in _registrations_in(alice_ns["schema"])


# ---------- C5: Kafka sync creation ----------


@pytest.fixture
def bob_kafka_connection(users, resource_factory):
    return resource_factory(
        db_models.KafkaConnection,
        connection_name="bob-kafka",
        bootstrap_servers="localhost:1",
        security_protocol="PLAINTEXT",
        user_id=users["bob"].id,
    )


@pytest.fixture
def no_kafka_network(monkeypatch):
    """The route tolerates schema-inference/group-reset failures; skip real brokers."""
    from flowfile_core.routes import kafka as kafka_routes

    def _raise(*args, **kwargs):
        raise RuntimeError("no broker in tests")

    monkeypatch.setattr(kafka_routes, "infer_topic_schema", _raise)
    monkeypatch.setattr(kafka_routes, "reset_consumer_group", _raise)


def test_kafka_sync_denied_then_manage_allows(
    users, client_for, alice_ns, team, grant_factory, bob_kafka_connection, no_kafka_network, registration_cleanup
):
    bob = client_for("bob")
    body = {
        "sync_name": "bobsync",
        "kafka_connection_id": bob_kafka_connection,
        "topic_name": "events",
        "namespace_id": alice_ns["schema"],
        "table_name": "events_table",
    }
    denied = bob.post("/kafka/sync", json=body)
    assert denied.status_code == 403, denied.text
    assert _registrations_in(alice_ns["schema"]) == []
    assert not (Path(storage.flows_directory) / "sync-bobsync.yaml").exists()

    grant_factory("catalog_namespace", alice_ns["schema"], team, permission="manage", granted_by=users["alice"].id)
    allowed = bob.post("/kafka/sync", json=body)
    assert allowed.status_code == 201, allowed.text
    (Path(storage.flows_directory) / "sync-bobsync.yaml").unlink(missing_ok=True)


# ---------- C6: ghost-registration reclaim ----------


@pytest.fixture
def public_ns(users, resource_factory):
    catalog_id = resource_factory(
        db_models.CatalogNamespace,
        name="GhostCat",
        parent_id=None,
        level=0,
        owner_id=users["alice"].id,
        is_public=True,
    )
    schema_id = resource_factory(
        db_models.CatalogNamespace,
        name="ghost_schema",
        parent_id=catalog_id,
        level=1,
        owner_id=users["alice"].id,
        is_public=True,
    )
    return schema_id


def test_ghost_reclaim_refused_for_stranger_allowed_for_owner(
    users, client_for, public_ns, resource_factory, registration_cleanup, flow_session_cleanup, tmp_flow_path, tmp_path
):
    ghost_id = resource_factory(
        db_models.FlowRegistration,
        name="Ghost",
        flow_path=str(tmp_path / "missing_ghost.yaml"),
        namespace_id=public_ns,
        owner_id=users["alice"].id,
    )

    bob = client_for("bob")
    bob_flow = _own_flow(bob, tmp_flow_path, "bob_ghost")
    refused = bob.post(
        "/save_flow_to_catalog",
        params={"flow_id": bob_flow, "flow_name": "Ghost", "namespace_id": public_ns},
    )
    assert refused.status_code == 409, refused.text
    with get_db_context() as db:
        assert db.get(db_models.FlowRegistration, ghost_id) is not None

    alice = client_for("alice")
    alice_flow = _own_flow(alice, tmp_flow_path, "alice_ghost")
    reclaimed = alice.post(
        "/save_flow_to_catalog",
        params={"flow_id": alice_flow, "flow_name": "Ghost", "namespace_id": public_ns},
    )
    assert reclaimed.status_code == 200, reclaimed.text
    with get_db_context() as db:
        assert db.get(db_models.FlowRegistration, ghost_id) is None


# ---------- previously guarded but untested: /catalog router creates ----------


def test_catalog_flows_route_denied_for_reader(users, client_for, alice_ns, team, grant_factory):
    bob = client_for("bob")
    grant_factory("catalog_namespace", alice_ns["schema"], team, permission="use", granted_by=users["alice"].id)
    resp = bob.post(
        "/catalog/flows",
        json={"name": "bob_reg", "flow_path": "/tmp/bob_reg.yaml", "namespace_id": alice_ns["schema"]},
    )
    assert resp.status_code == 403, resp.text


def test_catalog_tables_route_denied_for_reader(users, client_for, alice_ns, team, grant_factory):
    bob = client_for("bob")
    grant_factory("catalog_namespace", alice_ns["schema"], team, permission="use", granted_by=users["alice"].id)
    resp = bob.post(
        "/catalog/tables",
        json={
            "name": "bob_table",
            "file_path": str(_TMP_FLOW_DIR / "does_not_exist.csv"),
            "namespace_id": alice_ns["schema"],
        },
    )
    assert resp.status_code == 403, resp.text


# ---------- review follow-ups ----------


def test_save_flow_onto_foreign_registered_path_denied_before_write(
    users, client_for, public_ns, resource_factory, registration_cleanup, flow_session_cleanup, tmp_flow_path
):
    """Saving onto a path registered to someone else's flow must refuse BEFORE
    overwriting the YAML, even when the namespace itself is writable (public)."""
    alice_path = tmp_flow_path("alice_precious")
    Path(alice_path).write_text("flowfile_version: 1\n")
    resource_factory(
        db_models.FlowRegistration,
        name="alice_precious",
        flow_path=alice_path,
        namespace_id=public_ns,
        owner_id=users["alice"].id,
    )
    bob = client_for("bob")
    bob_flow = _own_flow(bob, tmp_flow_path, "bob_attacker")
    resp = bob.post(
        "/save_flow",
        params={"flow_id": bob_flow, "flow_path": alice_path, "namespace_id": public_ns},
    )
    assert resp.status_code == 403, resp.text
    assert Path(alice_path).read_text() == "flowfile_version: 1\n"


def test_save_flow_stale_namespace_no_existence_oracle(
    client_for, registration_cleanup, flow_session_cleanup, tmp_flow_path
):
    """A restricted user gets the same 403 for a missing and an unwritable
    namespace (no cross-tenant existence oracle); a privileged user gets a clean 404."""
    bob = client_for("bob")
    flow_id = _own_flow(bob, tmp_flow_path, "bob_stale_ns")
    new_path = tmp_flow_path("bob_stale_saveas")
    denied = bob.post(
        "/save_flow", params={"flow_id": flow_id, "flow_path": new_path, "namespace_id": 10_000_000}
    )
    assert denied.status_code == 403, denied.text

    admin = client_for("admin")
    admin_flow = _own_flow(admin, tmp_flow_path, "admin_stale_ns")
    admin_path = tmp_flow_path("admin_stale_saveas")
    resp = admin.post(
        "/save_flow", params={"flow_id": admin_flow, "flow_path": admin_path, "namespace_id": 10_000_000}
    )
    assert resp.status_code == 404, resp.text


def test_save_flow_name_collision_returns_409(
    users, client_for, public_ns, resource_factory, registration_cleanup, flow_session_cleanup, tmp_flow_path
):
    """A display-name clash maps to 409 with guidance, not a 500."""
    resource_factory(
        db_models.FlowRegistration,
        name="bob_clash",
        flow_path=str(_TMP_FLOW_DIR / "someone_elses_clash.yaml"),
        namespace_id=public_ns,
        owner_id=users["alice"].id,
    )
    bob = client_for("bob")
    flow_id = _own_flow(bob, tmp_flow_path, "bob_clash")
    resp = bob.post("/save_flow", params={"flow_id": flow_id, "namespace_id": public_ns})
    assert resp.status_code == 409, resp.text


def test_catalog_writer_unchanged_settings_pass_for_reader(
    users, client_for, alice_ns, team, grant_factory, registration_cleanup, flow_session_cleanup, tmp_flow_path
):
    """Re-POSTing identical writer settings (the drawer does this on close) must not
    403 for a user who can view but not write the configured target."""
    grant_factory("catalog_namespace", alice_ns["schema"], team, permission="manage", granted_by=users["alice"].id)
    bob = client_for("bob")
    flow_id = _own_flow(bob, tmp_flow_path, "bob_writer_view")
    bob.post(
        "/editor/add_node",
        params={"flow_id": flow_id, "node_id": 1, "node_type": "catalog_writer", "pos_x": 0, "pos_y": 0},
    )
    payload = _writer_payload(flow_id, alice_ns["schema"])
    assert bob.post("/update_settings/", json=payload, params={"node_type": "catalog_writer"}).status_code == 200

    # Access downgraded after configuration: an unchanged re-save still passes...
    with get_db_context() as db:
        db.query(db_models.ResourceGrant).filter_by(
            resource_type="catalog_namespace", resource_id=alice_ns["schema"]
        ).update({"permission": "use"})
        db.commit()
    unchanged = bob.post("/update_settings/", json=payload, params={"node_type": "catalog_writer"})
    assert unchanged.status_code == 200, unchanged.text

    # ...but retargeting to another table name is a new write and is refused.
    retarget = bob.post(
        "/update_settings/",
        json=_writer_payload(flow_id, alice_ns["schema"], table_name="another_table"),
        params={"node_type": "catalog_writer"},
    )
    assert retarget.status_code == 403, retarget.text


def test_unchanged_writer_resave_preserves_author_identity(
    users, client_for, alice_ns, team, grant_factory, registration_cleanup, flow_session_cleanup, tmp_flow_path
):
    """A viewer closing the writer drawer (unchanged re-POST) must not re-stamp the
    node's run-time principal, which would break the owner's later runs."""
    grant_factory("catalog_namespace", alice_ns["schema"], team, permission="manage", granted_by=users["alice"].id)
    alice = client_for("alice")
    flow_id = _own_flow(alice, tmp_flow_path, "alice_writer_flow")
    alice.post(
        "/editor/add_node",
        params={"flow_id": flow_id, "node_id": 1, "node_type": "catalog_writer", "pos_x": 0, "pos_y": 0},
    )
    payload = _writer_payload(flow_id, alice_ns["schema"])
    assert alice.post("/update_settings/", json=payload, params={"node_type": "catalog_writer"}).status_code == 200

    # Bob (only a use-grantee on the flow's shared graph) re-POSTs the unchanged
    # settings, as the drawer does on close.
    bob = client_for("bob")
    assert bob.post("/update_settings/", json=payload, params={"node_type": "catalog_writer"}).status_code == 200

    node = flow_file_handler.get_flow(flow_id).get_node(1)
    assert node.setting_input.user_id == users["alice"].id


# ---------- in-place re-registration (flow-manage, not namespace-create) ----------


def test_flow_manager_can_resave_in_place_without_namespace_write(
    users, client_for, alice_ns, team, grant_factory, resource_factory, registration_cleanup, flow_session_cleanup
):
    """A user who manages a flow may re-save it in place even with only read access
    to its namespace — an in-place update needs flow-manage, not namespace-create."""
    _TMP_FLOW_DIR.mkdir(parents=True, exist_ok=True)
    flow_path = str(_TMP_FLOW_DIR / "alice_shared_flow.yaml")
    Path(flow_path).unlink(missing_ok=True)
    try:
        alice = client_for("alice")
        flow_id = alice.post("/editor/create_flow/", params={"flow_path": flow_path}).json()
        # Move the flow into alice's private schema, then share it manage-level with bob.
        reg_name = None
        with get_db_context() as db:
            reg = db.query(db_models.FlowRegistration).filter_by(flow_path=flow_path).first()
            reg.namespace_id = alice_ns["schema"]
            reg_name = reg.name
            reg_id = reg.id
            db.commit()
        grant_factory("catalog_namespace", alice_ns["schema"], team, permission="use", granted_by=users["alice"].id)
        grant_factory("flow", reg_id, team, permission="manage", granted_by=users["alice"].id)

        bob = client_for("bob")
        # bob (namespace: read-only; flow: manage) re-saves the shared flow in place.
        resp = bob.post(
            "/save_flow", params={"flow_id": flow_id, "flow_path": flow_path, "namespace_id": alice_ns["schema"]}
        )
        assert resp.status_code == 200, resp.text
        assert reg_name in _registrations_in(alice_ns["schema"])
    finally:
        Path(flow_path).unlink(missing_ok=True)
        for f in list(flow_file_handler.flowfile_flows):
            flow_file_handler.delete_flow(f.flow_id)


# ---------- C5 heal safety: never force-publish a foreign private 'sync' ----------


def test_kafka_sync_does_not_publish_foreign_private_sync(
    users, client_for, resource_factory, bob_kafka_connection, no_kafka_network, registration_cleanup
):
    # Place a PRIVATE 'sync' owned by alice under the SEEDED public General — the
    # one _ensure_sync_namespace actually resolves (get_namespace_by_name is
    # owner-agnostic q.first(), i.e. the lowest-rowid General) — so bob's
    # default-namespace sync lands on it and the owner-only heal guard is exercised.
    with get_db_context() as db:
        general_id = (
            db.query(db_models.CatalogNamespace)
            .filter_by(name="General", parent_id=None)
            .order_by(db_models.CatalogNamespace.id)
            .first()
            .id
        )
    alice_sync = resource_factory(
        db_models.CatalogNamespace,
        name="sync",
        parent_id=general_id,
        level=1,
        owner_id=users["alice"].id,
        is_public=False,
    )
    bob = client_for("bob")
    resp = bob.post(
        "/kafka/sync",
        json={
            "sync_name": "bobsync2",
            "kafka_connection_id": bob_kafka_connection,
            "topic_name": "events",
            "table_name": "events_table",
        },
    )
    (Path(storage.flows_directory) / "sync-bobsync2.yaml").unlink(missing_ok=True)
    # bob is not the owner: the private sync is neither published nor writable by him.
    assert resp.status_code == 403, resp.text
    with get_db_context() as db:
        assert db.get(db_models.CatalogNamespace, alice_sync).is_public is False


# ---------- Train Model catalog publish authorization ----------


def _train_payload(flow_id: int, namespace_id: int) -> dict:
    return {
        "flow_id": flow_id,
        "node_id": 1,
        "train_input": {
            "publish_to_catalog": True,
            "model_name": "bob_model",
            "target_column": "y",
            "namespace_id": namespace_id,
        },
    }


def test_train_model_publish_settings_denied_for_reader(
    users, client_for, alice_ns, team, grant_factory, registration_cleanup, flow_session_cleanup, tmp_flow_path
):
    bob = client_for("bob")
    grant_factory("catalog_namespace", alice_ns["schema"], team, permission="use", granted_by=users["alice"].id)
    flow_id = _own_flow(bob, tmp_flow_path, "bob_train_flow")
    bob.post(
        "/editor/add_node",
        params={"flow_id": flow_id, "node_id": 1, "node_type": "train_model", "pos_x": 0, "pos_y": 0},
    )
    resp = bob.post(
        "/update_settings/", json=_train_payload(flow_id, alice_ns["schema"]), params={"node_type": "train_model"}
    )
    assert resp.status_code == 403, resp.text


def test_train_model_publish_settings_allowed_with_manage(
    users, client_for, alice_ns, team, grant_factory, registration_cleanup, flow_session_cleanup, tmp_flow_path
):
    bob = client_for("bob")
    grant_factory("catalog_namespace", alice_ns["schema"], team, permission="manage", granted_by=users["alice"].id)
    flow_id = _own_flow(bob, tmp_flow_path, "bob_train_ok")
    bob.post(
        "/editor/add_node",
        params={"flow_id": flow_id, "node_id": 1, "node_type": "train_model", "pos_x": 0, "pos_y": 0},
    )
    resp = bob.post(
        "/update_settings/", json=_train_payload(flow_id, alice_ns["schema"]), params={"node_type": "train_model"}
    )
    assert resp.status_code == 200, resp.text
