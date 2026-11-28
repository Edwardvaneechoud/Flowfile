"""Delta Sharing on a multi-user server: only a table's managers share it, and a share follows its owner's rights."""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import polars as pl
import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from shared.storage_config import storage


@pytest.fixture
def team(users, group_factory):
    return group_factory("ds-team", users["alice"].id, {users["alice"].id: "owner", users["bob"].id: "member"})


@pytest.fixture
def alice_table(users, resource_factory):
    path = Path(storage.catalog_tables_directory) / f"ds_share_{uuid.uuid4().hex}"
    pl.DataFrame({"id": [1, 2]}).write_delta(path)
    catalog = resource_factory(db_models.CatalogNamespace, name=f"ds_alice_{uuid.uuid4().hex[:6]}", level=0,
                               owner_id=users["alice"].id)
    schema = resource_factory(db_models.CatalogNamespace, name="private", level=1, parent_id=catalog,
                              owner_id=users["alice"].id)
    table = resource_factory(db_models.CatalogTable, name="payroll", namespace_id=schema, owner_id=users["alice"].id,
                             file_path=str(path), storage_format="delta", table_type="physical")
    yield table
    with get_db_context() as db:
        db.query(db_models.ApiConsumerDeltaShare).delete()
        db.query(db_models.DeltaShareTable).delete()
        db.query(db_models.DeltaShare).delete()
        for consumer in db.query(db_models.ApiConsumer).filter(db_models.ApiConsumer.name.like("ds-%")).all():
            db.query(db_models.FlowApiKey).filter_by(consumer_id=consumer.id).delete()
            db.delete(consumer)
        db.commit()
    shutil.rmtree(path, ignore_errors=True)


def _add(client, share_id, table_id):
    return client.post(f"/delta-sharing/manage/shares/{share_id}/tables", json={"table_id": table_id})


def test_a_use_grant_is_not_enough_to_share_a_table(users, client_for, alice_table, team, grant_factory):
    bob = client_for("bob")
    share_id = bob.post("/delta-sharing/manage/shares", json={"name": "bobs"}).json()["id"]
    assert _add(bob, share_id, alice_table).status_code == 403
    grant_factory("catalog_table", alice_table, team, permission="use", granted_by=users["alice"].id)
    assert _add(bob, share_id, alice_table).status_code == 403
    assert alice_table not in {t["table_id"] for t in bob.get("/delta-sharing/manage/eligible-tables").json()}

    with get_db_context() as db:
        db.query(db_models.ResourceGrant).filter_by(resource_type="catalog_table", resource_id=alice_table).update(
            {"permission": "manage"}
        )
        db.commit()
    assert _add(bob, share_id, alice_table).status_code == 200


def test_a_share_stops_serving_a_table_its_owner_no_longer_manages(users, client_for, alice_table, team,
                                                                  grant_factory):
    grant_factory("catalog_table", alice_table, team, permission="manage", granted_by=users["alice"].id)
    bob = client_for("bob")
    share_id = bob.post("/delta-sharing/manage/shares", json={"name": "bobs"}).json()["id"]
    assert _add(bob, share_id, alice_table).status_code == 200
    consumer_id = bob.post("/api-consumers", json={"name": "ds-bob"}).json()["id"]
    bob.post(f"/delta-sharing/manage/shares/{share_id}/recipients", json={"consumer_id": consumer_id})
    token = bob.post(f"/delta-sharing/manage/shares/{share_id}/profile",
                     json={"consumer_id": consumer_id}).json()["profile"]["bearerToken"]
    recipient = TestClient(main.app)
    headers = {"Authorization": f"Bearer {token}"}
    table_url = "/delta-sharing/shares/bobs/schemas/private/tables/payroll"
    assert recipient.get(f"{table_url}/version", headers=headers).status_code == 200

    with get_db_context() as db:
        db.query(db_models.ResourceGrant).filter_by(resource_type="catalog_table", resource_id=alice_table).delete()
        db.commit()
    assert recipient.get(f"{table_url}/version", headers=headers).status_code == 404
    assert recipient.get("/delta-sharing/shares/bobs/all-tables", headers=headers).json()["items"] == []
    shown = bob.get(f"/delta-sharing/manage/shares/{share_id}").json()["tables"][0]
    assert shown["served"] is False and "no longer manages" in shown["refusal"]


def test_only_the_owner_or_an_admin_changes_a_share_and_grants_go_to_own_consumers(users, client_for, alice_table):
    alice, bob, admin = client_for("alice"), client_for("bob"), client_for("admin")
    share_id = alice.post("/delta-sharing/manage/shares", json={"name": "alices"}).json()["id"]
    assert bob.get(f"/delta-sharing/manage/shares/{share_id}").status_code == 404
    assert bob.delete(f"/delta-sharing/manage/shares/{share_id}").status_code == 404
    assert [s["name"] for s in bob.get("/delta-sharing/manage/shares").json()] == []
    assert admin.get(f"/delta-sharing/manage/shares/{share_id}").status_code == 200

    bobs_consumer = bob.post("/api-consumers", json={"name": "ds-bob-c"}).json()["id"]
    granted = alice.post(f"/delta-sharing/manage/shares/{share_id}/recipients", json={"consumer_id": bobs_consumer})
    assert granted.status_code == 404


def test_deleting_a_user_takes_their_shares(users, client_for, alice_table):
    carol = client_for("carol")
    share_id = carol.post("/delta-sharing/manage/shares", json={"name": "carols"}).json()["id"]
    admin = client_for("admin")
    assert admin.delete(f"/auth/users/{users['carol'].id}").status_code in (200, 204)
    with get_db_context() as db:
        assert db.get(db_models.DeltaShare, share_id) is None
