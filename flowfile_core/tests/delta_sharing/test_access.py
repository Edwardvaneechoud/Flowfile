"""Who may read a share, what a share can hold, and what is left when a table, key or consumer goes away."""

from __future__ import annotations

import datetime as dt
import json
from urllib.parse import urlsplit

import polars as pl

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context

BASE = "/delta-sharing"


def _status(anonymous, shared, path: str, headers=None) -> int:
    return anonymous.get(f"{BASE}{path}", headers=headers if headers is not None else shared.headers).status_code


def test_a_missing_unknown_or_malformed_token_is_unauthenticated(anonymous, catalog, share_tables):
    shared = share_tables(catalog.table("orders", pl.DataFrame({"id": [1]}))[0])
    for headers in ({}, {"Authorization": "Bearer ffk_nope"}, {"Authorization": f"Basic {shared.token}"}):
        response = anonymous.get(f"{BASE}/shares", headers=headers)
        assert response.status_code == 401
        assert response.json()["errorCode"] == "UNAUTHENTICATED"


def test_an_expired_or_disabled_key_and_a_disabled_consumer_are_unauthenticated(anonymous, catalog, share_tables):
    shared = share_tables(catalog.table("orders", pl.DataFrame({"id": [1]}))[0])
    assert _status(anonymous, shared, "/shares") == 200
    with get_db_context() as db:
        key = db.query(db_models.FlowApiKey).filter_by(consumer_id=shared.consumer["id"]).one()
        key.expires_at = dt.datetime.now(dt.UTC) - dt.timedelta(minutes=1)
        db.commit()
    assert _status(anonymous, shared, "/shares") == 401
    with get_db_context() as db:
        key = db.query(db_models.FlowApiKey).filter_by(consumer_id=shared.consumer["id"]).one()
        key.expires_at = None
        key.enabled = False
        db.commit()
    assert _status(anonymous, shared, "/shares") == 401
    with get_db_context() as db:
        key = db.query(db_models.FlowApiKey).filter_by(consumer_id=shared.consumer["id"]).one()
        key.enabled = True
        db.get(db_models.ApiConsumer, shared.consumer["id"]).enabled = False
        db.commit()
    assert _status(anonymous, shared, "/shares") == 401


def test_a_share_not_granted_answers_like_one_that_does_not_exist(anonymous, designer, catalog, share_tables):
    shared = share_tables(catalog.table("orders", pl.DataFrame({"id": [1]}))[0])
    other = designer.post("/delta-sharing/manage/shares", json={"name": "secret"}).json()
    unknown = anonymous.get(f"{BASE}/shares/does_not_exist", headers=shared.headers)
    not_granted = anonymous.get(f"{BASE}/shares/{other['name']}", headers=shared.headers)
    assert unknown.status_code == not_granted.status_code == 404
    assert unknown.json()["errorCode"] == not_granted.json()["errorCode"] == "RESOURCE_DOES_NOT_EXIST"
    assert [s["name"] for s in anonymous.get(f"{BASE}/shares", headers=shared.headers).json()["items"]] == ["retail"]

    designer.delete(f"/delta-sharing/manage/shares/{shared.share['id']}/recipients/{shared.consumer['id']}")
    assert _status(anonymous, shared, "/shares/retail") == 404
    assert anonymous.get(f"{BASE}/shares", headers=shared.headers).json()["items"] == []


def test_a_deleted_table_leaves_the_share_and_a_reused_id_is_not_served(anonymous, designer, catalog, share_tables):
    table_id, path = catalog.table("orders", pl.DataFrame({"id": [1]}))
    shared = share_tables(table_id)
    assert _status(anonymous, shared, "/shares/retail/schemas/sales/tables/orders/version") == 200

    lines = anonymous.post(
        f"{BASE}/shares/retail/schemas/sales/tables/orders/query", json={}, headers=shared.headers
    ).text.splitlines()
    url = next(json.loads(line)["file"]["url"] for line in lines if '"file"' in line)

    from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository

    with get_db_context() as db:
        SQLAlchemyCatalogRepository(db).delete_table(table_id)
        assert db.query(db_models.DeltaShareTable).filter_by(table_id=table_id).count() == 0
    assert _status(anonymous, shared, "/shares/retail/schemas/sales/tables/orders/version") == 404
    assert anonymous.get(urlsplit(url).path).status_code == 404

    with get_db_context() as db:
        db.add(db_models.DeltaShareTable(share_id=shared.share["id"], table_id=table_id, schema_name="sales",
                                         table_name="orders", table_path=str(path)))
        db.add(db_models.CatalogTable(id=table_id, name="impostor", namespace_id=catalog.schema_id,
                                      owner_id=catalog.owner_id, file_path=str(path) + "_other",
                                      storage_format="delta", table_type="physical"))
        db.commit()
    catalog.table_ids.append(table_id)
    assert _status(anonymous, shared, "/shares/retail/schemas/sales/tables/orders/version") == 404


def test_only_physical_delta_tables_can_be_added(designer, catalog, cleanup_sharing):
    share_id = designer.post("/delta-sharing/manage/shares", json={"name": "picky"}).json()["id"]
    with get_db_context() as db:
        virtual = db_models.CatalogTable(name="v", namespace_id=catalog.schema_id, owner_id=catalog.owner_id,
                                         table_type="virtual", storage_format="delta")
        iceberg = db_models.CatalogTable(name="i", namespace_id=catalog.schema_id, owner_id=catalog.owner_id,
                                         table_type="physical", storage_format="iceberg")
        db.add_all([virtual, iceberg])
        db.commit()
        catalog.table_ids += [virtual.id, iceberg.id]
        refused_ids = [virtual.id, iceberg.id]
    for table_id in refused_ids:
        response = designer.post(f"/delta-sharing/manage/shares/{share_id}/tables", json={"table_id": table_id})
        assert response.status_code == 422, response.text

    dv_id, dv_path = catalog.table("with_dv", pl.DataFrame({"id": [1, 2]}))
    from deltalake import DeltaTable

    DeltaTable(str(dv_path)).alter.set_table_properties({"delta.enableDeletionVectors": "true"})
    assert "deletionVectors" in (DeltaTable(str(dv_path)).protocol().reader_features or [])
    refused = designer.post(f"/delta-sharing/manage/shares/{share_id}/tables", json={"table_id": dv_id})
    assert refused.status_code == 422 and "deletion vectors" in refused.json()["detail"]

    eligible = {t["table_id"]: t for t in designer.get("/delta-sharing/manage/eligible-tables").json()}
    assert virtual.id not in eligible and iceberg.id not in eligible


def test_names_are_unique_in_a_share_and_a_table_is_added_once(designer, catalog, cleanup_sharing):
    first, _ = catalog.table("orders", pl.DataFrame({"id": [1]}))
    second, _ = catalog.table("orders_v2", pl.DataFrame({"id": [1]}))
    share_id = designer.post("/delta-sharing/manage/shares", json={"name": "dup"}).json()["id"]
    assert designer.post(f"/delta-sharing/manage/shares/{share_id}/tables", json={"table_id": first}).status_code == 200
    again = designer.post(f"/delta-sharing/manage/shares/{share_id}/tables", json={"table_id": first})
    assert again.status_code == 409
    clash = designer.post(
        f"/delta-sharing/manage/shares/{share_id}/tables", json={"table_id": second, "table_name": "orders"}
    )
    assert clash.status_code == 409
    assert designer.post("/delta-sharing/manage/shares", json={"name": "dup"}).status_code == 409
    assert designer.post("/delta-sharing/manage/shares", json={"name": "bad name!"}).status_code == 422


def test_a_profile_needs_a_grant_and_holds_a_working_token(anonymous, designer, catalog, cleanup_sharing):
    table_id, _ = catalog.table("orders", pl.DataFrame({"id": [1]}))
    share_id = designer.post("/delta-sharing/manage/shares", json={"name": "prof"}).json()["id"]
    designer.post(f"/delta-sharing/manage/shares/{share_id}/tables", json={"table_id": table_id})
    consumer_id = designer.post("/api-consumers", json={"name": "ds-test-prof"}).json()["id"]
    refused = designer.post(f"/delta-sharing/manage/shares/{share_id}/profile", json={"consumer_id": consumer_id})
    assert refused.status_code == 422

    designer.post(f"/delta-sharing/manage/shares/{share_id}/recipients", json={"consumer_id": consumer_id})
    created = designer.post(
        f"/delta-sharing/manage/shares/{share_id}/profile",
        json={"consumer_id": consumer_id, "endpoint": "https://data.example.com/api", "expires_in_days": 30},
    ).json()
    profile = created["profile"]
    assert profile["shareCredentialsVersion"] == 1
    assert profile["endpoint"] == "https://data.example.com/api/delta-sharing"
    assert profile["expirationTime"].endswith("Z")
    assert created["file_name"] == "prof.share"
    assert created["table_urls"] == ["prof.sales.orders"]
    listed = anonymous.get(f"{BASE}/shares", headers={"Authorization": f"Bearer {profile['bearerToken']}"})
    assert [s["name"] for s in listed.json()["items"]] == ["prof"]

    shares_of = designer.get(f"/delta-sharing/manage/consumers/{consumer_id}/shares").json()
    assert [s["name"] for s in shares_of] == ["prof"]
    designer.delete(f"/api-consumers/{consumer_id}")
    with get_db_context() as db:
        assert db.query(db_models.ApiConsumerDeltaShare).filter_by(consumer_id=consumer_id).count() == 0


def test_deleting_a_share_takes_its_tables_and_grants(designer, catalog, share_tables):
    shared = share_tables(catalog.table("orders", pl.DataFrame({"id": [1]}))[0])
    assert designer.delete(f"/delta-sharing/manage/shares/{shared.share['id']}").status_code == 204
    with get_db_context() as db:
        assert db.query(db_models.DeltaShareTable).filter_by(share_id=shared.share["id"]).count() == 0
        assert db.query(db_models.ApiConsumerDeltaShare).filter_by(share_id=shared.share["id"]).count() == 0


def test_turned_off_both_routers_answer_404(anonymous, designer, catalog, share_tables, monkeypatch):
    shared = share_tables(catalog.table("orders", pl.DataFrame({"id": [1]}))[0])
    monkeypatch.setenv("FLOWFILE_DELTA_SHARING", "0")
    assert _status(anonymous, shared, "/shares") == 404
    assert designer.get("/delta-sharing/manage/shares").status_code == 404
    assert designer.get("/delta-sharing/manage/status").json()["enabled"] is False
