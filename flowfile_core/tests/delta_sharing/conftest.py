"""Fixtures for Delta Sharing: Delta tables in the local catalog, a share, a consumer and its profile."""

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


def _token() -> str:
    with TestClient(main.app) as client:
        return client.post("/auth/token").json()["access_token"]


@pytest.fixture(scope="module")
def designer() -> TestClient:
    """The designer's client, signed in as the desktop's local user."""
    client = TestClient(main.app)
    client.headers["Authorization"] = f"Bearer {_token()}"
    return client


@pytest.fixture(scope="module")
def anonymous() -> TestClient:
    return TestClient(main.app)


class Catalog:
    """Delta tables written under the catalog directory and registered on a schema of their own."""

    def __init__(self) -> None:
        self.table_ids: list[int] = []
        self.paths: list[Path] = []
        with get_db_context() as db:
            owner = db.query(db_models.User).filter_by(username="local_user").first()
            self.owner_id = owner.id if owner else 1
            catalog = db_models.CatalogNamespace(name=f"DSCat_{uuid.uuid4().hex[:6]}", level=0, owner_id=self.owner_id)
            db.add(catalog)
            db.commit()
            schema = db_models.CatalogNamespace(name="sales", level=1, parent_id=catalog.id, owner_id=self.owner_id)
            db.add(schema)
            db.commit()
            self.catalog_id, self.schema_id = catalog.id, schema.id

    def table(self, name: str, frame: pl.DataFrame, **write) -> tuple[int, Path]:
        path = Path(storage.catalog_tables_directory) / f"ds_test_{uuid.uuid4().hex}"
        frame.write_delta(path, **write)
        self.paths.append(path)
        with get_db_context() as db:
            row = db_models.CatalogTable(
                name=name,
                namespace_id=self.schema_id,
                owner_id=self.owner_id,
                file_path=str(path),
                storage_format="delta",
                table_type="physical",
            )
            db.add(row)
            db.commit()
            self.table_ids.append(row.id)
            return row.id, path

    def cleanup(self) -> None:
        with get_db_context() as db:
            for table_id in self.table_ids:
                db.query(db_models.DeltaShareTable).filter_by(table_id=table_id).delete()
                row = db.get(db_models.CatalogTable, table_id)
                if row is not None:
                    db.delete(row)
            for ns_id in (self.schema_id, self.catalog_id):
                row = db.get(db_models.CatalogNamespace, ns_id)
                if row is not None:
                    db.delete(row)
            db.commit()
        for path in self.paths:
            shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def catalog():
    cat = Catalog()
    yield cat
    cat.cleanup()


@pytest.fixture
def cleanup_sharing():
    yield
    with get_db_context() as db:
        db.query(db_models.ApiConsumerDeltaShare).delete()
        db.query(db_models.DeltaShareTable).delete()
        db.query(db_models.DeltaShare).delete()
        for consumer in db.query(db_models.ApiConsumer).filter(db_models.ApiConsumer.name.like("ds-test-%")).all():
            db.query(db_models.FlowApiKey).filter_by(consumer_id=consumer.id).delete()
            db.delete(consumer)
        db.commit()


class Shared:
    """A share holding tables, granted to a consumer, with the bearer token of its profile."""

    def __init__(self, designer: TestClient, share: dict, consumer: dict, profile: dict) -> None:
        self.designer = designer
        self.share = share
        self.consumer = consumer
        self.profile = profile
        self.token = profile["profile"]["bearerToken"]

    @property
    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}


@pytest.fixture
def share_tables(designer, cleanup_sharing):
    """``share_tables(*table_ids, name=...)``: a share of those tables, a consumer granted it and its profile."""

    def _make(*table_ids: int, name: str = "retail", history: bool = True) -> Shared:
        share = designer.post("/delta-sharing/manage/shares", json={"name": name})
        assert share.status_code == 201, share.text
        share_id = share.json()["id"]
        for table_id in table_ids:
            added = designer.post(
                f"/delta-sharing/manage/shares/{share_id}/tables",
                json={"table_id": table_id, "share_history": history},
            )
            assert added.status_code == 200, added.text
        consumer = designer.post("/api-consumers", json={"name": f"ds-test-{uuid.uuid4().hex[:6]}"})
        assert consumer.status_code == 201, consumer.text
        consumer_id = consumer.json()["id"]
        granted = designer.post(f"/delta-sharing/manage/shares/{share_id}/recipients", json={"consumer_id": consumer_id})
        assert granted.status_code == 200, granted.text
        profile = designer.post(
            f"/delta-sharing/manage/shares/{share_id}/profile",
            json={"consumer_id": consumer_id, "endpoint": "http://testserver"},
        )
        assert profile.status_code == 200, profile.text
        return Shared(designer, granted.json(), consumer.json(), profile.json())

    return _make
