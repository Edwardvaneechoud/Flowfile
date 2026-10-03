"""A notebook kernel session opens no catalog database connection: its metadata lookups go through core.

The ``kernel-sim`` kernel shares core's engine, so ``conftest.kernel_db_opens`` (a pool listener under the sim's
in-op marker) sees exactly the connections the kernel's own work would open in a container. The census over the
whole corpus is the gate of the next phase, which deletes the kernel's database copy. ``POST
/notebook/session/lookup`` (``notebook.lookup``) answers what a build reads, metadata only, as the kernel's owner.
"""

from __future__ import annotations

import json
import sqlite3

import pytest
from cryptography.fernet import Fernet
from pydantic import SecretStr

from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    get_cloud_connection,
    get_database_connection,
    store_cloud_connection,
    store_database_connection,
)
from flowfile_core.notebook import lookup
from flowfile_core.notebook.render import render
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection
from tests.notebook.conftest import NOTEBOOK_OWNER_ID, cell_provenance

LOOPBACK = ("127.0.0.1", 50123)
DEMO_SCHEMA = "Demo.sales_analytics"
DATABASE_CONNECTION = "notebook_census_database"
CLOUD_CONNECTION = "notebook_census_s3"
CLOUD_CATALOG = "notebook_census_cloud"


@pytest.fixture
def client(client_as, kernel_sim):
    return client_as(NOTEBOOK_OWNER_ID, client=LOOPBACK)


@pytest.fixture
def database_connection(tmp_path) -> str:
    """A stored SQLite connection over a file with a ``movies`` table."""
    path = tmp_path / "census.db"
    with sqlite3.connect(path) as db:
        db.execute("create table movies (id integer, title text)")
        db.executemany("insert into movies values (?, ?)", [(1, "The Matrix"), (2, "Inception")])
    connection = input_schema.FullDatabaseConnection(
        connection_name=DATABASE_CONNECTION,
        database_type="sqlite",
        username="",
        password=SecretStr(""),
        database=str(path),
    )
    with get_db_context() as db:
        if get_database_connection(db, DATABASE_CONNECTION, NOTEBOOK_OWNER_ID) is None:
            store_database_connection(db, connection, user_id=NOTEBOOK_OWNER_ID)
    return DATABASE_CONNECTION


@pytest.fixture
def cloud_catalog() -> int:
    """A catalog whose tables live in cloud storage through a stored connection; its namespace id."""
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository

    connection = FullCloudStorageConnection(
        connection_name=CLOUD_CONNECTION,
        storage_type="s3",
        auth_method="access_key",
        aws_access_key_id="census-key",
        aws_secret_access_key=SecretStr("census-secret"),
        aws_region="us-east-1",
    )
    with get_db_context() as db:
        if get_cloud_connection(db, CLOUD_CONNECTION, NOTEBOOK_OWNER_ID) is None:
            store_cloud_connection(db, connection, user_id=NOTEBOOK_OWNER_ID)
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        existing = service.repo.get_namespace_by_name(CLOUD_CATALOG, parent_id=None)
        if existing is not None:
            return existing.id
        return service.create_namespace(
            name=CLOUD_CATALOG,
            owner_id=NOTEBOOK_OWNER_ID,
            storage_uri="s3://notebook-census/catalog",
            storage_connection_name=CLOUD_CONNECTION,
        ).id


def _execute(client, flow_id: int, kernel_id: str, cell_id: str, code: str) -> dict:
    body = {"flow_id": flow_id, "kernel_id": kernel_id, "cell_id": cell_id, "code": code}
    response = client.post("/notebook/session/execute", json=body)
    assert response.status_code == 200, response.text
    return response.json()


def test_no_kernel_op_opens_the_catalog_over_the_corpus(notebook_corpus, open_as, client, kernel_sim, kernel_db_opens):
    """Every corpus flow: open its session in the kernel, run every rendered cell there, plan a push (the clean run
    in the kernel). No op opens the catalog database; the sites that did are listed when one does."""
    failed: dict[str, list[str]] = {}
    for name, graph in notebook_corpus:
        live = open_as(graph)
        key = {"flow_id": live.flow_id, "kernel_id": kernel_sim.kernel.id}
        opened = client.post("/notebook/session/open", json=key)
        assert opened.status_code == 200, (name, opened.text)
        rendering = render(live)
        for cell in rendering.cells:
            result = _execute(client, live.flow_id, kernel_sim.kernel.id, cell.cell_id, cell.code)
            if not result["success"]:
                failed.setdefault(name, []).append(f"{cell.cell_id}: {result['error']}")
        planned = client.post(
            "/notebook/plan",
            json={
                **key,
                "cells": [(c.cell_id, c.code) for c in rendering.cells],
                "provenance": cell_provenance(live, rendering),
                "code_fingerprint": rendering.code_fingerprint,
                "client_max_node_id": max(n.node_id for n in live.nodes),
            },
        )
        assert planned.status_code == 200, (name, planned.text)
    assert not failed, json.dumps(failed, indent=1)
    assert not kernel_db_opens, f"a kernel op opened the catalog database from {sorted(set(kernel_db_opens))}"
    assert {body["kind"] for body in kernel_sim.lookups} >= {"catalog_table", "flow_registration", "flow_registrations"}


def test_the_frame_lookups_answer_through_core(
    notebook_corpus, orders_flow, client, kernel_sim, kernel_db_opens, database_connection
):
    """``ff.kernels``, the catalog references, ``flow_ref`` and the connection listings in a cell are answered by
    core; the database connection comes without its password."""
    cell = (
        "print('kernels', sorted(ff.kernels))\n"
        "print('catalogs', sorted(c.name for c in ff.list_catalogs()))\n"
        "print('default', ff.default_schema())\n"
        f"schema = ff.get_catalog('Demo').get_schema({DEMO_SCHEMA.split('.')[1]!r})\n"
        "print('tables', sorted(t.name for t in schema.list_tables()))\n"
        f"print('flow', ff.flow_ref({DEMO_SCHEMA!r}, 'Clean orders').name, len(schema.list_flows()))\n"
        "print('cloud', [c.connection_name for c in ff.get_all_available_cloud_storage_connections()])\n"
        "print('databases', sorted(c.connection_name for c in ff.get_all_available_database_connections()))\n"
        f"conn = ff.get_database_connection_by_name({database_connection!r})\n"
        "print('connection', conn.connection_name, conn.database_type, repr(conn.password.get_secret_value()))\n"
    )
    result = _execute(client, orders_flow.flow_id, kernel_sim.kernel.id, "cell-lookups", cell)
    assert result["success"], result
    lines = dict(line.split(" ", 1) for line in result["stdout"].strip().splitlines())
    assert "Demo" in lines["catalogs"] and "General" in lines["catalogs"]
    assert lines["default"].startswith("SchemaReference(catalog='General', name='default'")
    assert lines["tables"] == "['regions', 'sales']"
    assert lines["flow"] == "Clean orders 1"
    assert database_connection in lines["databases"]
    assert lines["connection"] == f"{database_connection} sqlite ''"
    assert not kernel_db_opens, f"a kernel op opened the catalog database from {sorted(set(kernel_db_opens))}"
    kinds = {body["kind"] for body in kernel_sim.lookups}
    assert kinds >= {
        "kernels",
        "namespaces",
        "default_namespace_id",
        "namespace",
        "namespace_by_name",
        "tables",
        "namespace_id_by_full_name",
        "flow_registrations",
        "cloud_connections",
        "database_connections",
        "database_connection",
    }, kinds


def test_a_lookup_answers_metadata_only(notebook_corpus, database_connection, cloud_catalog, monkeypatch):
    """Every kind, answered for a catalog with a cloud-backed namespace, a database connection and a registered
    flow, carries no ciphertext and decrypts nothing."""
    from flowfile_frame import _metadata

    decrypted: list[str] = []
    original = Fernet.decrypt
    monkeypatch.setattr(Fernet, "decrypt", lambda self, *a, **k: (decrypted.append("decrypt"), original(self, *a, **k))[1])
    [registration] = _metadata.local_flow_registrations(name="Clean orders", user_id=NOTEBOOK_OWNER_ID)
    schema_id = _metadata.local_namespace_id_by_full_name(DEMO_SCHEMA, user_id=NOTEBOOK_OWNER_ID)
    reader = input_schema.NodeDatabaseReader(
        flow_id=1,
        node_id=1,
        user_id=NOTEBOOK_OWNER_ID,
        database_settings=input_schema.DatabaseSettings(
            connection_mode="reference", database_connection_name=database_connection, table_name="movies"
        ),
    )
    asked = {
        "catalog_table": {"node_id": 1, "catalog_table_name": "sales", "catalog_namespace_id": schema_id},
        "catalog_sql_tables": {"node_id": 1},
        "catalog_storage": {"namespace_id": cloud_catalog},
        "flow_registration": {"registration_id": registration.id, "flow_uuid": registration.flow_uuid},
        "placement_refusal": {"settings_type": "NodeDatabaseReader", "settings": reader.model_dump(mode="json")},
        "namespaces": {"parent_id": None},
        "namespace": {"namespace_id": schema_id},
        "namespace_by_name": {"name": "Demo", "parent_id": None},
        "default_namespace_id": {},
        "namespace_full_name": {"namespace_id": schema_id},
        "namespace_id_by_full_name": {"full_name": DEMO_SCHEMA},
        "tables": {"namespace_id": schema_id},
        "flow_registrations": {"namespace_id": schema_id},
        "kernels": {},
        "cloud_connections": {},
        "database_connection": {"name": database_connection},
        "database_connections": {},
    }
    assert set(asked) == set(lookup.KINDS), "every kind is covered here"
    answers = {kind: lookup.answer(kind, args, NOTEBOOK_OWNER_ID) for kind, args in asked.items()}
    text = json.dumps(answers)
    assert "$ffsec$" not in text and "census-secret" not in text, text
    assert decrypted == []
    assert answers["catalog_table"]["table_name"] == "sales" and answers["catalog_table"]["serialized_lf"] is None
    assert answers["catalog_storage"] == {
        "is_cloud": True,
        "base": "s3://notebook-census/catalog",
        "connection_name": CLOUD_CONNECTION,
    }
    assert answers["flow_registration"]["name"] == "Clean orders"
    assert answers["flow_registration"]["namespace"] == DEMO_SCHEMA
    assert answers["placement_refusal"] is None
    assert "password" not in answers["database_connection"]
    assert [c["connection_name"] for c in answers["cloud_connections"]] == [CLOUD_CONNECTION]
    with pytest.raises(ValueError, match="Unknown lookup"):
        lookup.answer("secrets", {}, NOTEBOOK_OWNER_ID)
    with pytest.raises(ValueError, match="No placement check"):
        lookup.answer("placement_refusal", {"settings_type": "NodeRead", "settings": {}}, NOTEBOOK_OWNER_ID)


def test_a_lookup_is_bound_to_the_kernels_session(orders_flow, kernel_sim, monkeypatch):
    from fastapi import HTTPException

    from flowfile_core.auth.models import User as PydanticUser
    from flowfile_core.notebook import kernel_runner, validate

    monkeypatch.setattr(validate, "MAX_PAYLOAD_BYTES", 4096)

    owner = PydanticUser(username="owner", id=kernel_sim.owner_id, disabled=False)
    other = PydanticUser(username="other", id=kernel_sim.owner_id + 1, disabled=False)
    body = lookup.LookupRequest(flow_id=orders_flow.flow_id, kind="default_namespace_id")
    with pytest.raises(HTTPException) as no_session:
        lookup.answer_request(kernel_sim.kernel.id, owner, body)
    assert no_session.value.status_code == 403 and "no notebook session" in no_session.value.detail
    kernel_runner._sessions[orders_flow.flow_id] = {kernel_sim.kernel.id: None}
    with pytest.raises(HTTPException) as not_owner:
        lookup.answer_request(kernel_sim.kernel.id, other, body)
    assert not_owner.value.status_code == 403
    assert isinstance(lookup.answer_request(kernel_sim.kernel.id, owner, body)["result"], int)
    with pytest.raises(HTTPException) as unknown:
        lookup.answer_request(kernel_sim.kernel.id, owner, body.model_copy(update={"kind": "secrets"}))
    assert unknown.value.status_code == 422 and "Unknown lookup" in unknown.value.detail
    huge = body.model_copy(update={"kind": "namespace", "args": {"namespace_id": "x" * 4096}})
    with pytest.raises(HTTPException) as oversized:
        lookup.answer_request(kernel_sim.kernel.id, owner, huge)
    assert oversized.value.status_code == 422 and "exceed" in oversized.value.detail
