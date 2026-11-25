"""A small catalog with a semantic model over real Delta tables, shared by the semantic and the sharing tests.

``<catalog>.sales.orders`` (many) joins ``<catalog>.crm.customers`` (one) on ``customer_id``. Every name is unique per
world, so ledger facts of one test never meet another's. Facts are written straight into the ledger with ``say`` (as
a person, accepted unless told otherwise); the routes are what the tests exercise.
"""

from __future__ import annotations

import json
import shutil
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl

from flowfile_core.catalog.semantic import activity, host, service
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogNamespace, CatalogTable
from shared.storage_config import storage

ORDERS = pl.DataFrame(
    {
        "order_id": [1, 2, 3, 4, 5, 6],
        "customer_id": [10, 10, 11, 12, 12, 12],
        "amount": [5.0, 7.5, 12.0, 3.0, 4.5, 8.0],
        "region": ["West", "East", "West", "North", "East", "West"],
        "card_number": ["CARDVAL-0001", "CARDVAL-0002", "CARDVAL-0003", "CARDVAL-0004", "CARDVAL-0005", "CARDVAL-0006"],
    }
)
CUSTOMERS = pl.DataFrame(
    {
        "customer_id": [10, 11, 12],
        "email": ["mailval-ann@example.org", "mailval-bob@example.org", "mailval-cy@example.org"],
        "segment": ["Retail", "Retail", "Business"],
    }
)


@dataclass
class World:
    catalog: str
    catalog_id: int
    sales_id: int
    crm_id: int
    orders_id: int
    customers_id: int
    paths: tuple[Path, ...]
    extra: list[tuple[int, Path, str]] = field(default_factory=list)

    def key(self, table: str) -> str:
        return f"{self.catalog}.sales.orders" if table == "orders" else f"{self.catalog}.crm.customers"

    def table_id(self, table: str) -> int:
        return self.orders_id if table == "orders" else self.customers_id

    def field(self, table: str, column: str) -> str:
        return f"field:{self.key(table)}/{column}"

    def dataset(self, table: str) -> str:
        return f"dataset:{self.key(table)}"


def _tables_root() -> Path:
    """Where the session's worker reads catalog tables by directory name: its own ``catalog_tables`` folder, which in
    docker mode is not core's (core's sits under the user data directory the sharing fixture points elsewhere)."""
    root = storage.base_directory / "catalog_tables"
    root.mkdir(parents=True, exist_ok=True)
    return root


def add_table(world: World, schema_id: int, name: str, df: pl.DataFrame, owner_id: int) -> int:
    """Another real Delta table in one of the world's schemas, synced into the ledger; dropped with the world."""
    path = _tables_root() / f"{world.catalog}_{name}"
    df.write_delta(str(path))
    with get_db_context() as db:
        row = CatalogTable(
            name=name,
            namespace_id=schema_id,
            owner_id=owner_id,
            file_path=str(path),
            storage_format="delta",
            schema_json=_schema(df),
            table_type="physical",
            row_count=df.height,
            column_count=df.width,
        )
        db.add(row)
        db.commit()
        table_id = row.id
    world.extra.append((table_id, path, f"{world.catalog}.{'sales' if schema_id == world.sales_id else 'crm'}.{name}"))
    service.maybe_sync(force=True)
    return table_id


def _schema(df: pl.DataFrame) -> str:
    return json.dumps([{"name": n, "dtype": str(t)} for n, t in df.schema.items()])


def build_world(owner_id: int = 1, *, schema_owner_id: int | None = None) -> World:
    """The catalog, its two schemas and two tables, written as real Delta tables, synced into the ledger."""
    name = f"sem{uuid.uuid4().hex[:10]}"
    root = _tables_root()
    paths = (root / f"{name}_orders", root / f"{name}_customers")
    ORDERS.write_delta(str(paths[0]))
    CUSTOMERS.write_delta(str(paths[1]))
    with get_db_context() as db:
        cat = CatalogNamespace(name=name, level=0, owner_id=owner_id)
        db.add(cat)
        db.flush()
        schema_owner = owner_id if schema_owner_id is None else schema_owner_id
        sales = CatalogNamespace(name="sales", level=1, parent_id=cat.id, owner_id=schema_owner)
        crm = CatalogNamespace(name="crm", level=1, parent_id=cat.id, owner_id=schema_owner)
        db.add_all([sales, crm])
        db.flush()
        tables = []
        for ns, table, df, path in ((sales, "orders", ORDERS, paths[0]), (crm, "customers", CUSTOMERS, paths[1])):
            row = CatalogTable(
                name=table,
                namespace_id=ns.id,
                owner_id=schema_owner,
                file_path=str(path),
                storage_format="delta",
                schema_json=_schema(df),
                table_type="physical",
                row_count=df.height,
                column_count=df.width,
            )
            db.add(row)
            tables.append(row)
        db.commit()
        world = World(name, cat.id, sales.id, crm.id, tables[0].id, tables[1].id, paths)
    service.maybe_sync(force=True)
    return world


def drop_world(world: World) -> None:
    """Delete the world as the catalog does, the ledger told of each table (SQLite reuses the row ids, and a pull sync
    would read a reused id with the same columns as a rename)."""
    extra_ids = [table_id for table_id, _path, _key in world.extra]
    with get_db_context() as db:
        db.query(CatalogTable).filter(CatalogTable.id.in_([world.orders_id, world.customers_id, *extra_ids])).delete()
        db.query(CatalogNamespace).filter(CatalogNamespace.id.in_([world.sales_id, world.crm_id])).delete()
        db.query(CatalogNamespace).filter(CatalogNamespace.id == world.catalog_id).delete()
        db.commit()
    for key in [world.key("orders"), world.key("customers"), *(key for _id, _path, key in world.extra)]:
        host.on_table_deleted(key)
    for path in [*world.paths, *(path for _id, path, _key in world.extra)]:
        shutil.rmtree(path, ignore_errors=True)
    reset_process_state()


def reset_process_state() -> None:
    """What the semantic code keeps per process: the last sync, the known values and the Suggest runs."""
    from flowfile_core.ai import chart_profile, semantic_routes

    service._last_sync = 0.0
    chart_profile.clear_cache()
    semantic_routes._known_cache.clear()
    with activity._lock:
        activity._running.clear()
        activity._last.clear()


def say(subject: str, attribute: str, value: Any, *, user_id: int = 1, accept: bool = True) -> Any:
    """A fact stated by user ``user_id`` as an admin, straight into the ledger (accepted unless ``accept`` is False)."""
    from glosslayer_flowfile import actor_for

    view = host.host().embedded.view(user_id=user_id, admin=True)
    return view.propose(subject, attribute, value, source=str(actor_for(user_id)), accept=accept)
