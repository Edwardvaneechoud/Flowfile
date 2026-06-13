"""Optional demo catalog: one-call seed and one-call teardown.

Seeds a self-contained ``Demo`` catalog: sample tables under ``sales_analytics``
plus two flows imported from the bundled YAML in ``demo_flows/`` (a daily FX sync
under ``market`` and a sales-by-region summary under ``sales_analytics``). The
flows are the source of truth — edit the YAML, not Python. Removes the whole
``Demo`` subtree in one call. Idempotent.
"""

import datetime
import logging
import random
from pathlib import Path

import polars as pl

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.catalog_helpers import register_python_editor_flow
from flowfile_core.flowfile.flow_data_engine.sample_data import create_fake_data
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from shared.storage_config import storage

logger = logging.getLogger(__name__)

__all__ = ["seed_demo_catalog", "remove_demo_catalog"]

DEMO_CATALOG = "Demo"
SCHEMA_ANALYTICS = "sales_analytics"
SCHEMA_MARKET = "market"

FX_FLOW_NAME = "Daily FX Sync"
FX_CRON = "0 6 * * *"
SALES_FLOW_NAME = "Sales by Region"

DEMO_FLOWS_DIR = Path(__file__).parent / "demo_flows"


def _ensure_namespace(service, repo, db, name, parent_id, owner_id, description):
    """Get-or-create a public namespace, mirroring init_db's seeded containers."""
    existing = repo.get_namespace_by_name(name, parent_id)
    if existing is not None:
        return existing
    ns = service.create_namespace(name=name, owner_id=owner_id, parent_id=parent_id, description=description)
    ns.is_public = True
    db.commit()
    db.refresh(ns)
    return ns


def _register_delta_table(service, df, *, name, namespace_id, owner_id, description):
    """Write ``df`` as a managed Delta table and register it. Idempotent by name."""
    if any(t.name == name for t in service.list_tables(namespace_id=namespace_id)):
        return False
    table_dir = storage.catalog_tables_directory / f"demo_{name}"
    df.write_delta(str(table_dir), mode="overwrite")
    schema = [{"name": c, "dtype": str(dt)} for c, dt in df.schema.items()]
    size_bytes = sum(p.stat().st_size for p in table_dir.rglob("*") if p.is_file())
    service.register_table_from_data(
        name=name,
        table_path=str(table_dir),
        owner_id=owner_id,
        namespace_id=namespace_id,
        description=description,
        storage_format="delta",
        schema=schema,
        row_count=df.height,
        column_count=df.width,
        size_bytes=size_bytes,
    )
    return True


def _gen_regions() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "region": ["North", "South", "East", "West", "Central"],
            "region_name": [
                "Northern Territory",
                "Southern District",
                "Eastern Division",
                "Western Region",
                "Central Hub",
            ],
            "manager": ["Sarah Chen", "Michael Torres", "Jennifer Park", "Robert Kim", "Aisha Mensah"],
            "target_sales": [150000, 120000, 180000, 100000, 140000],
            "bonus_rate": [0.12, 0.10, 0.15, 0.08, 0.11],
        }
    )


def _gen_products() -> pl.DataFrame:
    rows = [
        (1, "Laptop Pro 15", "Electronics", 1299.99),
        (2, "Wireless Headphones", "Electronics", 149.99),
        (3, "Ergonomic Chair", "Furniture", 449.99),
        (4, "Standing Desk", "Furniture", 599.99),
        (5, "4K Monitor", "Electronics", 399.99),
        (6, "Mechanical Keyboard", "Electronics", 119.99),
        (7, "Desk Lamp", "Home", 39.99),
        (8, "Notebook Set", "Office", 14.99),
        (9, "Coffee Maker", "Home", 89.99),
        (10, "Webcam HD", "Electronics", 79.99),
        (11, "Office Plant", "Home", 24.99),
        (12, "Whiteboard", "Office", 64.99),
    ]
    return pl.DataFrame(rows, schema=["product_id", "product", "category", "unit_price"], orient="row")


def _gen_customers(n: int = 500) -> pl.DataFrame:
    fake = create_fake_data(n_records=n)
    return fake.with_row_index("customer_id", offset=1).select(
        "customer_id",
        pl.col("Name").alias("name"),
        pl.col("City").alias("city"),
        pl.col("Country").alias("country"),
        pl.col("Email").alias("email"),
    )


def _gen_sales(customers: pl.DataFrame, products: pl.DataFrame, regions: pl.DataFrame, n: int = 300) -> pl.DataFrame:
    rng = random.Random(42)
    cust_ids = customers["customer_id"].to_list()
    prods = products.to_dicts()
    regs = regions["region"].to_list()
    statuses = ["Completed", "Completed", "Completed", "Pending", "Cancelled"]
    today = datetime.date.today()
    rows = []
    for i in range(n):
        p = rng.choice(prods)
        qty = rng.randint(1, 8)
        rows.append(
            {
                "order_id": 1000 + i,
                "order_date": today - datetime.timedelta(days=rng.randint(0, 90)),
                "customer_id": rng.choice(cust_ids),
                "product": p["product"],
                "category": p["category"],
                "quantity": qty,
                "unit_price": p["unit_price"],
                "amount": round(qty * p["unit_price"], 2),
                "region": rng.choice(regs),
                "status": rng.choice(statuses),
            }
        )
    return pl.DataFrame(rows)


def _import_demo_flow(yaml_name: str, name: str, namespace_id: int, user_id: int):
    """Load a bundled demo flow, repoint its catalog writers, register it.

    Catalog *readers* resolve by full table name at load time (namespace-id
    independent); catalog *writers* resolve their target at run time, so
    repointing ``namespace_id`` after load is enough. Returns ``(registration_id, flow)``.
    """
    flow = open_flow(DEMO_FLOWS_DIR / yaml_name, user_id=user_id)
    flow.flow_settings.execution_location = "local"
    for node in flow.nodes:
        if node.node_type == "catalog_writer":
            node.setting_input.catalog_write_settings.namespace_id = namespace_id
    flow_path = storage.python_editor_flows_directory / yaml_name
    reg_id = register_python_editor_flow(
        flow, name=name, namespace_id=namespace_id, flow_path=str(flow_path), user_id=user_id
    )
    return reg_id, flow


def seed_demo_catalog(user_id: int = 1) -> dict:
    """Create the demo catalog (static tables + imported flows). Idempotent."""
    summary: dict = {
        "tables_created": [],
        "sales_flow_registration_id": None,
        "fx_flow_registration_id": None,
        "schedule_id": None,
        "fx_populate": "skipped",
    }

    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        service = CatalogService(repo)

        demo = _ensure_namespace(service, repo, db, DEMO_CATALOG, None, user_id, "Sample datasets for exploration")
        analytics = _ensure_namespace(
            service, repo, db, SCHEMA_ANALYTICS, demo.id, user_id, "Sample sales and customer data"
        )
        market = _ensure_namespace(service, repo, db, SCHEMA_MARKET, demo.id, user_id, "Daily-synced market data")
        market_id = market.id
        analytics_id = analytics.id

        regions = _gen_regions()
        products = _gen_products()
        customers = _gen_customers()
        sales = _gen_sales(customers, products, regions)
        static_tables = [
            ("regions", regions, "Sales regions with targets and managers"),
            ("products", products, "Product catalog with prices"),
            ("customers", customers, "Sample customer records"),
            ("sales", sales, "Sample order transactions over the last 90 days"),
        ]
        for name, df, desc in static_tables:
            if _register_delta_table(
                service, df, name=name, namespace_id=analytics.id, owner_id=user_id, description=desc
            ):
                summary["tables_created"].append(name)

    # Sales-by-region flow: reads the (now-registered) demo sales table; static
    # source, so just run it once at seed time.
    sales_reg, sales_flow = _import_demo_flow("demo_sales_by_region.yaml", SALES_FLOW_NAME, analytics_id, user_id)
    summary["sales_flow_registration_id"] = sales_reg
    try:
        sales_flow.run_graph()
    except Exception:
        logger.info("Sales-by-region populate skipped", exc_info=True)

    # Daily FX sync flow: schedule it and trigger an immediate first run.
    fx_reg, _ = _import_demo_flow("demo_fx_sync.yaml", FX_FLOW_NAME, market_id, user_id)
    summary["fx_flow_registration_id"] = fx_reg
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        existing = service.list_schedules(registration_id=fx_reg)
        if existing:
            schedule_id = existing[0].id
        else:
            sched = service.create_schedule(
                registration_id=fx_reg,
                owner_id=user_id,
                schedule_type="cron",
                cron_expression=FX_CRON,
                cron_timezone="UTC",
                name=FX_FLOW_NAME,
                description="Daily FX rate sync",
            )
            schedule_id = sched.id
        summary["schedule_id"] = schedule_id

        # Best-effort immediate populate; never fail the seed.
        try:
            service.trigger_schedule_now(schedule_id, user_id)
            summary["fx_populate"] = "triggered"
        except Exception:
            logger.info("Immediate FX populate skipped", exc_info=True)

    logger.info("Demo catalog seeded: %s", summary)
    return summary


def remove_demo_catalog(user_id: int = 1) -> dict:
    """Remove everything under the ``Demo`` catalog (tables, flows, schedules, namespaces)."""
    summary: dict = {"removed": False, "tables": 0, "flows": 0, "schedules": 0, "namespaces": 0}

    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        service = CatalogService(repo)

        demo = repo.get_namespace_by_name(DEMO_CATALOG, None)
        if demo is None:
            return summary

        for schema in repo.list_child_namespaces(demo.id):
            for flow in service.list_flows(user_id, namespace_id=schema.id):
                for sched in service.list_schedules(registration_id=flow.id):
                    service.delete_schedule(sched.id)
                    summary["schedules"] += 1
                service.delete_flow(flow.id, delete_file=True)
                summary["flows"] += 1
            for table in service.list_tables(namespace_id=schema.id):
                service.delete_table(table.id, delete_file=True)
                summary["tables"] += 1
            service.delete_namespace(schema.id)
            summary["namespaces"] += 1

        service.delete_namespace(demo.id)
        summary["namespaces"] += 1
        summary["removed"] = True

    logger.info("Demo catalog removed: %s", summary)
    return summary
