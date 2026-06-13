"""Optional demo catalog: one-call seed and one-call teardown.

Seeds a self-contained ``Demo`` catalog (sample tables under ``sales_analytics`` plus
a daily FX-sync flow under ``market``) and removes it as a single subtree. Idempotent.
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
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, schemas, transform_schema
from shared.storage_config import storage

logger = logging.getLogger(__name__)

__all__ = ["seed_demo_catalog", "remove_demo_catalog"]

DEMO_CATALOG = "Demo"
SCHEMA_ANALYTICS = "sales_analytics"
SCHEMA_MARKET = "market"

FX_FLOW_NAME = "Daily FX Sync"
FX_TABLE = "fx_rates"
FX_LOG_TABLE = "fx_sync_log"
FX_FLOW_ID = 920001
FX_URL = "https://api.frankfurter.app/latest?base=USD"
FX_CRON = "0 6 * * *"

SALES_FLOW_NAME = "Sales by Region"
SALES_FLOW_ID = 920002
SALES_SUMMARY_TABLE = "sales_by_region"

# Major currencies present in the frankfurter.app USD-base response.
FX_CURRENCIES = ["GBP", "JPY", "CHF", "AUD", "CAD", "CNY"]


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


def _build_fx_flow(user_id: int, market_namespace_id: int, flow_path: Path) -> FlowGraph:
    """Build the FX-sync flow.

    REST read -> select majors -> unpivot (numeric) -> filter -> name columns,
    then two outputs: a date-cast long ``fx_rates`` table and a ``fx_sync_log``
    stats row per sync date.
    """
    flow_settings = schemas.FlowSettings(
        flow_id=FX_FLOW_ID,
        name=FX_FLOW_NAME,
        path=str(flow_path),
        description="Daily ECB foreign-exchange rates from frankfurter.app",
        track_history=False,
    )
    graph = FlowGraph(flow_settings=flow_settings)
    graph.flow_settings.execution_location = "local"

    graph.add_rest_api_reader(
        input_schema.NodeRestApiReader(
            flow_id=FX_FLOW_ID,
            node_id=1,
            user_id=user_id,
            description="Fetch latest USD FX rates",
            rest_api_settings=input_schema.RestApiSettings(url=FX_URL, method="GET", record_path=""),
        )
    )
    majors = [
        transform_schema.SelectInput(old_name="date", new_name="date", keep=True),
        transform_schema.SelectInput(old_name="amount", new_name="amount", keep=True),
    ]
    majors += [transform_schema.SelectInput(old_name=f"rates.{c}", new_name=c, keep=True) for c in FX_CURRENCIES]
    graph.add_select(
        input_schema.NodeSelect(
            flow_id=FX_FLOW_ID,
            node_id=2,
            user_id=user_id,
            depending_on_id=1,
            keep_missing=False,
            description="Keep date + major currencies",
            select_input=majors,
        )
    )
    graph.add_unpivot(
        input_schema.NodeUnpivot(
            flow_id=FX_FLOW_ID,
            node_id=3,
            user_id=user_id,
            depending_on_id=2,
            description="Currencies to long format",
            unpivot_input=transform_schema.UnpivotInput(
                index_columns=["date", "amount"],
                data_type_selector="numeric",
                data_type_selector_mode="data_type",
            ),
        )
    )
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=FX_FLOW_ID,
            node_id=9,
            user_id=user_id,
            depending_on_id=3,
            description="variable != amount",
            filter_input=transform_schema.FilterInput(
                mode="basic",
                basic_filter=transform_schema.BasicFilter(field="variable", operator="not_equals", value="amount"),
            ),
        )
    )
    graph.add_select(
        input_schema.NodeSelect(
            flow_id=FX_FLOW_ID,
            node_id=4,
            user_id=user_id,
            depending_on_id=9,
            keep_missing=False,
            description="Name currency/rate columns",
            select_input=[
                transform_schema.SelectInput(old_name="date", new_name="date", keep=True),
                transform_schema.SelectInput(old_name="variable", new_name="currency", keep=True),
                transform_schema.SelectInput(old_name="value", new_name="rate", keep=True),
            ],
        )
    )
    graph.add_formula(
        input_schema.NodeFormula(
            flow_id=FX_FLOW_ID,
            node_id=8,
            user_id=user_id,
            depending_on_id=4,
            description="date = to_date([date])",
            function=transform_schema.FunctionInput(
                field=transform_schema.FieldInput(name="date", data_type="Datetime"),
                function="to_date([date])",
            ),
        )
    )
    # Output 1: the long fx_rates table (date cast to a real date).
    graph.add_catalog_writer(
        input_schema.NodeCatalogWriter(
            flow_id=FX_FLOW_ID,
            node_id=5,
            user_id=user_id,
            depending_on_id=8,
            description=f"Upsert into {FX_TABLE}",
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name=FX_TABLE,
                namespace_id=market_namespace_id,
                write_mode="upsert",
                merge_keys=["date", "currency"],
                description="Daily USD exchange rates for major currencies (ECB via frankfurter.app)",
            ),
        )
    )
    # Output 2: a per-sync stats row that accumulates into a log over days.
    graph.add_group_by(
        input_schema.NodeGroupBy(
            flow_id=FX_FLOW_ID,
            node_id=6,
            user_id=user_id,
            depending_on_id=4,
            description="Per-date sync stats",
            groupby_input=transform_schema.GroupByInput(
                agg_cols=[
                    transform_schema.AggColl("date", "groupby", "date"),
                    transform_schema.AggColl("currency", "count", "currencies"),
                    transform_schema.AggColl("rate", "min", "min_rate"),
                    transform_schema.AggColl("rate", "max", "max_rate"),
                    transform_schema.AggColl("rate", "mean", "avg_rate"),
                ]
            ),
        )
    )
    graph.add_catalog_writer(
        input_schema.NodeCatalogWriter(
            flow_id=FX_FLOW_ID,
            node_id=7,
            user_id=user_id,
            depending_on_id=6,
            description=f"Upsert into {FX_LOG_TABLE}",
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name=FX_LOG_TABLE,
                namespace_id=market_namespace_id,
                write_mode="upsert",
                merge_keys=["date"],
                description="Daily FX sync log: currency count and rate stats per sync date",
            ),
        )
    )
    for a, b in ((1, 2), (2, 3), (3, 9), (9, 4), (4, 8), (8, 5), (4, 6), (6, 7)):
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(a, b, "main"))
    graph.apply_layout()
    return graph


def _build_sales_flow(user_id: int, analytics_namespace_id: int, flow_path: Path) -> FlowGraph:
    """Build the sales-summary flow: read demo sales -> aggregate -> catalog write."""
    flow_settings = schemas.FlowSettings(
        flow_id=SALES_FLOW_ID,
        name=SALES_FLOW_NAME,
        path=str(flow_path),
        description="Aggregate the demo sales table into revenue per region",
        track_history=False,
    )
    graph = FlowGraph(flow_settings=flow_settings)
    graph.flow_settings.execution_location = "local"

    graph.add_catalog_reader(
        input_schema.NodeCatalogReader(
            flow_id=SALES_FLOW_ID,
            node_id=1,
            user_id=user_id,
            catalog_table_name="sales",
            catalog_namespace_id=analytics_namespace_id,
            description="Read demo sales table",
        )
    )
    graph.add_group_by(
        input_schema.NodeGroupBy(
            flow_id=SALES_FLOW_ID,
            node_id=2,
            user_id=user_id,
            depending_on_id=1,
            description="Revenue per region",
            groupby_input=transform_schema.GroupByInput(
                agg_cols=[
                    transform_schema.AggColl("region", "groupby", "region"),
                    transform_schema.AggColl("unit_price", "count", "order_count"),
                    transform_schema.AggColl("amount", "sum", "total_revenue"),
                    transform_schema.AggColl("amount", "mean", "avg_order_value"),
                ]
            ),
        )
    )
    graph.add_catalog_writer(
        input_schema.NodeCatalogWriter(
            flow_id=SALES_FLOW_ID,
            node_id=3,
            user_id=user_id,
            depending_on_id=2,
            description=f"Write {SALES_SUMMARY_TABLE}",
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name=SALES_SUMMARY_TABLE,
                namespace_id=analytics_namespace_id,
                write_mode="overwrite",
                description="Revenue per region, derived from the demo sales table",
            ),
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2, "main"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3, "main"))
    graph.apply_layout()
    return graph


def seed_demo_catalog(user_id: int = 1) -> dict:
    """Create the demo catalog (static tables + daily FX-sync flow). Idempotent."""
    summary: dict = {
        "tables_created": [],
        "flow_registration_id": None,
        "sales_flow_registration_id": None,
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

    # Sales-by-region flow: reads the demo sales table and writes an aggregated
    # summary table. Source data is static, so it just runs once at seed time.
    sales_flow_path = storage.python_editor_flows_directory / "demo_sales_by_region.yaml"
    sales_graph = _build_sales_flow(user_id, analytics_id, sales_flow_path)
    summary["sales_flow_registration_id"] = register_python_editor_flow(
        sales_graph, name=SALES_FLOW_NAME, namespace_id=analytics_id, flow_path=str(sales_flow_path), user_id=user_id
    )
    try:
        sales_graph.execution_location = "local"
        sales_graph.run_graph()
    except Exception:
        logger.info("Sales-by-region populate skipped", exc_info=True)

    fx_flow_path = storage.python_editor_flows_directory / "demo_fx_sync.yaml"
    graph = _build_fx_flow(user_id, market_id, fx_flow_path)
    reg_id = register_python_editor_flow(
        graph, name=FX_FLOW_NAME, namespace_id=market_id, flow_path=str(fx_flow_path), user_id=user_id
    )
    summary["flow_registration_id"] = reg_id

    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        existing = service.list_schedules(registration_id=reg_id)
        if existing:
            schedule_id = existing[0].id
        else:
            sched = service.create_schedule(
                registration_id=reg_id,
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

        schemas_under = repo.list_child_namespaces(demo.id)
        for schema in schemas_under:
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
