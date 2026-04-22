"""A long, narrative showcase of flowfile_frame.

Walks through the Polars-like LazyFrame API end-to-end and the new
Python-as-first-class-catalog-citizen features added on top. Every
transformation is lazy and composes into a single FlowGraph — nothing
actually touches the catalog or materialises until ``.execute()`` is called.

Run it directly::

    poetry run python examples/catalog_showcase.py

The script is idempotent: it creates a demo catalog called ``Lakehouse`` with
``raw`` / ``mart`` schemas, writes a handful of tables, then queries them
back. Feel free to re-run — later writes overwrite by default.
"""

from __future__ import annotations

import flowfile as ff

# ---------------------------------------------------------------------------
# 1. Catalog setup
# ---------------------------------------------------------------------------
# Catalogs (level 0) hold schemas (level 1) hold tables. All three are
# Python-addressable. create_* calls are "if not exists" via try/except —
# we just swallow ambiguity on re-runs.


def ensure(fn, *a, **kw):
    """Call ``fn`` tolerating already-exists errors on re-runs."""
    try:
        return fn(*a, **kw)
    except Exception:
        return None


# Clear any leftovers from earlier runs so the demo is fully idempotent.
try:
    from flowfile_core.catalog import CatalogService
    from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context

    with get_db_context() as db:
        svc = CatalogService(SQLAlchemyCatalogRepository(db))
        for t in svc.list_tables():
            ns = svc.repo.get_namespace(t.namespace_id) if t.namespace_id else None
            parent = svc.repo.get_namespace(ns.parent_id) if ns and ns.parent_id else None
            if parent is not None and parent.name == "Lakehouse":
                svc.delete_table(t.id)
except Exception:
    pass

ensure(ff.create_catalog, "Lakehouse", description="Demo catalog")
ensure(ff.create_schema, "raw", catalog="Lakehouse")
ensure(ff.create_schema, "mart", catalog="Lakehouse")

print("Catalogs:", [c.name for c in ff.list_catalogs()])
print("Schemas under Lakehouse:", [s.name for s in ff.list_schemas(catalog="Lakehouse")])


# ---------------------------------------------------------------------------
# 2. Building raw data in memory (normally this would be read_csv / scan_parquet)
# ---------------------------------------------------------------------------
# from_dict returns a FlowFrame. Every subsequent call is lazy — no
# computation happens yet. It just adds nodes to an internal graph.

customers = ff.from_dict(
    {
        "customer_id": [1, 2, 3, 4, 5, 6],
        "name": ["Alice", "Bob", "Charlie", "Dana", "Eve", "Frank"],
        "country": ["NL", "DE", "NL", "FR", "DE", "NL"],
        "signup_year": [2021, 2022, 2022, 2023, 2023, 2024],
    }
)

products = ff.from_dict(
    {
        "product_id": [101, 102, 103, 104],
        "product": ["Notebook", "Pen", "Eraser", "Ruler"],
        "category": ["writing", "writing", "writing", "drawing"],
        "unit_price": [8.50, 1.20, 0.75, 1.90],
    }
)

orders = ff.from_dict(
    {
        "order_id": [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009],
        "customer_id": [1, 1, 2, 3, 3, 4, 5, 5, 6],
        "product_id": [101, 102, 101, 103, 101, 102, 104, 101, 103],
        "qty": [1, 3, 2, 10, 1, 5, 4, 2, 2],
        "order_date": [
            "2024-01-03", "2024-01-05", "2024-01-08",
            "2024-02-01", "2024-02-02", "2024-02-15",
            "2024-03-01", "2024-03-04", "2024-03-10",
        ],
    }
)


# ---------------------------------------------------------------------------
# 3. Physical writes to the catalog
# ---------------------------------------------------------------------------
# No explicit table name given — the table name is inferred from the variable
# (`customers` → "customers"). Wrapping the writes in catalog_context groups
# them under one FlowRegistration so lineage is clean.

with ff.catalog_context(name="raw_ingest", flow_path="<demo>/raw_ingest"):
    customers.write_catalog_table(schema="Lakehouse.raw")
    products.write_catalog_table(schema="Lakehouse.raw")
    orders.write_catalog_table(schema="Lakehouse.raw")

    # Nothing has actually been written yet — writes are queued as graph nodes.
    # .execute() runs the whole graph and records a FlowRun for lineage.
    customers.execute()
    products.execute()
    orders.execute()

print("\nTables in Lakehouse.raw:")
for t in ff.list_tables(schema="Lakehouse.raw"):
    print(f"  {t.full_table_name}  type={t.table_type}  produced_by={t.source_registration_name}")


# ---------------------------------------------------------------------------
# 4. Read, transform, compose — the lazy API
# ---------------------------------------------------------------------------
# Bring the raw tables back as FlowFrames. Everything from here is lazy.

cust = ff.read_catalog_table("customers", schema="Lakehouse.raw")
prod = ff.read_catalog_table("products", schema="Lakehouse.raw")
ord_ = ff.read_catalog_table("orders", schema="Lakehouse.raw")

# Polars-style column expressions
order_lines = (
    ord_.join(cust, on="customer_id", how="left")
    .join(prod, on="product_id", how="left")
    .with_columns(
        (ff.col("qty") * ff.col("unit_price")).alias("line_total"),
        ff.col("order_date").str.to_date("%Y-%m-%d").alias("order_date"),
    )
)

# when/then/otherwise for a derived categorical
order_lines = order_lines.with_columns(
    ff.when(ff.col("line_total") > 10.0)
    .then(ff.lit("high"))
    .when(ff.col("line_total") > 2.0)
    .then(ff.lit("mid"))
    .otherwise(ff.lit("low"))
    .alias("size_bucket")
)

# Aggregation: revenue per country + category
country_cat_revenue = (
    order_lines.group_by("country", "category")
    .agg(
        ff.sum("line_total").alias("revenue"),
        ff.count("order_id").alias("n_orders"),
        ff.mean("qty").alias("avg_qty"),
    )
    .sort("revenue", descending=True)
)

# Different aggregation: total spend per customer
per_customer_spend = (
    order_lines.group_by("customer_id", "name")
    .agg(
        ff.sum("line_total").alias("total_spend"),
        ff.count("order_id").alias("n_orders"),
    )
    .sort("total_spend", descending=True)
)


# ---------------------------------------------------------------------------
# 5. Explicit flow binding + physical writes of the derived tables
# ---------------------------------------------------------------------------
# Wrap a block in catalog_context to give the flow a meaningful name and a
# stable path. Everything written inside this block gets grouped under one
# FlowRegistration for clean lineage.
#
# (Virtual writes — write_mode="virtual" — also work, but re-reading a virtual
#  table requires the producing flow to exist on disk as a .flowfile.yaml.
#  Running this .py script wouldn't satisfy that; see the integration tests
#  in flowfile_frame/tests/test_catalog_integration.py for the virtual flow.)

with ff.catalog_context(name="daily_mart_refresh", flow_path="<demo>/daily_mart_refresh"):
    country_cat_revenue.write_catalog_table(schema="Lakehouse.mart")
    per_customer_spend.write_catalog_table(schema="Lakehouse.mart")

    # One execute() fires everything added above in dependency order.
    country_cat_revenue.execute()
    per_customer_spend.execute()


# ---------------------------------------------------------------------------
# 6. Selectors, row ops, concat, pivot, unpivot
# ---------------------------------------------------------------------------
# A grab-bag tour of the expression API to show what flowfile_frame carries
# over from Polars.

# Selectors — pick columns by type, name pattern, etc.
print("\nCustomer numeric + string columns:")
print(cust.select(ff.numeric(), ff.string()).collect())

# Row-index + head
print("\nFirst 3 orders with a stable row id:")
print(ord_.with_row_index("rn").head(3).collect())

# Concatenation (stacking two frames vertically)
vip_customers = ff.from_dict(
    {
        "customer_id": [7, 8],
        "name": ["Grace", "Henry"],
        "country": ["NL", "DE"],
        "signup_year": [2024, 2024],
    }
)
all_customers = ff.concat([cust, vip_customers], how="vertical_relaxed")
print("\nCustomers after VIP append:", all_customers.collect().height, "rows")

# Pivot: revenue per (country × category) to a wide shape
wide_revenue = country_cat_revenue.pivot(
    on="category", index="country", values="revenue", aggregate_function="sum"
)
print("\nRevenue pivoted wide:")
print(wide_revenue.collect())

# Unpivot the pivot back to long form
long_again = wide_revenue.unpivot(index="country", variable_name="category", value_name="revenue")
print("\n...and back to long:")
print(long_again.collect())


# ---------------------------------------------------------------------------
# 7. SQL against the catalog (Polars SQLContext under the hood)
# ---------------------------------------------------------------------------
# Every Delta table in the catalog becomes a table in Polars' SQLContext.
# Note: virtual tables aren't included — only materialised Delta.

top_countries = ff.read_catalog_sql(
    """
    SELECT country, SUM(line_total) AS revenue
    FROM (
        SELECT c.country, (o.qty * p.unit_price) AS line_total
        FROM orders o
        JOIN customers c USING (customer_id)
        JOIN products  p USING (product_id)
    )
    GROUP BY country
    ORDER BY revenue DESC
    """
)
print("\nTop countries via SQL:")
print(top_countries.collect())


# ---------------------------------------------------------------------------
# 8. Introspecting the catalog from Python
# ---------------------------------------------------------------------------
print("\n--- Catalog contents ---")
for cat in ff.list_catalogs():
    print(f"[{cat.id}] {cat.name}: {cat.description or ''}")
    for schema in ff.list_schemas(catalog=cat.id):
        print(f"  └─ {schema.name}")
        for tbl in ff.list_tables(schema=schema.id):
            producer = tbl.source_registration_name or "—"
            rows = tbl.row_count if tbl.row_count is not None else "?"
            print(
                f"     • {tbl.name:24s} "
                f"type={tbl.table_type:8s} rows={rows:>5}  produced_by={producer}"
            )


# ---------------------------------------------------------------------------
# 9. Drill into a single mart table
# ---------------------------------------------------------------------------
# Every table read is itself a FlowFrame — chain more transforms before collect.
mart = ff.read_catalog_table("per_customer_spend", schema="Lakehouse.mart")
print("\nTop 3 customers by spend:")
print(mart.head(3).collect())

print("\nDone. Explore the 'Lakehouse' catalog in the Flowfile UI to see the")
print("FlowRegistration + FlowRun rows, lineage links, and snapshotted flows.")
