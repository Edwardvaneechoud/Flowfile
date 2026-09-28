"""Flowfile showcase: read the catalog, publish reusable flows, gate the analysis, write back to the catalog.

``main()`` publishes the 'Clean orders' child flow, builds 'Sales analytics' on it, runs it once per gate
mode and opens it in the Flowfile designer. Call ``main()`` to run it; there is no ``__main__`` guard because
the notebook tests exec this file as a cell.
"""

import time

import polars as pl

import flowfile as fl

CATALOG, SCHEMA = "Demo", "sales_analytics"

ORDERS_CLEAN = fl.FlowOutput("orders_clean")
MIN_AMOUNT = fl.Parameter("min_amount", default=0, type="integer", description="Drop orders below this amount")
MODE = fl.Parameter("mode", default="full", type="enum", enum_values=["full", "quick"])
KERNEL = "lite"  # a kernel id, see Flowfile app > Settings > Execution > Python Kernels
TARGET_PCT = fl.Parameter("target_pct", default=100, type="integer", description="Share of target that counts as met")


# A notebook node as a function: `# %%` splits the body into cells, `monthly` is the input, the return the output.
@fl.python_script(
    kernel=KERNEL,
    outputs=["forecast"],
    returns={"month": fl.Int64, "revenue_forecast": fl.Float64, "growing": fl.Boolean},
    description="Revenue trend forecast",
)
def forecast(monthly: pl.LazyFrame) -> pl.DataFrame:
    """Revenue trend: a least-squares line through monthly revenue, extended three months."""
    import numpy as np

    df = monthly.collect()

    # %% [markdown]
    # ## Fit
    # One slope for the whole period; good enough for a demo, not for a quarter close.

    # %%
    slope, intercept = np.polyfit(df["month"], df["revenue"], deg=1)
    ahead = np.arange(df["month"].max() + 1, df["month"].max() + 4)

    # %% Forecast
    projected = pl.DataFrame({"month": ahead, "revenue_forecast": np.round(slope * ahead + intercept, 2)})
    return projected.with_columns(growing=pl.lit(slope > 0))


def banner(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def publish_clean_orders(schema):
    """Step 1: a reusable child flow (typed parameter filter + derived columns), published to the catalog."""
    child = fl.create_flow_graph()
    fl.add_flow_parameter(child, MIN_AMOUNT)
    # The input port: a parent's RunFlow feeds it; on its own it holds real sample rows from the catalog.
    orders = fl.FlowInput("orders", sample=schema.read_table("sales").head(20).collect(), flow_graph=child)
    clean = orders.filter((fl.col("status") == "Completed") & (fl.col("amount") >= MIN_AMOUNT)).with_columns(
        fl.col("order_date").dt.month().alias("month"),
        (fl.col("amount") * 0.35).round(2).alias("margin"),
    )
    clean.to_flow_output(ORDERS_CLEAN)
    return schema.register_flow(child, name="Clean orders", overwrite=True)


def build_sales_analytics(schema, clean_ref):
    """Step 2: the parent flow. Runs the published child, enriches, analyses, gates and writes back."""
    sales = schema.read_table("sales")
    regions = schema.read_table("regions")

    # The child's output only exists once the flow runs: `clean` is a deferred frame, nothing executes here.
    run = fl.RunFlow(clean_ref, orders=sales, params={"min_amount": 25}, description="Clean orders (published flow)")
    clean = run.get_output(ORDERS_CLEAN)
    enriched = clean.join(regions, on="region", how="left")

    monthly = (
        enriched.group_by("month")
        .agg(fl.col("amount").sum().alias("revenue"), fl.col("margin").sum().alias("margin"))
        .sort("month")
        .with_columns((fl.col("revenue") / fl.col("revenue").shift(1) - 1).round(3).alias("mom_growth"))
    )
    monthly.write_catalog_table("sales_monthly", schema=schema)

    # A notebook step: its cells run on a Docker kernel when the flow runs, so its output is deferred too.
    trend = forecast(monthly)
    # returns= typed that output, so it filters before anything runs: only an upward trend publishes a forecast.
    trend.filter(fl.col("growing")).write_catalog_table("sales_forecast", schema=schema)

    top_products = (
        enriched.group_by("region", "product")
        .agg(fl.col("amount").sum().alias("revenue"))
        .with_columns(fl.col("revenue").rank(method="dense", descending=True).over("region").alias("rank"))
        .filter(fl.col("rank") <= 3)
        .sort("region", "rank")
    )
    top_products.write_catalog_table("sales_top_products", schema=schema)

    revenue_per_region = fl.sql(
        """
        SELECT r.region, r.manager, ROUND(SUM(o.amount), 2) AS revenue, r.target_sales
        FROM orders o JOIN regions r ON o.region = r.region
        GROUP BY r.region, r.manager, r.target_sales
        """,
        orders=clean,
        regions=regions,
        description="Revenue per region",
    )
    vs_target = revenue_per_region.with_columns(
        (100 * fl.col("revenue") / fl.col("target_sales")).round(1).alias("pct_of_target")
    ).sort("pct_of_target", descending=True)
    # An installed custom node, by its key; the threshold is a flow parameter, resolved when the flow runs.
    fl.add_flow_parameter(vs_target, TARGET_PCT)
    vs_target = fl.custom_nodes.mood_emoji(
        vs_target,
        source_column="pct_of_target",
        threshold_value=TARGET_PCT,
        emoji_column_name="mood",
        add_random_sparkle=False,
    )
    vs_target.write_catalog_table("sales_vs_target", schema=schema)

    # A gate on an enum parameter: only one exit is live per run; the writers below it write on that side only.
    fl.add_flow_parameter(enriched, MODE)
    gate = fl.Gate(enriched, parameter=MODE, value="full", description="Full detail or quick summary?")
    full = (
        gate.then.group_by("category", "product", "month")
        .agg(fl.col("amount").sum().alias("revenue"), fl.col("quantity").sum().alias("units"))
        .with_columns(fl.lit("full").alias("mode"))
        .sort("month", "revenue", descending=[False, True])
    )
    full.write_catalog_table("sales_detail_full", schema=schema)
    quick = (
        gate.otherwise.group_by("region")
        .agg(fl.col("amount").sum().alias("revenue"), fl.col("order_id").count().alias("orders"))
        .with_columns(fl.lit("quick").alias("mode"))
    )
    # A union survives the closed side: the summary holds whichever branch ran.
    summary = fl.concat([full, quick], how="diagonal_relaxed")
    summary.write_catalog_table("sales_summary", schema=schema)

    schema.register_flow(summary, name="Sales analytics", overwrite=True)
    return summary.flow_graph


def run_and_report(flow, mode: str) -> None:
    banner(f"Running '{flow.flow_settings.name}' with mode = {mode!r}")
    started = time.perf_counter()
    info = flow.run_graph()
    types = {node.node_id: node.node_type for node in flow.nodes}
    for result in sorted(info.node_step_result, key=lambda r: r.node_id):
        if result.skipped:
            state = "skipped  (dead gate side)"
        elif result.success:
            state = f"ok       {result.run_time_ms} ms"
        else:
            state = f"FAILED   {result.error}"
        print(f"  node {result.node_id:>3}  {types.get(result.node_id, '?'):<22} {state}")
    elapsed = time.perf_counter() - started
    print(f"\n{info.nodes_completed}/{info.number_of_nodes} nodes, success={info.success}, {elapsed:.1f}s")


def show(schema, table: str, *sort_by: str) -> None:
    """Read a table back through the catalog (a Delta read keeps no row order, hence the sort)."""
    print(f"\n-- {CATALOG}.{SCHEMA}.{table}")
    frame = schema.read_table(table)
    print((frame.sort(*sort_by) if sort_by else frame).collect())


def main() -> None:
    schema = fl.get_catalog(CATALOG).get_schema(SCHEMA)
    fl.get_version()

    banner("1. Publish the reusable 'Clean orders' flow to the catalog")
    clean_ref = publish_clean_orders(schema)
    print(f"registered {clean_ref.name!r} in {clean_ref.namespace_full_name}  (uuid {clean_ref.flow_uuid})")

    banner("2. Build 'Sales analytics' on top of it (nothing runs yet)")
    flow = build_sales_analytics(schema, clean_ref)
    print(f"{len(flow.nodes)} nodes built and registered as 'Sales analytics'")

    run_and_report(flow, "full")
    show(schema, "sales_monthly", "month")
    show(schema, "sales_forecast", "month")
    show(schema, "sales_vs_target", "region")
    show(schema, "sales_top_products", "region", "rank")
    show(schema, "sales_summary", "month", "category", "product")

    fl.set_flow_parameter(flow, MODE, "quick")
    run_and_report(flow, "quick")
    show(schema, "sales_summary", "region")

    banner("Done")
    print(f"Flowfile app > Catalog > {CATALOG} > {SCHEMA}: flows 'Clean orders' + 'Sales analytics', tables sales_*")
    fl.open_graph_in_editor(flow)
