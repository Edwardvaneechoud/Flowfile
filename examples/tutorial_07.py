"""Write the routed city report to files and a catalog table."""

from pathlib import Path

import flowfile as ff

# Chapters 1-3: read, clean, and enrich the invoices.
SALES = "https://raw.githubusercontent.com/edwardvaneechoud/flowfile/main/data/templates/supermarket_sales.csv"
sales = ff.read_csv(SALES, description="Supermarket invoices")

valid = sales.unique().filter(flowfile_formula="[quantity] > 0 and [unit_price] > 0")
orders = valid.with_columns(flowfile_formulas=["[unit_price] * [quantity]"], output_column_names=["revenue"])

members = orders.filter(ff.col("customer_type") == "Member")
regulars = orders.filter(ff.col("customer_type") == "Normal")
combined = ff.concat([members, regulars], how="diagonal_relaxed")

targets = ff.from_dict(
    {
        "city": ["Bago", "Mandalay", "Naypyitaw", "Taunggyi", "Yangon"],
        "target": [30000.0, 35000.0, 25000.0, 30000.0, 35000.0],
    }
)
enriched = combined.join(targets, on="city", how="left")

# Chapters 4-6: the parameterized city report, routed by a Gate.
minimum = ff.add_flow_parameter(enriched, ff.Parameter("min_quantity", default=8, type="integer"))
bulk = enriched.filter(ff.col("quantity") >= minimum)
summary = bulk.group_by("city").agg(
    ff.col("invoice_id").count().alias("orders"),
    ff.col("revenue").sum().alias("revenue"),
    ff.col("target").first().alias("target"),
)
report = summary.with_columns((ff.col("target") - ff.col("revenue")).alias("shortfall")).sort("city")

gate = ff.Gate(report, ff.col("shortfall") > 0)
review = gate.then.with_columns(ff.lit("review").alias("route"))
ready = gate.otherwise.with_columns(ff.lit("ready").alias("route"))
routed = ff.concat([review, ready], how="diagonal_relaxed")

# Chapter 7: write the routed report.
# --8<-- [start:files]
output_dir = Path("city_report").resolve()
output_dir.mkdir(exist_ok=True)
routed.write_csv(output_dir / "city_report.csv")
routed.write_parquet(output_dir / "city_report.parquet")
routed.flow_graph.run_graph()
# --8<-- [end:files]

# --8<-- [start:catalog]
reports = ff.CatalogReference("tutorial_sales", auto_create=True).schema("reports", auto_create=True)
reports.write_table(routed, "city_report", write_mode="overwrite").collect()
# --8<-- [end:catalog]

# --8<-- [start:check]
routed.flow_graph.print_tree()
print(ff.read_parquet(str(output_dir / "city_report.parquet")).collect())
print(reports.read_table("city_report").sort("city").collect())
# --8<-- [end:check]
