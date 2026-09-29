"""Add a shortfall percentage with SQL, a custom node, or a Python Script."""

import polars as pl

import flowfile as ff
from flowfile import node_designer as nd

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

# Chapter 8: the report comes from the reusable child flow.
incoming = ff.FlowInput("invoices", schema=enriched.schema)
child_minimum = ff.add_flow_parameter(incoming, ff.Parameter("min_quantity", default=8, type="integer"))
child_summary = (
    incoming.filter(ff.col("quantity") >= child_minimum)
    .group_by("city")
    .agg(
        ff.col("invoice_id").count().alias("orders"),
        ff.col("revenue").sum().alias("revenue"),
        ff.col("target").first().alias("target"),
    )
)
child_report = child_summary.with_columns((ff.col("target") - ff.col("revenue")).alias("shortfall")).sort("city")
child_report.to_flow_output("city_report")
report_flow = ff.register_flow(child_report, name="Tutorial city summary")

minimum = ff.add_flow_parameter(enriched, ff.Parameter("min_quantity", default=8, type="integer"))
report = ff.RunFlow(report_flow, invoices=enriched, params={"min_quantity": minimum})["city_report"]

# Chapter 9: score each city's shortfall as a percentage of its target.
# --8<-- [start:sql]
scored = ff.sql("SELECT *, ROUND(100.0 * shortfall / target, 2) AS shortfall_pct FROM report", report=report)
# --8<-- [end:sql]

# Chapter 6: the Gate now reads the scored report.
gate = ff.Gate(scored, ff.col("shortfall") > 0)
review = gate.then.with_columns(ff.lit("review").alias("route"))
ready = gate.otherwise.with_columns(ff.lit("ready").alias("route"))
routed = ff.concat([review, ready], how="diagonal_relaxed")


# --8<-- [start:custom]
class ShortfallSettings(nd.NodeSettings):
    options: nd.Section = nd.Section(
        title="Scoring",
        target_column=nd.TextInput(label="Target column", default="target"),
    )


class ShortfallPercent(nd.CustomNodeBase):
    node_name: str = "Tutorial Shortfall Percent"
    settings_schema: ShortfallSettings = ShortfallSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        target = self.settings_schema.options.target_column.value
        return inputs[0].with_columns((100.0 * pl.col("shortfall") / pl.col(target)).round(2).alias("shortfall_pct"))


score = ff.custom_node(ShortfallPercent)
custom_scored = score(report, target_column="target")
# --8<-- [end:custom]


# --8<-- [start:script]
@ff.python_script(
    kernel="my-kernel",
    returns={
        "city": ff.String,
        "orders": ff.UInt32,
        "revenue": ff.Float64,
        "target": ff.Float64,
        "shortfall": ff.Float64,
        "shortfall_pct": ff.Float64,
    },
)
def score_in_python(city_report: pl.LazyFrame) -> pl.LazyFrame:
    return city_report.with_columns((100.0 * pl.col("shortfall") / pl.col("target")).round(2).alias("shortfall_pct"))


script_scored = score_in_python(report)
# --8<-- [end:script]

# --8<-- [start:check]
routed.flow_graph.print_tree()
print(scored.collect())
print(custom_scored.collect())
# --8<-- [end:check]
