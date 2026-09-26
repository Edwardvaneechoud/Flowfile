"""Native node classes: a gate diamond, a subflow call, scripts, a custom node and a SQL node."""

# --8<-- [start:imports]
import polars as pl

import flowfile as ff
from flowfile import node_designer as nd

# --8<-- [end:imports]

# --8<-- [start:gate]
orders = ff.from_dict({"id": [1, 2, 3], "amount": [120.0, 40.0, 900.0]})
flow = orders.flow_graph
ff.add_flow_parameter(flow, ff.Parameter("mode", default="full", type="enum", enum_values=["full", "quick"]))

gate = ff.Gate(orders, parameter="mode", operator="equals", value="full")
full = gate.then.with_columns(ff.lit("full").alias("tier"))
quick = gate.otherwise.with_columns(ff.lit("quick").alias("tier"))
combined = ff.concat([full, quick], how="diagonal_relaxed")

ff.set_flow_parameter(flow, "mode", "quick")
run = flow.run_graph()
skipped = {result.node_id: result.skipped for result in run.node_step_result}
routed = combined.collect()
# --8<-- [end:gate]

assert run.success
assert routed["tier"].to_list() == ["quick"] * 3
assert gate.is_open is False
assert skipped[full.node_id] is True
assert skipped[quick.node_id] is False
assert skipped[gate.node_id] is False
assert flow.get_node(combined.node_id).get_resulting_data().collect()["tier"].to_list() == ["quick"] * 3

# --8<-- [start:subflow]
large_orders = ff.FlowOutput("large_orders")
child = ff.create_flow_graph()
incoming = ff.FlowInput("orders", schema={"id": ff.Int64, "amount": ff.Float64}, flow_graph=child)
incoming.filter(ff.col("amount") > 100).to_flow_output(large_orders)
large_orders_flow = ff.register_flow(child, name="Docs large orders")

parent_orders = ff.from_dict({"id": [1, 2, 3], "amount": [120.0, 40.0, 900.0]})
run_child = ff.RunFlow(large_orders_flow, orders=parent_orders)
large = run_child.get_output(large_orders).collect()
# --8<-- [end:subflow]

assert run_child.outputs == ["large_orders"]
assert large.sort("id")["id"].to_list() == [1, 3]
assert ff.register_flow(child, name="Docs large orders").registration_id == large_orders_flow.registration_id


# --8<-- [start:script-decorator]
@ff.python_script(kernel="docs-kernel", returns={"month": ff.Int64, "revenue_forecast": ff.Float64})
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
    return pl.DataFrame({"month": ahead, "revenue_forecast": slope * ahead + intercept})


history = {"month": [1, 2, 3, 4, 5, 6], "revenue": [100.0, 110.0, 120.0, 130.0, 140.0, 150.0]}
monthly = ff.from_dict(history)
sales_forecast = forecast(monthly)  # places the node; a deferred frame
in_thousands = sales_forecast.with_columns((ff.col("revenue_forecast") / 1000).alias("revenue_k"))
local_check = forecast.fn(pl.LazyFrame(history))  # the plain function, run here without a kernel
# --8<-- [end:script-decorator]

assert forecast.cells == [
    "import polars as pl",
    '# flowfile: inputs\nmonthly = flowfile_ctx.read_inputs()["main"][0]',
    "# Revenue trend: a least-squares line through monthly revenue, extended three months.",
    "import numpy as np\n\ndf = monthly.collect()",
    "# ## Fit\n# One slope for the whole period; good enough for a demo, not for a quarter close.",
    'slope, intercept = np.polyfit(df["month"], df["revenue"], deg=1)\n'
    'ahead = np.arange(df["month"].max() + 1, df["month"].max() + 4)',
    "# Forecast\n# flowfile: outputs\n"
    '_result = pl.DataFrame({"month": ahead, "revenue_forecast": slope * ahead + intercept})\n'
    'flowfile_ctx.publish_output(_result, "main")',
]
assert sales_forecast.columns == ["month", "revenue_forecast"]
assert in_thousands.columns == ["month", "revenue_forecast", "revenue_k"]
forecast_node = sales_forecast.flow_graph.get_node(sales_forecast.node_id)
assert forecast_node.node_type == "python_script"
assert forecast_node.setting_input.python_script_input.kernel_id == "docs-kernel"
assert [cell.code for cell in forecast_node.setting_input.python_script_input.cells] == forecast.cells
assert local_check["month"].to_list() == [7, 8, 9]
assert [round(value, 6) for value in local_check["revenue_forecast"]] == [160.0, 170.0, 180.0]


# --8<-- [start:script-outputs]
@ff.python_script(kernel="docs-kernel", outputs=["matched", "unmatched"])
def match_customers(orders: pl.LazyFrame, customers: pl.LazyFrame) -> dict[str, pl.LazyFrame]:
    known = customers.select("id")
    return {
        "matched": orders.join(known, on="id", how="semi"),
        "unmatched": orders.join(known, on="id", how="anti"),
    }


order_lines = ff.from_dict({"id": [1, 2, 3], "amount": [120.0, 40.0, 900.0]})
known_customers = ff.from_dict({"id": [1, 3], "name": ["Ann", "Cy"]})
matching = match_customers.node(order_lines, known_customers)  # the PythonScript node
matched = matching["matched"]
# --8<-- [end:script-outputs]

assert matching.outputs == ["matched", "unmatched"]
assert matched.columns == ["id", "amount"]
assert match_customers.cells[0] == (
    "# flowfile: inputs\n"
    'orders = flowfile_ctx.read_inputs()["main"][0]\n'
    'customers = flowfile_ctx.read_inputs()["main"][1]'
)
assert match_customers.cells[-1].endswith(
    'for _name in ["matched", "unmatched"]:\n    flowfile_ctx.publish_output(_result[_name], _name)'
)
local_split = match_customers.fn(pl.LazyFrame({"id": [1, 2]}), pl.LazyFrame({"id": [2]}))
assert local_split["matched"].collect()["id"].to_list() == [2]
try:
    match_customers(order_lines, known_customers)
except ff.NativeNodeError as exc:
    assert "match_customers.node(...)" in str(exc)
else:
    raise AssertionError("a function with two outputs must be placed with .node(...)")

# --8<-- [start:script]
readings = ff.from_dict({"id": [1, 2, 3], "amount": [120.0, 40.0, 900.0]})
script = ff.PythonScript(
    readings,
    cells=[
        "import polars as pl",
        "df = flowfile_ctx.read_input()\nflowfile_ctx.publish_output(df.with_columns(pl.col('amount').round(0)))",
    ],
    kernel="docs-kernel",
)
rounded = script.output
# --8<-- [end:script]

assert rounded.columns == ["id", "amount"]
assert script.node.setting_input.python_script_input.code.startswith("import polars as pl\n\n")


# --8<-- [start:custom-node]
class DocsTrimSettings(nd.NodeSettings):
    options: nd.Section = nd.Section(
        title="Options",
        column=nd.TextInput(label="Column", default="name"),
        upper=nd.ToggleSwitch(label="Upper case", default=False),
    )


class DocsTrimNode(nd.CustomNodeBase):
    node_name: str = "Docs Trim Text"
    settings_schema: DocsTrimSettings = DocsTrimSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        options = self.settings_schema.options
        cleaned = pl.col(options.column.value).str.strip_chars()
        if options.upper.value:
            cleaned = cleaned.str.to_uppercase()
        return inputs[0].with_columns(cleaned)


people = ff.from_dict({"name": ["  ann ", "bob  "]})
trim = ff.custom_node(DocsTrimNode)
trimmed = trim(people, upper=True)
same_node = ff.CustomNode(DocsTrimNode, people, settings={"options": {"upper": True}})
# --8<-- [end:custom-node]

assert trimmed.columns == ["name"]
assert trimmed.flow_graph.get_node(trimmed.node_id).setting_input.settings == same_node.node.setting_input.settings

# --8<-- [start:sql]
sales = ff.from_dict({"id": [1, 2, 3], "amount": [120.0, 40.0, 900.0]})
customers = ff.from_dict({"id": [1, 3], "name": ["Ann", "Cy"]})
joined = ff.Node(
    "sql_query",
    sales,
    customers,
    settings={
        "sql_query_input": {
            "sql_code": "select c.name, s.amount from input_1 s join input_2 c on s.id = c.id order by s.id"
        }
    },
).output
result = joined.collect()
# --8<-- [end:sql]

assert result.to_dicts() == [{"name": "Ann", "amount": 120.0}, {"name": "Cy", "amount": 900.0}]
