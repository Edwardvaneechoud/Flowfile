"""Native node classes: a gate diamond, a subflow call, a script, a custom node and a SQL node."""

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
child = ff.create_flow_graph()
incoming = ff.FlowInput("orders", schema={"id": ff.Int64, "amount": ff.Float64}, flow_graph=child)
incoming.filter(ff.col("amount") > 100).to_flow_output("large_orders")
large_orders_flow = ff.register_flow(child, name="Docs large orders")

parent_orders = ff.from_dict({"id": [1, 2, 3], "amount": [120.0, 40.0, 900.0]})
run_child = ff.RunFlow(large_orders_flow, orders=parent_orders)
large = run_child["large_orders"].collect()
# --8<-- [end:subflow]

assert run_child.outputs == ["large_orders"]
assert large.sort("id")["id"].to_list() == [1, 3]
assert ff.register_flow(child, name="Docs large orders").registration_id == large_orders_flow.registration_id

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
