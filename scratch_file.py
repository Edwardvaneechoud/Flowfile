import flowfile as fl

schema = fl.get_catalog("Demo").get_schema("sales_analytics")
sales = schema.read_table("sales")


mode_param = fl.Parameter("mode", default="full", type="enum", enum_values=["full", "quick"])
fl.add_flow_parameter(sales, mode_param)

g = fl.Gate(sales, parameter=mode_param, operator=fl.GateOperator.EQUALS, value="full")
full = g.then.group_by("region").agg(fl.col("amount").sum().alias("revenue")).head(2)
quick = g.otherwise.head(1)

print(g.is_open)                 # True
fl.set_flow_parameter(sales, "mode", "quick")
print(g.is_open)       # False: the run will skip the `full` branch, green not red


output = fl.concat([full, quick], how="diagonal_relaxed")

errors = sales.filter(fl.col("status") == "error")
g = fl.Gate(sales, "[status] == 'error'", control=sales, else_output=False)
g.then.write_parquet("/tmp/errors_today.parquet")   # written only on runs that hit an error row


amount_parameter = fl.Parameter(name="min_amount", default=0, type="integer")


child = fl.create_flow_graph()
raw = fl.FlowInput("orders", schema={"id": fl.Int64, "amount": fl.Float64, "region": fl.String}, flow_graph=child)
fl.add_flow_parameter(child, amount_parameter)
raw.filter(fl.col("amount") >= fl.lit(amount_parameter).cast(fl.Int64)).to_flow_output("orders_clean")

# fl.open_graph_in_editor(child)

clean_ref = schema.register_flow(child, name="Clean orders")

run = fl.RunFlow(clean_ref, orders=sales, params={"min_amount": 50})
clean = run["orders_clean"]        # deferred: nothing has run yet
clean.count()               # runs the graph, including the child flow

fl.open_graph_in_editor(clean.flow_graph)