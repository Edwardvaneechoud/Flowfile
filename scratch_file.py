import flowfile as fl

schema = fl.get_catalog("Demo").get_schema("sales_analytics")
sales = schema.read_table("sales")

fl.add_flow_parameter(sales, "mode", default="full", type="enum", enum_values=["full", "quick"])

g = fl.Gate(sales, parameter="mode", operator=fl.GateOperator.EQUALS, value="full")
full = g.then.group_by("region").agg(fl.col("amount").sum().alias("revenue")).head(2)
quick = g.otherwise.head(1)

print(g.is_open)                 # True
fl.set_flow_parameter(sales, "mode", "full")
print(g.is_open)       # False: the run will skip the `full` branch, green not red


output = fl.concat([full, quick], how="diagonal_relaxed")


print(len(output.collect()))

fl.open_graph_in_editor(sales.flow_graph)   # T/E handles on the canvas, same graph