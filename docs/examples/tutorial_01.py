"""Read the supermarket invoices and build a lazy revenue pipeline."""

# --8<-- [start:read]
import flowfile as ff

SALES = "https://raw.githubusercontent.com/edwardvaneechoud/flowfile/main/data/templates/supermarket_sales.csv"
sales = ff.read_csv(SALES, description="Supermarket invoices")
# --8<-- [end:read]

# --8<-- [start:transform]
orders = sales.unique().with_columns((ff.col("unit_price") * ff.col("quantity")).alias("revenue"))
preview = orders.select("invoice_id", "city", "revenue").sort("invoice_id")
# --8<-- [end:transform]

# --8<-- [start:check]
preview.flow_graph.print_tree()
print(f"{sales.collect().height} rows read")
print(preview.collect())
# --8<-- [end:check]

# Alternative setup: add the reader to a graph you already have.
# --8<-- [start:existing-graph]
graph = ff.create_flow_graph()
sales_on_graph = ff.read_csv(SALES, flow_graph=graph)
# --8<-- [end:existing-graph]
