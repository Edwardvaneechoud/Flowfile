"""Clean the invoices with editable formulas and compare them with expressions."""

import flowfile as ff

# Chapter 1: read the invoices.
SALES = "https://raw.githubusercontent.com/edwardvaneechoud/flowfile/main/data/templates/supermarket_sales.csv"
sales = ff.read_csv(SALES, description="Supermarket invoices")

# Chapter 2: keep valid invoices and add revenue.
# --8<-- [start:clean]
valid = sales.unique().filter(flowfile_formula="[quantity] > 0 and [unit_price] > 0")
orders = valid.with_columns(flowfile_formulas=["[unit_price] * [quantity]"], output_column_names=["revenue"])
# --8<-- [end:clean]

# --8<-- [start:expressions]
expression_orders = valid.with_columns((ff.col("unit_price") * ff.col("quantity")).alias("revenue"))
scaled = orders.with_columns(
    (ff.col("revenue") / 1000).alias("revenue"),
    (ff.col("revenue") * 0.05).alias("expected_income"),
)
# --8<-- [end:expressions]

# --8<-- [start:check]
orders.flow_graph.print_tree()
print(scaled.select("invoice_id", "revenue", "expected_income").sort("invoice_id").head(3).collect())
# --8<-- [end:check]
