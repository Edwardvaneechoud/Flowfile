"""Combine the customer segments and attach a revenue target to every invoice."""

import flowfile as ff

# Chapters 1-2: read and clean the invoices.
SALES = "https://raw.githubusercontent.com/edwardvaneechoud/flowfile/main/data/templates/supermarket_sales.csv"
sales = ff.read_csv(SALES, description="Supermarket invoices")

valid = sales.unique().filter(flowfile_formula="[quantity] > 0 and [unit_price] > 0")
orders = valid.with_columns(flowfile_formulas=["[unit_price] * [quantity]"], output_column_names=["revenue"])

# Chapter 3: union the two segments, then join the city targets.
# --8<-- [start:combine]
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
# --8<-- [end:combine]

# --8<-- [start:check]
enriched.flow_graph.print_tree()
result = enriched.select("invoice_id", "city", "customer_type", "revenue", "target").sort("invoice_id").collect()
print(result)
print(f"Invoices without a target: {result['target'].null_count()}")
# --8<-- [end:check]
