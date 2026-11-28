"""Name-based catalog handles: create a catalog and schema, write a table, read it back."""

# --8<-- [start:example]
import flowfile as ff

SALES = "https://raw.githubusercontent.com/edwardvaneechoud/flowfile/main/data/templates/supermarket_sales.csv"

catalog = ff.CatalogReference("docs_sales", auto_create=True)
schema = catalog.schema("raw", auto_create=True)

orders = ff.read_csv(SALES).select("invoice_id", "city", "quantity", "gross_income")
schema.write_table(orders, "orders")

# Use the schema handle anywhere a namespace_id used to be required
bulk = schema.read_table("orders").filter(ff.col("quantity") > 7)
schema.write_table(bulk, "bulk_orders")
# --8<-- [end:example]

result = schema.read_table("bulk_orders").collect()
assert result.height == 326  # raw rows with quantity > 7 (incl. the 30 seeded duplicates)
assert set(result.columns) == {"invoice_id", "city", "quantity", "gross_income"}
assert result["quantity"].min() == 8
