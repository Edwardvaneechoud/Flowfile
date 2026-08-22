"""Your first Flowfile pipeline: a derived column, a filter, and a group-by."""

# --8<-- [start:example]
import flowfile as ff

SALES = "https://raw.githubusercontent.com/edwardvaneechoud/flowfile/main/data/templates/supermarket_sales.csv"

revenue_by_line = (
    ff.read_csv(SALES)
    .with_columns((ff.col("unit_price") * ff.col("quantity")).alias("revenue"))
    .filter(ff.col("quantity") > 5)
    .group_by("product_line")
    .agg(ff.col("revenue").sum().alias("total_revenue"))
    .collect()
)
# --8<-- [end:example]

assert revenue_by_line.columns == ["product_line", "total_revenue"]
assert revenue_by_line.height == 6

by_line = {row["product_line"]: row["total_revenue"] for row in revenue_by_line.to_dicts()}
assert round(by_line["Fashion accessories"], 2) == 43859.10
assert round(by_line["Health and beauty"], 2) == 34165.95
