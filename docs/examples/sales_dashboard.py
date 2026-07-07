"""Enrich supermarket sales once, then branch into a product ranking, a city pivot, and a monthly trend."""

# --8<-- [start:example]
import flowfile as ff

SALES = "https://raw.githubusercontent.com/edwardvaneechoud/flowfile/main/data/templates/supermarket_sales.csv"

# Clean and enrich once — every view below reads from this.
base = (
    ff.read_csv(SALES)
    .unique()
    .with_columns((ff.col("unit_price") * ff.col("quantity")).alias("revenue"))
)

# A small lookup: each product line's department and annual revenue target.
targets = ff.from_dict(
    {
        "product_line": [
            "Home and lifestyle", "Fashion accessories", "Food and beverages",
            "Sports and travel", "Health and beauty", "Electronic accessories",
        ],
        "department": ["Homeware", "Apparel", "Grocery", "Leisure", "Personal care", "Electronics"],
        "annual_target": [55000, 50000, 52000, 50000, 48000, 45000],
    }
)

# View 1 — product leaderboard, joined to targets and ranked by revenue.
products = (
    base.group_by("product_line")
    .agg(
        ff.col("revenue").sum().alias("total_revenue"),
        ff.col("invoice_id").n_unique().alias("orders"),
        ff.col("quantity").sum().alias("units_sold"),
    )
    .join(targets, on="product_line", how="left")
    .with_columns((ff.col("total_revenue") / ff.col("annual_target") * 100).alias("pct_of_target"))
    .sort("total_revenue", descending=True)
)

# View 2 — revenue matrix: one row per city, one column per customer type.
city_matrix = base.pivot(on="customer_type", index="city", values="revenue", aggregate_function="sum")

# View 3 — monthly revenue trend.
monthly = (
    base.with_columns(ff.col("date").dt.strftime(format="%Y-%m").alias("month"))
    .group_by("month")
    .agg(ff.col("revenue").sum().alias("monthly_revenue"))
    .sort("month")
)
# --8<-- [end:example]

prod = products.collect()
assert prod.height == 6
assert prod["product_line"][0] == "Home and lifestyle"
assert round(prod["total_revenue"][0], 2) == 54383.40
assert prod["orders"][0] == 182
by_line = {r["product_line"]: r for r in prod.to_dicts()}
assert by_line["Home and lifestyle"]["department"] == "Homeware"
assert round(by_line["Fashion accessories"]["pct_of_target"], 2) == 107.25
assert round(by_line["Home and lifestyle"]["pct_of_target"], 2) == 98.88

city = city_matrix.collect()
assert city.height == 5
assert set(city.columns) == {"city", "Member", "Normal"}
by_city = {r["city"]: r for r in city.to_dicts()}
assert round(by_city["Mandalay"]["Member"], 2) == 37913.27
assert round(by_city["Naypyitaw"]["Normal"], 2) == 21739.14

month = monthly.collect()
assert month.height == 12
assert month["month"][0] == "2024-01"
assert month["month"][-1] == "2024-12"
assert round(month["monthly_revenue"][0], 2) == 29817.92
