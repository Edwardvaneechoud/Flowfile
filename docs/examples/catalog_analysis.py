"""Write an aggregate to the Flowfile catalog, then query it back with SQL."""

# --8<-- [start:example]
import flowfile as ff
from flowfile_frame import read_catalog_sql

income_by_city = (
    ff.read_csv("data/templates/supermarket_sales.csv")
    .group_by("city")
    .agg(ff.col("gross_income").sum().alias("total_income"))
)

ff.write_catalog_table(
    income_by_city, "docs_sales_by_city", schema=ff.default_schema(), write_mode="overwrite"
)

top_cities = read_catalog_sql(
    "SELECT city, total_income FROM docs_sales_by_city ORDER BY total_income DESC"
).collect()
# --8<-- [end:example]

assert top_cities.columns == ["city", "total_income"]
assert top_cities.height == 5

rows = top_cities.to_dicts()
assert rows[0]["city"] == "Taunggyi"
assert round(rows[0]["total_income"], 2) == 3525.60

by_city = {row["city"]: round(row["total_income"], 2) for row in rows}
assert by_city == {
    "Bago": 3051.94,
    "Mandalay": 3520.57,
    "Naypyitaw": 2521.64,
    "Taunggyi": 3525.60,
    "Yangon": 3312.56,
}
