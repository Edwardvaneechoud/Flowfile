"""Dedupe, filter, and aggregate supermarket sales by city with the FlowFrame API."""

# --8<-- [start:example]
import flowfile as ff

SALES = "https://raw.githubusercontent.com/edwardvaneechoud/flowfile/main/data/templates/supermarket_sales.csv"

result = (
    ff.read_csv(SALES)
    .unique()
    .filter(ff.col("quantity") > 7)
    .group_by("city")
    .agg(
        ff.col("gross_income").sum().alias("total_income"),
        ff.col("gross_income").median().alias("median_income"),
    )
)
# --8<-- [end:example]

by_city = {row["city"]: row for row in result.collect().to_dicts()}
df = result.collect()
assert df.height == 5
assert set(by_city) == {"Bago", "Mandalay", "Naypyitaw", "Taunggyi", "Yangon"}

assert round(by_city["Bago"]["total_income"], 2) == 1429.66
assert round(by_city["Mandalay"]["total_income"], 2) == 1846.88
assert round(by_city["Naypyitaw"]["total_income"], 2) == 1186.66
assert round(by_city["Taunggyi"]["total_income"], 2) == 1635.53
assert round(by_city["Yangon"]["total_income"], 2) == 1704.81

assert round(by_city["Mandalay"]["median_income"], 3) == 27.220
assert round(by_city["Yangon"]["median_income"], 3) == 26.085
