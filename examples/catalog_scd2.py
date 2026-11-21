"""Track dimension history with an SCD2 catalog write."""

# --8<-- [start:example]
import flowfile as ff

customers_day1 = ff.from_dict(
    {"customer_id": [1, 2], "tier": ["free", "pro"], "city": ["Amsterdam", "Berlin"]}
)
ff.write_catalog_table(
    customers_day1, "docs_customers_scd2",
    schema=ff.default_schema(), write_mode="scd2", merge_keys=["customer_id"],
)

customers_day2 = ff.from_dict(
    {"customer_id": [1, 2], "tier": ["pro", "pro"], "city": ["Amsterdam", "Berlin"]}
)
# The write hands the rows back with their surrogate key and validity window attached.
keyed_day2 = ff.write_catalog_table(
    customers_day2, "docs_customers_scd2",
    schema=ff.default_schema(), write_mode="scd2", merge_keys=["customer_id"],
)

current = ff.read_catalog_table("docs_customers_scd2", schema=ff.default_schema(), scd2_view="active")
history = ff.read_catalog_table("docs_customers_scd2", schema=ff.default_schema(), scd2_view="all")
# --8<-- [end:example]

assert keyed_day2.columns == ["customer_id", "tier", "city", "sk", "valid_from", "valid_to", "is_current"]
assert keyed_day2.collect()["sk"].null_count() == 0
assert current.collect().height == 2
assert history.collect().height == 3
