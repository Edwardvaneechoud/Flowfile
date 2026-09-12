"""The two pipelines from the Building Flows with Code tutorial."""

# --8<-- [start:first]
import flowfile as ff
from flowfile import col

df = ff.from_dict({
    "id": [1, 2, 2],
    "value": [10, 20, 15]
})

result = (
    df.filter(col("value") > 12, description="filter value > 12")
    .with_columns((col("value") * 10).alias("scaled_value"), description="get a scaled value")
    .group_by(col("id"))
    .agg(
        col("value").sum().alias("sum_value"),
        col("value").max().alias("max_value"),
        col("value").min().alias("min_value"),
    )
)

frame = result.collect()  # a Polars DataFrame
# --8<-- [end:first]

assert frame.height == 1
row = frame.row(0, named=True)
assert row == {"id": 2, "sum_value": 35, "max_value": 20, "min_value": 15}

# --8<-- [start:involved]
df = ff.from_dict({
    "id": [1, 2, 3, 4, 5],
    "category": ["A", "B", "A", "C", "B"],
    "value": [100, 200, 150, 300, 250]
})

aggregated = (
    df.filter(ff.col("value") > 120, description="Filter on value greater than 120")
    .with_columns(
        [
            (ff.col("value") * 1.1).alias("adjusted_value"),
            ff.when(ff.col("category") == "A").then(ff.lit("Premium"))
            .when(ff.col("category") == "B").then(ff.lit("Standard"))
            .otherwise(ff.lit("Basic")).alias("tier"),
        ],
        description="Calculate their tier",
    )
    .group_by("tier")
    .agg(
        ff.col("adjusted_value").sum().alias("total_value"),
        ff.col("id").count().alias("count"),
    )
)
# --8<-- [end:involved]

tiers = {r["tier"]: r for r in aggregated.collect().to_dicts()}
assert round(tiers["Standard"]["total_value"], 2) == 495.0 and tiers["Standard"]["count"] == 2
assert round(tiers["Premium"]["total_value"], 2) == 165.0 and tiers["Premium"]["count"] == 1
assert round(tiers["Basic"]["total_value"], 2) == 330.0 and tiers["Basic"]["count"] == 1
