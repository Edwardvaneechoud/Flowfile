"""Explode a bill of materials and a chart of accounts with explode_hierarchy."""

# --8<-- [start:example]
import flowfile as ff

# One row per BOM line: how many of `component` go into one `assembly`.
bom = ff.FlowFrame(
    [
        ("bike", "frame", 1.0),
        ("bike", "wheel", 2.0),
        ("bike", "screw", 10.0),
        ("bike", "handlebar", 1.0),
        ("ebike", "frame", 1.0),
        ("ebike", "wheel", 2.0),
        ("ebike", "battery", 1.0),
        ("ebike", "motor", 1.0),
        ("ebike", "screw", 12.0),
        ("frame", "steel_tube", 3.5),
        ("frame", "screw", 6.0),
        ("wheel", "rim", 1.0),
        ("wheel", "spoke", 32.0),
        ("wheel", "tyre", 1.0),
        ("wheel", "screw", 2.0),
        ("rim", "aluminium", 0.75),
        ("handlebar", "steel_tube", 0.5),
        ("handlebar", "grip", 2.0),
        ("motor", "copper_wire", 12.0),
        ("motor", "screw", 4.0),
    ],
    schema=["assembly", "component", "qty"],
    orient="row",
)

# Totals: one row per product and part. Quantities multiply down a route and add across routes.
parts = bom.explode_hierarchy("assembly", "component", quantity="qty", top_level_only=True)

# Purchase requirements: leaf parts per product x production plan, minus stock.
plan = ff.from_dict({"product": ["bike", "ebike"], "units": [100, 50]})
stock = ff.from_dict({"part": ["screw", "tyre", "battery"], "on_hand": [1000, 150, 10]})
to_buy = (
    parts.filter(ff.col("is_leaf"))
    .join(plan, left_on="ancestor", right_on="product")
    .group_by("descendant")
    .agg((ff.col("quantity") * ff.col("units")).sum().alias("needed"))
    .join(stock, left_on="descendant", right_on="part", how="left")
    .select(
        ff.col("descendant").alias("part"),
        (ff.col("needed") - ff.col("on_hand").fill_null(0)).alias("to_buy"),
    )
    .collect()
)

# Paths: one row per route, depth-first. These three routes add up to the bike's 20 screws.
screw_routes = (
    bom.explode_hierarchy("assembly", "component", quantity="qty", output_detail="paths")
    .filter((ff.col("ancestor") == "bike") & (ff.col("descendant") == "screw"))
    .select(ff.col("path").list.join(" > ").alias("route"), ff.col("quantity"))
    .collect()
)

# No quantities, so every edge counts 1; include_self makes each account count its own postings.
chart = ff.from_dict(
    {
        "parent_account": [1000, 1000, 1100, 1100, 1200],
        "account": [1100, 1200, 1110, 1120, 1210],
    }
)
journal = ff.from_dict({"account": [1110, 1110, 1120, 1210], "amount": [100, 50, 30, 500]})
balances = (
    chart.explode_hierarchy("parent_account", "account", include_self=True)
    .join(journal, left_on="descendant", right_on="account")
    .group_by("ancestor")
    .agg(ff.col("amount").sum())
    .collect()
)
# --8<-- [end:example]

totals = parts.collect()
assert totals.columns == ["ancestor", "descendant", "level", "quantity", "is_leaf"]
assert totals.height == 21
assert totals["is_leaf"].sum() == 13
screws = {a: q for a, d, q in totals.select("ancestor", "descendant", "quantity").iter_rows() if d == "screw"}
assert screws == {"bike": 20.0, "ebike": 26.0}

assert dict(to_buy.iter_rows()) == {
    "screw": 100 * 20 + 50 * 26 - 1000,
    "spoke": 150 * 64,
    "tyre": 150 * 2 - 150,
    "steel_tube": 100 * 4.0 + 50 * 3.5,
    "aluminium": 150 * 1.5,
    "grip": 100 * 2,
    "battery": 50 - 10,
    "copper_wire": 50 * 12,
}

assert screw_routes.rows() == [
    ("bike > frame > screw", 6.0),
    ("bike > wheel > screw", 4.0),
    ("bike > screw", 10.0),
]

assert dict(balances.iter_rows()) == {1000: 680, 1100: 180, 1110: 150, 1120: 30, 1200: 500, 1210: 500}
