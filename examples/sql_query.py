"""SQL over one or more frames: each query becomes a SQL Query node."""

import flowfile as ff

# --8<-- [start:example]
orders = ff.from_dict(
    {"order_id": [1, 2, 3, 4], "region": ["EU", "US", "EU", "APAC"], "amount": [120.0, 40.0, 900.0, 75.0]}
)
regions = ff.from_dict({"region": ["EU", "US", "APAC"], "target": [800.0, 100.0, 50.0]})

# One frame: it is the table `self`, as in Polars
large = orders.sql("SELECT order_id, region, amount FROM self WHERE amount >= 75")

# Several frames: each keyword names a table
per_region = ff.sql(
    """
    SELECT r.region, SUM(o.amount) AS revenue, r.target
    FROM orders o JOIN regions r ON o.region = r.region
    GROUP BY r.region, r.target
    ORDER BY region
    """,
    orders=large,
    regions=regions,
    description="Revenue per region",
)
result = per_region.collect()
# --8<-- [end:example]

assert result.to_dicts() == [
    {"region": "APAC", "revenue": 75.0, "target": 50.0},
    {"region": "EU", "revenue": 1020.0, "target": 800.0},
]
node = per_region.flow_graph.get_node(per_region.node_id)
assert node.node_type == "sql_query"
assert node.setting_input.sql_query_input.sql_code.startswith(
    "WITH orders AS (SELECT * FROM input_1),\n     regions AS (SELECT * FROM input_2)\n"
)
