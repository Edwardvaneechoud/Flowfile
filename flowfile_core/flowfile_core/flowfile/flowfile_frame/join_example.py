import polars as pl
from flowfile_core.flowfile.flowfile_frame import flow_frame as fl


self = fl.FlowFrame({
    "cust_id": [1, 2, 3],
    "shop_id": ["A", "B", "C"],
    "amount": [100, 200, 300]
})

other = fl.FlowFrame({
    "customer_id": [1, 2, 4],
    "store_id": ["A", "B", "C"],
    "amount": [20, 2102, 120],
    "shop_id": ["A", "B", "C"]
}, flow_graph=self.flow_graph)


other.select(('customer_id',))

# Join using expressions on multiple columns
result1 = self.join(
    other,
    left_on=["cust_id", fl.col("shop_id")],
    right_on=["customer_id", fl.col("store_id")]
)

result2 = self.join(
    other,
    on=fl.col("shop_id")
)

result3 = self.join(
    other,
    on="shop_id"
)


result4 = self.join(
    other,
    on=["shop_id"]
)

