from flowfile_core.schemas import node_interface, transformation_settings, RawData
from flowfile_core.flowfile.flow_graph import add_connection
import flowfile as ff
raw_data = [
    {"id": 1, "region": "North", "quantity": 10, "price": 150},
    {"id": 2, "region": "South", "quantity": 5, "price": 300},
    {"id": 3, "region": "East", "quantity": 8, "price": 200},
    {"id": 4, "region": "West", "quantity": 12, "price": 100},
    {"id": 5, "region": "North", "quantity": 20, "price": 250},
    {"id": 6, "region": "South", "quantity": 15, "price": 400},
    {"id": 7, "region": "East", "quantity": 18, "price": 350},
    {"id": 8, "region": "West", "quantity": 25, "price": 500},
]

from flowfile_core.flowfile.flow_graph import FlowGraph

graph: FlowGraph = ff.create_flow_graph()

# Create pipeline with fluent API
df_1 = ff.FlowFrame(raw_data, flow_graph=graph)

df_2 = df_1.with_columns(
    flowfile_formulas=['[quantity] * [price]'],
    output_column_names=["total"]
)

df_3 = df_2.filter(flowfile_formula="[total]>1500")

df_4 = df_3.group_by(['region']).agg([
    ff.col("total").sum().alias("total_revenue"),
    ff.col("total").mean().alias("avg_transaction"),
])


ff.open_graph_in_editor(graph)