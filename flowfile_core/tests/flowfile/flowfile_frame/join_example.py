import polars as pl
from flowfile_core.flowfile.flowfile_frame import flow_frame as fl


main = fl.FlowFrame({
    "cust_id": [1, 2, 3],
    "shop_id": ["A", "B", "C"],
    "amount": [100, 200, 300]
})

other = fl.FlowFrame({
    "customer_id": [1, 2, 4],
    "store_id": ["a", "b", "c"],
    "amount": [20, 2102, 120],
    "shop_id": ["A", "B", "C"]
}, flow_graph=main.flow_graph)

result2 = main.join(
    other,
    on=fl.col("shop_id"), how='left'
)


# Join using expressions on multiple columns
result1 = main.join(
    other,
    left_on=["cust_id", fl.col("shop_id").str.to_lowercase()],
    right_on=["customer_id", fl.col("store_id").str.to_lowercase()]
)

main.save_graph('join_example.flowfile')

from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.FlowfileFlow import EtlGraph, add_connection, RunInformation
from flowfile_core.schemas import input_schema, transform_schema, schemas
from flowfile_core.flowfile.flowfile_table.flowfile_table import FlowfileTable
from flowfile_core.flowfile.analytics.analytics_processor import AnalyticsProcessor
from flowfile_core.configs.flow_logger import FlowLogger
from flowfile_core.flowfile.database_connection_manager.db_connections import (get_local_database_connection,
                                                                               store_database_connection,)
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.flowfile_frame import flow_frame as ff
from pathlib import Path


def create_flowfile_handler():
    handler = FlowfileHandler()
    assert handler._flows == {}, 'Flow should be empty'
    return handler


handler = create_flowfile_handler()
flow_path = "singlular_start.flowfile"
flow_id = handler.import_flow(Path(flow_path))

flow = handler.get_flow(flow_id)
# flow.flow_settings.execution_mode = "Development"
#
# from flowfile_core.schemas import input_schema, transform_schema
# flow.add_polars_code(input_schema.NodePolarsCode(flow_id=flow_id, node_id=20, polars_code_input=transform_schema.PolarsCodeInput(polars_code="output_df = pl.LazyFrame([1,2,3])")))
#
flow.run_graph()
#
