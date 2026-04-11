from flowfile_core.schemas.input_schema import RawData
from polars_grouper import graph_solver
import flowfile as ff

df_1 = ff.from_raw_data(RawData(
    **{'columns': [{'name': 'from', 'data_type': 'String'}, {'name': 'to', 'data_type': 'String'}],
       'data': [['a', 'b', 'g'], ['b', 'c', 'd']]}))
df_2 = df_1.with_columns(graph_solver(ff.col("from"), ff.col("to")).alias("g"))


if __name__ == "__main__":
    pipeline_output = run_etl_pipeline()
