from flowfile_core.flowfile.flowfile_frame import flow_frame as pl
from flowfile_core.flowfile.flowfile_frame import selectors as scf
from polars import selectors as sc
from flowfile_core.flowfile.flowfile_frame.expr import col


# Create a simple DataFrame
data = {
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
    "age": ['25', '30', '35', '40', '45'],
    "city": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
    "salary": [50000, 60000, 70000, 80000, 90000],
}


df = pl.from_dict(data, description='Some random input data')

other_selection = df.select(scf.numeric(), description='this is with a selector')
columns = pl.col("city").alias("new_city").str.contains('Houston', literal=True), "age", "id"
sales_df = df.select(columns, description='sales_df')

output_df = sales_df.select(pl.col("new_city").alias("tf_city"), pl.col("age").cast(pl.UInt32), pl.col("id"), description='output_df')

other_df = df.select(
    pl.col("city").alias("new_city").str.contains('Houston', literal=True).cast(pl.Boolean, strict=True).sum(),
    pl.col('salary').sum(), 'other_df')

another_option = df.select(pl.col('name'), description='another_option')


input_df = (pl.read_parquet('flowfile_core/tests/support_files/data/fake_data.parquet',
            flow_graph=another_option.flow_graph, description='fake_data_df'))

output_df = (input_df.select(pl.col('sales_data').cast(pl.Int64), *[c for c in input_df.columns if c != 'sales_data'])
             .group_by('City').agg([col('sales_data').sum().alias('sum_sales_data'), col('sales_data').min()])
             .sort(['sales_data'], description='output data for')).cache()

sorted_df = input_df.sort(by=pl.col('sales_data'), descending=True, description='cached_df').cache()


filtered_df = sorted_df.filter(flowfile_formula='contains([Email],"@")', description='filter with flowfile formula')
filtered_df2 = filtered_df.filter(pl.col("Email").str.contains('@'), description='Filter with polars code')

data_with_formula = filtered_df2.with_columns([pl.lit("new_value").alias('output_column'), 'test'], description='this is native polars')

data_with_formula_2 = filtered_df2.with_columns(flowfile_formulas=['now()'], output_column_names=['today'], description='this is ff formula for the frontend')


def create_sample_sets(df_: pl.FlowFrame) -> pl.FlowFrame:
    for i in range(2):
        df_.limit(i, description=f"take sample of {i} records")
    return df_


output_df = create_sample_sets(data_with_formula_2)

graph = output_df.to_graph()  # Obtain the graph that is linked to the latest out
graph.flow_settings.execution_mode = 'Performance'


final_output_frame = output_df.write_parquet(
    'output_advanced.parquet',
    description='Write final data to Parquet with snappy compression and stats'
)



output_df.write_csv('output_csv.csv', separator=';', description='output_csv')
output_df.save_graph('flowfile_core/tests/support_files/flows/flow_from_df_operations.flowfile')
