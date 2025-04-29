from flowfile_core.flowfile.flowfile_frame import flow_frame as pl
from flowfile_core.flowfile.flowfile_frame.expr import Expr, col


# Create a simple DataFrame
data = {
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
    "age": ['25', '30', '35', '40', '45'],
    "city": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
    "salary": [50000, 60000, 70000, 80000, 90000],
}


df = pl.from_dict(data, description='local_input_data')

columns = pl.col("city").alias("new_city").str.contains('Houston', literal=True), "age", "id"
sales_df = df.select(columns, description='sales_df')

output_df = sales_df.select(pl.col("new_city").alias("tf_city"), pl.col("age").cast(pl.UInt32), pl.col("id"), description='output_df')

other_df = df.select(
    pl.col("city").alias("new_city").str.contains('Houston', literal=True).cast(pl.Boolean, strict=True).sum(),
    pl.col('salary').sum(), 'other_df')

another_option = df.select(pl.col('name'), description='another_option')


input_df = (pl.read_parquet('flowfile_core/tests/support_files/data/fake_data.parquet',
                            flow_graph=another_option.flow_graph, description='fake_data_df'))
#
output_df = (input_df.select(pl.col('sales_data').cast(pl.Int64), *[c for c in input_df.columns if c != 'sales_data'])
             .group_by('City').agg([col('sales_data').sum().alias('sum_sales_data'), col('sales_data').min()])
             .sort(['sales_data']))
sorted_df = input_df.sort(by=pl.col('sales_data'), descending=True)


filtered_df = sorted_df.filter(flowfile_formula='contains([Email],"@")', description='filter with flowfile formula')
filtered_df2 = filtered_df.filter(pl.col("Email").str.contains('@'), description='Filter with polars code')

data_with_formula = filtered_df2.with_columns([pl.lit("new_value").alias('output_column'), 'test'], description='this is native polars')

data_with_formula_2 = filtered_df2.with_columns(flowfile_formulas=['now()'], output_column_names=['today'], description='this is ff formula for the frontend')

data_with_formula_2.save_graph('new.flowfile')