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

# Create test data in long format for pivoting
long_data = {
    "id": [1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
    "name": ["Alice", "Alice", "Bob", "Bob", "Charlie", "Charlie", "David", "David", "Eve", "Eve"],
    "metric": ["salary", "bonus", "salary", "bonus", "salary", "bonus", "salary", "bonus", "salary", "bonus"],
    "value": [50000, 10000, 60000, 12000, 70000, 14000, 80000, 16000, 90000, 18000],
    "department": ["Sales", "Sales", "Marketing", "Marketing", "Engineering", "Engineering",
                  "Customer Service", "Customer Service", "Management", "Management"]
}

# Create a wide-format DataFrame suitable for unpivoting
data = {
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
    "salary_2022": [50000, 60000, 70000, 80000, 90000],
    "salary_2023": [55000, 65000, 75000, 85000, 95000],
    "salary_2024": [60000, 70000, 80000, 90000, 100000],
    "bonus_2022": [5000, 6000, 7000, 8000, 9000],
    "bonus_2023": [5500, 6500, 7500, 8500, 9500],
    "bonus_2024": [6000, 7000, 8000, 9000, 10000],
}

# Create a FlowFrame from the dictionary
wide_df = pl.from_dict(data, description='Wide-format employee compensation data', flow_graph=df.flow_graph)

# Use the unpivot method to transform to long format
# Keep 'id' and 'name' as identifier variables, unpivot all other columns
long_df = wide_df.unpivot(
    index=["id", "name"],
    description="Unpivot compensation data to long format"
)

print("\nUnpivoted data (long format):")
print(long_df.collect())

# Let's try a more specific unpivot that only includes salary columns
# and uses custom names for the variable and value columns
salary_df = wide_df.unpivot(
    on=[scf.integer()],
    index=["id", "name"],
    variable_name="year",
    value_name="salary",
    description="Unpivot salary data by year"
)

print("\nUnpivoted salary data with custom column names:")
print(salary_df.collect())

# Let's do the same for bonus data
bonus_df = wide_df.unpivot(
    on=["bonus_2022", "bonus_2023", "bonus_2024"],
    index=["id", "name"],
    variable_name="year",
    value_name="bonus",
    description="Unpivot bonus data by year"
)

print("\nUnpivoted bonus data with custom column names:")
print(bonus_df.collect())

# Now let's join the salary and bonus data
# First, let's extract the year from the variable names
salary_with_year = salary_df.with_columns([
    col("year").str.replace("salary_", "").alias("year")
])

bonus_with_year = bonus_df.with_columns([
    col("year").str.replace("bonus_", "").alias("year")
])


# Create a FlowFrame for the long data
df_long = pl.from_dict(long_data, description='Employee metrics in long format', flow_graph=df.flow_graph)
pivot_by_metric = df_long.pivot(
    on="metric",
    index=["id", "name", "department"],
    values="value",
    description="Employee compensation by metric"
)


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
