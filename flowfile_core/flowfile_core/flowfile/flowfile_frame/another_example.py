from flowfile_core.flowfile.flowfile_frame import flow_frame as pl
from flowfile_core.flowfile.flowfile_frame import selectors as scf
from polars import selectors as sc
from flowfile_core.flowfile.flowfile_frame.expr import col

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
df = pl.from_dict(data, description='Wide-format employee compensation data')

print("Original wide-format data:")
print(df.collect())

# Use the unpivot method to transform to long format
# Keep 'id' and 'name' as identifier variables, unpivot all other columns
long_df = df.unpivot(
    index=["id", "name"],
    description="Unpivot compensation data to long format"
)

print("\nUnpivoted data (long format):")
print(long_df.collect())

# Let's try a more specific unpivot that only includes salary columns
# and uses custom names for the variable and value columns
salary_df = df.unpivot(
    on=[scf.integer()],
    index=["id", "name"],
    variable_name="year",
    value_name="salary",
    description="Unpivot salary data by year"
)

print("\nUnpivoted salary data with custom column names:")
print(salary_df.collect())

# Let's do the same for bonus data
bonus_df = df.unpivot(
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

