from flowfile_core.flowfile.flowfile_frame import flow_frame as pl
from flowfile_core.flowfile.flowfile_frame import selectors as scf
from polars import selectors as sc
from flowfile_core.flowfile.flowfile_frame.expr import col


input_df = (pl.read_parquet('flowfile_core/tests/support_files/data/fake_data.parquet', description='fake_data_df'))
transformed_df = input_df.with_columns([pl.col("sales_data").cast(pl.Int64).alias('sales_data')])
r = transformed_df.group_by(['City']).agg(pl.col("sales_data").sum())

vals = transformed_df.group_by('City').len()


