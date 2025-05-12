
import polars as pl
from flowfile_core.utils.fl_executor import process_executor
from flowfile_core.flowfile.flow_data_engine import utils

# calculate_schema_threaded = process_executor(wait_on_completion=True, max_workers=1)(utils.calculate_schema)
write_threaded = process_executor(False, max_workers=1)(utils.write_polars_frame)
collect_threaded = process_executor(wait_on_completion=False, max_workers=1)(utils.collect)
cache_polars_frame_to_temp_thread = process_executor(wait_on_completion=True, max_workers=1)(
    utils.cache_polars_frame_to_temp)


@process_executor(False, max_workers=1)
def do_something_random():
    print('10 seconds')


# @process_executor(False, max_workers=1)
def get_join_count(left: pl.LazyFrame, right: pl.LazyFrame, left_on_keys, right_on_keys, how):
    left_joined_df = left.group_by(left_on_keys).count()
    right_joined_df = right.group_by(right_on_keys).count()
    data: pl.LazyFrame = left_joined_df.join(right_joined_df, left_on=left_on_keys,
                                             right_on=right_on_keys, how=how)
    data = data.with_columns(pl.lit(1).alias('total').cast(pl.UInt64))
    result = data.select((pl.col('total') * pl.col('count') * pl.col('count_right'))).sum()
    n_records = result.collect().to_series().to_list()[0]
    return n_records
