from flowfile_core.flowfile.flow_data_engine import utils
from flowfile_core.utils.fl_executor import process_executor

write_threaded = process_executor(False, max_workers=1)(utils.write_polars_frame)
cache_polars_frame_to_temp_thread = process_executor(wait_on_completion=True, max_workers=1)(
    utils.cache_polars_frame_to_temp
)
