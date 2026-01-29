from flowfile_core.flowfile.flow_data_engine import utils
from flowfile_core.utils.fl_executor import process_executor

write_threaded = process_executor(False, max_workers=1)(utils.write_polars_frame)
