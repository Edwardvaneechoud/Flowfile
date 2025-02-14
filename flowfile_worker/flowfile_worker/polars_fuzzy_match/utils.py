import polars as pl
from flowfile_worker.configs import logger
from flowfile_worker.utils import collect_lazy_frame
import os
import uuid


def write_polars_frame(_df: pl.LazyFrame | pl.DataFrame, path: str,
                       estimated_size: int = 0):
    is_lazy = isinstance(_df, pl.LazyFrame)
    logger.info('Caching data frame')
    if is_lazy:
        if estimated_size > 0:
            fit_memory = estimated_size / 1024 / 1000 / 1000 < 8
            if fit_memory:
                _df = _df.collect()
                is_lazy = False

        if is_lazy:
            logger.info("Writing in memory efficient mode")
            write_method = getattr(_df, 'sink_ipc')
            try:
                write_method(path)
                return True
            except Exception as e:
                pass
            try:
                write_method(path)
                return True
            except Exception as e:
                pass
        if is_lazy:
            _df = collect_lazy_frame(_df)
    try:
        write_method = getattr(_df, 'write_ipc')
        write_method(path)
        return True
    except Exception as e:
        print('error', e)
        return False


def cache_polars_frame_to_temp(_df: pl.LazyFrame | pl.DataFrame, tempdir: str = None) -> pl.LazyFrame:
    path = f'{tempdir}{os.sep}{uuid.uuid4()}'
    result = write_polars_frame(_df, path)
    if result:
        df = pl.read_ipc(path)
        return df.lazy()
    else:
        raise Exception('Could not cache the data')
