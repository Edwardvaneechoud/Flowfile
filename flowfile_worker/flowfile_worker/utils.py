import polars as pl
from polars.exceptions import PanicException
from dataclasses import dataclass


def collect_lazy_frame(lf: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lf.collect(engine="streaming")
    except PanicException:
        return lf.collect(engine="auto")


@dataclass
class CollectStreamingInfo:
    __slots__ = 'df', 'streaming_collect_available'
    df: pl.DataFrame
    streaming_collect_available: bool


def collect_lazy_frame_and_get_streaming_info(lf: pl.LazyFrame) -> CollectStreamingInfo:
    try:
        df = lf.collect(engine="streaming")
        return CollectStreamingInfo(df, True)
    except PanicException:
        return CollectStreamingInfo(lf.collect(), False)
