"""Tests for worker utils module."""

import polars as pl
from polars.exceptions import PanicException

from flowfile_worker.utils import CollectStreamingInfo, collect_lazy_frame, collect_lazy_frame_and_get_streaming_info


class TestCollectLazyFrame:
    """Test collect_lazy_frame function."""

    def test_simple_collection(self):
        lf = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        df = collect_lazy_frame(lf)
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 3

    def test_with_operations(self):
        lf = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).lazy()
        lf = lf.filter(pl.col("a") > 1)
        df = collect_lazy_frame(lf)
        assert len(df) == 2


class TestCollectStreamingInfo:
    """Test CollectStreamingInfo dataclass."""

    def test_create(self):
        df = pl.DataFrame({"a": [1]})
        info = CollectStreamingInfo(df=df, streaming_collect_available=True)
        assert info.df is df
        assert info.streaming_collect_available is True

    def test_slots(self):
        assert CollectStreamingInfo.__slots__ == ("df", "streaming_collect_available")


class TestCollectLazyFrameAndGetStreamingInfo:
    """Test collect_lazy_frame_and_get_streaming_info function."""

    def test_simple_collection(self):
        lf = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        result = collect_lazy_frame_and_get_streaming_info(lf)
        assert isinstance(result, CollectStreamingInfo)
        assert isinstance(result.df, pl.DataFrame)
        assert len(result.df) == 3
        assert result.streaming_collect_available is True

    def test_with_filter(self):
        lf = pl.DataFrame({"x": [10, 20, 30]}).lazy().filter(pl.col("x") >= 20)
        result = collect_lazy_frame_and_get_streaming_info(lf)
        assert len(result.df) == 2
