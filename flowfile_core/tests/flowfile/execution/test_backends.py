from uuid import uuid4

import polars as pl
import pytest

from flowfile_core.flowfile.execution.backends import (
    LocalBackend,
    RemoteWorkerBackend,
    resolve_backend,
)
from flowfile_core.flowfile.execution.handles import LocalResultHandle, TaskHandle
from flowfile_core.utils.arrow_reader import read_top_n


@pytest.fixture(params=["local", "remote"])
def backend(request):
    return resolve_backend(request.param)


def _sample_lf() -> pl.LazyFrame:
    return pl.LazyFrame({"a": [1, 2, 3, 4, 5], "b": ["x", "y", "z", "x", "y"]})


def test_resolve_backend_mapping():
    assert isinstance(resolve_backend("local"), LocalBackend)
    assert isinstance(resolve_backend("remote"), RemoteWorkerBackend)


def test_remote_location_never_resolves_local():
    # Guard for the core-never-collects contract: any non-local location must
    # route full materialisation to the worker backend.
    assert isinstance(resolve_backend("remote"), RemoteWorkerBackend)


def test_run_lazyframe_returns_equivalent_result(backend):
    handle = backend.run_lazyframe(
        _sample_lf(), flow_id=1, node_id=-1, file_ref=str(uuid4()), wait_on_completion=True
    )
    assert isinstance(handle, TaskHandle)
    result = handle.get_result()
    assert isinstance(result, pl.LazyFrame)
    assert result.collect().sort("a").equals(_sample_lf().collect().sort("a"))


def test_count_records_parity(backend):
    assert backend.count_records(_sample_lf(), flow_id=1, node_id=-1) == 5


def test_sample_writes_readable_arrow_file(backend):
    handle = backend.sample(
        _sample_lf(), file_ref=str(uuid4()), flow_id=1, node_id=-1, sample_size=3, wait_on_completion=True
    )
    assert handle.status is not None
    table = read_top_n(handle.status.file_ref, n=3)
    assert table.num_rows == 3
    assert set(table.column_names) == {"a", "b"}


def test_local_backend_has_no_result_cache():
    backend = LocalBackend()
    assert backend.results_exist("does-not-exist") is False
    assert backend.clear_result("does-not-exist") is False
    with pytest.raises(Exception):
        backend.get_cached_lazyframe("does-not-exist")


def test_remote_backend_result_cache_roundtrip():
    backend = resolve_backend("remote")
    file_ref = str(uuid4())
    handle = backend.run_lazyframe(_sample_lf(), flow_id=1, node_id=-1, file_ref=file_ref, wait_on_completion=True)
    handle.get_result()
    assert backend.results_exist(file_ref) is True
    cached = backend.get_cached_lazyframe(file_ref)
    assert cached.collect().sort("a").equals(_sample_lf().collect().sort("a"))
    assert backend.clear_result(file_ref) is True
    assert backend.results_exist(file_ref) is False


def test_local_result_handle_is_task_handle():
    handle = LocalResultHandle(result=42, file_ref="x")
    assert isinstance(handle, TaskHandle)
    assert handle.get_result() == 42
    assert handle.error_code == 0
    assert handle.error_description is None
    handle.cancel()
