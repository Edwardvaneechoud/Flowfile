"""The worker's ``polars_code`` job on a multi-user server: refused before any plan is loaded or any code runs."""

from multiprocessing import Queue

import polars as pl
import pytest

from flowfile_worker import mp_context
from flowfile_worker.funcs import polars_code
from shared.polars_code_parser import IN_PROCESS_REFUSED_MESSAGE


def _run(code: str, inputs: list[bytes], file_path: str = ""):
    """Run the job in this process, as its child would; answers ``(progress, error text, queue)``."""
    progress, error_message, queue = mp_context.Value("i", 0), mp_context.Array("c", 1024), Queue(maxsize=1)
    polars_code(
        polars_serializable_object=b"",
        progress=progress,
        error_message=error_message,
        queue=queue,
        file_path=file_path,
        flowfile_flow_id=1,
        flowfile_node_id=1,
        code=code,
        inputs=inputs,
    )
    return progress.value, error_message.value.decode(), queue


@pytest.mark.worker
@pytest.mark.parametrize("mode", ["docker", "server"])
def test_the_job_fails_on_a_server_before_the_code_runs(mode, tmp_path, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", mode)
    written = tmp_path / "written_by_polars_code.txt"
    code = f"pl.DataFrame({{'c': ['any text']}}).write_csv({str(written)!r})\noutput_df = input_df"

    progress, error, queue = _run(code, [pl.LazyFrame({"a": [1]}).serialize()])

    assert progress == -1
    assert error == f"PermissionError: {IN_PROCESS_REFUSED_MESSAGE}"
    assert queue.empty()
    assert not written.exists()


@pytest.mark.worker
def test_the_job_fails_on_a_server_before_a_plan_is_loaded(monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "docker")

    progress, error, queue = _run("input_df", [b"not a plan"])

    assert progress == -1
    assert error == f"PermissionError: {IN_PROCESS_REFUSED_MESSAGE}"
    assert queue.empty()


@pytest.mark.worker
def test_the_job_runs_on_a_desktop(tmp_path, monkeypatch):
    monkeypatch.delenv("FLOWFILE_MODE", raising=False)
    plan = pl.LazyFrame({"a": [1]}).serialize()

    progress, error, queue = _run("input_df.select(pl.col('a') + 1)", [plan], str(tmp_path / "result.arrow"))

    assert (progress, error) == (100, "")
    assert queue.get(timeout=5) is not None
