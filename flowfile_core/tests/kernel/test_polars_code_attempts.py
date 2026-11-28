"""No attempt of GHSA-f34x-gw22-9wwh runs in core on a multi-user server (no Docker)."""

import polars as pl
import pytest
from polars_code_attempts import NAMES, attempts, written_files

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine, execute_polars_code
from shared.polars_code_parser import IN_PROCESS_REFUSED_MESSAGE


@pytest.mark.parametrize("mode", ["docker", "server"])
@pytest.mark.parametrize("name", NAMES)
def test_no_attempt_runs_in_core_on_a_server(name, mode, tmp_path, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", mode)
    code = attempts(tmp_path)[name]

    with pytest.raises(PermissionError, match=IN_PROCESS_REFUSED_MESSAGE):
        execute_polars_code(FlowDataEngine(pl.LazyFrame({"a": [1]})), code=code)

    assert [path for path in written_files(tmp_path) if path.exists()] == []
