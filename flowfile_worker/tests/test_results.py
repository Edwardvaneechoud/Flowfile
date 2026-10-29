"""The registry of what the ``/v1`` routes hold."""

import os

import pytest

from flowfile_worker import results as results_module
from flowfile_worker.results import ResultRecord, ResultRegistry

pytestmark = pytest.mark.worker


def test_a_file_the_sweep_cannot_remove_is_logged_and_the_sweep_goes_on(tmp_path, monkeypatch, caplog):
    registry = ResultRegistry()
    for name in ("stuck", "free"):
        path = tmp_path / f"{name}.arrow"
        path.write_bytes(b"rows")
        registry.put(
            ResultRecord(
                result_id=name, state="materialised", columns=[], row_count=0, flow_id=1, node_id=1, path=str(path)
            )
        )
        registry.mark_ended(name)
    real_remove = os.remove

    def remove(path):
        if str(path).endswith("stuck.arrow"):
            raise PermissionError("the file is in use")
        real_remove(path)

    monkeypatch.setattr(results_module.os, "remove", remove)

    assert sorted(registry.expire(0)) == ["free", "stuck"]

    assert not (tmp_path / "free.arrow").exists()
    assert (tmp_path / "stuck.arrow").exists()
    assert registry.get("stuck") is None and registry.get("free") is None
    assert "stuck" in caplog.text and "the file is in use" in caplog.text


def _record(**fields) -> ResultRecord:
    given = {"result_id": "r", "columns": [], "row_count": None, "flow_id": 1, "node_id": 1}
    given.update(fields)
    return ResultRecord(**given)


def test_a_record_holds_what_its_state_says():
    assert _record(state="materialised", path="/tmp/r.arrow").path == "/tmp/r.arrow"
    assert _record(state="deferred", plan=b"plan").plan == b"plan"
    assert _record(state="materialised", path="/tmp/r.blob", kind="blob").kind == "blob"


@pytest.mark.parametrize(
    "fields",
    [
        {"state": "materialised"},
        {"state": "materialised", "path": "/tmp/r.arrow", "plan": b"plan"},
        {"state": "deferred"},
        {"state": "deferred", "plan": b"plan", "path": "/tmp/r.arrow"},
        {"state": "deferred", "plan": b"plan", "kind": "blob"},
        {"state": "materialised", "path": "/tmp/r.blob", "kind": "blob", "row_count": 3},
    ],
    ids=["file-less", "file-and-plan", "plan-less", "plan-and-file", "deferred-blob", "counted-blob"],
)
def test_a_record_whose_fields_do_not_fit_its_state_is_refused(fields):
    with pytest.raises(ValueError, match="r$"):
        _record(**fields)
