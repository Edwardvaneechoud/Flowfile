"""Pure unit tests for the core-side WS streaming client's complete-message handling.

No worker needed: they exercise Status construction from a raw 'complete' frame,
covering the row-count ride-along being present or absent.
"""

from flowfile_core.flowfile.flow_data_engine.subprocess_operations.streaming import _handle_complete_message


def test_handle_complete_message_with_count():
    data = {
        "type": "complete",
        "result_type": "polars",
        "file_ref": "/tmp/x.arrow",
        "has_result": True,
        "number_of_records": 42,
    }
    status = _handle_complete_message(data, "task-1")
    assert status.status == "Completed"
    assert status.result_type == "polars"
    assert status.number_of_records == 42


def test_handle_complete_message_without_count():
    data = {
        "type": "complete",
        "result_type": "other",
        "file_ref": "/tmp/x.arrow",
        "has_result": False,
    }
    status = _handle_complete_message(data, "task-2")
    assert status.number_of_records is None
