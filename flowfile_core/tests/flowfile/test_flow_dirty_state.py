"""Tests for the has_unsaved_changes / dirty-state lifecycle on FlowGraph."""

from pathlib import Path

import pytest

from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.schemas import schemas

from tests.flowfile.conftest import add_test_manual_input


SAMPLE = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
SAMPLE_OTHER = [{"a": 9, "b": 9}]


@pytest.fixture
def handler():
    return FlowfileHandler()


def _register(handler: FlowfileHandler, flow_path: Path, flow_id: int = 1):
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="dirt",
            path=str(flow_path),
            execution_mode="Development",
        )
    )
    return handler.get_flow(flow_id)


def test_new_empty_flow_is_clean(handler, tmp_path):
    flow = _register(handler, tmp_path / "f.yaml")
    assert flow.has_unsaved_changes() is False


def test_adding_node_marks_dirty(handler, tmp_path):
    flow = _register(handler, tmp_path / "f.yaml")
    add_test_manual_input(flow, SAMPLE)
    assert flow.has_unsaved_changes() is True


def test_save_clears_dirty(handler, tmp_path):
    path = tmp_path / "f.yaml"
    flow = _register(handler, path)
    add_test_manual_input(flow, SAMPLE)
    flow.save_flow(str(path))
    assert flow.has_unsaved_changes() is False


def test_modify_after_save_is_dirty(handler, tmp_path):
    path = tmp_path / "f.yaml"
    flow = _register(handler, path)
    add_test_manual_input(flow, SAMPLE, node_id=1)
    flow.save_flow(str(path))
    add_test_manual_input(flow, SAMPLE_OTHER, node_id=2)
    assert flow.has_unsaved_changes() is True


def test_resave_clears_dirty_again(handler, tmp_path):
    path = tmp_path / "f.yaml"
    flow = _register(handler, path)
    add_test_manual_input(flow, SAMPLE, node_id=1)
    flow.save_flow(str(path))
    add_test_manual_input(flow, SAMPLE_OTHER, node_id=2)
    flow.save_flow(str(path))
    assert flow.has_unsaved_changes() is False


def test_open_flow_is_clean(handler, tmp_path):
    """Regression: open_flow used to leave the saved-hash baseline pointing at the
    empty initial graph, so freshly opened flows reported dirty immediately."""
    path = tmp_path / "f.yaml"
    flow = _register(handler, path)
    add_test_manual_input(flow, SAMPLE)
    flow.save_flow(str(path))

    reopened = open_flow(path)
    assert reopened.has_unsaved_changes() is False


def test_open_modify_save_clears_dirty(handler, tmp_path):
    """End-to-end user scenario: open from disk, edit, save — dirty should clear."""
    path = tmp_path / "f.yaml"
    flow = _register(handler, path)
    add_test_manual_input(flow, SAMPLE, node_id=1)
    flow.save_flow(str(path))

    reopened = open_flow(path)
    assert reopened.has_unsaved_changes() is False
    add_test_manual_input(reopened, SAMPLE_OTHER, node_id=2)
    assert reopened.has_unsaved_changes() is True
    reopened.save_flow(str(path))
    assert reopened.has_unsaved_changes() is False


def test_handler_import_flow_reports_clean(handler, tmp_path):
    """Going through FlowfileHandler.import_flow (the route's call path) should
    surface a clean flow_settings.has_unsaved_changes for an unmodified file."""
    path = tmp_path / "f.yaml"
    flow = _register(handler, path)
    add_test_manual_input(flow, SAMPLE)
    flow.save_flow(str(path))

    fresh_handler = FlowfileHandler()
    imported_id = fresh_handler.import_flow(path)
    imported = fresh_handler.get_flow(imported_id)
    assert imported.has_unsaved_changes() is False
    assert imported.flow_settings.has_unsaved_changes is False
