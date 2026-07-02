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
    surface a clean has_unsaved_changes for an unmodified file."""
    path = tmp_path / "f.yaml"
    flow = _register(handler, path)
    add_test_manual_input(flow, SAMPLE)
    flow.save_flow(str(path))

    fresh_handler = FlowfileHandler()
    imported_id = fresh_handler.import_flow(path)
    imported = fresh_handler.get_flow(imported_id)
    assert imported.has_unsaved_changes() is False
    assert fresh_handler.get_flow_info_with_runtime(imported_id).has_unsaved_changes is False


def test_reopen_registered_flow_preserves_unsaved_changes(handler, tmp_path):
    """Reopening a flow whose catalog registration is already open in THIS user's session must
    reuse the live in-memory flow, not reload from disk and clobber unsaved edits."""
    path = tmp_path / "f.yaml"
    seed = _register(handler, path)
    add_test_manual_input(seed, SAMPLE, node_id=1)
    seed.save_flow(str(path))

    editor = FlowfileHandler()  # simulates the editor session
    user_id = 7
    # First open: no registration resolved yet (route passes None), so it loads fresh.
    flow_id = editor.import_flow(path, user_id=user_id, source_registration_id=None)
    live = editor.get_flow(flow_id)
    live.flow_settings.source_registration_id = 42  # route stamps this after auto_register
    add_test_manual_input(live, SAMPLE_OTHER, node_id=2)  # unsaved edit
    assert live.has_unsaved_changes() is True

    # Second open ("open flow" again from the catalog): route resolves the registration id.
    reopened_id = editor.import_flow(path, user_id=user_id, source_registration_id=42)

    assert reopened_id == flow_id
    assert editor.get_flow(reopened_id) is live  # same object — not replaced from disk
    assert live.has_unsaved_changes() is True
    assert live.get_node(2) is not None  # unsaved node survived


def test_reopen_unregistered_flow_reloads(handler, tmp_path):
    """An unregistered flow has no catalog identity to key reuse on, so each open loads fresh —
    documents that reuse applies only to registered flows."""
    path = tmp_path / "f.yaml"
    seed = _register(handler, path)
    add_test_manual_input(seed, SAMPLE, node_id=1)
    seed.save_flow(str(path))

    editor = FlowfileHandler()
    flow_id = editor.import_flow(path, user_id=7, source_registration_id=None)
    live = editor.get_flow(flow_id)
    add_test_manual_input(live, SAMPLE_OTHER, node_id=2)

    reopened_id = editor.import_flow(path, user_id=7, source_registration_id=None)
    assert reopened_id == flow_id  # deterministic flow_id from the file
    assert editor.get_flow(reopened_id) is not live  # reloaded fresh, not reused
    assert editor.get_flow(reopened_id).get_node(2) is None  # unsaved edit gone (expected)


def test_find_open_flow_for_user_scoped(handler, tmp_path):
    """_find_open_flow_for_user is scoped to the requesting user and keyed on the registration id:
    a different user never matches another user's open registration (the cross-user leak fix)."""
    path = tmp_path / "f.yaml"
    seed = _register(handler, path)
    add_test_manual_input(seed, SAMPLE, node_id=1)
    seed.save_flow(str(path))

    editor = FlowfileHandler()
    flow_id = editor.import_flow(path, user_id=1, source_registration_id=42)

    assert editor._find_open_flow_for_user(1, 42) == flow_id
    assert editor._find_open_flow_for_user(2, 42) is None  # different user: no match
    assert editor._find_open_flow_for_user(1, 999) is None  # different registration: no match


def test_reopen_by_different_user_loads_fresh(handler, tmp_path):
    """A different user opening the same registration must NOT reuse the first user's live
    (edited, differently-authorized) flow — it loads fresh (fixes the cross-user leak)."""
    path = tmp_path / "f.yaml"
    seed = _register(handler, path)
    add_test_manual_input(seed, SAMPLE, node_id=1)
    seed.save_flow(str(path))

    editor = FlowfileHandler()
    a_id = editor.import_flow(path, user_id=1, source_registration_id=42)
    a_flow = editor.get_flow(a_id)
    add_test_manual_input(a_flow, SAMPLE_OTHER, node_id=2)  # user 1's unsaved edit

    b_id = editor.import_flow(path, user_id=2, source_registration_id=42)
    b_flow = editor.get_flow(b_id)

    assert b_flow is not a_flow  # user 2 did NOT reuse user 1's live object
    assert b_flow.get_node(2) is None  # user 2 got a fresh disk load, not user 1's edits
