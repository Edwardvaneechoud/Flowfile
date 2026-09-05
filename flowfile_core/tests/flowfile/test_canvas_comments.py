"""
Tests for canvas comments (free text notes; no execution impact).

Run with:
    pytest flowfile_core/tests/flowfile/test_canvas_comments.py -v
"""
import tempfile
from pathlib import Path

import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.schemas import input_schema, schemas


def create_graph(flow_id: int = 1) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(flow_id=flow_id, name="test_flow", path=".", execution_mode="Development")
    )
    graph = handler.get_flow(flow_id)
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
        )
    )
    return graph


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


def test_create_comment_assigns_id_and_defaults():
    graph = create_graph()
    comment = graph.create_comment("Check the join keys", 10.0, 20.0)
    assert comment.id == 1
    assert graph._comments[1].text == "Check the join keys"
    assert (comment.x_position, comment.y_position) == (10.0, 20.0)
    assert comment.width > 0 and comment.height > 0
    sized = graph.create_comment("", 0.0, 0.0, width=300.0, height=80.0)
    assert sized.id == 2
    assert (sized.width, sized.height) == (300.0, 80.0)


def test_update_and_delete_comment():
    graph = create_graph()
    comment = graph.create_comment("draft", 0.0, 0.0)
    graph.update_comment(comment.id, text="final")
    assert graph._comments[comment.id].text == "final"
    graph.update_comment(comment.id, bounds=schemas.CommentBounds(5.0, 6.0, 7.0, 8.0))
    stored = graph._comments[comment.id]
    assert (stored.x_position, stored.y_position, stored.width, stored.height) == (5.0, 6.0, 7.0, 8.0)
    assert stored.text == "final"  # a bounds-only update leaves the text alone
    graph.delete_comment(comment.id)
    assert graph._comments == {}
    graph.delete_comment(comment.id)  # deleting twice is a no-op
    with pytest.raises(ValueError):
        graph.update_comment(comment.id, text="gone")


def test_comments_ride_along_in_flowfile_data_and_vue_flow_input():
    graph = create_graph()
    graph.create_comment("note", 1.0, 2.0)
    data = graph.get_flowfile_data()
    assert [comment.text for comment in data.comments] == ["note"]
    assert "comments" in data.model_dump()
    assert [comment.text for comment in graph.get_vue_flow_input().comments] == ["note"]


def test_yaml_round_trip_preserves_comments(temp_dir):
    graph = create_graph()
    comment = graph.create_comment("multi\nline", 11.0, 22.0, width=333.0, height=44.0)
    path = temp_dir / "flow.yaml"
    graph.save_flow(str(path))
    reloaded = open_flow(path)
    restored = reloaded._comments[comment.id]
    assert restored.text == "multi\nline"
    assert (restored.x_position, restored.y_position, restored.width, restored.height) == (11.0, 22.0, 333.0, 44.0)
    # The id counter resumes above the restored id.
    assert reloaded.create_comment("next", 0.0, 0.0).id == comment.id + 1


def test_backward_compat_flow_without_comments(temp_dir):
    graph = create_graph()
    path = temp_dir / "flow.yaml"
    graph.save_flow(str(path))
    assert open_flow(path)._comments == {}


def test_comment_operations_are_undoable():
    graph = create_graph()
    comment = graph.create_comment("first", 0.0, 0.0)
    graph.update_comment(comment.id, text="second")
    graph.undo()
    assert graph._comments[comment.id].text == "first"
    graph.undo()
    assert graph._comments == {}
    graph.redo()
    assert graph._comments[comment.id].text == "first"
    graph.delete_comment(comment.id)
    graph.undo()
    assert graph._comments[comment.id].text == "first"


def test_set_comment_bounds_persists_without_history():
    graph = create_graph()
    comment = graph.create_comment("note", 0.0, 0.0)
    graph.set_comment_bounds(
        [schemas.CommentBoundsUpdate(comment_id=comment.id, x_position=1.0, y_position=2.0, width=3.0, height=4.0)]
    )
    stored = graph._comments[comment.id]
    assert (stored.x_position, stored.y_position, stored.width, stored.height) == (1.0, 2.0, 3.0, 4.0)
    graph.set_comment_bounds(
        [schemas.CommentBoundsUpdate(comment_id=999, x_position=0.0, y_position=0.0, width=1.0, height=1.0)]
    )  # unknown ids are ignored


def test_comment_id_never_reuses_a_freed_id():
    graph = create_graph()
    first = graph.create_comment("a", 0.0, 0.0)
    graph.delete_comment(first.id)
    assert graph.create_comment("b", 0.0, 0.0).id == first.id + 1
