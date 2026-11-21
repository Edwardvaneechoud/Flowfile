"""``shared.flow_merge`` against core: what it writes loads, and it writes a saved flow exactly as core saves it."""

import pytest

from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.schemas.schemas import FlowfileData
from shared.flow_merge.canonical import canonical, emit
from shared.flow_merge.diff import diff
from shared.tests.flow_merge.fixtures import fixture_id, flow_files, load

FLOW_FILES = flow_files()


@pytest.mark.parametrize("path", FLOW_FILES, ids=fixture_id)
def test_emitted_document_validates_as_flowfile_data(path):
    flow, _ = canonical(load(path))
    FlowfileData.model_validate(emit(flow))


@pytest.mark.parametrize("path", FLOW_FILES, ids=fixture_id)
def test_a_flow_core_saves_reads_and_writes_back_unchanged(path):
    """Open the file in core, take what a save would write, and read that once: nothing is repaired or reordered."""
    graph = open_flow(path.resolve())
    try:
        saved = graph.get_flowfile_data().model_dump(mode="json")
    finally:
        graph.close_flow()
    flow, issues = canonical(saved)
    assert issues == []
    assert emit(flow) == saved


@pytest.mark.parametrize("path", FLOW_FILES, ids=fixture_id)
def test_open_and_save_is_noise(path, tmp_path):
    """Saving a flow, opening that file and saving it again changes nothing a review would show, layout included."""
    first = open_flow(path.resolve())
    saved_path = tmp_path / f"{path.stem}.yaml"
    try:
        first.save_flow(str(saved_path))
        once = first.get_flowfile_data().model_dump(mode="json")
    finally:
        first.close_flow()
    second = open_flow(saved_path)
    try:
        twice = second.get_flowfile_data().model_dump(mode="json")
    finally:
        second.close_flow()
    result = diff(once, twice)
    assert result.changes == () and result.layout == () and result.format == ()
