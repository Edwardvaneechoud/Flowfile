"""A flow file that still holds merge conflicts is refused wherever core would open, run or apply it."""

import json

import pytest

from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.project.importer import ProjectFileError, _validate_project_file
from shared.flow_merge.conflict import UnresolvedMergeError, conflict_document
from shared.flow_merge.merge import merge
from shared.flow_merge.yaml_io import dump
from shared.tests.flow_merge.editing import copied, sales, settings


def _conflicted() -> dict:
    base = sales()
    ours, theirs = copied(base), copied(base)
    settings(ours, 6)["join_input"]["how"] = "inner"
    settings(theirs, 6)["join_input"]["how"] = "outer"
    document = conflict_document(merge(base, ours, theirs))
    document["flow_uuid"] = "00000000-0000-0000-0000-000000000001"
    return document


@pytest.mark.parametrize("suffix", [".yaml", ".json"])
def test_open_flow_refuses_conflict_document(tmp_path, suffix):
    path = tmp_path / f"sales{suffix}"
    document = _conflicted()
    path.write_text(json.dumps(document) if suffix == ".json" else dump(document), encoding="utf-8")
    with pytest.raises(UnresolvedMergeError, match="1 merge conflict that"):
        open_flow(path)


def test_apply_refuses_conflicted_flow_file():
    with pytest.raises(ProjectFileError, match="still holds merge conflicts"):
        _validate_project_file("flows/sales.flow.yaml", "flow", dump(_conflicted()))


def test_a_settled_flow_opens(tmp_path):
    from shared.flow_merge.conflict import resolve

    path = tmp_path / "sales.yaml"
    document = resolve(_conflicted(), {"step:6": "theirs"})
    path.write_text(dump(document), encoding="utf-8")
    graph = open_flow(path)
    try:
        assert graph.get_node(6).setting_input.join_input.how == "outer"
    finally:
        graph.close_flow()
