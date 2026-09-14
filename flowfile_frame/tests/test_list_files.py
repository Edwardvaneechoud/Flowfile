"""Tests for the ff.list_files source function."""
from pathlib import Path

import pytest

import flowfile_frame as ff


@pytest.fixture
def sample_tree(tmp_path: Path) -> Path:
    (tmp_path / "a.csv").write_text("v\n1\n")
    (tmp_path / "b.csv").write_text("v\n2\n")
    (tmp_path / "c.parquet").write_bytes(b"PAR1")
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "d.csv").write_text("v\n3\n")
    return tmp_path


def test_lists_a_directory(sample_tree):
    frame = ff.list_files(sample_tree)
    names = frame.collect()["file_name"].to_list()
    assert sorted(names) == ["a.csv", "b.csv", "c.parquet"]


def test_accepts_a_path_object_and_a_string(sample_tree):
    assert ff.list_files(sample_tree).collect().height == ff.list_files(str(sample_tree)).collect().height


def test_filters_and_recurses(sample_tree):
    frame = ff.list_files(sample_tree, file_types=["csv"], recursive=True)
    assert sorted(frame.collect()["file_name"].to_list()) == ["a.csv", "b.csv", "d.csv"]


def test_emits_a_native_list_files_node(sample_tree):
    frame = ff.list_files(sample_tree, description="my folder")
    node = frame.flow_graph.get_node(frame.node_id)
    assert node.node_type == "list_files"
    assert node.setting_input.description == "my folder"


def test_chains_into_downstream_operations(sample_tree):
    frame = ff.list_files(sample_tree, file_types=["csv"]).filter(ff.col("file_name") == "a.csv")
    assert frame.collect()["file_name"].to_list() == ["a.csv"]


def test_output_is_readable_by_a_reader(sample_tree):
    """The point of the node: file_path feeds a real read."""
    import polars as pl

    paths = ff.list_files(sample_tree, file_types=["csv"]).collect()["file_path"].to_list()
    total = sum(pl.read_csv(p).height for p in paths)
    assert total == 2


def test_missing_directory_raises(tmp_path):
    from fastapi.exceptions import HTTPException

    with pytest.raises(HTTPException):
        ff.list_files(tmp_path / "nope")
