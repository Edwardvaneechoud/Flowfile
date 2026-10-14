"""A read names its file by the path's last component under either separator."""

import platform

import polars as pl
import pytest

import flowfile_frame as ff


@pytest.mark.skipif(platform.system() == "Windows", reason="a backslash cannot be part of a file name on Windows")
def test_a_windows_path_read_on_posix_names_its_file(tmp_path):
    """A notebook kernel runs on Linux, where ``titanic\\train.csv`` is one file name; the name must still be
    ``train.csv`` so core does not join the whole path onto itself on the host."""
    path = tmp_path / "titanic\\train.csv"
    pl.DataFrame({"a": [1, 2]}).write_csv(path)

    frame = ff.read_csv(str(path))

    settings = frame.flow_graph.get_node(frame.node_id).setting_input.received_file
    assert settings.name == "train.csv"
    assert settings.abs_file_path == str(path)
    assert frame.collect()["a"].to_list() == [1, 2]
