"""Verify partition_by threads from the frame API into the catalog writer node."""

from __future__ import annotations

import flowfile_frame as ff


def test_write_catalog_table_threads_partition_by():
    df = ff.from_dict({"a": [1, 2], "b": ["x", "y"]})
    child = df.write_catalog_table("ptest", namespace_id=None, partition_by=["b"])
    settings = df.flow_graph.get_node(child.node_id).setting_input.catalog_write_settings
    assert settings.partition_by == ["b"]


def test_write_catalog_table_default_no_partition():
    df = ff.from_dict({"a": [1, 2], "b": ["x", "y"]})
    child = df.write_catalog_table("ptest2", namespace_id=None)
    settings = df.flow_graph.get_node(child.node_id).setting_input.catalog_write_settings
    assert settings.partition_by == []
