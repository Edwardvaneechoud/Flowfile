
import uuid
import os
from typing import Any, Iterable, List, Literal, Optional, Tuple, Union
from pathlib import Path

import re
import polars as pl
from polars._typing import FrameInitTypes, SchemaDefinition, SchemaDict, Orientation
from flowfile_frame.lazy_methods import add_lazyframe_methods


# Assume these imports are correct from your original context
from flowfile_core.flowfile.FlowfileFlow import FlowGraph, add_connection
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.schemas import input_schema, transform_schema

from flowfile_frame.expr import Expr, Column, lit, col
from flowfile_frame.utils import _parse_inputs_as_iterable, create_flow_graph
from flowfile_frame.flow_frame import generate_node_id, FlowFrame


def sum(expr):
    """Sum aggregation function."""
    if isinstance(expr, str):
        expr = col(expr)
    return expr.sum()


def mean(expr):
    """Mean aggregation function."""
    if isinstance(expr, str):
        expr = col(expr)
    return expr.mean()


def min(expr):
    """Min aggregation function."""
    if isinstance(expr, str):
        expr = col(expr)
    return expr.min()


def max(expr):
    """Max aggregation function."""
    if isinstance(expr, str):
        expr = col(expr)
    return expr.max()


def count(expr):
    """Count aggregation function."""
    if isinstance(expr, str):
        expr = col(expr)
    return expr.count()


def read_csv(file_path, *, flow_graph: FlowGraph = None, separator: str = ';',
             convert_to_absolute_path: bool = True,
             description: str = None, **options):
    """
    Read a CSV file into a FlowFrame.

    Args:
        file_path: Path to CSV file
        flow_graph: if you want to add it to an existing graph
        separator: Single byte character to use as separator in the file.
        convert_to_absolute_path: If the path needs to be set to a fixed location
        description: if you want to add a readable name in the frontend (advised)
        **options: Options for polars.read_csv

    Returns:
        A FlowFrame with the CSV data
    """
    # Create new node ID
    node_id = generate_node_id()
    if flow_graph is None:
        flow_graph = create_flow_graph()

    flow_id = flow_graph.flow_id

    has_headers = options.get('has_header', True)
    encoding = options.get('encoding', 'utf-8')

    if '~' in file_path:
        file_path = os.path.expanduser(file_path)

    received_table = input_schema.ReceivedTable(
        file_type='csv',
        path=file_path,
        name=Path(file_path).name,
        delimiter=separator,
        has_headers=has_headers,
        encoding=encoding
    )

    if convert_to_absolute_path:
        received_table.path = received_table.abs_file_path

    read_node = input_schema.NodeRead(
        flow_id=flow_id,
        node_id=node_id,
        received_file=received_table,
        pos_x=100,
        pos_y=100,
        is_setup=True
    )

    flow_graph.add_read(read_node)

    return FlowFrame(
        data=flow_graph.get_node(node_id).get_resulting_data().data_frame,
        flow_graph=flow_graph,
        node_id=node_id
    )


def read_parquet(file_path, *, flow_graph: FlowGraph = None, description: str = None,
                 convert_to_absolute_path: bool = True, **options) -> FlowFrame:
    """
    Read a Parquet file into a FlowFrame.

    Args:
        file_path: Path to Parquet file
        flow_graph: if you want to add it to an existing graph
        description: if you want to add a readable name in the frontend (advised)
        convert_to_absolute_path: If the path needs to be set to a fixed location
        **options: Options for polars.read_parquet

    Returns:
        A FlowFrame with the Parquet data
    """
    if '~' in file_path:
        file_path = os.path.expanduser(file_path)
    node_id = generate_node_id()

    if flow_graph is None:
        flow_graph = create_flow_graph()

    flow_id = flow_graph.flow_id

    received_table = input_schema.ReceivedTable(
        file_type='parquet',
        path=file_path,
        name=Path(file_path).name,
    )
    if convert_to_absolute_path:
        received_table.path = received_table.abs_file_path

    read_node = input_schema.NodeRead(
        flow_id=flow_id,
        node_id=node_id,
        received_file=received_table,
        pos_x=100,
        pos_y=100,
        is_setup=True,
        description=description
    )

    flow_graph.add_read(read_node)

    return FlowFrame(
        data=flow_graph.get_node(node_id).get_resulting_data().data_frame,
        flow_graph=flow_graph,
        node_id=node_id
    )


def from_dict(data, *, flow_graph: FlowGraph = None, description: str = None) -> FlowFrame:
    """
    Create a FlowFrame from a dictionary or list of dictionaries.

    Args:
        data: Dictionary of lists or list of dictionaries
        flow_graph: if you want to add it to an existing graph
        description: if you want to add a readable name in the frontend (advised)
    Returns:
        A FlowFrame with the data
    """
    # Create new node ID
    node_id = generate_node_id()

    if not flow_graph:
        flow_graph = create_flow_graph()
    flow_id = flow_graph.flow_id

    input_node = input_schema.NodeManualInput(
        flow_id=flow_id,
        node_id=node_id,
        raw_data=FlowDataEngine(data).to_pylist(),
        pos_x=100,
        pos_y=100,
        is_setup=True,
        description=description
    )

    # Add to graph
    flow_graph.add_manual_input(input_node)

    # Return new frame
    return FlowFrame(
        data=flow_graph.get_node(node_id).get_resulting_data().data_frame,
        flow_graph=flow_graph,
        node_id=node_id
    )


def concat(frames: List['FlowFrame'],
                  how: str = 'vertical',
                  rechunk: bool = False,
                  parallel: bool = True,
                  description: str = None) -> 'FlowFrame':
    """
    Concatenate multiple FlowFrames into one.

    Parameters
    ----------
    frames : List[FlowFrame]
        List of FlowFrames to concatenate
    how : str, default 'vertical'
        How to combine the FlowFrames (see concat method documentation)
    rechunk : bool, default False
        Whether to ensure contiguous memory in result
    parallel : bool, default True
        Whether to use parallel processing for the operation
    description : str, optional
        Description of this operation

    Returns
    -------
    FlowFrame
        A new FlowFrame with the concatenated data
    """
    if not frames:
        raise ValueError("No frames provided to concat_frames")

    if len(frames) == 1:
        return frames[0]

    # Use first frame's concat method with remaining frames
    first_frame = frames[0]
    remaining_frames = frames[1:]

    return first_frame.concat(remaining_frames, how=how,
                              rechunk=rechunk, parallel=parallel,
                              description=description)


