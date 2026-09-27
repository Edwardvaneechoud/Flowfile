"""``fl.polars_code``: one Polars Code node over zero or more frames, returned as its output frame."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from flowfile_core.schemas import input_schema, transform_schema
from flowfile_frame.native import NativeNodeError, source_frame
from flowfile_frame.utils import _implicit_graph, generate_node_id

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph
    from flowfile_frame.flow_frame import FlowFrame


def polars_code(
    code: str | Callable[..., Any],
    *inputs: FlowFrame,
    flow_graph: FlowGraph | None = None,
    description: str | None = None,
) -> FlowFrame:
    """Place one Polars Code node reading ``inputs`` in order, or none: a source node.

    ``code`` is stored as :meth:`FlowFrame.polars_code` stores it (a string, or a ``def`` whose body
    is read). With inputs this is ``inputs[0].polars_code(code, *inputs[1:])``. Without, the code
    builds its own frame (``output_df = pl.LazyFrame(...)``) and the node lands on ``flow_graph``,
    else the implicit graph (the notebook session's in notebook mode, a new one otherwise), the way
    ``fl.from_dict`` places a source.
    """
    from flowfile_frame.flow_frame import FlowFrame, _polars_code_source

    if inputs:
        first = inputs[0]
        if not isinstance(first, FlowFrame):
            raise NativeNodeError(f"polars_code takes FlowFrames as inputs, got {type(first).__name__}")
        if flow_graph is not None and first.flow_graph is not flow_graph:
            raise NativeNodeError("polars_code: the inputs live on another graph than flow_graph")
        return first.polars_code(code, *inputs[1:], description=description)
    text = _polars_code_source(code)
    graph = flow_graph if flow_graph is not None else _implicit_graph()
    node_id = generate_node_id()
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=node_id,
            polars_code_input=transform_schema.PolarsCodeInput(polars_code=text),
            is_setup=True,
            depending_on_ids=[],
            description=description,
        )
    )
    error = graph.get_node(node_id).results.errors
    if not error:
        try:
            return source_frame(graph, node_id)
        except Exception as exc:
            error = exc
    graph.delete_node(node_id)
    raise NativeNodeError(f"polars_code node {node_id}: {error}")
