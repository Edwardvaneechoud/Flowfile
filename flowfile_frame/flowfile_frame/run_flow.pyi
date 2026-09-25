# Auto-generated stub for flowfile_frame.run_flow — do not edit.
# Run `make stubs` to regenerate from the Python source.
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
import polars as pl
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import input_schema
from flowfile_frame.catalog_reference import CatalogReference, SchemaReference
from flowfile_frame.native import NativeNode
from polars._typing import PolarsDataType
from flowfile_frame.flow_frame import FlowFrame

class FlowRef:
    registration_id: int
    flow_uuid: str
    flow_path: str
    name: str
    schema: SchemaReference | None
    def __init__(self, registration_id: int, flow_uuid: str, flow_path: str, name: str, schema: SchemaReference | None=None) -> None: ...
    @property
    def namespace_full_name(self) -> str | None: ...
    def to_subflow_reference(self) -> input_schema.SubflowReference: ...

class RunFlow(NativeNode):
    flow: FlowRef
    def __init__(self, flow: FlowRef | int | FlowGraph | FlowFrame, *, name: str | None=None, params: Mapping[str, Any] | None=None, param_frame: FlowFrame | None=None, iterate: bool=False, append_metadata: bool=True, description: str | None=None, flow_graph: FlowGraph | None=None, **input_frames: FlowFrame) -> None: ...


def flow_ref(namespace: str | SchemaReference | CatalogReference | None=None, name: str | None=None, *, uuid: str | None=None, registration_id: int | None=None) -> FlowRef: ...
def register_flow(flow_or_frame: FlowGraph | FlowFrame, *, name: str, schema: SchemaReference | None=None, overwrite: bool=False) -> FlowRef: ...
def FlowInput(name: str, *, schema: Mapping[str, PolarsDataType] | Sequence[tuple[str, PolarsDataType]] | None=None, sample: Mapping[str, Sequence[Any]] | pl.DataFrame | None=None, flow_graph: FlowGraph | None=None, description: str | None=None) -> FlowFrame: ...
