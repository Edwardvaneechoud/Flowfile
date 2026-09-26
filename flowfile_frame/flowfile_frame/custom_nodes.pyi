# Auto-generated stub for flowfile_frame.custom_nodes — do not edit.
# Run `make stubs` to regenerate from the Python source.
from __future__ import annotations

import builtins
import os
from collections.abc import Iterator
from pathlib import Path
from typing import NamedTuple
from flowfile_frame.custom_node import CustomNodeFactory
from flowfile_frame.native import NativeNodeError
from shared.node_designer.custom_node import CustomNodeBase

custom_nodes: CustomNodes

class CustomNodeLookupError(NativeNodeError, AttributeError):
    ...

class CustomNodeInfo(NamedTuple):
    key: str
    name: str
    category: str
    environment: str
    inputs: int
    outputs: list[str]
    file: Path | None
    error: str | None

class CustomNodes:
    def list(self) -> builtins.list[CustomNodeInfo]: ...
    def get(self, name: str) -> CustomNodeFactory: ...
    def install(self, node: type[CustomNodeBase] | str | os.PathLike[str], *, overwrite: bool=False) -> Path: ...
    def __getattr__(self, name: str) -> CustomNodeFactory: ...
    def __getitem__(self, name: str) -> CustomNodeFactory: ...
    def __contains__(self, name: object) -> bool: ...
    def __iter__(self) -> Iterator[str]: ...
    def __len__(self) -> int: ...
    def __dir__(self) -> builtins.list[str]: ...

