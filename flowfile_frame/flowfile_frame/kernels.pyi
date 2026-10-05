# Auto-generated stub for flowfile_frame.kernels — do not edit.
# Run `make stubs` to regenerate from the Python source.
from __future__ import annotations

import builtins
from collections.abc import Iterator
from typing import NamedTuple
from flowfile_frame.native import NativeNodeError

kernels: Kernels

class KernelLookupError(NativeNodeError, KeyError):
    ...

class KernelInfo(NamedTuple):
    id: str
    name: str
    flavour: str
    packages: list[str]

class Kernels:
    def list(self) -> builtins.list[KernelInfo]: ...
    def get(self, kernel_id: str) -> KernelInfo: ...
    def __getitem__(self, kernel_id: str) -> KernelInfo: ...
    def __contains__(self, kernel_id: object) -> bool: ...
    def __iter__(self) -> Iterator[str]: ...
    def __len__(self) -> int: ...

