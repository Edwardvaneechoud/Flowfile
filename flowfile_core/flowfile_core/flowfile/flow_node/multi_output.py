"""Typed return value and handle naming for multi-output nodes."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine

DEFAULT_OUTPUT_HANDLE = "output-0"
_HANDLE_PREFIX = "output-"


def output_handle(index: int) -> str:
    return f"{_HANDLE_PREFIX}{index}"


def output_handle_index(handle: str) -> int:
    """Return the numeric index of an ``output-N`` handle id.

    Raises ValueError for anything that is not a well-formed output handle, so a
    malformed handle degrades at the parse boundary rather than travelling on as a
    negative index.
    """
    if not handle.startswith(_HANDLE_PREFIX):
        raise ValueError(f"Invalid output handle: {handle!r}")
    try:
        index = int(handle[len(_HANDLE_PREFIX) :])
    except ValueError as exc:
        raise ValueError(f"Invalid output handle: {handle!r}") from exc
    if index < 0:
        raise ValueError(f"Invalid output handle: {handle!r}")
    return index


class NamedOutputs:
    """Typed multi-output return value for node functions.

    The ordered ``outputs`` map preserves declared labels for the UI; runtime
    routing is positional (first entry → ``output-0``, second → ``output-1``,
    …). Checking ``isinstance(result, NamedOutputs)`` is how the node runtime
    distinguishes a multi-output return from a plain ``FlowDataEngine``.
    """

    __slots__ = ("outputs",)

    def __init__(self, outputs: dict[str, FlowDataEngine]) -> None:
        if not outputs:
            raise ValueError("NamedOutputs must contain at least one output")
        self.outputs = outputs

    @property
    def labels(self) -> list[str]:
        return list(self.outputs.keys())

    @property
    def engines(self) -> list[FlowDataEngine]:
        return list(self.outputs.values())

    def by_handle(self) -> dict[str, FlowDataEngine]:
        return {output_handle(i): e for i, e in enumerate(self.outputs.values())}

    def default(self) -> FlowDataEngine:
        return next(iter(self.outputs.values()))
