"""``fl.PythonScript``: a Python Script node, code run on a kernel container when the flow runs."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import uuid4

from pydantic import ValidationError

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import input_schema
from flowfile_frame.native import NativeNode, NativeNodeError

if TYPE_CHECKING:
    from flowfile_frame.flow_frame import FlowFrame


def _kernel_id(kernel: str | Any | None) -> str | None:
    """A kernel id as given, or the ``.id`` of a kernel object; never looked up here."""
    if kernel is None or isinstance(kernel, str):
        return kernel
    kernel_id = getattr(kernel, "id", None)
    if isinstance(kernel_id, str):
        return kernel_id
    raise NativeNodeError(f"kernel= takes a kernel id or an object with an .id, got {type(kernel).__name__}")


class PythonScript(NativeNode):
    """A Python Script node: its code runs on a kernel container when the flow runs.

    Give the script as ``code`` (one cell) or ``cells`` (notebook cells, run in order as one
    script). Every input frame is wired to the script in order; in the kernel each is named
    after its upstream node's ``node_reference``, else ``df_<node id>``. ``outputs`` names the
    output handles (``["main"]`` by default), reached with ``.output`` or ``node[name]``. ``kernel``
    is a kernel id (or an object with an ``.id``) stored as given: it is not checked until the
    flow runs, and a node without one fails the run. The outputs are deferred: typed zero-row
    placeholders carrying the first input's schema (no columns without inputs) until
    ``collect()`` runs the flow.
    """

    code: str
    cells: list[str]
    kernel: str | None

    def __init__(
        self,
        *inputs: FlowFrame,
        code: str | None = None,
        cells: list[str] | None = None,
        kernel: str | Any | None = None,
        outputs: list[str] | None = None,
        description: str | None = None,
        flow_graph: FlowGraph | None = None,
    ) -> None:
        if (code is None) == (cells is None):
            raise NativeNodeError("PythonScript takes exactly one of code= or cells=")
        cell_codes = [code] if code is not None else list(cells)
        if not cell_codes:
            raise NativeNodeError("cells= needs at least one cell")
        if not all(isinstance(cell, str) for cell in cell_codes):
            raise NativeNodeError("code= is a string and cells= a list of strings")
        if outputs is not None and not outputs:
            raise NativeNodeError("outputs= needs at least one output name")
        self.cells = cell_codes
        # Core runs only .code; the drawer joins non-empty cells the same way.
        self.code = "\n\n".join(cell for cell in cell_codes if cell)
        self.kernel = _kernel_id(kernel)

        def make_settings(base: dict[str, Any]) -> input_schema.NodePythonScript:
            try:
                return input_schema.NodePythonScript(
                    python_script_input=input_schema.PythonScriptInput(
                        code=self.code,
                        kernel_id=self.kernel,
                        cells=[input_schema.NotebookCell(id=uuid4().hex, code=cell) for cell in cell_codes],
                    ),
                    output_names=list(outputs) if outputs is not None else ["main"],
                    **base,
                )
            except ValidationError as exc:
                raise NativeNodeError(f"Invalid python_script settings: {exc}") from exc

        self._build(
            "python_script",
            input_schema.NodePythonScript,
            inputs,
            make_settings,
            deferred=None,
            description=description,
            flow_graph=flow_graph,
        )
