"""Request / response shapes for the kernel ``/lsp/*`` endpoints.

Mirrored on the core side in ``flowfile_core.lsp.models`` (core can't import
``kernel_runtime``), so keep the two in sync.
"""

from pydantic import BaseModel


class LspRequest(BaseModel):
    code: str  # current cell text
    line: int  # 1-based line within the cell (CodeMirror convention)
    column: int  # 0-based column
    # Namespace/session key. Real flow_id for pythonScript nodes; negative
    # sessionFlowId for the standalone Catalog notebook. Same value the cell
    # executes with, so completions read the matching live namespace.
    flow_id: int = 0
    node_id: int | None = None  # optional, unused in v1


class CompletionItem(BaseModel):
    label: str
    type: str = ""  # jedi completion type: function/instance/module/keyword/class/...
    detail: str = ""  # short signature / description
    documentation: str = ""  # docstring (truncated)


class CompleteResponse(BaseModel):
    items: list[CompletionItem] = []


class HoverResponse(BaseModel):
    contents: str | None = None  # docstring body, RST cleaned
    kind: str = ""  # display kind: function / method / class / variable / module / ...
    name: str = ""
    signature: str = ""


class SignatureInfo(BaseModel):
    label: str
    parameters: list[str] = []
    active_parameter: int = 0
    documentation: str = ""


class SignatureResponse(BaseModel):
    signatures: list[SignatureInfo] = []
    active_signature: int = 0


class Diagnostic(BaseModel):
    line: int  # 1-based
    column: int  # 0-based
    end_line: int
    end_column: int
    message: str
    severity: str = "error"  # error | warning
    source: str = ""  # jedi | pyflakes


class DiagnosticsResponse(BaseModel):
    diagnostics: list[Diagnostic] = []


class LspCapabilities(BaseModel):
    enabled: bool = True
    version: str = ""
    features: list[str] = []


class DataframeSchemasRequest(BaseModel):
    flow_id: int  # namespace/session key, same value the cell executes with
    node_id: int | None = None  # optional, unused in v1
    # Opt-in: resolving a lazy plan runs the planner and can do I/O, so callers ask for it.
    resolve_lazy_frames: bool = False


class DataframeColumn(BaseModel):
    name: str
    dtype: str  # str(polars dtype), e.g. "Int64"


class DataframeSchema(BaseModel):
    name: str  # the variable name in the namespace
    kind: str  # DataFrame | LazyFrame
    state: str  # ready | unresolved (a LazyFrame is unresolved unless the caller opted in)
    columns: list[DataframeColumn] = []
    truncated: bool = False  # column list hit the per-frame cap


class DataframeSchemasResponse(BaseModel):
    namespace_generation: str = ""
    revision: int = 0
    state: str = "unavailable"  # ready | busy | unavailable
    dataframes: list[DataframeSchema] = []
