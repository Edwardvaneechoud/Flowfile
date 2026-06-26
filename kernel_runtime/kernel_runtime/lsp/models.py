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
    insert_text: str | None = None  # text to append after the trigger, if it differs


class CompleteResponse(BaseModel):
    items: list[CompletionItem] = []


class HoverResponse(BaseModel):
    contents: str | None = None  # markdown-ish: full name + signature + docstring


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
