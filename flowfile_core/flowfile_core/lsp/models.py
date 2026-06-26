"""Core-side mirrors of ``kernel_runtime.lsp.models`` (core can't import the kernel).

Keep field names/shapes in sync with the kernel models so ``Model(**kernel_json)``
round-trips through the bridge unchanged.
"""

from pydantic import BaseModel


class LspRequest(BaseModel):
    code: str
    line: int
    column: int
    flow_id: int = 0
    node_id: int | None = None


class CompletionItem(BaseModel):
    label: str
    type: str = ""
    detail: str = ""
    documentation: str = ""


class CompleteResponse(BaseModel):
    items: list[CompletionItem] = []


class HoverResponse(BaseModel):
    contents: str | None = None


class SignatureInfo(BaseModel):
    label: str
    parameters: list[str] = []
    active_parameter: int = 0
    documentation: str = ""


class SignatureResponse(BaseModel):
    signatures: list[SignatureInfo] = []
    active_signature: int = 0


class Diagnostic(BaseModel):
    line: int
    column: int
    end_line: int
    end_column: int
    message: str
    severity: str = "error"
    source: str = ""


class DiagnosticsResponse(BaseModel):
    diagnostics: list[Diagnostic] = []


class LspCapabilities(BaseModel):
    enabled: bool = True
    version: str = ""
    features: list[str] = []
