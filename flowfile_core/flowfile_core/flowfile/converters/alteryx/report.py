"""Conversion report models for the Alteryx `.yxmd` importer."""

from typing import Literal

from pydantic import BaseModel, Field

from flowfile_core.schemas import schemas

ToolStatus = Literal["converted", "partial", "commented", "placeholder", "skipped"]


class ToolReportRow(BaseModel):
    """What happened to a single Alteryx tool during conversion."""

    alteryx_tool_id: int
    alteryx_tool: str
    flowfile_node_ids: list[int] = Field(default_factory=list)
    flowfile_node_type: str | None = None
    status: ToolStatus
    messages: list[str] = Field(default_factory=list)


class ConversionReport(BaseModel):
    """Per-workflow summary of the conversion, surfaced in the import dialog."""

    workflow_name: str
    total_tools: int
    converted: int = 0
    partial: int = 0
    commented: int = 0
    placeholder: int = 0
    skipped: int = 0
    rows: list[ToolReportRow] = Field(default_factory=list)


class ConversionResult(BaseModel):
    """The converted flow plus the report describing how it was produced."""

    flow_data: schemas.FlowfileData
    report: ConversionReport
