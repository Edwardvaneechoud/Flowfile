"""Conversion report models for the Alteryx `.yxmd` importer."""

from typing import Literal

from pydantic import BaseModel, Field

from flowfile_core.schemas import schemas

ToolStatus = Literal["converted", "partial", "commented", "placeholder", "skipped"]
ToolEntity = Literal["tool", "annotation"]

MAPPED_STATUSES: tuple[ToolStatus, ...] = ("converted", "partial", "commented")


class ToolReportRow(BaseModel):
    """What happened to a single Alteryx tool during conversion."""

    alteryx_tool_id: int
    alteryx_tool: str
    entity: ToolEntity = "tool"
    flowfile_node_ids: list[int] = Field(default_factory=list)
    flowfile_node_type: str | None = None
    status: ToolStatus
    messages: list[str] = Field(default_factory=list)


class CoverageSummary(BaseModel):
    """The two coverage numbers, always together, plus the sentence defining them.

    A canvas comment is an annotation, not a tool, so it never enters either number —
    counting comments is what let the first version of this report claim 74% coverage on
    a corpus where 45% of the tools were mapped.
    """

    tools: int
    mapped: int
    converted: int
    mapped_percent: int
    converted_percent: int
    definition: str


class ConversionReport(BaseModel):
    """Per-workflow summary of the conversion, surfaced in the import dialog.

    Every count except ``total_annotations`` is over Alteryx tools only.
    """

    workflow_name: str
    total_tools: int
    total_annotations: int = 0
    converted: int = 0
    partial: int = 0
    commented: int = 0
    placeholder: int = 0
    skipped: int = 0
    coverage: CoverageSummary
    rows: list[ToolReportRow] = Field(default_factory=list)


class ConversionResult(BaseModel):
    """The converted flow plus the report describing how it was produced."""

    flow_data: schemas.FlowfileData
    report: ConversionReport


def _percent(part: int, whole: int) -> int:
    return round(100 * part / whole) if whole else 0


def build_coverage(tool_rows: list[ToolReportRow]) -> CoverageSummary:
    """Summarise the tool rows into both percentages and the definition that reads them."""
    tools = len(tool_rows)
    mapped = sum(1 for row in tool_rows if row.status in MAPPED_STATUSES)
    converted = sum(1 for row in tool_rows if row.status == "converted")
    mapped_percent = _percent(mapped, tools)
    converted_percent = _percent(converted, tools)
    return CoverageSummary(
        tools=tools,
        mapped=mapped,
        converted=converted,
        mapped_percent=mapped_percent,
        converted_percent=converted_percent,
        definition=(
            f"{mapped} of {tools} Alteryx tools reached a Flowfile node ({mapped_percent}%), "
            f"of which {converted} need no manual work ({converted_percent}%). "
            "Canvas comments are annotations, not tools, and are counted separately."
        ),
    )
