"""Conversion report models for the Alteryx `.yxmd` importer."""

from collections import Counter
from typing import Literal

from pydantic import BaseModel, Field

from flowfile_core.flowfile.converters.alteryx.tool_identity import CUSTOM_PLUGIN
from flowfile_core.schemas import schemas

ToolStatus = Literal["converted", "partial", "commented", "placeholder", "out_of_scope", "no_op", "skipped"]
ToolEntity = Literal["tool", "annotation"]

MAPPED_STATUSES: tuple[ToolStatus, ...] = ("converted", "partial", "commented")
UNCONVERTIBLE_STATUSES: tuple[ToolStatus, ...] = ("out_of_scope", "no_op")
# What a reader has to act on first, down to what needs nothing.
STATUS_ORDER: tuple[ToolStatus, ...] = (
    "placeholder",
    "commented",
    "partial",
    "out_of_scope",
    "no_op",
    "skipped",
    "converted",
)
MESSAGE_CELL_LIMIT = 300


class ToolReportRow(BaseModel):
    """What happened to a single Alteryx tool during conversion.

    ``reason`` is why the row has the status it has, from a closed vocabulary so the numbers can
    be grouped without parsing English messages. No other value may be written:

    - ``converted`` — the tool became a Flowfile node with nothing left to do.
    - ``viewer`` — an Alteryx Browse; a viewer, not a transformation.
    - ``annotation`` — a canvas comment, on every row whose ``entity`` is ``annotation``.
    - ``translator_refused`` — the expression translator would not convert the formula.
    - ``option_unsupported`` — an option, mode or flag Flowfile cannot express.
    - ``connection_string`` — the tool reads or writes a database connection, not a file.
    - ``dropped_connection`` — a wire between two tools could not be reconnected.
    - ``file_format`` — Flowfile does not read or write that file format.
    - ``row_order_unknown`` — the result depends on a row order the workflow does not state.
    - ``unmapped_tool`` — an in-scope tool no mapper covers yet.
    - ``mapper_refused`` — the mapper read the configuration and would not guess.
    - ``scope:<bucket>`` — a settled non-goal; the bucket is a key of ``scope.BUCKETS``.
    - ``no_op`` — the tool does nothing to the data.

    ``census_name`` is the tool's identity for scope and for grouping (a macro's bare filename),
    while ``alteryx_tool`` stays the label a user sees, which for a macro is its full path.
    ``requestable`` says whether the tool is something Alteryx ships, so a node for it can be
    asked for in public; the dialog must not offer to file an issue naming someone's own macro.
    """

    alteryx_tool_id: int
    alteryx_tool: str
    census_name: str = ""
    entity: ToolEntity = "tool"
    alteryx_tool_key: str = CUSTOM_PLUGIN
    flowfile_node_ids: list[int] = Field(default_factory=list)
    flowfile_node_type: str | None = None
    status: ToolStatus
    reason: str
    requestable: bool = False
    messages: list[str] = Field(default_factory=list)


class CoverageSummary(BaseModel):
    """The two coverage numbers, always together, plus the sentence defining them.

    A canvas comment is an annotation, not a tool, so it never enters either number —
    counting comments is what let the first version of this report claim 74% coverage on
    a corpus where 45% of the tools were mapped. ``in_scope`` leaves out the tools Flowfile
    has decided not to convert, so the percentage a reader judges the importer by is not
    diluted by spatial and reporting tools that are never coming.
    """

    tools: int
    in_scope: int
    mapped: int
    converted: int
    mapped_percent: int
    converted_percent: int
    in_scope_percent: int
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
    out_of_scope: int = 0
    no_op: int = 0
    skipped: int = 0
    coverage: CoverageSummary
    rows: list[ToolReportRow] = Field(default_factory=list)

    def to_markdown(self) -> str:
        """The coverage table as Markdown, one row per (tool, status) pair.

        ``rows`` is per tool *instance*, and a tool type reaches several statuses in a corpus
        of any size, so the table groups on ``census_name`` and status rather than showing the
        same tool 135 times. Canvas annotations are not tools and become a single count line.
        Deterministic, so the published table only changes when the importer does.
        """
        tools = [row for row in self.rows if row.entity == "tool"]
        groups: dict[tuple[str, ToolStatus], list[ToolReportRow]] = {}
        for row in tools:
            groups.setdefault((row.census_name or row.alteryx_tool, row.status), []).append(row)

        counts = Counter(row.status for row in tools)
        tally = " · ".join(f"{counts[status]} {status}" for status in STATUS_ORDER if counts[status])
        lines = [
            f"# {self.workflow_name}",
            "",
            self.coverage.definition,
            "",
            f"{len(tools)} tools: {tally}" if tally else f"{len(tools)} tools",
            "",
            f"Canvas annotations (comments), counted apart from the tools: {self.total_annotations}",
            "",
            "| Tool | Key | Instances | Status | Reason | Message |",
            "|---|---|---|---|---|---|",
        ]
        ordered = sorted(groups.items(), key=lambda item: (STATUS_ORDER.index(item[0][1]), item[0][0]))
        for (name, status), rows in ordered:
            messages = list(dict.fromkeys(message for row in rows for message in row.messages))
            reasons = list(dict.fromkeys(row.reason for row in rows))
            lines.append(
                f"| {_cell(name)} | {_cell(rows[0].alteryx_tool_key)} | {len(rows)} | {status} "
                f"| {_cell(' · '.join(reasons))} | {_cell(' · '.join(messages))} |"
            )
        return "\n".join(lines) + "\n"


class ConversionResult(BaseModel):
    """The converted flow plus the report describing how it was produced."""

    flow_data: schemas.FlowfileData
    report: ConversionReport


def _cell(value: str) -> str:
    """One table cell: no newlines, no unescaped pipes, and never long enough to break the table."""
    collapsed = " ".join(value.split()).replace("|", "\\|")
    return collapsed if len(collapsed) <= MESSAGE_CELL_LIMIT else collapsed[: MESSAGE_CELL_LIMIT - 1] + "…"


def _percent(part: int, whole: int) -> int:
    return round(100 * part / whole) if whole else 0


def _plural(count: int, noun: str) -> str:
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def build_coverage(tool_rows: list[ToolReportRow]) -> CoverageSummary:
    """Summarise the tool rows into both percentages and the definition that reads them."""
    tools = len(tool_rows)
    in_scope = tools - sum(1 for row in tool_rows if row.status in UNCONVERTIBLE_STATUSES)
    mapped = sum(1 for row in tool_rows if row.status in MAPPED_STATUSES)
    converted = sum(1 for row in tool_rows if row.status == "converted")
    mapped_percent = _percent(mapped, tools)
    in_scope_percent = _percent(mapped, in_scope)
    return CoverageSummary(
        tools=tools,
        in_scope=in_scope,
        mapped=mapped,
        converted=converted,
        mapped_percent=mapped_percent,
        converted_percent=_percent(converted, tools),
        in_scope_percent=in_scope_percent,
        definition=(
            f"{mapped} of {_plural(tools, 'Alteryx tool')} reached a Flowfile node "
            f"({mapped_percent}% of all tools); "
            f"of the {_plural(in_scope, 'tool')} Flowfile aims to convert, {in_scope_percent}%. "
            f"{converted} of the mapped tools need no manual work. "
            "Canvas comments are annotations, not tools, and are counted separately."
        ),
    )
