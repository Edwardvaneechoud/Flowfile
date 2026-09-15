"""Alteryx tools Flowfile deliberately does not convert, and what to tell the user instead.

Every unconverted tool used to get the same placeholder — "needs manual conversion" — which
is a promise for a Filter and a lie for a PDF renderer. This registry states in code which
tools are settled non-goals (``out_of_scope``, by bucket) and which do nothing to the data
(``no_op``), so the report can say *why* a node is a pass-through rather than implying that
a future release will convert it.

Keys are **census tool names** — ``AlteryxTool.tool_name``, or the macro's filename when the
tool is a macro — not :func:`tool_identity.tool_key`, which collapses every vendor plugin to
``custom_plugin`` and every unshipped macro to ``user_macro`` and therefore cannot carry
scope. The tables are Python literals rather than a data file: the corpus taxonomy they were
transcribed from lives in a private folder that the public repo never ships. A test guards
the two against drift when that folder is present.

A name this registry does not list is in scope, so a new Alteryx tool fails toward
"Flowfile should convert this one day" rather than toward a silent non-goal.
"""

from typing import NamedTuple

from flowfile_core.flowfile.converters.alteryx.report import ToolStatus
from flowfile_core.flowfile.converters.alteryx.yxmd_parser import AlteryxTool

UNKNOWN_TOOL = "<unknown>"

OUT_OF_SCOPE: dict[str, str] = {
    "PortfolioComposerImage": "reporting",
    "PortfolioComposerTable": "reporting",
    "PortfolioComposerText": "reporting",
    "PortfolioComposerRender": "reporting",
    "PortfolioComposerLayout": "reporting",
    "ReportMap": "reporting",
    "PlotlyCharting": "reporting",
    "FooterMacro.yxmc": "reporting",
    "HeaderMacro.yxmc": "reporting",
    "Legend_Builder.yxmc": "reporting",
    "Legend_Splitter.yxmc": "reporting",
    "VisualLayout": "reporting",
    "WordCloud_1_0": "reporting",
    "TradeArea": "spatial",
    "SpatialProcess": "spatial",
    "Distance": "spatial",
    "SpatialInfo": "spatial",
    "FindNearest": "spatial",
    "MakeGrid": "spatial",
    "SpatialMatch": "spatial",
    "Buffer": "spatial",
    "HeatMap.yxmc": "spatial",
    "CreatePoints": "spatial",
    "Generalize": "spatial",
    "PolyBuild": "spatial",
    "PolySplit": "spatial",
    "Smooth": "spatial",
    "ImageInput_1_0": "computer_vision",
    "ImageProcessing_1_0": "computer_vision",
    "PDFtoText_1_0": "computer_vision",
    "Barcode_1_0": "computer_vision",
    "ImageProfile_1_0": "computer_vision",
    "ImageTemplate_1_0": "computer_vision",
    "ImagetoText_1_0": "computer_vision",
    "ImageRecognition_1_0": "computer_vision",
    "KeyValuePairExtraction_1_0": "computer_vision",
    "TextPreProcessing_1_0": "text_mining",
    "SentimentAnalysis_1_0": "text_mining",
    "NamedEntityRecognition_1_0": "text_mining",
    "TopicModelling_1_0": "text_mining",
    "ZeroShotTextClassification_1_0": "text_mining",
    "PartOfSpeechTagger_1_0": "text_mining",
    "TextSummary_1_0": "text_mining",
    "Prompt_1_0": "genai",
    "LLMOverride_1_0": "genai",
    "Precision_Match.yxmc": "genai",
    "Schema_Fit.yxmc": "genai",
    "CalgaryInput": "calgary",
    "CalgaryJoin": "calgary",
    "CalgaryCrossCount": "calgary",
    "CalgaryCrossCountAppend": "calgary",
    "CalgaryLoader": "calgary",
    "FeatureTypes_1_0": "machine_learning",
    "BuildFeatures_1_0": "machine_learning",
    "Predict_1_0": "machine_learning",
    "RunCommand": "os_and_binary",
    "BlobOutput": "os_and_binary",
}

NO_OP_TOOLS: frozenset[str] = frozenset(
    {"Message", "Test", "ExpectEqual", "BlockUntilDone", "Throttle", "Detour", "DetourEnd"}
)

NO_OP_BUCKET = "no_op"
PASSES_THROUGH = "the node is a pass-through so the rest of the flow still imports."

BUCKETS: dict[str, str] = {
    "reporting": "Flowfile builds data pipelines, not rendered documents, so this tool's page layout"
    f" has no Flowfile equivalent; {PASSES_THROUGH}",
    "spatial": f"Flowfile has no geospatial engine, so this tool's output cannot be reproduced; {PASSES_THROUGH}",
    "computer_vision": "Flowfile does not read or process images and PDFs, so this tool's output"
    f" cannot be reproduced; {PASSES_THROUGH}",
    "text_mining": "This tool runs a language model Alteryx bundles and Flowfile does not ship, so"
    f" its output cannot be reproduced; {PASSES_THROUGH}",
    "genai": "This tool calls a generative model through Alteryx's own credentials, which Flowfile"
    f" does not provide, so its output cannot be reproduced; {PASSES_THROUGH}",
    "calgary": "Calgary is an Alteryx-only database format that Flowfile cannot read or write, so"
    f" this tool has no Flowfile equivalent; {PASSES_THROUGH}",
    "machine_learning": "This tool belongs to Alteryx's Assisted Modeling suite, which Flowfile does"
    f" not implement, so its output cannot be reproduced; {PASSES_THROUGH}",
    "os_and_binary": "This tool runs operating-system commands or writes binary blobs, which a"
    f" Flowfile flow deliberately cannot do; {PASSES_THROUGH}",
    NO_OP_BUCKET: "This tool has no effect on the data (messages, tests, ordering hints); its input is wired"
    " straight to what it fed.",
}


class ScopeVerdict(NamedTuple):
    """Why a tool is not converted: the status to report, its reason slug and the user-facing sentence."""

    status: ToolStatus
    reason: str
    sentence: str


def census_tool_name(tool: AlteryxTool) -> str:
    """The tool's identity for scope purposes: its name, or a macro's filename with extension and case kept.

    Macros carry no ``tool_name`` — the plugin string holds the ``Macro`` attribute instead,
    sometimes with a directory (``Precision Match\\Precision_Match.yxmc``), so only the
    basename identifies the macro. This is the key ``tools/alteryx_census.py`` groups by, which
    is what makes the census and this registry comparable.
    """
    if tool.tool_name:
        return tool.tool_name
    base = tool.plugin.replace("\\", "/").rsplit("/", 1)[-1]
    return base or UNKNOWN_TOOL


def classify(tool_name: str) -> ScopeVerdict | None:
    """The scope verdict for a census tool name, or ``None`` when the tool is in scope."""
    bucket = OUT_OF_SCOPE.get(tool_name)
    if bucket is not None:
        return ScopeVerdict("out_of_scope", f"scope:{bucket}", BUCKETS[bucket])
    if tool_name in NO_OP_TOOLS:
        return ScopeVerdict("no_op", NO_OP_BUCKET, BUCKETS[NO_OP_BUCKET])
    return None
