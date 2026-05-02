"""Prompt-context construction — owned by W22 + W24.

Serialises the current subgraph, schemas, and (opt-in per **D009**)
sample rows into a token-budgeted system / user :class:`Message` pair.
``mentions`` parses ``@node`` / ``@schema`` / ``@flow`` / ``@selection``
references; ``budget`` enforces caps so we never blow the context
window. Per **D008** the system prompt is layered (``prompts/base.md``
+ ``prompts/{level}.md``).

Public surface — single import for downstream workstreams (W20, W21,
W23, W30, W40, W50, W51):
"""

from flowfile_core.ai.context.budget import (
    BudgetReport,
    SurfaceBudget,
    apply_budget,
    estimate_tokens,
    surface_budget,
)
from flowfile_core.ai.context.builder import (
    SURFACE_TO_LEVEL,
    ColumnSnapshot,
    NodeSnapshot,
    PromptContext,
    PromptLevel,
    SamplesMode,
    SubgraphSnapshot,
    SurfaceLiteral,
    assemble_system_prompt,
    extract_subgraph,
    render_prompt_context,
    render_user_message,
    snapshot_node,
)
from flowfile_core.ai.context.mentions import (
    Mention,
    MentionKind,
    ResolvedMention,
    parse_mentions,
    resolve_mentions,
)

__all__ = [
    # Top-level entry point
    "render_prompt_context",
    # Building blocks
    "extract_subgraph",
    "snapshot_node",
    "assemble_system_prompt",
    "render_user_message",
    # Mention parsing (W24 owns the frontend autocomplete)
    "parse_mentions",
    "resolve_mentions",
    # Budgeting helpers
    "apply_budget",
    "estimate_tokens",
    "surface_budget",
    # Types
    "ColumnSnapshot",
    "NodeSnapshot",
    "SubgraphSnapshot",
    "PromptContext",
    "BudgetReport",
    "Mention",
    "MentionKind",
    "ResolvedMention",
    "SurfaceLiteral",
    "SamplesMode",
    "SurfaceBudget",
    "PromptLevel",
    "SURFACE_TO_LEVEL",
]
