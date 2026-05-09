"""Prompt-context construction.

Serialises the current subgraph, schemas, and (opt-in) sample rows
into a token-budgeted system / user :class:`Message` pair.
``mentions`` parses ``@node`` / ``@schema`` / ``@flow`` /
``@selection`` references; ``budget`` enforces caps so we never blow
the context window. The system prompt is layered (``prompts/base.md``
+ ``prompts/{level}.md``).

Public surface — single import for downstream callers:
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
    # Mention parsing (the frontend autocomplete uses these)
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
