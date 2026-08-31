"""Mint a browser share link for a flow.

``build_share_link(flow)`` is the whole public surface: it serialises the live
graph into the browser build's envelope, encodes it into a
``https://demo.flowfile.org/designer#flow=...`` URL, and reports per node
whether it travels as itself or as a placeholder.

Nothing here touches disk or the save path, and no executable setting (custom
Polars code, an advanced filter expression) ever reaches the payload.
"""

import json

from flowfile_core.flowfile.share.encoder import (
    SHARE_HASH_PREFIX,
    WASM_DESIGNER_URL,
    build_share_url,
    decode_share_hash,
    encode_envelope,
)
from flowfile_core.flowfile.share.transform import build_share_envelope
from flowfile_core.schemas import output_model

__all__ = [
    "SHARE_HASH_PREFIX",
    "WASM_DESIGNER_URL",
    "build_share_envelope",
    "build_share_link",
    "build_share_url",
    "decode_share_hash",
    "encode_envelope",
]

# Judgement values: chat and mail clients start mangling links well before any
# hard URL limit, and past the last one a link is not a useful way to share.
_LONG_LINK_CHARS = 16_000
_VERY_LONG_LINK_CHARS = 64_000
_REFUSE_LINK_CHARS = 200_000


def _biggest_contributor(envelope: dict) -> str:
    """The node whose settings dominate an oversized payload."""
    nodes = envelope.get("flow", {}).get("nodes", [])
    if not nodes:
        return "the flow itself"
    node = max(nodes, key=lambda item: len(json.dumps(item.get("setting_input") or {})))
    return f"node {node.get('id')} ({node.get('type')})"


def build_share_link(flow) -> output_model.ShareLinkResponse:
    """Build the share link for a live ``FlowGraph``."""
    result = build_share_envelope(flow)
    warnings = list(result.warnings)
    url, hash_chars = build_share_url(result.envelope)

    if hash_chars > _REFUSE_LINK_CHARS:
        warnings.append(
            f"This flow is too large to share as a link ({hash_chars:,} characters); "
            f"most of it is {_biggest_contributor(result.envelope)}."
        )
        url = None
    elif hash_chars > _VERY_LONG_LINK_CHARS:
        warnings.append(
            f"Very long link ({hash_chars:,} characters) — many chat and email clients will truncate it. "
            "Share the flow file instead if it does not open."
        )
    elif hash_chars > _LONG_LINK_CHARS:
        warnings.append(
            f"Long link ({hash_chars:,} characters) — some chat and email clients truncate links this long."
        )

    placeholder_count = sum(1 for report in result.node_reports if report.status == "placeholder")
    return output_model.ShareLinkResponse(
        url=url,
        hash_chars=hash_chars,
        compatible=url is not None and placeholder_count == 0,
        nodes_report=[
            output_model.ShareLinkNodeReport(
                node_id=report.node_id,
                node_type=report.node_type,
                status=report.status,
                reason=report.reason,
            )
            for report in result.node_reports
        ],
        warnings=warnings,
        placeholder_count=placeholder_count,
        local_file_nodes=result.local_file_nodes,
    )
