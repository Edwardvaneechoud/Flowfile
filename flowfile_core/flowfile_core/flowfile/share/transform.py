"""Turn a live ``FlowGraph`` into the envelope the browser build imports.

The only module here that touches a ``FlowGraph``. It serialises the graph
through the normal ``get_flowfile_data()`` path (so the ``setting_input``
field serializer runs), then rewrites the result into the browser's dialect:
unsupported nodes become placeholders, machine-local settings are scrubbed, the
edge list is made explicit, and the flow settings are forced to the ones the
browser always runs with.

One rewrite happens *before* the compatibility check rather than after it: an
advanced filter that is a plain comparison is replaced by the basic filter it
means (``filter_translation``), so the node travels and runs instead of being
demoted. Its auto-generated description is dropped along with the expression it
described; a description the user typed still travels.
"""

import functools
from dataclasses import dataclass, field

from flowfile_core.flowfile.share import filter_translation, sanitize
from flowfile_core.flowfile.share.compatibility import NodeShareStatus, classify_node, placeholder_type

DEFAULT_OUTPUT_HANDLE = "output-0"
ENVELOPE_VERSION = 1


@dataclass
class ShareTransformResult:
    """The share payload plus everything the caller has to tell the user about it."""

    envelope: dict
    node_reports: list[NodeShareStatus] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    local_file_nodes: list[int] = field(default_factory=list)


@functools.lru_cache(maxsize=1)
def _node_templates() -> dict:
    from flowfile_core.configs.node_store.nodes import get_all_standard_nodes

    return get_all_standard_nodes()[1]


def build_connections(nodes: list[dict]) -> list[dict]:
    """The flow's edges as ``{from_node, to_node, from_handle, to_handle}``.

    Built by iterating **targets**: a source feeding both sides of a join lists
    that target once in ``outputs``, so walking sources would lose an edge. The
    source-side handle comes from the source's ``output_handles`` (a gate's else
    exit is ``output-1``), defaulting to ``output-0`` for older flows.
    """
    by_id = {node["id"]: node for node in nodes}

    def from_handle(source_id: int, target_id: int) -> str:
        source = by_id.get(source_id) or {}
        outputs = source.get("outputs") or []
        handles = source.get("output_handles") or []
        try:
            return handles[outputs.index(target_id)] or DEFAULT_OUTPUT_HANDLE
        except (ValueError, IndexError):
            return DEFAULT_OUTPUT_HANDLE

    connections: list[dict] = []
    seen: set[tuple[int, int, str]] = set()

    def add(source_id, target_id: int, to_handle: str) -> None:
        if source_id is None or (source_id, target_id, to_handle) in seen:
            return
        seen.add((source_id, target_id, to_handle))
        connections.append(
            {
                "from_node": source_id,
                "to_node": target_id,
                "from_handle": from_handle(source_id, target_id),
                "to_handle": to_handle,
            }
        )

    for node in nodes:
        target_id = node["id"]
        keyed = node.get("input_connections")
        if keyed:
            for edge in keyed:
                connections.append(
                    {
                        "from_node": edge["from_id"],
                        "to_node": target_id,
                        "from_handle": edge.get("source_handle") or DEFAULT_OUTPUT_HANDLE,
                        "to_handle": edge["input_handle"],
                    }
                )
            continue
        input_ids = list(node.get("input_ids") or [])
        left_input_id = node.get("left_input_id")
        if left_input_id is not None and left_input_id not in input_ids:
            add(left_input_id, target_id, "input-0")
        add(node.get("right_input_id"), target_id, "input-1")
        for source_id in input_ids:
            add(source_id, target_id, "input-0")
    return connections


def _handle_span(handle: str) -> int:
    """How many handles a node needs to expose ``handle`` (``input-1`` needs 2)."""
    _, _, index = handle.rpartition("-")
    return int(index) + 1 if index.isdigit() else 1


def _handle_counts(connections: list[dict]) -> tuple[dict[int, int], dict[int, int]]:
    inbound: dict[int, int] = {}
    outbound: dict[int, int] = {}
    for edge in connections:
        target, source = edge["to_node"], edge["from_node"]
        inbound[target] = max(inbound.get(target, 0), _handle_span(edge["to_handle"]))
        outbound[source] = max(outbound.get(source, 0), _handle_span(edge["from_handle"]))
    return inbound, outbound


def _placeholder_shape(node_type: str, used_in: int, used_out: int) -> tuple[str, int, int]:
    """``(label, inputs, outputs)`` for a placeholder's rendered handles.

    The browser needs real handle counts or it silently drops the edges of a
    two-input or two-output node. The registry gives the shape; the flow's own
    connectivity raises it when the node is wired beyond its declared shape.
    Multi/dynamic-input types declare a large ``input`` but render one handle
    that accepts many connections.
    """
    template = _node_templates().get(node_type)
    if template is None:
        return node_type.replace("_", " ").title(), max(used_in, 1), max(used_out, 1)
    multi = bool(getattr(template, "multi", False) or getattr(template, "dynamic_inputs", False))
    base_in = 1 if multi else template.input
    return template.name, max(base_in, used_in), max(template.output, used_out)


def _join_key_divergence(node_id: int, settings: dict) -> str | None:
    join_input = settings.get("join_input") or {}
    for mapping in join_input.get("join_mapping") or []:
        left, right = mapping.get("left_col"), mapping.get("right_col")
        if left and right and left != right:
            return (
                f"Node {node_id}: the join keys are named differently on each side "
                f"('{left}' / '{right}'). The browser keeps only the left-hand key column."
            )
    return None


def _user_description(setting_input) -> str:
    """The description the user actually typed, or "".

    A save/open round trip copies the auto-generated description ("Read from
    analytics.customer_pii", the literal polars code) into the user field, so
    "non-empty" is not proof of user authorship. Anything that matches what
    ``get_default_description()`` derives from the settings is treated as
    auto-generated and suppressed; if the comparison itself fails we fail
    closed rather than ship an unproven string.
    """
    description = getattr(setting_input, "description", "") or ""
    if not description:
        return ""
    if hasattr(setting_input, "get_default_description"):
        try:
            if description == setting_input.get_default_description():
                return ""
        except Exception:
            return ""
    return description


def build_share_envelope(flow) -> ShareTransformResult:
    """Serialise ``flow`` into the ``{v, flow}`` envelope a share link carries."""
    raw = flow.get_flowfile_data().model_dump(mode="json")
    raw_nodes = raw["nodes"]

    # The node-level description falls back to an auto-generated one that embeds
    # the settings, so a placeholder may only ever carry what the user typed.
    user_descriptions = {node.node_id: _user_description(node.setting_input) for node in flow.nodes}

    connections = build_connections(raw_nodes)
    inbound, outbound = _handle_counts(connections)

    reports: list[NodeShareStatus] = []
    warnings: list[str] = []
    local_file_nodes: list[int] = []
    nodes: list[dict] = []

    for raw_node in raw_nodes:
        node_id = raw_node["id"]
        node_type = raw_node["type"]
        settings = raw_node.get("setting_input")
        translated = (
            filter_translation.rewrite_filter_settings(settings)
            if node_type == "filter" and isinstance(settings, dict)
            else None
        )
        if translated is not None:
            settings = translated
        status = classify_node(node_id, node_type, settings if isinstance(settings, dict) else None)
        reports.append(status)

        node = {key: value for key, value in raw_node.items() if key != "group_id"}
        if status.status == "placeholder":
            label, inputs, outputs = _placeholder_shape(node_type, inbound.get(node_id, 0), outbound.get(node_id, 0))
            node["type"] = placeholder_type(node_type)
            node["description"] = user_descriptions.get(node_id, "")
            node["setting_input"] = {
                "is_placeholder": True,
                "original_type": node_type,
                "reason": status.reason,
                "label": label,
                "inputs": inputs,
                "outputs": outputs,
                "description": user_descriptions.get(node_id, ""),
            }
        else:
            scrubbed = sanitize.scrub_settings(settings) if isinstance(settings, dict) else settings
            if translated is not None:
                node["description"] = user_descriptions.get(node_id, "")
            if node_type == "read" and isinstance(scrubbed, dict) and isinstance(scrubbed.get("received_file"), dict):
                received_file, needs_local = sanitize.rewrite_read_path(scrubbed["received_file"])
                scrubbed["received_file"] = received_file
                if needs_local:
                    local_file_nodes.append(node_id)
                    warnings.append(
                        f"Node {node_id} reads the local file '{received_file.get('path') or ''}'. "
                        "The recipient has to supply that file in the browser."
                    )
            node["setting_input"] = scrubbed
            if node_type == "join" and isinstance(scrubbed, dict):
                divergence = _join_key_divergence(node_id, scrubbed)
                if divergence:
                    warnings.append(divergence)
        nodes.append(sanitize.drop_nulls(node))

    if any(edge["from_handle"] != DEFAULT_OUTPUT_HANDLE for edge in connections):
        warnings.append("This flow uses secondary output handles; those edges need an up-to-date browser app to load.")
    if raw["flowfile_settings"].get("parameters"):
        warnings.append("Flow parameters do not travel in the link; the browser runs the values as configured.")

    envelope = {
        "v": ENVELOPE_VERSION,
        "flow": {
            "flowfile_version": raw["flowfile_version"],
            "flowfile_id": 1,
            "flowfile_name": raw["flowfile_name"],
            "flowfile_settings": sanitize.wasm_flow_settings(raw["flowfile_settings"].get("description")),
            "nodes": nodes,
            "connections": connections,
        },
    }
    return ShareTransformResult(
        envelope=envelope, node_reports=reports, warnings=warnings, local_file_nodes=local_file_nodes
    )
