"""Refusal-message generators.

Pure functions that translate a low-level validation failure into a
structured, course-correctable refusal_detail string the LLM can act on.
The refusal pipeline depends on these heavily — rich, specific error
messages are what let smaller models recover in one retry instead of
exhausting the budget.
"""

from __future__ import annotations

import json
from typing import Any, Final

from pydantic import BaseModel, ValidationError

from flowfile_core.ai.tools.node_docs import NODE_AGENT_PAYLOAD_EXAMPLES
from flowfile_core.ai.tools.registry import _inline_ref_schema


def _navigate_schema(schema: dict[str, Any], loc_parts: list[str]) -> dict[str, Any] | None:
    """Walk into a JSON Schema following a Pydantic ``loc`` path.

    Pydantic ``loc`` is a tuple like ``("raw_data_format", "columns", 0, "name")``.
    Schema navigation: property names dive into ``properties[name]``; integer
    indices indicate the failing array element so we step into ``items``;
    schema branches under ``anyOf`` are flattened by picking the first
    object-typed branch.
    """
    node: dict[str, Any] | None = schema
    for part in loc_parts:
        if node is None:
            return None
        if "anyOf" in node:
            object_branches = [b for b in node["anyOf"] if isinstance(b, dict) and b.get("type") == "object"]
            if object_branches:
                node = object_branches[0]
        if isinstance(part, int):
            node = node.get("items") if isinstance(node, dict) else None
            continue
        if isinstance(part, str):
            properties = node.get("properties") if isinstance(node, dict) else None
            if properties and part in properties:
                node = properties[part]
                continue
            return None
        return None
    if node is not None and "anyOf" in node:
        object_branches = [b for b in node["anyOf"] if isinstance(b, dict) and b.get("type") == "object"]
        if object_branches:
            node = object_branches[0]
    return node


def _summarize_expected_shape(field_schema: dict[str, Any] | None) -> str:
    """Render a JSON Schema fragment as a short human-readable shape summary."""
    if not field_schema:
        return "the value documented in the catalog"
    title = field_schema.get("title")
    type_ = field_schema.get("type")
    if type_ == "object":
        return f"an object ({title})" if title else "an object"
    if type_ == "array":
        items = field_schema.get("items") or {}
        items_type = items.get("type")
        if items_type == "object":
            items_title = items.get("title")
            return f"an array of objects ({items_title})" if items_title else "an array of objects"
        if items_type:
            return f"an array of {items_type}"
        return "an array"
    if isinstance(type_, str):
        return f"a {type_}"
    return "the value documented in the catalog"


def _expects_object(field_schema: dict[str, Any] | None) -> bool:
    """Return True iff the field expects an object (top-level or via array items)."""
    if not field_schema:
        return False
    if field_schema.get("type") == "object":
        return True
    if field_schema.get("type") == "array":
        items = field_schema.get("items") or {}
        return items.get("type") == "object"
    return False


_PRIMITIVE_DEFAULTS: Final[dict[str, Any]] = {
    "string": "",
    "integer": 0,
    "number": 0.0,
    "boolean": False,
    "null": None,
}


def _synthesize_example_from_schema(
    schema: dict[str, Any] | None,
    *,
    depth: int = 0,
    max_depth: int = 5,
) -> Any:
    """Synthesize a structurally-faithful placeholder for a JSON-Schema fragment.

    Used in settings-validation refusals to give the LLM a template
    payload it can pattern-match on. Required object fields are filled;
    optional fields are skipped to keep the example minimal. Cycle /
    depth bound prevents runaway recursion on self-referential schemas.
    """
    if schema is None or depth >= max_depth:
        return None

    if "anyOf" in schema:
        object_branches = [b for b in schema["anyOf"] if isinstance(b, dict) and b.get("type") == "object"]
        if object_branches:
            return _synthesize_example_from_schema(object_branches[0], depth=depth, max_depth=max_depth)
        for branch in schema["anyOf"]:
            if isinstance(branch, dict) and branch.get("type") not in (None, "null"):
                return _synthesize_example_from_schema(branch, depth=depth, max_depth=max_depth)
        return None

    if "enum" in schema:
        enum_values = schema["enum"]
        if enum_values:
            return enum_values[0]

    if "default" in schema:
        return schema["default"]

    type_ = schema.get("type")
    if type_ == "object":
        properties = schema.get("properties") or {}
        required = schema.get("required") or list(properties.keys())[:2]
        result: dict[str, Any] = {}
        for key in required:
            sub = properties.get(key)
            value = _synthesize_example_from_schema(sub, depth=depth + 1, max_depth=max_depth)
            if value is not None or (sub and sub.get("type") == "null"):
                result[key] = value
            else:
                result[key] = ""
        return result
    if type_ == "array":
        items = schema.get("items")
        if isinstance(items, dict) and items.get("type") == "object":
            return [_synthesize_example_from_schema(items, depth=depth + 1, max_depth=max_depth)]
        return []
    if isinstance(type_, str) and type_ in _PRIMITIVE_DEFAULTS:
        return _PRIMITIVE_DEFAULTS[type_]
    return None


def _example_from_payload(node_type: str, loc_parts: list[str]) -> Any:
    """Try to extract the failing-field fragment from
    :data:`NODE_AGENT_PAYLOAD_EXAMPLES` (per the spec's preferred cascade).

    Returns ``None`` if no payload is registered for this node type or if the
    loc path doesn't resolve cleanly inside it.
    """
    payload_json = NODE_AGENT_PAYLOAD_EXAMPLES.get(node_type)
    if not payload_json:
        return None
    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError:
        return None
    node: Any = payload
    for part in loc_parts:
        if isinstance(part, str) and isinstance(node, dict) and part in node:
            node = node[part]
            continue
        if isinstance(part, int) and isinstance(node, list) and 0 <= part < len(node):
            node = node[part]
            continue
        return None
    return node


def _detect_sink_upstreams(flow, upstream_ids: list[int]) -> list[tuple[int, str]]:
    """Return ``(node_id, node_type)`` for any upstream id that resolves to a
    sink in ``flow.nodes`` (i.e. ``NodeTemplate.output == 0``).

    Sinks (``explore_data`` / ``output`` / ``database_writer`` /
    ``cloud_storage_writer`` / ``catalog_writer``) consume data and have
    no output port, so wiring a downstream node to one is a static error.
    The LLM occasionally proposes this when chat history doesn't
    disambiguate and the Tier-6 fallback skips sinks — this guard catches
    the explicit-Tier-1 case where the LLM names a sink in
    ``upstream_node_ids`` directly.

    Ids absent from ``flow.nodes`` (i.e. staged-this-session or invalid)
    are silently skipped here; the missing-id case is handled by downstream
    refusal stages, not this guard.
    """
    sinks: list[tuple[int, str]] = []
    for uid in upstream_ids:
        upstream_node = flow.get_node(uid)
        if upstream_node is None:
            continue
        template = getattr(upstream_node, "node_template", None)
        if template is not None and getattr(template, "output", 1) == 0:
            sinks.append((uid, upstream_node.node_type))
    return sinks


def _format_sink_upstream_refusal(flow, sink_upstreams: list[tuple[int, str]]) -> str:
    """Build the refusal detail string for a sink-upstream rejection.

    Lists each offending id + its node type and proposes the live non-sink
    candidates so the LLM can retry with a corrected ``upstream_node_ids``
    on the next turn (planner re-feeds refusal_detail back as a tool message).
    """
    sink_str = ", ".join(f"{nid} ({nt})" for nid, nt in sink_upstreams)
    non_sinks: list[int] = []
    for live in flow.nodes:
        live_template = getattr(live, "node_template", None)
        if live_template is not None and getattr(live_template, "output", 1) > 0:
            try:
                non_sinks.append(int(live.node_id))
            except (TypeError, ValueError, AttributeError):
                continue
    candidates = sorted(set(non_sinks))
    return (
        f"upstream node(s) {sink_str} are sinks (no output port) and cannot "
        f"have downstream nodes — sink types consume data, they don't produce "
        f"it. Non-sink candidates available: {candidates}. Pick a "
        f"transformation node and retry."
    )


def _format_settings_validation_refusal(
    *,
    exc: ValidationError,
    settings_cls: type[BaseModel],
    node_type: str,
) -> str:
    """Translate a Pydantic ``ValidationError`` on a settings class into a
    course-correctable refusal detail.

    The bare ``str(exc)`` is a stack-shaped string the LLM treats as
    opaque, leading to retry loops on the same misshapen field. The
    translated message names the failing field, the expected shape (from
    the inlined catalog schema), the received Python type, and embeds
    a concrete example payload.
    """
    errors = exc.errors()
    if not errors:
        return f"settings validation failed: {exc}"

    first = errors[0]
    raw_loc = first.get("loc", ())
    loc_parts: list = list(raw_loc)
    loc_str = ".".join(str(p) for p in loc_parts) if loc_parts else "<root>"
    received = first.get("input")
    received_type = type(received).__name__

    full_schema = _inline_ref_schema(dict(settings_cls.model_json_schema()))
    field_schema = _navigate_schema(full_schema, loc_parts)
    expected_summary = _summarize_expected_shape(field_schema)

    example = _example_from_payload(node_type, loc_parts)
    if example is None:
        example = _synthesize_example_from_schema(field_schema)

    # FunctionInput-specific disambiguation. The naming collision (outer
    # ``function`` parameter holding a FunctionInput object whose inner
    # ``function`` field is a string) trips small models into reading
    # *"got str"* as *"send a str"* — the LLM inverts the constraint on
    # the second retry. Detect the case narrowly (FunctionInput summary
    # + received str) and emit a rewritten refusal that names the OUTER
    # vs INNER ``function`` references explicitly, dropping the
    # misread-prone *"not as a JSON-encoded string"* clause.
    if (
        node_type == "formula"
        and isinstance(received, str)
        and "FunctionInput" in expected_summary
    ):
        truncated = received if len(received) <= 80 else received[:77] + "..."
        return (
            "formula's `function` parameter is an OBJECT with two keys:\n"
            "  - `field`: the new column descriptor, e.g. "
            '{"name": "full_name", "data_type": "String"}\n'
            "  - `function`: the row-wise expression STRING in "
            "[column_name] syntax, e.g. \"[first] + ' ' + [last]\"\n"
            f"Your call sent `function` as a single string ({truncated!r}). "
            'Re-emit as: {"field": {"name": "<col>", "data_type": '
            '"<type>"}, "function": "<your-expression>"}.'
        )

    parts = [
        f"Field `{loc_str}` expects {expected_summary} matching the schema in tool "
        f"`flowfile.graph.add_{node_type}` (see catalog); got {received_type}.",
    ]
    if example is not None:
        try:
            example_json = json.dumps(example)
            parts.append(f"Example payload for `{loc_str}`: {example_json}.")
        except (TypeError, ValueError):
            pass
    if isinstance(received, str) and _expects_object(field_schema):
        parts.append("Pass the structured object directly, not as a JSON-encoded string.")

    return " ".join(parts)
