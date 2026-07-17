"""Canonical code generation: DesignerState -> deterministic .py source.

The inverse of ``parsing.parse_source`` for the designer subset. Output is
ruff-format-stable (double quotes, 4-space indent, trailing commas on multiline
calls) and forms a fixed point with the parser:
``parse_source(generate_source(s)).designer_state == s`` and
``generate_source(parse_source(generate_source(s)).designer_state) == generate_source(s)``.

Canonical import style is ``from flowfile import node_designer as nd`` with
``nd.``-prefixed symbols, matching the docs. Data-type filters are emitted as
their normalized value lists (never ``nd.Types.X``) so the round-trip is stable.
"""

import json
import keyword
import math

from flowfile_core.flowfile.node_designer.state import (
    ColumnActionInputState,
    ColumnSelectorState,
    ComponentState,
    DesignerState,
    NumericInputState,
    SecretSelectorState,
    SectionState,
    SelectOption,
    SelectState,
    SliderInputState,
    TextInputState,
    ToggleSwitchState,
    VisibleWhenState,
)


class CodegenError(ValueError):
    """DesignerState cannot be rendered to valid source (bad identifiers, non-JSON data)."""


_INDENT = "    "


class _CallValue:
    """A kwarg value that renders as its own exploded call (e.g. options=nd.AvailableArtifacts(...))."""

    def __init__(self, callee: str, kwargs: list):
        self.callee = callee
        self.kwargs = kwargs


# Component kwargs in canonical emit order. Only kwargs that differ from the
# component's designer default are emitted; ``label`` always leads when present.
_MARKER_FOR_OPTIONS_SOURCE = {
    "incoming_columns": "IncomingColumns",
    "available_artifacts": "AvailableArtifacts",
}


def _check_identifier(value: str, what: str) -> None:
    if not isinstance(value, str) or not value.isidentifier() or keyword.iskeyword(value):
        raise CodegenError(f"{what} {value!r} is not a valid Python identifier")


def _py_literal(value: object) -> str:
    """Render a JSON-safe scalar as a Python literal (double-quoted strings).

    ``bool``/``None`` become Python keywords (``json`` would emit
    ``true``/``false``/``null``). Containers go through
    ``_render_data_literal``; this handles leaf scalars only.
    """
    if value is None:
        return "None"
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, int | float):
        if isinstance(value, float) and not math.isfinite(value):
            # repr() emits the bare tokens nan/inf/-inf, which are NameErrors when the
            # generated .py runs; float("...") is the valid literal form.
            if math.isnan(value):
                return 'float("nan")'
            return 'float("-inf")' if value < 0 else 'float("inf")'
        return repr(value)
    raise CodegenError(f"value {value!r} is not a JSON-safe scalar")


def _render_data_literal(value: object, indent: str) -> list[str]:
    """Render JSON-safe data as an always-exploded, magic-trailing-comma literal.

    Non-empty lists/dicts explode one item per line with a trailing comma at
    every level; ruff leaves this form untouched, so it stays lint-stable for
    arbitrarily deep example data. Returns the value's lines with the first line
    carrying no leading indent (the caller places it after ``key=``).
    """
    if isinstance(value, dict):
        if not value:
            return ["{}"]
        inner = indent + _INDENT
        lines = ["{"]
        for key, sub in value.items():
            if not isinstance(key, str):
                raise CodegenError(f"dict key {key!r} must be a string")
            sub_lines = _render_data_literal(sub, inner)
            lines.append(f"{inner}{_py_literal(key)}: {sub_lines[0]}")
            for extra in sub_lines[1:]:
                lines.append(extra)
            lines[-1] = f"{lines[-1]},"
        lines.append(f"{indent}}}")
        return lines
    if isinstance(value, list):
        if not value:
            return ["[]"]
        inner = indent + _INDENT
        lines = ["["]
        for item in value:
            item_lines = _render_data_literal(item, inner)
            lines.append(f"{inner}{item_lines[0]}")
            for extra in item_lines[1:]:
                lines.append(extra)
            lines[-1] = f"{lines[-1]},"
        lines.append(f"{indent}]")
        return lines
    return [_py_literal(value)]


def _options_literal(options: list[SelectOption]) -> list[str]:
    parts: list[str] = []
    for opt in options:
        if opt.label is None or opt.label == opt.value:
            parts.append(_py_literal(opt.value))
        else:
            parts.append(f"({_py_literal(opt.value)}, {_py_literal(opt.label)})")
    return parts


def _artifact_literal(decl) -> str:
    """Render an ArtifactDecl as ``nd.Artifact("name")`` / ``nd.Artifact("name", type="...")``."""
    if decl.type is None:
        return f"nd.Artifact({_py_literal(decl.name)})"
    return f"nd.Artifact({_py_literal(decl.name)}, type={_py_literal(decl.type)})"


def _data_types_kwarg(spec) -> list[str] | None:
    """Emit ``data_types`` only when narrowed; ``"ALL"`` is the default.

    Returns the item literals as a list so it renders as a magic-trailing-comma
    list (ruff-stable regardless of length).
    """
    if spec == "ALL":
        return None
    return [_py_literal(item) for item in spec]


class _Renderer:
    def __init__(self, state: DesignerState):
        self.state = state

    # -- kwargs helpers ------------------------------------------------------

    def _component_kwargs(self, comp: ComponentState) -> list[tuple[str, str]]:
        kwargs: list[tuple[str, str]] = []
        if comp.label is not None:
            kwargs.append(("label", _py_literal(comp.label)))

        if isinstance(comp, TextInputState):
            if comp.default is not None:
                kwargs.append(("default", _py_literal(comp.default)))
            if comp.placeholder is not None:
                kwargs.append(("placeholder", _py_literal(comp.placeholder)))
        elif isinstance(comp, NumericInputState):
            if comp.default is not None:
                kwargs.append(("default", _py_literal(comp.default)))
            if comp.min_value is not None:
                kwargs.append(("min_value", _py_literal(comp.min_value)))
            if comp.max_value is not None:
                kwargs.append(("max_value", _py_literal(comp.max_value)))
        elif isinstance(comp, SliderInputState):
            if comp.default is not None:
                kwargs.append(("default", _py_literal(comp.default)))
            if comp.min_value != 0:
                kwargs.append(("min_value", _py_literal(comp.min_value)))
            if comp.max_value != 100:
                kwargs.append(("max_value", _py_literal(comp.max_value)))
            if comp.step != 1:
                kwargs.append(("step", _py_literal(comp.step)))
        elif isinstance(comp, ToggleSwitchState):
            if comp.default:
                kwargs.append(("default", _py_literal(comp.default)))
            if comp.description is not None:
                kwargs.append(("description", _py_literal(comp.description)))
        elif isinstance(comp, SelectState):
            kwargs.extend(self._select_kwargs(comp))
        elif isinstance(comp, ColumnSelectorState):
            if comp.required:
                kwargs.append(("required", _py_literal(comp.required)))
            if comp.multiple:
                kwargs.append(("multiple", _py_literal(comp.multiple)))
            dt = _data_types_kwarg(comp.data_types)
            if dt is not None:
                kwargs.append(("data_types", dt))
        elif isinstance(comp, SecretSelectorState):
            if comp.required:
                kwargs.append(("required", _py_literal(comp.required)))
            if comp.description is not None:
                kwargs.append(("description", _py_literal(comp.description)))
            if comp.name_prefix is not None:
                kwargs.append(("name_prefix", _py_literal(comp.name_prefix)))
        elif isinstance(comp, ColumnActionInputState):
            kwargs.extend(self._column_action_kwargs(comp))
        else:
            raise CodegenError(f"unsupported component type {comp.component_type!r}")
        return kwargs

    def _select_kwargs(self, comp: SelectState) -> list[tuple[str, object]]:
        kwargs: list[tuple[str, object]] = []
        if comp.options_source == "available_artifacts":
            kwargs.append(("options", self._available_artifacts_value(comp)))
        elif comp.options_source in _MARKER_FOR_OPTIONS_SOURCE:
            kwargs.append(("options", f"nd.{_MARKER_FOR_OPTIONS_SOURCE[comp.options_source]}"))
        else:
            kwargs.append(("options", _options_literal(comp.options)))
        if comp.default is not None:
            # MultiSelect default is a list; scalar for SingleSelect.
            kwargs.append(
                (
                    "default",
                    [_py_literal(v) for v in comp.default]
                    if isinstance(comp.default, list)
                    else _py_literal(comp.default),
                )
            )
        return kwargs

    def _available_artifacts_value(self, comp: SelectState) -> object:
        # Bare marker when both params are default; an exploded call otherwise
        # (single-line would get rewrapped by ruff and break the fixed point).
        if comp.artifact_scope == "upstream" and not comp.artifact_type_filter:
            return "nd.AvailableArtifacts"
        inner: list[tuple[str, object]] = []
        if comp.artifact_scope != "upstream":
            inner.append(("scope", _py_literal(comp.artifact_scope)))
        if comp.artifact_type_filter:
            inner.append(("type", [_py_literal(t) for t in comp.artifact_type_filter]))
        return _CallValue("nd.AvailableArtifacts", inner)

    def _column_action_kwargs(self, comp: ColumnActionInputState) -> list[tuple[str, str]]:
        kwargs: list[tuple[str, str]] = []
        if comp.actions:
            kwargs.append(("actions", self._actions_literal(comp.actions)))
        if comp.output_name_template != "{column}_{action}":
            kwargs.append(("output_name_template", _py_literal(comp.output_name_template)))
        if comp.show_group_by:
            kwargs.append(("show_group_by", _py_literal(comp.show_group_by)))
        if comp.show_order_by:
            kwargs.append(("show_order_by", _py_literal(comp.show_order_by)))
        dt = _data_types_kwarg(comp.data_types)
        if dt is not None:
            kwargs.append(("data_types", dt))
        return kwargs

    def _actions_literal(self, actions: list[SelectOption]) -> list[str]:
        parts: list[str] = []
        for opt in actions:
            if opt.label is None or opt.label == opt.value:
                parts.append(_py_literal(opt.value))
            else:
                parts.append(f"nd.ActionOption({_py_literal(opt.value)}, {_py_literal(opt.label)})")
        return parts

    # -- component / section rendering --------------------------------------

    def _render_call(self, callee: str, kwargs: list, indent: str) -> list[str]:
        """Render ``callee(k=v, ...)`` — always multiline with trailing commas when non-empty."""
        if not kwargs:
            return [f"{callee}()"]
        lines = [f"{callee}("]
        for key, value in kwargs:
            lines.extend(self._render_kwarg(key, value, indent + _INDENT))
        lines.append(f"{indent})")
        return lines

    def _render_kwarg(self, key: str, value, indent: str) -> list[str]:
        """A single ``key=value,`` line, or a multiline block for list-/call-valued kwargs."""
        if isinstance(value, _CallValue):
            call_lines = self._render_call(value.callee, value.kwargs, indent)
            lines = [f"{indent}{key}={call_lines[0]}", *call_lines[1:]]
            lines[-1] = f"{lines[-1]},"
            return lines
        if isinstance(value, list):
            if not value:
                return [f"{indent}{key}=[],"]
            lines = [f"{indent}{key}=["]
            for item in value:
                lines.append(f"{indent}{_INDENT}{item},")
            lines.append(f"{indent}],")
            return lines
        return [f"{indent}{key}={value},"]

    def _render_component(self, comp: ComponentState, indent: str) -> list[str]:
        _check_identifier(comp.name, "component name")
        callee = f"nd.{comp.component_type}"
        kwargs = self._component_kwargs(comp)
        call_lines = self._render_call(callee, kwargs, indent)
        first = f"{indent}{comp.name}={call_lines[0]}"
        rest = call_lines[1:]
        if not rest:
            return [f"{first},"]
        body = [first, *rest]
        body[-1] = f"{body[-1]},"
        return body

    def _render_section(self, section: SectionState, indent: str) -> list[str]:
        _check_identifier(section.name, "section name")
        kwargs: list[tuple[str, str]] = []
        if section.title is not None:
            kwargs.append(("title", _py_literal(section.title)))
        if section.description is not None:
            kwargs.append(("description", _py_literal(section.description)))
        if section.hidden:
            kwargs.append(("hidden", _py_literal(section.hidden)))
        if section.layout != "vertical":
            kwargs.append(("layout", _py_literal(section.layout)))

        inner = indent + _INDENT
        visible_when_lines = self._visible_when_lines(section.visible_when, inner)

        if not kwargs and not visible_when_lines and not section.components:
            # ruff collapses an empty call to one line — emit it that way directly.
            return [f"{indent}{section.name}: nd.Section = nd.Section()"]

        lines = [f"{indent}{section.name}: nd.Section = nd.Section("]
        for key, value in kwargs:
            lines.append(f"{inner}{key}={value},")
        lines.extend(visible_when_lines)
        for comp in section.components:
            lines.extend(self._render_component(comp, inner))
        lines.append(f"{indent})")
        return lines

    def _visible_when_lines(self, spec: VisibleWhenState | None, inner: str) -> list[str]:
        # Emitted after the scalar section kwargs as an nd.VisibleWhen(...) call;
        # `equals` is omitted when True (the default) to keep the fixed point stable.
        if spec is None:
            return []
        body = inner + _INDENT
        lines = [f"{inner}visible_when=nd.VisibleWhen(", f"{body}field={_py_literal(spec.field)},"]
        if spec.equals is not True:
            lines.append(f"{body}equals={_py_literal(spec.equals)},")
        lines.append(f"{inner}),")
        return lines

    # -- top-level rendering -------------------------------------------------

    def _render_settings_class(self) -> list[str]:
        state = self.state
        _check_identifier(state.settings_class_name, "settings class name")
        lines = [f"class {state.settings_class_name}(nd.NodeSettings):"]
        if not state.sections:
            lines.append(f"{_INDENT}pass")
            return lines
        for i, section in enumerate(state.sections):
            lines.extend(self._render_section(section, _INDENT))
            if i != len(state.sections) - 1:
                lines.append("")
        return lines

    def _render_node_attrs(self) -> list[str]:
        state = self.state
        # (name, annotation, value) — value rendered inline if scalar, exploded if a container.
        attrs: list[tuple[str, str, object]] = [("node_name", "str", state.node_name)]

        if state.node_category != "Custom":
            attrs.append(("node_category", "str", state.node_category))
        if state.node_group != "custom":
            attrs.append(("node_group", "str", state.node_group))
        if state.node_icon != "user-defined-icon.png":
            attrs.append(("node_icon", "str", state.node_icon))
        if state.title:
            attrs.append(("title", "str", state.title))
        if state.intro:
            attrs.append(("intro", "str", state.intro))
        if state.author:
            attrs.append(("author", "str", state.author))
        if state.version:
            attrs.append(("version", "str", state.version))
        if state.tags:
            attrs.append(("tags", "list[str]", list(state.tags)))
        if state.number_of_inputs != 1:
            attrs.append(("number_of_inputs", "int", state.number_of_inputs))
        if state.number_of_outputs != 1:
            attrs.append(("number_of_outputs", "int", state.number_of_outputs))
        if state.output_names != ["main"]:
            attrs.append(("output_names", "list[str]", list(state.output_names)))
        if state.environment.kind == "kernel":
            attrs.append(("environment", "str", "kernel"))
        if state.environment.dependencies:
            attrs.append(("dependencies", "list[str]", list(state.environment.dependencies)))
        if state.environment.default_kernel_id is not None:
            attrs.append(("kernel_id", "str", state.environment.default_kernel_id))
        if state.example_inputs is not None:
            attrs.append(("example_inputs", "list[dict[str, list]]", [ex.data for ex in state.example_inputs]))
        if state.example_settings is not None:
            attrs.append(("example_settings", "dict[str, dict]", state.example_settings))

        lines: list[str] = []
        for name, annotation, value in attrs:
            value_lines = _render_data_literal(value, _INDENT)
            lines.append(f"{_INDENT}{name}: {annotation} = {value_lines[0]}")
            lines.extend(value_lines[1:])
        lines.extend(self._render_publishes())
        return lines

    def _render_publishes(self) -> list[str]:
        # Emitted after example_settings; a dedicated emitter because these are
        # nd.Artifact(...) calls, not JSON handled by _render_data_literal.
        publishes = self.state.publishes
        if not publishes:
            return []
        inner = _INDENT + _INDENT
        lines = [f"{_INDENT}publishes: list[nd.Artifact] = ["]
        for decl in publishes:
            lines.append(f"{inner}{_artifact_literal(decl)},")
        lines.append(f"{_INDENT}]")
        return lines

    def _render_node_class(self) -> list[str]:
        state = self.state
        _check_identifier(state.class_name, "node class name")
        lines = [f"class {state.class_name}(nd.CustomNodeBase):"]
        lines.extend(self._render_node_attrs())

        if state.sections:
            _check_identifier(state.settings_class_name, "settings class name")
            lines.append(f"{_INDENT}settings_schema: {state.settings_class_name} = {state.settings_class_name}()")

        lines.append("")
        lines.extend(self._render_process())

        for block in state.class_extra:
            lines.append("")
            lines.extend(self._indent_block(block, _INDENT))
        return lines

    def _render_process(self) -> list[str]:
        code = self.state.process_code.strip("\n")
        if not code:
            raise CodegenError("process_code is empty; a node must define process()")
        return self._indent_block(code, _INDENT)

    @staticmethod
    def _indent_block(block: str, indent: str) -> list[str]:
        return [f"{indent}{line}".rstrip() if line.strip() else "" for line in block.split("\n")]

    def render(self) -> str:
        state = self.state
        sections: list[list[str]] = []

        header = ["import polars as pl", "from flowfile import node_designer as nd"]
        header.extend(imp.strip("\n") for imp in state.extra_imports)
        sections.append(header)

        for block in state.module_extra:
            sections.append(block.strip("\n").split("\n"))

        if state.sections:
            sections.append(self._render_settings_class())
        sections.append(self._render_node_class())

        body = "\n\n\n".join("\n".join(part) for part in sections)
        return body.rstrip("\n") + "\n"


def generate_source(state: DesignerState) -> str:
    """Render a DesignerState into canonical, ruff-stable Python source."""
    return _Renderer(state).render()
