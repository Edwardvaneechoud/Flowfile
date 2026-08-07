"""Pure-AST parser lifting custom-node .py files into DesignerState.

Never executes user code: designer-literal expressions are evaluated
structurally, and normalized with the same SDK helpers the runtime uses so the
lifted state matches what executing the file would produce.
"""

import ast
import textwrap
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from flowfile_core.flowfile.node_designer.state import (
    ArtifactDecl,
    ColumnActionInputState,
    ColumnSelectorState,
    DesignerState,
    EnvironmentState,
    ExampleInput,
    NodeManifest,
    NumericInputState,
    ParseIssue,
    ParseResult,
    SecretSelectorState,
    SectionState,
    SelectOption,
    SelectState,
    SliderInputState,
    TextInputState,
    ToggleSwitchState,
)
from shared import node_designer as _sdk
from shared.node_designer.types import DataType, TypeGroup, Types
from shared.node_designer.ui_components import normalize_input_to_data_types_preserving_groups


class ParseIssueCode(str, Enum):
    SYNTAX_ERROR = "SYNTAX_ERROR"
    NO_NODE_CLASS = "NO_NODE_CLASS"
    MULTIPLE_NODE_CLASSES = "MULTIPLE_NODE_CLASSES"
    UNSUPPORTED_CONSTRUCT = "UNSUPPORTED_CONSTRUCT"
    NON_LITERAL_ATTR = "NON_LITERAL_ATTR"
    NON_LITERAL_COMPONENT_KWARG = "NON_LITERAL_COMPONENT_KWARG"
    UNKNOWN_COMPONENT_TYPE = "UNKNOWN_COMPONENT_TYPE"
    UNKNOWN_COMPONENT_KWARG = "UNKNOWN_COMPONENT_KWARG"
    UNRESOLVED_SECTION_REF = "UNRESOLVED_SECTION_REF"
    DYNAMIC_SETTINGS = "DYNAMIC_SETTINGS"
    INVALID_EXAMPLE_INPUT = "INVALID_EXAMPLE_INPUT"
    INVALID_OUTPUT_NAMES = "INVALID_OUTPUT_NAMES"
    LEGACY_KERNEL_FIELDS = "LEGACY_KERNEL_FIELDS"
    LEGACY_IMPORT_PATH = "LEGACY_IMPORT_PATH"
    DATAFRAME_HINT = "DATAFRAME_HINT"
    PROCESS_SIGNATURE_NONSTANDARD = "PROCESS_SIGNATURE_NONSTANDARD"


_SDK_MODULES = {
    "flowfile.node_designer",
    "flowfile_core.flowfile.node_designer",
    "flowfile_core.flowfile.node_designer.custom_node",
    "flowfile_core.flowfile.node_designer.ui_components",
    "flowfile_core.flowfile.node_designer._type_registry",
    "flowfile_core.types",
    "shared.node_designer",
    "shared.node_designer.custom_node",
    "shared.node_designer.ui_components",
    "shared.node_designer.types",
    "shared.node_designer.secrets",
}

_SDK_SYMBOLS = set(_sdk.__all__) | {"TypeGroup"}

_COMPONENT_TYPES = {
    "TextInput",
    "NumericInput",
    "SliderInput",
    "ToggleSwitch",
    "SingleSelect",
    "MultiSelect",
    "ColumnSelector",
    "SecretSelector",
    "ColumnActionInput",
}

_MARKERS = {"IncomingColumns", "AvailableArtifacts", "AvailableSecrets"}
_BUILDERS = {"SectionBuilder", "NodeSettingsBuilder"}

_COMPONENT_KWARGS: dict[str, set[str]] = {
    "TextInput": {"label", "default", "placeholder"},
    "NumericInput": {"label", "default", "min_value", "max_value"},
    "SliderInput": {"label", "default", "min_value", "max_value", "step"},
    "ToggleSwitch": {"label", "default", "description"},
    "SingleSelect": {"label", "options", "default"},
    "MultiSelect": {"label", "options", "default"},
    "ColumnSelector": {"label", "required", "multiple", "data_types"},
    "SecretSelector": {"label", "required", "description", "name_prefix", "options"},
    "ColumnActionInput": {
        "label",
        "actions",
        "output_name_template",
        "show_group_by",
        "show_order_by",
        "data_types",
    },
}

_RECOGNIZED_NODE_ATTRS = {
    "node_name",
    "node_category",
    "node_group",
    "node_icon",
    "title",
    "intro",
    "author",
    "version",
    "tags",
    "number_of_inputs",
    "number_of_outputs",
    "output_names",
    "input_labels",
    "environment",
    "dependencies",
    "settings_schema",
    "example_inputs",
    "example_settings",
    "publishes",
    "requires_kernel",
    "requires_data_for_prediction",
    "kernel_id",
}

_STR_NODE_ATTRS = {"node_name", "node_category", "node_group", "node_icon", "title", "intro", "author", "version"}

_DATAFRAME_HINTS = {"pl.DataFrame", "polars.DataFrame", "DataFrame"}

_SCALARS = (str, int, float, bool, type(None))


class _Marker:
    def __init__(self, name: str, scope: str = "upstream", type_filter: list[str] | None = None):
        self.name = name
        self.scope = scope
        self.type_filter = type_filter


class _LiteralError(Exception):
    def __init__(self, node: ast.AST, detail: str = ""):
        self.node = node
        self.detail = detail
        super().__init__(detail)


def _dotted(expr: ast.expr) -> str | None:
    parts: list[str] = []
    while isinstance(expr, ast.Attribute):
        parts.append(expr.attr)
        expr = expr.value
    if not isinstance(expr, ast.Name):
        return None
    parts.append(expr.id)
    return ".".join(reversed(parts))


class _SourceParser:
    def __init__(self, source: str, file_name: str | None = None):
        self.source = source
        self.file_name = file_name
        self.lines = source.splitlines()
        self.issues: list[ParseIssue] = []
        self.module_aliases: dict[str, str] = {}  # local name -> SDK module path
        self.plain_sdk_modules: set[str] = set()  # dotted paths imported without alias
        self.symbol_bindings: dict[str, str] = {}  # local name -> SDK symbol
        self.section_candidates: dict[str, ast.Call] = {}
        self.component_candidates: dict[str, ast.Call] = {}
        self.settings_instance_candidates: dict[str, ast.Call] = {}
        self.consumed_names: set[str] = set()
        self.settings_classes: dict[str, ast.ClassDef] = {}
        self.consumed_settings_class: str | None = None
        self.extra_imports: list[str] = []
        self._module_stmts: list[tuple[ast.stmt, str, str | None]] = []  # (stmt, kind, name)

    # -- issue helpers -------------------------------------------------------

    def error(self, code: ParseIssueCode, message: str, node: ast.AST | None = None) -> None:
        self.issues.append(
            ParseIssue(code=code.value, message=message, line=getattr(node, "lineno", None), severity="error")
        )

    def warn(self, code: ParseIssueCode, message: str, node: ast.AST | None = None) -> None:
        self.issues.append(
            ParseIssue(code=code.value, message=message, line=getattr(node, "lineno", None), severity="warning")
        )

    def has_errors(self) -> bool:
        return any(issue.severity == "error" for issue in self.issues)

    # -- verbatim extraction -------------------------------------------------

    def verbatim(self, node: ast.stmt) -> str:
        start = node.lineno
        for deco in getattr(node, "decorator_list", []):
            start = min(start, deco.lineno)
        segment = "\n".join(self.lines[start - 1 : node.end_lineno])
        return textwrap.dedent(segment)

    # -- name resolution -----------------------------------------------------

    def resolve_module(self, expr: ast.expr) -> str | None:
        if isinstance(expr, ast.Name):
            return self.module_aliases.get(expr.id)
        dotted = _dotted(expr)
        if dotted is not None and dotted in self.plain_sdk_modules:
            return dotted
        return None

    def resolve_symbol(self, expr: ast.expr) -> str | None:
        if isinstance(expr, ast.Name):
            return self.symbol_bindings.get(expr.id)
        if isinstance(expr, ast.Attribute) and self.resolve_module(expr.value) is not None:
            return expr.attr
        return None

    def _references_builder(self, expr: ast.expr) -> bool:
        for node in ast.walk(expr):
            if isinstance(node, ast.Name | ast.Attribute) and self.resolve_symbol(node) in _BUILDERS:
                return True
        return False

    # -- parse entry ---------------------------------------------------------

    def parse(self) -> ParseResult:
        try:
            module = ast.parse(self.source)
        except SyntaxError as e:
            issue = ParseIssue(
                code=ParseIssueCode.SYNTAX_ERROR.value,
                message=f"Syntax error: {e.msg}",
                line=e.lineno,
                severity="error",
            )
            return ParseResult(mode="code_only", designer_state=None, issues=[issue])

        node_classes = self._classify_module(module)

        if not node_classes:
            self.error(ParseIssueCode.NO_NODE_CLASS, "No CustomNodeBase subclass found in file")
        elif len(node_classes) > 1:
            names = ", ".join(c.name for c in node_classes)
            self.error(
                ParseIssueCode.MULTIPLE_NODE_CLASSES,
                f"A node file must define exactly one CustomNodeBase subclass; found: {names}",
                node_classes[1],
            )

        state: DesignerState | None = None
        if len(node_classes) == 1:
            state = self._lift_node_class(node_classes[0])

        if self.has_errors() or state is None:
            return ParseResult(mode="code_only", designer_state=None, issues=self.issues)
        return ParseResult(mode="designer", designer_state=state, issues=self.issues)

    # -- module-level classification -----------------------------------------

    def _classify_module(self, module: ast.Module) -> list[ast.ClassDef]:
        node_classes: list[ast.ClassDef] = []
        body = list(module.body)
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
            if isinstance(body[0].value.value, str):
                body = body[1:]  # module docstring

        for stmt in body:
            if isinstance(stmt, ast.Import | ast.ImportFrom):
                self._classify_import(stmt)
                continue
            if isinstance(stmt, ast.ClassDef):
                base_kind = self._class_base_kind(stmt)
                if base_kind == "node":
                    node_classes.append(stmt)
                    self._module_stmts.append((stmt, "node_class", stmt.name))
                elif base_kind == "settings":
                    self.settings_classes[stmt.name] = stmt
                    self._module_stmts.append((stmt, "settings_class", stmt.name))
                else:
                    self._module_stmts.append((stmt, "extra", None))
                continue
            candidate = self._as_candidate(stmt)
            if candidate is not None:
                name, call, kind = candidate
                if kind == "section":
                    self.section_candidates[name] = call
                elif kind == "component":
                    self.component_candidates[name] = call
                else:
                    self.settings_instance_candidates[name] = call
                self._module_stmts.append((stmt, "candidate", name))
                continue
            self._module_stmts.append((stmt, "extra", None))
        return node_classes

    def _classify_import(self, stmt: ast.Import | ast.ImportFrom) -> None:
        recognized = True
        legacy = False
        if isinstance(stmt, ast.Import):
            for alias in stmt.names:
                if alias.name == "polars" and alias.asname == "pl":
                    continue
                if alias.name in _SDK_MODULES:
                    legacy = legacy or alias.name.startswith("flowfile_core")
                    if alias.asname:
                        self.module_aliases[alias.asname] = alias.name
                    else:
                        self.plain_sdk_modules.add(alias.name)
                else:
                    recognized = False
        else:
            if stmt.level or stmt.module is None:
                recognized = False
            else:
                module_path = stmt.module
                if module_path in _SDK_MODULES:
                    legacy = module_path.startswith("flowfile_core")
                    for alias in stmt.names:
                        self.symbol_bindings[alias.asname or alias.name] = alias.name
                        if alias.name not in _SDK_SYMBOLS:
                            recognized = False
                else:
                    for alias in stmt.names:
                        sub_path = f"{module_path}.{alias.name}"
                        if sub_path in _SDK_MODULES:
                            legacy = legacy or sub_path.startswith("flowfile_core")
                            self.module_aliases[alias.asname or alias.name] = sub_path
                        else:
                            recognized = False
        if legacy:
            self.warn(
                ParseIssueCode.LEGACY_IMPORT_PATH,
                "Node imports from a legacy flowfile_core path; the canonical import is "
                "'from flowfile import node_designer as nd'",
                stmt,
            )
        if not recognized:
            self.extra_imports.append(self.verbatim(stmt))

    def _class_base_kind(self, cls: ast.ClassDef) -> str | None:
        kinds = {self.resolve_symbol(base) for base in cls.bases}
        if "CustomNodeBase" in kinds:
            if len(cls.bases) > 1 or cls.keywords or cls.decorator_list:
                self.error(
                    ParseIssueCode.UNSUPPORTED_CONSTRUCT,
                    f"Node class '{cls.name}' must inherit exactly from CustomNodeBase, "
                    "without extra bases, keywords or decorators",
                    cls,
                )
            return "node"
        if "NodeSettings" in kinds:
            if len(cls.bases) > 1 or cls.keywords or cls.decorator_list:
                self.error(
                    ParseIssueCode.UNSUPPORTED_CONSTRUCT,
                    f"Settings class '{cls.name}' must inherit exactly from NodeSettings, "
                    "without extra bases, keywords or decorators",
                    cls,
                )
            return "settings"
        return None

    def _as_candidate(self, stmt: ast.stmt) -> tuple[str, ast.Call, str] | None:
        if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
            target, value = stmt.targets[0].id, stmt.value
        elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name) and stmt.value is not None:
            target, value = stmt.target.id, stmt.value
        else:
            return None
        if not isinstance(value, ast.Call):
            if self._references_builder(value):
                self.error(
                    ParseIssueCode.UNSUPPORTED_CONSTRUCT,
                    f"'{target}' uses SectionBuilder/NodeSettingsBuilder, which the designer cannot edit",
                    stmt,
                )
            return None
        symbol = self.resolve_symbol(value.func)
        if symbol in ("Section", "create_section"):
            return target, value, "section"
        if symbol in _COMPONENT_TYPES:
            return target, value, "component"
        if symbol in ("NodeSettings", "create_node_settings"):
            return target, value, "settings_instance"
        if symbol in _BUILDERS or self._references_builder(value):
            self.error(
                ParseIssueCode.UNSUPPORTED_CONSTRUCT,
                f"'{target}' uses SectionBuilder/NodeSettingsBuilder, which the designer cannot edit",
                stmt,
            )
        return None

    # -- designer-literal evaluation -----------------------------------------

    def eval_literal(self, node: ast.expr) -> Any:
        if isinstance(node, ast.Constant):
            if isinstance(node.value, _SCALARS):
                return node.value
            raise _LiteralError(node, f"unsupported constant {node.value!r}")
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub | ast.UAdd):
            operand = self.eval_literal(node.operand)
            if isinstance(operand, bool) or not isinstance(operand, int | float):
                raise _LiteralError(node, "unary +/- only applies to numbers")
            return -operand if isinstance(node.op, ast.USub) else operand
        if isinstance(node, ast.List):
            return [self.eval_literal(item) for item in node.elts]
        if isinstance(node, ast.Tuple):
            return tuple(self.eval_literal(item) for item in node.elts)
        if isinstance(node, ast.Dict):
            result = {}
            for key, value in zip(node.keys, node.values, strict=False):
                if key is None:
                    raise _LiteralError(node, "dict unpacking is not a designer literal")
                key_value = self.eval_literal(key)
                if not isinstance(key_value, _SCALARS):
                    raise _LiteralError(key, "dict keys must be scalars")
                result[key_value] = self.eval_literal(value)
            return result
        if isinstance(node, ast.Attribute):
            base_symbol = self.resolve_symbol(node.value)
            if base_symbol == "Types":
                value = getattr(Types, node.attr, None)
            elif base_symbol == "DataType":
                value = getattr(DataType, node.attr, None)
            elif base_symbol == "TypeGroup":
                value = getattr(TypeGroup, node.attr, None)
            else:
                symbol = self.resolve_symbol(node)
                if symbol in _MARKERS:
                    return _Marker(symbol)
                raise _LiteralError(node, f"'{ast.unparse(node)}' is not a designer literal")
            if value is None:
                raise _LiteralError(node, f"unknown type reference '{ast.unparse(node)}'")
            return value
        if isinstance(node, ast.Name):
            symbol = self.symbol_bindings.get(node.id)
            if symbol in _MARKERS:
                return _Marker(symbol)
            if symbol in ("Types", "DataType", "TypeGroup"):
                raise _LiteralError(node, f"'{node.id}' must be used with an attribute, e.g. Types.String")
            raise _LiteralError(node, f"name '{node.id}' is not a designer literal")
        if isinstance(node, ast.Call):
            func_symbol = self.resolve_symbol(node.func)
            if func_symbol == "ActionOption":
                return self._eval_action_option(node)
            if func_symbol == "VisibleWhen":
                return self._eval_visible_when(node)
            if func_symbol == "AvailableArtifacts":
                return self._eval_available_artifacts(node)
            raise _LiteralError(node, f"call '{ast.unparse(node.func)}(...)' is not a designer literal")
        raise _LiteralError(node, f"'{ast.unparse(node)}' is not a designer literal")

    def _eval_action_option(self, call: ast.Call) -> SelectOption:
        values: dict[str, Any] = {}
        positional = ["value", "label"]
        if len(call.args) > 2:
            raise _LiteralError(call, "ActionOption takes (value, label)")
        for i, arg in enumerate(call.args):
            values[positional[i]] = self.eval_literal(arg)
        for kw in call.keywords:
            if kw.arg not in ("value", "label"):
                raise _LiteralError(call, "ActionOption takes (value, label)")
            values[kw.arg] = self.eval_literal(kw.value)
        if not isinstance(values.get("value"), str) or not isinstance(values.get("label"), str):
            raise _LiteralError(call, "ActionOption value and label must be strings")
        return SelectOption(value=values["value"], label=values["label"])

    def _eval_visible_when(self, call: ast.Call) -> dict:
        # Returns the {field, equals} dict; _lift_section validates + coerces to VisibleWhenState.
        values: dict[str, Any] = {}
        positional = ["field", "equals"]
        if len(call.args) > 2:
            raise _LiteralError(call, "VisibleWhen takes (field, equals)")
        for i, arg in enumerate(call.args):
            values[positional[i]] = self.eval_literal(arg)
        for kw in call.keywords:
            if kw.arg not in ("field", "equals"):
                raise _LiteralError(call, "VisibleWhen takes (field, equals)")
            values[kw.arg] = self.eval_literal(kw.value)
        if not isinstance(values.get("field"), str):
            raise _LiteralError(call, "VisibleWhen field must be a string")
        return values

    def _eval_available_artifacts(self, call: ast.Call) -> _Marker:
        values: dict[str, Any] = {}
        positional = ["scope", "type"]
        if len(call.args) > 2:
            raise _LiteralError(call, "AvailableArtifacts takes (scope, type)")
        for i, arg in enumerate(call.args):
            values[positional[i]] = self.eval_literal(arg)
        for kw in call.keywords:
            if kw.arg not in ("scope", "type"):
                raise _LiteralError(call, "AvailableArtifacts only accepts scope and type")
            values[kw.arg] = self.eval_literal(kw.value)
        scope = values.get("scope", "upstream")
        if scope not in ("upstream", "global", "all"):
            raise _LiteralError(call, "AvailableArtifacts scope must be 'upstream', 'global' or 'all'")
        type_filter = self._normalize_artifact_type_filter(values.get("type"), call)
        return _Marker("AvailableArtifacts", scope=scope, type_filter=type_filter)

    def _normalize_artifact_type_filter(self, value: Any, node: ast.expr) -> list[str] | None:
        if value is None:
            return None
        if isinstance(value, str):
            items = [value]
        elif isinstance(value, tuple | list):
            items = list(value)
        else:
            raise _LiteralError(node, "AvailableArtifacts type must be a string or list of strings")
        if not all(isinstance(v, str) for v in items):
            raise _LiteralError(node, "AvailableArtifacts type must be strings")
        result = sorted(set(items))
        return result or None

    # -- component lifting -----------------------------------------------------

    def _lift_section(self, name: str, call: ast.Call) -> SectionState | None:
        if call.args:
            self.error(
                ParseIssueCode.UNSUPPORTED_CONSTRUCT,
                f"Section '{name}' uses positional arguments; only keyword arguments are supported",
                call,
            )
            return None
        title: str | None = None
        description: str | None = None
        hidden = False
        visible_when: dict | None = None
        layout = "vertical"
        components = []
        ok = True
        for kw in call.keywords:
            if kw.arg is None:
                self.error(
                    ParseIssueCode.DYNAMIC_SETTINGS,
                    f"Section '{name}' is built with **kwargs unpacking",
                    call,
                )
                ok = False
                continue
            if kw.arg in ("title", "description", "hidden", "visible_when", "layout"):
                try:
                    value = self.eval_literal(kw.value)
                except _LiteralError as e:
                    self.error(
                        ParseIssueCode.NON_LITERAL_COMPONENT_KWARG,
                        f"'{name}.{kw.arg}' is not a designer literal: {e.detail}",
                        kw.value,
                    )
                    ok = False
                    continue
                if kw.arg == "hidden":
                    hidden = bool(value)
                elif kw.arg == "title":
                    title = value
                elif kw.arg == "visible_when":
                    if not (isinstance(value, dict) and isinstance(value.get("field"), str)):
                        self.error(
                            ParseIssueCode.NON_LITERAL_COMPONENT_KWARG,
                            f"'{name}.visible_when' must be nd.VisibleWhen(field='section.toggle') "
                            "or a dict like {'field': 'section.toggle'}",
                            kw.value,
                        )
                        ok = False
                        continue
                    visible_when = value
                elif kw.arg == "layout":
                    if value not in ("vertical", "horizontal"):
                        self.error(
                            ParseIssueCode.NON_LITERAL_COMPONENT_KWARG,
                            f"'{name}.layout' must be 'vertical' or 'horizontal'",
                            kw.value,
                        )
                        ok = False
                        continue
                    layout = value
                else:
                    description = value
                continue
            component = self._lift_component(name, kw.arg, kw.value)
            if component is None:
                ok = False
            else:
                components.append(component)
        if not ok:
            return None
        try:
            return SectionState(
                name=name,
                title=title,
                description=description,
                hidden=hidden,
                visible_when=visible_when,
                layout=layout,
                components=components,
            )
        except ValidationError as e:
            self.error(ParseIssueCode.NON_LITERAL_COMPONENT_KWARG, f"Section '{name}' is invalid: {e}", call)
            return None

    def _lift_component(self, section_name: str, comp_name: str, expr: ast.expr):
        path = f"{section_name}.{comp_name}"
        if isinstance(expr, ast.Name):
            candidate = self.component_candidates.get(expr.id)
            if candidate is not None:
                self.consumed_names.add(expr.id)
                return self._lift_component(section_name, comp_name, candidate)
            self.error(
                ParseIssueCode.NON_LITERAL_COMPONENT_KWARG,
                f"'{path}' references '{expr.id}', which is not a component defined at module level",
                expr,
            )
            return None
        if not isinstance(expr, ast.Call):
            self.error(
                ParseIssueCode.NON_LITERAL_COMPONENT_KWARG,
                f"'{path}' must be a component constructor call",
                expr,
            )
            return None
        symbol = self.resolve_symbol(expr.func)
        if symbol not in _COMPONENT_TYPES:
            self.error(
                ParseIssueCode.UNKNOWN_COMPONENT_TYPE,
                f"'{path}' uses unknown component type '{ast.unparse(expr.func)}'",
                expr,
            )
            return None
        if expr.args:
            self.error(
                ParseIssueCode.UNSUPPORTED_CONSTRUCT,
                f"'{path}' uses positional arguments; only keyword arguments are supported",
                expr,
            )
            return None

        known = _COMPONENT_KWARGS[symbol]
        kwargs: dict[str, Any] = {}
        ok = True
        for kw in expr.keywords:
            if kw.arg is None:
                self.error(
                    ParseIssueCode.NON_LITERAL_COMPONENT_KWARG,
                    f"'{path}' is built with **kwargs unpacking",
                    expr,
                )
                ok = False
                continue
            if kw.arg not in known:
                self.error(
                    ParseIssueCode.UNKNOWN_COMPONENT_KWARG,
                    f"'{path}' passes unknown {symbol} argument '{kw.arg}'",
                    kw.value,
                )
                ok = False
                continue
            try:
                kwargs[kw.arg] = self.eval_literal(kw.value)
            except _LiteralError as e:
                self.error(
                    ParseIssueCode.NON_LITERAL_COMPONENT_KWARG,
                    f"'{path}.{kw.arg}' is not a designer literal: {e.detail}",
                    kw.value,
                )
                ok = False
        if not ok:
            return None
        try:
            return self._build_component_state(symbol, comp_name, kwargs, expr)
        except _LiteralError as e:
            self.error(
                ParseIssueCode.NON_LITERAL_COMPONENT_KWARG,
                f"'{path}': {e.detail}",
                e.node,
            )
            return None
        except ValidationError as e:
            first = e.errors()[0]
            loc = ".".join(str(p) for p in first.get("loc", ()))
            self.error(
                ParseIssueCode.NON_LITERAL_COMPONENT_KWARG,
                f"'{path}.{loc}': {first.get('msg', 'invalid value')}",
                expr,
            )
            return None

    def _build_component_state(self, symbol: str, comp_name: str, kwargs: dict[str, Any], expr: ast.expr):
        common = {"name": comp_name, "label": kwargs.get("label")}
        if symbol == "TextInput":
            return TextInputState(**common, default=kwargs.get("default"), placeholder=kwargs.get("placeholder"))
        if symbol == "NumericInput":
            return NumericInputState(
                **common,
                default=kwargs.get("default"),
                min_value=kwargs.get("min_value"),
                max_value=kwargs.get("max_value"),
            )
        if symbol == "SliderInput":
            state = {k: v for k, v in kwargs.items() if k in ("min_value", "max_value", "step") and v is not None}
            return SliderInputState(**common, default=kwargs.get("default"), **state)
        if symbol == "ToggleSwitch":
            return ToggleSwitchState(
                **common, default=bool(kwargs.get("default", False)), description=kwargs.get("description")
            )
        if symbol in ("SingleSelect", "MultiSelect"):
            default = kwargs.get("default")
            if isinstance(default, _Marker) or (
                isinstance(default, list | tuple) and any(isinstance(v, _Marker) for v in default)
            ):
                raise _LiteralError(expr, "IncomingColumns/AvailableArtifacts markers are only valid for options")
            options_source, options, artifact_scope, artifact_type_filter = self._normalize_options(
                kwargs.get("options"), expr
            )
            return SelectState(
                **common,
                component_type=symbol,
                options_source=options_source,
                options=options,
                default=self._jsonify(default),
                artifact_scope=artifact_scope,
                artifact_type_filter=artifact_type_filter,
            )
        if symbol == "ColumnSelector":
            return ColumnSelectorState(
                **common,
                required=bool(kwargs.get("required", False)),
                multiple=bool(kwargs.get("multiple", False)),
                data_types=self._normalize_data_types(kwargs.get("data_types"), expr),
            )
        if symbol == "SecretSelector":
            options = kwargs.get("options")
            if options is not None and not (isinstance(options, _Marker) and options.name == "AvailableSecrets"):
                raise _LiteralError(expr, "SecretSelector options only supports AvailableSecrets")
            return SecretSelectorState(
                **common,
                required=bool(kwargs.get("required", False)),
                description=kwargs.get("description"),
                name_prefix=kwargs.get("name_prefix"),
            )
        actions = self._normalize_select_items(kwargs.get("actions", []), expr, kind="actions")
        state = {
            "output_name_template": kwargs.get("output_name_template", "{column}_{action}"),
            "show_group_by": bool(kwargs.get("show_group_by", False)),
            "show_order_by": bool(kwargs.get("show_order_by", False)),
        }
        return ColumnActionInputState(
            **common,
            actions=actions,
            data_types=self._normalize_data_types(kwargs.get("data_types"), expr),
            **state,
        )

    def _normalize_options(self, value: Any, expr: ast.expr) -> tuple[str, list[SelectOption], str, list[str]]:
        if value is None:
            return "static", [], "upstream", []
        if isinstance(value, _Marker):
            if value.name == "IncomingColumns":
                return "incoming_columns", [], "upstream", []
            if value.name == "AvailableArtifacts":
                return "available_artifacts", [], value.scope, list(value.type_filter or [])
            raise _LiteralError(expr, f"'{value.name}' is not valid for options")
        return "static", self._normalize_select_items(value, expr, kind="options"), "upstream", []

    def _normalize_select_items(self, value: Any, expr: ast.expr, *, kind: str) -> list[SelectOption]:
        if isinstance(value, tuple):
            value = list(value)
        if not isinstance(value, list):
            raise _LiteralError(expr, f"{kind} must be a list")
        items: list[SelectOption] = []
        for item in value:
            if isinstance(item, SelectOption):
                items.append(item)
            elif isinstance(item, str):
                items.append(SelectOption(value=item))
            elif isinstance(item, tuple | list) and len(item) == 2 and all(isinstance(x, str) for x in item):
                items.append(SelectOption(value=item[0], label=item[1]))
            else:
                raise _LiteralError(
                    expr, f"{kind} entries must be strings, (value, label) tuples or ActionOption(...) calls"
                )
        return items

    def _normalize_data_types(self, value: Any, expr: ast.expr) -> Any:
        if value is None:
            return "ALL"
        if isinstance(value, _Marker):
            raise _LiteralError(expr, f"'{value.name}' is not valid for data_types")
        if isinstance(value, tuple):
            value = list(value)
        if isinstance(value, list) and any(isinstance(item, _Marker) for item in value):
            raise _LiteralError(expr, "marker classes are not valid inside data_types")
        return normalize_input_to_data_types_preserving_groups(value)

    # -- node class lifting ----------------------------------------------------

    def _lift_node_class(self, cls: ast.ClassDef) -> DesignerState | None:
        attrs: dict[str, tuple[ast.expr, ast.stmt]] = {}
        process_def: ast.FunctionDef | None = None
        predict_schema_def: ast.FunctionDef | None = None
        class_extra: list[str] = []

        body = list(cls.body)
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
            if isinstance(body[0].value.value, str):
                body = body[1:]  # class docstring, dropped on canonical rewrite

        for stmt in body:
            target: str | None = None
            value: ast.expr | None = None
            if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                target, value = stmt.targets[0].id, stmt.value
            elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name) and stmt.value is not None:
                target, value = stmt.target.id, stmt.value

            if target is not None and target in _RECOGNIZED_NODE_ATTRS:
                attrs[target] = (value, stmt)
            elif isinstance(stmt, ast.FunctionDef) and stmt.name == "process":
                process_def = stmt
            elif isinstance(stmt, ast.FunctionDef) and stmt.name == "predict_output_schema":
                predict_schema_def = stmt
            elif isinstance(stmt, ast.Pass):
                continue
            else:
                class_extra.append(self.verbatim(stmt))

        if process_def is None:
            self.error(ParseIssueCode.UNSUPPORTED_CONSTRUCT, f"Node class '{cls.name}' must define process()", cls)
        else:
            self._lint_process(process_def)

        lifted = self._eval_node_attrs(attrs)
        sections, settings_class_name = self._resolve_settings(cls, attrs.get("settings_schema"))
        environment = self._resolve_environment(lifted, attrs)

        if "node_name" not in lifted:
            self.error(ParseIssueCode.UNSUPPORTED_CONSTRUCT, f"Node class '{cls.name}' must define node_name", cls)

        self._validate_output_names(lifted, attrs)

        if self.has_errors():
            return None

        state_kwargs: dict[str, Any] = {
            "class_name": cls.name,
            "settings_class_name": settings_class_name or f"{cls.name}Settings",
            "node_name": lifted["node_name"],
            "environment": environment,
            "sections": sections,
            "process_code": self.verbatim(process_def),
            "predict_schema_code": self.verbatim(predict_schema_def) if predict_schema_def is not None else "",
            "extra_imports": self.extra_imports,
            "module_extra": self._collect_module_extra(),
            "class_extra": class_extra,
        }
        for attr in ("node_category", "node_group", "node_icon", "title", "intro", "author", "version"):
            if lifted.get(attr) is not None:
                state_kwargs[attr] = lifted[attr]
        for attr in (
            "number_of_inputs",
            "number_of_outputs",
            "output_names",
            "input_labels",
            "tags",
            "requires_data_for_prediction",
        ):
            if attr in lifted:
                state_kwargs[attr] = lifted[attr]
        if "example_inputs" in lifted:
            state_kwargs["example_inputs"] = lifted["example_inputs"]
        if "example_settings" in lifted:
            state_kwargs["example_settings"] = lifted["example_settings"]
        if lifted.get("publishes"):
            state_kwargs["publishes"] = lifted["publishes"]

        try:
            return DesignerState(**state_kwargs)
        except ValidationError as e:
            first = e.errors()[0]
            loc = ".".join(str(p) for p in first.get("loc", ()))
            self.error(ParseIssueCode.UNSUPPORTED_CONSTRUCT, f"invalid designer state at '{loc}': {first['msg']}", cls)
            return None

    def _eval_node_attrs(self, attrs: dict[str, tuple[ast.expr, ast.stmt]]) -> dict[str, Any]:
        lifted: dict[str, Any] = {}
        for name, (value, _stmt) in attrs.items():
            if name == "settings_schema":
                continue
            if name == "example_inputs":
                lifted[name] = self._lift_example_inputs(value)
                continue
            if name == "publishes":
                lifted[name] = self._lift_publishes(value)
                continue
            try:
                evaluated = self.eval_literal(value)
            except _LiteralError as e:
                self.error(
                    ParseIssueCode.NON_LITERAL_ATTR,
                    f"'{name}' must be a designer literal: {e.detail}",
                    e.node,
                )
                continue
            if isinstance(evaluated, _Marker) or not self._is_plain_json(evaluated):
                self.error(ParseIssueCode.NON_LITERAL_ATTR, f"'{name}' must be a plain literal value", value)
                continue
            checked = self._check_attr_type(name, evaluated, value)
            if checked is not _INVALID:
                lifted[name] = checked
        return lifted

    def _check_attr_type(self, name: str, value: Any, node: ast.expr) -> Any:
        if value is None:
            return None
        if name in _STR_NODE_ATTRS or name == "kernel_id":
            if not isinstance(value, str):
                self.error(ParseIssueCode.NON_LITERAL_ATTR, f"'{name}' must be a string", node)
                return _INVALID
            return value
        if name in ("number_of_inputs", "number_of_outputs"):
            if type(value) is not int:
                self.error(ParseIssueCode.NON_LITERAL_ATTR, f"'{name}' must be an integer", node)
                return _INVALID
            return value
        if name in ("requires_kernel", "requires_data_for_prediction"):
            if not isinstance(value, bool):
                self.error(ParseIssueCode.NON_LITERAL_ATTR, f"'{name}' must be a boolean", node)
                return _INVALID
            return value
        if name == "environment":
            if value not in ("local", "kernel"):
                self.error(ParseIssueCode.NON_LITERAL_ATTR, "'environment' must be 'local' or 'kernel'", node)
                return _INVALID
            return value
        if name == "dependencies":
            if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
                self.error(ParseIssueCode.NON_LITERAL_ATTR, "'dependencies' must be a list of strings", node)
                return _INVALID
            return value
        if name == "tags":
            if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
                self.error(ParseIssueCode.NON_LITERAL_ATTR, "'tags' must be a list of strings", node)
                return _INVALID
            return value
        if name == "output_names":
            if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
                self.error(ParseIssueCode.INVALID_OUTPUT_NAMES, "'output_names' must be a list of strings", node)
                return _INVALID
            return value
        if name == "input_labels":
            if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
                self.error(ParseIssueCode.NON_LITERAL_ATTR, "'input_labels' must be a list of strings", node)
                return _INVALID
            return value
        if name == "example_settings":
            if not isinstance(value, dict) or not all(
                isinstance(k, str) and isinstance(v, dict) and all(isinstance(ck, str) for ck in v)
                for k, v in value.items()
            ):
                self.error(
                    ParseIssueCode.NON_LITERAL_ATTR,
                    "'example_settings' must be a dict of {section: {component: value}}",
                    node,
                )
                return _INVALID
            return self._jsonify(value)
        return value

    def _lift_example_inputs(self, node: ast.expr) -> Any:
        try:
            value = self.eval_literal(node)
        except _LiteralError as e:
            self.error(
                ParseIssueCode.INVALID_EXAMPLE_INPUT,
                f"'example_inputs' must be literal data: {e.detail}",
                e.node,
            )
            return _INVALID
        if isinstance(value, tuple):
            value = list(value)
        if not isinstance(value, list):
            self.error(ParseIssueCode.INVALID_EXAMPLE_INPUT, "'example_inputs' must be a list of dicts", node)
            return _INVALID
        examples: list[ExampleInput] = []
        for entry in value:
            if not isinstance(entry, dict) or not all(isinstance(k, str) for k in entry):
                self.error(
                    ParseIssueCode.INVALID_EXAMPLE_INPUT,
                    "each example input must be a dict of column -> list of values",
                    node,
                )
                return _INVALID
            data: dict[str, list[Any]] = {}
            for column, values in entry.items():
                if isinstance(values, tuple):
                    values = list(values)
                if not isinstance(values, list) or not all(isinstance(v, _SCALARS) for v in values):
                    self.error(
                        ParseIssueCode.INVALID_EXAMPLE_INPUT,
                        f"example values for column '{column}' must be a list of scalars",
                        node,
                    )
                    return _INVALID
                data[column] = values
            examples.append(ExampleInput(data=data))
        return examples

    def _lift_publishes(self, node: ast.expr) -> Any:
        if isinstance(node, ast.List | ast.Tuple):
            elts = list(node.elts)
        else:
            self.error(
                ParseIssueCode.NON_LITERAL_ATTR,
                "'publishes' must be a list of nd.Artifact(...) calls or strings",
                node,
            )
            return _INVALID
        decls: list[ArtifactDecl] = []
        for elt in elts:
            decl = self._lift_publish_entry(elt)
            if decl is None:
                return _INVALID
            decls.append(decl)
        return decls

    def _resolves_to_artifact(self, func: ast.expr) -> bool:
        if self.resolve_symbol(func) == "Artifact":
            return True
        dotted = _dotted(func)
        return dotted is not None and dotted.split(".")[-1] == "Artifact"

    def _lift_publish_entry(self, node: ast.expr) -> ArtifactDecl | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return ArtifactDecl(name=node.value)
        if isinstance(node, ast.Call) and self._resolves_to_artifact(node.func):
            return self._lift_artifact_call(node)
        self.error(
            ParseIssueCode.NON_LITERAL_ATTR,
            "'publishes' entries must be nd.Artifact(...) calls or string literals",
            node,
        )
        return None

    def _lift_artifact_call(self, call: ast.Call) -> ArtifactDecl | None:
        name: str | None = None
        type_value: str | None = None
        if len(call.args) > 1:
            self.error(ParseIssueCode.NON_LITERAL_ATTR, "Artifact takes (name, type=...)", call)
            return None
        if call.args:
            arg0 = call.args[0]
            if not (isinstance(arg0, ast.Constant) and isinstance(arg0.value, str)):
                self.error(ParseIssueCode.NON_LITERAL_ATTR, "Artifact name must be a string literal", call)
                return None
            name = arg0.value
        for kw in call.keywords:
            if kw.arg == "name":
                if not (isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str)):
                    self.error(ParseIssueCode.NON_LITERAL_ATTR, "Artifact name must be a string literal", call)
                    return None
                name = kw.value.value
            elif kw.arg == "type":
                if isinstance(kw.value, ast.Constant) and kw.value.value is None:
                    type_value = None
                elif isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                    type_value = kw.value.value
                else:
                    self.error(ParseIssueCode.NON_LITERAL_ATTR, "Artifact type must be a string literal", call)
                    return None
            else:
                self.error(ParseIssueCode.NON_LITERAL_ATTR, f"Artifact got unexpected argument '{kw.arg}'", call)
                return None
        if name is None:
            self.error(ParseIssueCode.NON_LITERAL_ATTR, "Artifact requires a name", call)
            return None
        return ArtifactDecl(name=name, type=type_value)

    def _is_plain_json(self, value: Any) -> bool:
        if isinstance(value, _SCALARS):
            return True
        if isinstance(value, list | tuple):
            return all(self._is_plain_json(v) for v in value)
        if isinstance(value, dict):
            return all(isinstance(k, str) and self._is_plain_json(v) for k, v in value.items())
        return False

    def _jsonify(self, value: Any) -> Any:
        if isinstance(value, list | tuple):
            return [self._jsonify(v) for v in value]
        if isinstance(value, dict):
            return {k: self._jsonify(v) for k, v in value.items()}
        return value

    def _resolve_environment(
        self, lifted: dict[str, Any], attrs: dict[str, tuple[ast.expr, ast.stmt]]
    ) -> EnvironmentState:
        legacy_fields = [name for name in ("requires_kernel", "kernel_id") if name in attrs]
        if legacy_fields:
            self.warn(
                ParseIssueCode.LEGACY_KERNEL_FIELDS,
                f"legacy field(s) {', '.join(legacy_fields)} normalized to the environment model",
                attrs[legacy_fields[0]][1],
            )
        kind = lifted.get("environment")
        if kind is None:
            kind = "kernel" if lifted.get("requires_kernel") else "local"
        return EnvironmentState(
            kind=kind,
            dependencies=lifted.get("dependencies") or [],
            default_kernel_id=lifted.get("kernel_id"),
        )

    def _validate_output_names(self, lifted: dict[str, Any], attrs: dict[str, tuple[ast.expr, ast.stmt]]) -> None:
        names = lifted.get("output_names")
        if names is None:
            return
        node = attrs["output_names"][0]
        if not names or len(set(names)) != len(names) or any(not n for n in names):
            self.error(ParseIssueCode.INVALID_OUTPUT_NAMES, "'output_names' must be unique, non-empty names", node)
            return
        explicit_outputs = lifted.get("number_of_outputs")
        if explicit_outputs is not None and len(names) != explicit_outputs:
            self.error(
                ParseIssueCode.INVALID_OUTPUT_NAMES,
                f"'output_names' has {len(names)} entries but number_of_outputs is {explicit_outputs}",
                node,
            )

    def _lint_process(self, fn: ast.FunctionDef) -> None:
        params = list(fn.args.posonlyargs) + list(fn.args.args)
        if not params or params[0].arg != "self":
            self.warn(
                ParseIssueCode.PROCESS_SIGNATURE_NONSTANDARD,
                "process() should be an instance method: def process(self, *inputs)",
                fn,
            )
        elif len(params) > 1 or fn.args.vararg is None or fn.args.kwonlyargs or fn.args.kwarg:
            self.warn(
                ParseIssueCode.PROCESS_SIGNATURE_NONSTANDARD,
                "the canonical process signature is def process(self, *inputs: pl.LazyFrame)",
                fn,
            )
        annotated = [p.annotation for p in params[1:]]
        if fn.args.vararg is not None:
            annotated.append(fn.args.vararg.annotation)
        for annotation in annotated:
            if annotation is not None and ast.unparse(annotation) in _DATAFRAME_HINTS:
                self.warn(
                    ParseIssueCode.DATAFRAME_HINT,
                    "process() inputs are hinted as pl.DataFrame but always receive pl.LazyFrame",
                    annotation,
                )
                break

    # -- settings resolution ---------------------------------------------------

    def _resolve_settings(
        self, cls: ast.ClassDef, settings_attr: tuple[ast.expr, ast.stmt] | None
    ) -> tuple[list[SectionState], str | None]:
        if settings_attr is None:
            return [], None
        value, stmt = settings_attr
        if isinstance(value, ast.Constant) and value.value is None:
            return [], None
        if isinstance(value, ast.Name):
            candidate = self.settings_instance_candidates.get(value.id)
            if candidate is None:
                self.error(
                    ParseIssueCode.UNSUPPORTED_CONSTRUCT,
                    f"settings_schema references '{value.id}', which is not a NodeSettings value the designer "
                    "understands",
                    value,
                )
                return [], None
            self.consumed_names.add(value.id)
            return self._lift_settings_call(candidate), None
        if not isinstance(value, ast.Call):
            self.error(
                ParseIssueCode.UNSUPPORTED_CONSTRUCT,
                "settings_schema must be SettingsClass(), NodeSettings(...), create_node_settings(...) or None",
                value,
            )
            return [], None
        symbol = self.resolve_symbol(value.func)
        if symbol in ("NodeSettings", "create_node_settings"):
            return self._lift_settings_call(value), None
        if isinstance(value.func, ast.Name) and value.func.id in self.settings_classes:
            if value.args or value.keywords:
                self.error(
                    ParseIssueCode.UNSUPPORTED_CONSTRUCT,
                    "settings_schema instantiation must not take arguments",
                    value,
                )
                return [], None
            settings_cls = self.settings_classes[value.func.id]
            self.consumed_settings_class = settings_cls.name
            return self._lift_settings_class(settings_cls), settings_cls.name
        self.error(
            ParseIssueCode.UNSUPPORTED_CONSTRUCT,
            f"settings_schema value '{ast.unparse(value)}' is not supported by the designer",
            value,
        )
        return [], None

    def _lift_settings_call(self, call: ast.Call) -> list[SectionState]:
        sections: list[SectionState] = []
        if call.args:
            self.error(
                ParseIssueCode.UNSUPPORTED_CONSTRUCT,
                "NodeSettings/create_node_settings only takes keyword arguments",
                call,
            )
            return []
        for kw in call.keywords:
            if kw.arg is None:
                self.error(ParseIssueCode.DYNAMIC_SETTINGS, "settings are built with **kwargs unpacking", call)
                continue
            section = self._lift_section_value(kw.arg, kw.value)
            if section is not None:
                sections.append(section)
        return sections

    def _lift_settings_class(self, cls: ast.ClassDef) -> list[SectionState]:
        sections: list[SectionState] = []
        body = list(cls.body)
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
            if isinstance(body[0].value.value, str):
                body = body[1:]
        for stmt in body:
            if isinstance(stmt, ast.Pass):
                continue
            if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                target, value = stmt.targets[0].id, stmt.value
            elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name) and stmt.value is not None:
                target, value = stmt.target.id, stmt.value
            else:
                self.error(
                    ParseIssueCode.DYNAMIC_SETTINGS,
                    f"settings class '{cls.name}' contains statements the designer cannot represent",
                    stmt,
                )
                continue
            section = self._lift_section_value(target, value)
            if section is not None:
                sections.append(section)
        return sections

    def _lift_section_value(self, name: str, value: ast.expr) -> SectionState | None:
        if isinstance(value, ast.Name):
            candidate = self.section_candidates.get(value.id)
            if candidate is None:
                self.error(
                    ParseIssueCode.UNRESOLVED_SECTION_REF,
                    f"section '{name}' references '{value.id}', which is not a module-level Section",
                    value,
                )
                return None
            self.consumed_names.add(value.id)
            return self._lift_section(name, candidate)
        if isinstance(value, ast.Call):
            symbol = self.resolve_symbol(value.func)
            if symbol in ("Section", "create_section"):
                return self._lift_section(name, value)
            inner = value.func
            if isinstance(inner, ast.Call) and self.resolve_symbol(inner.func) in ("Section", "create_section"):
                self.error(
                    ParseIssueCode.UNSUPPORTED_CONSTRUCT,
                    f"section '{name}' uses the Section()(...) call-append style",
                    value,
                )
                return None
        self.error(
            ParseIssueCode.DYNAMIC_SETTINGS,
            f"section '{name}' is not a Section(...) the designer can edit",
            value,
        )
        return None

    # -- module_extra assembly ---------------------------------------------------

    def _collect_module_extra(self) -> list[str]:
        extra: list[str] = []
        for stmt, kind, name in self._module_stmts:
            if kind == "extra":
                extra.append(self.verbatim(stmt))
            elif kind == "candidate" and name not in self.consumed_names:
                extra.append(self.verbatim(stmt))
            elif kind == "settings_class" and name != self.consumed_settings_class:
                extra.append(self.verbatim(stmt))
        return extra


_INVALID = object()


def parse_source(source: str, *, file_name: str | None = None) -> ParseResult:
    return _SourceParser(source, file_name).parse()


def parse_file(path: Path | str) -> ParseResult:
    path = Path(path)
    return parse_source(path.read_text(encoding="utf-8"), file_name=path.name)


def extract_example_inputs(source: str) -> list[dict[str, list]] | None:
    """Best-effort exec-free lift of the node class's ``example_inputs`` literal.

    Used by dry-run for code-only nodes (where the DesignerState is unavailable).
    Returns the column-oriented list of dicts, or None when absent/non-literal.
    """
    try:
        module = ast.parse(source)
    except (SyntaxError, ValueError):
        return None
    for stmt in module.body:
        if not isinstance(stmt, ast.ClassDef):
            continue
        if not any((name := _dotted(base)) and name.split(".")[-1] == "CustomNodeBase" for base in stmt.bases):
            continue
        for item in stmt.body:
            if isinstance(item, ast.Assign) and len(item.targets) == 1 and isinstance(item.targets[0], ast.Name):
                target, value = item.targets[0].id, item.value
            elif isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name) and item.value is not None:
                target, value = item.target.id, item.value
            else:
                continue
            if target != "example_inputs":
                continue
            try:
                data = ast.literal_eval(value)
            except (ValueError, SyntaxError):
                return None
            if not isinstance(data, list) or not all(isinstance(entry, dict) for entry in data):
                return None
            return data
        return None
    return None


def extract_example_settings(source: str) -> dict | None:
    """Best-effort exec-free lift of the node class's ``example_settings`` literal.

    Mirrors ``extract_example_inputs`` for the settings companion. Returns the
    section->values dict, or None when absent/non-literal/wrong shape.
    """
    try:
        module = ast.parse(source)
    except (SyntaxError, ValueError):
        return None
    for stmt in module.body:
        if not isinstance(stmt, ast.ClassDef):
            continue
        if not any((name := _dotted(base)) and name.split(".")[-1] == "CustomNodeBase" for base in stmt.bases):
            continue
        for item in stmt.body:
            if isinstance(item, ast.Assign) and len(item.targets) == 1 and isinstance(item.targets[0], ast.Name):
                target, value = item.targets[0].id, item.value
            elif isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name) and item.value is not None:
                target, value = item.target.id, item.value
            else:
                continue
            if target != "example_settings":
                continue
            try:
                data = ast.literal_eval(value)
            except (ValueError, SyntaxError):
                return None
            if not isinstance(data, dict) or not all(isinstance(v, dict) for v in data.values()):
                return None
            return data
        return None
    return None


def _node_classdefs(module: ast.Module) -> list[ast.ClassDef]:
    """Module-level classes whose direct bases name CustomNodeBase."""
    return [
        stmt
        for stmt in module.body
        if isinstance(stmt, ast.ClassDef)
        and any((base_name := _dotted(base)) and base_name.split(".")[-1] == "CustomNodeBase" for base in stmt.bases)
    ]


def _best_effort_publishes(value: ast.expr) -> list[ArtifactDecl]:
    """Lift a ``publishes`` list literal to ArtifactDecls, skipping anything malformed."""
    decls: list[ArtifactDecl] = []
    if not isinstance(value, ast.List | ast.Tuple):
        return decls
    for elt in value.elts:
        if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
            decls.append(ArtifactDecl(name=elt.value))
            continue
        if not isinstance(elt, ast.Call):
            continue
        dotted = _dotted(elt.func)
        if dotted is None or dotted.split(".")[-1] != "Artifact":
            continue
        name: str | None = None
        type_value: str | None = None
        if elt.args and isinstance(elt.args[0], ast.Constant) and isinstance(elt.args[0].value, str):
            name = elt.args[0].value
        for kw in elt.keywords:
            if kw.arg == "name" and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                name = kw.value.value
            elif kw.arg == "type" and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                type_value = kw.value.value
        if name is not None:
            decls.append(ArtifactDecl(name=name, type=type_value))
    return decls


def extract_manifest(source: str) -> NodeManifest:
    """Best-effort exec-free listing metadata; never raises."""
    try:
        module = ast.parse(source)
    except (SyntaxError, ValueError):
        return NodeManifest()
    try:
        classes = _node_classdefs(module)
        if not classes:
            return NodeManifest()
        node_cls = classes[0]

        attrs: dict[str, Any] = {}
        publishes: list[ArtifactDecl] = []
        for stmt in node_cls.body:
            if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                target, value = stmt.targets[0].id, stmt.value
            elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name) and stmt.value is not None:
                target, value = stmt.target.id, stmt.value
            else:
                continue
            if target == "publishes":
                publishes = _best_effort_publishes(value)
                continue
            try:
                attrs[target] = ast.literal_eval(value)
            except (ValueError, SyntaxError):
                continue

        kind = attrs.get("environment")
        if kind not in ("local", "kernel"):
            kind = "kernel" if attrs.get("requires_kernel") is True else "local"
        dependencies = attrs.get("dependencies")
        if not (isinstance(dependencies, list) and all(isinstance(d, str) for d in dependencies)):
            dependencies = []
        kernel_id = attrs.get("kernel_id")
        manifest_kwargs: dict[str, Any] = {
            "class_name": node_cls.name,
            "environment": EnvironmentState(
                kind=kind,
                dependencies=dependencies,
                default_kernel_id=kernel_id if isinstance(kernel_id, str) else None,
            ),
        }
        if isinstance(attrs.get("node_name"), str):
            manifest_kwargs["node_name"] = attrs["node_name"]
        for field in ("node_category", "node_icon", "title", "intro", "author", "version"):
            if isinstance(attrs.get(field), str):
                manifest_kwargs[field] = attrs[field]
        tags = attrs.get("tags")
        if isinstance(tags, list) and all(isinstance(t, str) for t in tags):
            manifest_kwargs["tags"] = tags
        for field in ("number_of_inputs", "number_of_outputs"):
            if type(attrs.get(field)) is int:
                manifest_kwargs[field] = attrs[field]
        # Empty lists are lifted too: manifest_to_template and to_node_template must
        # agree exactly or registry.ensure_class flaps the palette template.
        output_names = attrs.get("output_names")
        if isinstance(output_names, list) and all(isinstance(n, str) for n in output_names):
            manifest_kwargs["output_names"] = output_names
        input_labels = attrs.get("input_labels")
        if isinstance(input_labels, list) and all(isinstance(n, str) for n in input_labels):
            manifest_kwargs["input_labels"] = input_labels
        if isinstance(attrs.get("node_group"), str):
            manifest_kwargs["node_group"] = attrs["node_group"]
        if attrs.get("node_type") in ("input", "output", "process"):
            manifest_kwargs["node_type"] = attrs["node_type"]
        if attrs.get("transform_type") in ("narrow", "wide", "other"):
            manifest_kwargs["transform_type"] = attrs["transform_type"]
        if publishes:
            manifest_kwargs["publishes"] = publishes
        return NodeManifest(**manifest_kwargs)
    except Exception:
        return NodeManifest()


class NodeSourceError(Exception):
    """A node file failed AST-level validation at registry scan time."""


def scan_node_source(source: str) -> NodeManifest:
    """``extract_manifest`` with the registry's error ladder.

    Raises ``NodeSourceError`` (message-compatible with the exec-time loader)
    instead of degrading, so scan-time failures stay visible-with-error.
    """
    try:
        module = ast.parse(source)
    except SyntaxError as e:
        raise NodeSourceError(f"Syntax error at line {e.lineno}: {e.msg}") from e
    except ValueError as e:
        raise NodeSourceError(str(e)) from e
    classes = _node_classdefs(module)
    if not classes:
        raise NodeSourceError("No CustomNodeBase subclass found in module")
    # Count transitive in-file subclasses too, matching the exec-time loader's
    # "exactly one subclass defined in the module" rule.
    all_classdefs = [stmt for stmt in module.body if isinstance(stmt, ast.ClassDef)]
    node_names = {cls.name for cls in classes}
    changed = True
    while changed:
        changed = False
        for stmt in all_classdefs:
            if stmt.name in node_names:
                continue
            if any((name := _dotted(base)) and name.split(".")[-1] in node_names for base in stmt.bases):
                node_names.add(stmt.name)
                classes.append(stmt)
                changed = True
    if len(classes) > 1:
        names = ", ".join(sorted(cls.name for cls in classes))
        raise NodeSourceError(
            f"Multiple CustomNodeBase subclasses found ({names}); a node file must define exactly one"
        )
    manifest = extract_manifest(source)
    if manifest.node_name is None:
        raise NodeSourceError("node_name must be a string literal")
    return manifest
