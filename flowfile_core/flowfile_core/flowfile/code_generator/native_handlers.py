"""Native ``fl.*`` class emission for the FlowFrame export: gates, subflows, Python Scripts, flow ports, custom nodes.

These node types have no fluent frame method; the frame places them with its native classes
(``fl.Gate``, ``fl.RunFlow``, ``fl.PythonScript`` / ``@fl.python_script``, ``fl.FlowInput``,
``.to_flow_output``, ``fl.custom_nodes.<key>``), so the export spells them the same way and the
rebuilt graph holds the same node types. A node a native form cannot express is recorded in
``unsupported_nodes`` with the reason, like every other handler.
"""

import ast
import builtins
import json
import keyword
import re

from flowfile_core.flowfile.code_generator.base import ConverterMixinBase
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import safe_eval_pl_type
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.param_types import coerce_param_value, stringify_param_value
from flowfile_core.flowfile.parameter_resolver import find_unresolved_in_model
from flowfile_core.schemas import input_schema

FLOW_PARAMETER_HELPER = '''\
def _flowfile_flow_parameter(frame, name, value, **declaration):
    """Declare flow parameter ``name`` with this run's value on ``frame``'s graph; returns ``name``."""
    if any(p.name == name for p in frame.flow_graph.flow_settings.parameters):
        fl.set_flow_parameter(frame, name, value)
    else:
        fl.add_flow_parameter(frame, fl.Parameter(name, default=value, **declaration))
    return name'''

FLOW_VAR = "flow"
_INPUT_READ = re.compile(r'^(?P<name>[A-Za-z_]\w*) = flowfile_ctx\.read_inputs\(\)\["main"\]\[(?P<index>\d+)\]$')
_PUBLISH_STARTS = (
    "flowfile_ctx.publish_output(_result",
    "if isinstance(_result, dict):",
    "if not isinstance(_result, dict)",
)
_SESSION_PRELUDE = {"pl": "import polars as pl"}


def str_literal(text: str) -> str:
    """A string literal: triple-quoted for multi-line text it spells verbatim, else JSON-escaped."""
    if "\n" in text and '"""' not in text and "\\" not in text and "\r" not in text and not text.endswith('"'):
        return f'"""{text}"""'
    return json.dumps(text, ensure_ascii=False)


def value_literal(value) -> str | None:
    """A Python literal that evaluates back to ``value``, or None."""
    if isinstance(value, str):
        return str_literal(value)
    text = repr(value)
    try:
        restored = ast.literal_eval(text)
    except (ValueError, SyntaxError, TypeError, MemoryError, RecursionError):
        return None
    return text if type(restored) is type(value) and restored == value else None


def call(func: str, args: list[str]) -> str:
    """``func(args)`` on one line when it fits, else one argument per line."""
    one = f"{func}({', '.join(args)})"
    if len(one) <= 100 and "\n" not in one:
        return one
    return f"{func}(\n" + "".join(f"    {arg},\n" for arg in args) + ")"


def _dtype_expr(data_type: str) -> str | None:
    """``fl.<dtype>`` for a stored dtype string, when it parses to a Polars dtype."""
    try:
        safe_eval_pl_type(f"pl.{data_type}", bare_names=False)
    except ValueError:
        return None
    return "fl." + data_type


def schema_literal(columns: list[tuple[str, str]]) -> str | None:
    """``{"col": fl.Int64, ...}`` for ``(name, dtype string)`` pairs, or None when a dtype has no form."""
    entries = []
    for name, data_type in columns:
        dtype = _dtype_expr(data_type)
        if dtype is None:
            return None
        entries.append(f"{json.dumps(name, ensure_ascii=False)}: {dtype}")
    return "{" + ", ".join(entries) + "}"


def _nested_literal(literals: dict[str, str]) -> str:
    return "{" + ", ".join(f"{json.dumps(key)}: {value}" for key, value in literals.items()) + "}"


def _is_note(cell: str) -> bool:
    lines = cell.split("\n")
    return bool(lines) and all(line.startswith("#") for line in lines)


def _undocstring(cell: str) -> str | None:
    """The docstring text a ``#``-note cell was generated from, or None when it cannot be spelled back."""
    text = "\n".join(line[2:] if line.startswith("# ") else line[1:] for line in cell.split("\n"))
    if '"""' in text or "\\" in text or text.endswith('"') or not text.strip():
        return None
    return text


def _script_function_text(name: str, parameters: list[str], body_cells: list[str], docstring: str | None) -> str:
    """A ``def`` whose body is ``body_cells`` joined at ``# %%`` markers (``# %% [markdown]`` for notes)."""
    lines = [f"def {name}({', '.join(parameters)}):"]
    if docstring is not None:
        doc = docstring.split("\n")
        lines.append(f'    """{doc[0]}' + ("" if len(doc) > 1 else '"""'))
        if len(doc) > 1:
            lines.extend(f"    {line}" if line else "" for line in doc[1:])
            lines.append('    """')
    for index, cell in enumerate(body_cells):
        cell_lines = cell.split("\n")
        if index:
            lines.append("")
            if _is_note(cell):
                lines.append("    # %% [markdown]")
            elif len(cell_lines) > 1 and re.fullmatch(r"# [^%\s].*", cell_lines[0]):
                lines.append(f"    # %% {cell_lines.pop(0)[2:]}")
            else:
                lines.append("    # %%")
        lines.extend(f"    {line}" if line else "" for line in cell_lines)
    return "\n".join(lines)


def _decorator_parts(cells: list[str]) -> tuple | None:
    """Split stored cells into prelude lines, parameters, candidate ``(docstring, body cells)`` and a name, or None.

    The layout is ``python_script._notebook_cells``: optional prelude, the inputs cell, an optional
    docstring note, the body, and a last cell holding the outputs marker. Each body candidate (with or
    without the docstring) is checked by regenerating it. The function name is only recoverable from
    a dict guard's message, which spells it.
    """
    from flowfile_frame.python_script import INPUTS_MARKER, OUTPUTS_MARKER

    marker = next((i for i, cell in enumerate(cells) if cell.split("\n", 1)[0] == INPUTS_MARKER), None)
    if marker is None or marker > 1 or len(cells) < marker + 2:
        return None
    reads = [_INPUT_READ.match(line) for line in cells[marker].split("\n")[1:]]
    if not reads or any(match is None for match in reads):
        return None
    if [int(match["index"]) for match in reads] != list(range(len(reads))):
        return None
    last = cells[-1].split("\n")
    if OUTPUTS_MARKER not in last:
        return None
    at = last.index(OUTPUTS_MARKER)
    if at + 1 >= len(last) or not last[at + 1].startswith("_result = "):
        return None
    end = next((j for j in range(at + 2, len(last)) if last[j].startswith(_PUBLISH_STARTS)), None)
    if end is None:
        return None
    value = [last[at + 1][len("_result = ") :], *last[at + 2 : end]]
    closing = "\n".join([*last[:at], "return " + value[0], *value[1:]])
    body = [*cells[marker + 1 : -1], closing]
    candidates: list[tuple[str | None, list[str]]] = [(None, body)]
    docstring = _undocstring(body[0]) if len(body) > 1 and _is_note(body[0]) else None
    if docstring is not None:
        candidates.insert(0, (docstring, body[1:]))
    prelude = cells[0].split("\n") if marker == 1 else []
    named = re.search(r'raise \w+Error\("(?P<name>[A-Za-z_]\w*) (?:returns|must return) a dict', "\n".join(last[end:]))
    return prelude, [match["name"] for match in reads], candidates, named["name"] if named else None


def user_description(settings) -> str:
    """The description the user typed (not the auto-generated one), or ""."""
    from flowfile_core.flowfile.share.transform import _user_description

    return _user_description(settings)


class NativeHandlersMixin(ConverterMixinBase):
    """``fl.*`` native-class handlers; composed into the FlowFrame converter ahead of the shared handlers."""

    def _refuse(self, node_id: int, node_type: str, reason: str) -> None:
        self.unsupported_nodes.append((node_id, node_type, reason))

    def _add_statement(self, text: str) -> None:
        """One statement, one code line per physical line (so fusion indents every line), then a blank."""
        for line in text.split("\n"):
            self._add_code(line)
        self._add_code("")

    def _description_args(self, settings) -> list[str]:
        description = user_description(settings)
        return [f"description={self._py_str(description)}"] if description else []

    def _bind_outputs(self, node_id: int, var: str, accessors: list[str]) -> None:
        """Consumers of output handle ``k`` read ``var`` + ``accessors[k]`` (``.then``, ``["name"]``, ...)."""
        for index, accessor in enumerate(accessors):
            self.node_handle_var_mapping[(node_id, f"output-{index}")] = f"{var}{accessor}"

    def _handle_gate(self, settings: input_schema.NodeGate, var_name: str, input_vars: dict[str, str]) -> None:
        """``fl.Gate(frame, formula | parameter=, operator=, value=, control=, else_output=<stored>)``."""
        gate = settings.gate_input
        data, control = input_vars.get("main"), input_vars.get("right")
        if data is None:
            return self._refuse(settings.node_id, "gate", "the gate has no data input")
        args = [data]
        if gate.condition_source == "formula":
            if not gate.formula.strip():
                return self._refuse(settings.node_id, "gate", "Gate routes on a formula, but no formula is configured")
            if any(f"[${{{name}}}" in gate.formula for name in find_unresolved_in_model(gate.formula)):
                return self._refuse(settings.node_id, "gate", "the formula names a column through a parameter")
            args.append(self._gate_formula_arg(gate.formula))
            if control is not None:
                args.append(f"control={control}")
        else:
            if control is not None:
                reason = "a parameter gate with a control input has no fl.Gate form"
                return self._refuse(settings.node_id, "gate", reason)
            parameters = {p.name: p for p in self.flow_graph.flow_settings.parameters}
            names = {p.name for p in self._codegen_params}
            if self._gate_condition_expr(gate, parameters, names) is None:
                return self._refuse(settings.node_id, "gate", self._gate_refusal_reason(gate, parameters, names))
            param = parameters[gate.parameter]
            declaration = [data, json.dumps(param.name), param.name]
            if param.type != "string":
                declaration.append(f"type={json.dumps(param.type)}")
            if param.enum_values:
                declaration.append(f"enum_values={self._py_str(list(param.enum_values))}")
            if FLOW_PARAMETER_HELPER not in self._module_helpers:
                self._module_helpers.append(FLOW_PARAMETER_HELPER)
            args.append(f"parameter=_flowfile_flow_parameter({', '.join(declaration)})")
            if gate.operator != "equals":
                args.append(f"operator={json.dumps(gate.operator)}")
            args.append(f"value={self._py_str(gate.value)}")
        args.append(f"else_output={settings.else_output}")
        args += self._description_args(settings)
        self._bind_outputs(settings.node_id, var_name, [".then", ".otherwise"] if settings.else_output else [".then"])
        self._add_statement(f"{var_name} = {call('fl.Gate', args)}")

    def _keyed_vars(self, node: FlowNode) -> dict[str, str]:
        """Target handle -> the variable feeding it, for keyed-input nodes."""
        keyed = node.node_inputs.keyed_inputs or {}
        sources = node.node_inputs.keyed_source_handles or {}
        out = {}
        for handle, source in keyed.items():
            if source is None:
                continue
            per_handle = self.node_handle_var_mapping.get((source.node_id, sources.get(handle, "output-0")))
            out[handle] = per_handle or self.node_var_mapping.get(source.node_id, f"df_{source.node_id}")
        return out

    @staticmethod
    def _binding_literal(binding, specs: dict) -> str:
        """A constant binding: a typed literal when it stringifies back to the stored text, else the text."""
        value = binding.constant_value
        spec = specs.get(binding.parameter_name)
        if spec is not None and "${" not in value:
            try:
                typed = coerce_param_value(spec.type, value, spec.enum_values)
                literal = value_literal(typed)
                if literal is not None and stringify_param_value(typed) == value:
                    return literal
            except Exception:
                pass
        return json.dumps(value, ensure_ascii=False)

    def _handle_run_flow(self, settings: input_schema.NodeRunFlow, var_name: str, input_vars: dict[str, str]) -> None:
        """``fl.RunFlow(fl.flow_ref(uuid=...), <slot>=frame, params={...})``; an unresolvable child is refused."""
        from flowfile_core.flowfile.subflow import resolve_subflow_path
        from flowfile_frame.run_flow import _RUN_FLOW_KEYWORDS

        ref = settings.flow_reference
        try:
            resolve_subflow_path(ref, settings.user_id)
        except Exception as exc:
            return self._refuse(settings.node_id, "run_flow", f"the child flow does not resolve: {exc}")
        if ref.flow_uuid:
            target = f"fl.flow_ref(uuid={json.dumps(ref.flow_uuid)})"
        elif ref.registration_id is not None:
            target = f"fl.flow_ref(registration_id={ref.registration_id})"
        else:
            return self._refuse(settings.node_id, "run_flow", "the node names no registered flow")
        keyed = self._keyed_vars(self.flow_graph.get_node(settings.node_id))
        args, slots = [target], []
        for index, slot in enumerate(settings.input_slots):
            source = keyed.get(f"input-{index + 1}")
            if source is None:
                continue
            if slot.isidentifier() and not keyword.iskeyword(slot) and slot not in _RUN_FLOW_KEYWORDS:
                args.append(f"{slot}={source}")
            else:
                slots.append(f"{self._py_str(slot)}: {source}")
        if slots:
            args.append("inputs={" + ", ".join(slots) + "}")
        specs = {spec.name: spec for spec in settings.parameter_specs}
        params = []
        for binding in settings.parameter_bindings:
            name = self._py_str(binding.parameter_name)
            if binding.source == "constant":
                params.append(f"{name}: {self._binding_literal(binding, specs)}")
            elif binding.source == "column":
                params.append(f"{name}: fl.col({self._py_str(binding.column_name)})")
        if params:
            args.append("params={" + ", ".join(params) + "}")
        if keyed.get("input-0") is not None:
            args.append(f"param_frame={keyed['input-0']}")
        if settings.iteration_mode == "iterate":
            args.append("iterate=True")
        if not settings.append_run_metadata:
            args.append("append_metadata=False")
        args += self._description_args(settings)
        outputs = settings.output_slots
        if len(outputs) > 1:
            self._bind_outputs(settings.node_id, var_name, [f"[{self._py_str(n)}]" for n in outputs])
        else:
            self._bind_outputs(settings.node_id, var_name, [".output"])
        self._add_statement(f"{var_name} = {call('fl.RunFlow', args)}")

    def _handle_flow_input(
        self, settings: input_schema.NodeFlowInput, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """``fl.FlowInput(name, schema= | sample=pl.DataFrame(...), flow_graph=flow)``."""
        raw = settings.raw_data_format
        args = [self._py_str(settings.input_name)]
        if raw is not None and raw.columns:
            schema = schema_literal([(column.name, column.data_type) for column in raw.columns])
            if schema is None:
                return self._refuse(settings.node_id, "flow_input", "a sample column has a dtype with no fl.* form")
            if not raw.data or not any(raw.data[0] if raw.data else []):
                args.append(f"schema={schema}")
            else:
                columns = []
                for column, values in zip(raw.columns, raw.data, strict=False):
                    literal = value_literal(list(values))
                    if literal is None:
                        reason = f"sample column {column.name!r} holds values with no literal form"
                        return self._refuse(settings.node_id, "flow_input", reason)
                    columns.append(f"{self._py_str(column.name)}: {literal}")
                self.imports.add("import polars as pl")
                args.append(f"sample=pl.DataFrame({{{', '.join(columns)}}}, schema={schema}, strict=False)")
        args.append(f"flow_graph={FLOW_VAR}")
        args += self._description_args(settings)
        self._add_statement(f"{var_name} = {call('fl.FlowInput', args)}")

    def _handle_flow_output(
        self, settings: input_schema.NodeFlowOutput, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """``frame.to_flow_output(name)``; it returns its input, so downstream reads the input's variable."""
        source = input_vars.get("main")
        if source is None:
            return self._refuse(settings.node_id, "flow_output", "the flow output has no input")
        self.node_var_mapping[settings.node_id] = source
        args = [self._py_str(settings.output_name)] + self._description_args(settings)
        self._add_statement(f"{source}.to_flow_output({', '.join(args)})")

    @staticmethod
    def _script_schemas(node: FlowNode, settings: input_schema.NodePythonScript, outputs: list[str]):
        """``{output: [(column, dtype)]}``: declared ``output_schemas``, else seeded schemas beyond the input's."""
        if settings.output_schemas:
            return {name: [(f.name, f.data_type) for f in fields] for name, fields in settings.output_schemas.items()}
        seeded = getattr(node, "_named_schemas", None) or {}
        inputs = node.all_inputs
        try:
            first = [(c.column_name, c.data_type) for c in (inputs[0].schema if inputs else [])]
        except Exception:
            first = []
        declared = {}
        for index, name in enumerate(outputs):
            columns = [(c.column_name, c.data_type) for c in seeded.get(f"output-{index}") or []]
            if columns and columns != first:
                declared[name] = columns
        return declared or None

    def _handle_python_script(
        self, settings: input_schema.NodePythonScript, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """``@fl.python_script`` when the cells regenerate byte for byte, else ``fl.PythonScript(cells=...)``."""
        script = settings.python_script_input
        if not script.cells:
            reason = "a code-only Python Script has no cells to render; open it in the drawer to give it cells"
            return self._refuse(settings.node_id, "python_script", reason)
        inputs = list(input_vars.values())
        outputs = list(settings.output_names or ["main"])
        schemas = self._script_schemas(self.flow_graph.get_node(settings.node_id), settings, outputs)
        schema_literals = {}
        for name, columns in (schemas or {}).items():
            literal = schema_literal(columns)
            if literal is None:
                reason = f"output {name!r} has a column dtype with no fl.* form"
                return self._refuse(settings.node_id, "python_script", reason)
            schema_literals[name] = literal
        code = self._decorated_script(settings, var_name, inputs, outputs, schema_literals)
        if code is None:
            cells = "".join(f"        ({json.dumps(c.id)}, {str_literal(c.code)}),\n" for c in script.cells)
            args = [*inputs, "cells=[\n" + cells + "    ]"]
            if script.kernel_id:
                args.append(f"kernel={json.dumps(script.kernel_id)}")
            if outputs != ["main"]:
                args.append(f"outputs={json.dumps(outputs)}")
            if schema_literals:
                args.append(f"schemas={_nested_literal(schema_literals)}")
            args += self._description_args(settings)
            code = call("fl.PythonScript", args)
            code = f"{var_name} = {code}.output" if len(outputs) == 1 else f"{var_name} = {code}"
        if len(outputs) > 1:
            self._bind_outputs(settings.node_id, var_name, [f"[{json.dumps(name)}]" for name in outputs])
        self._add_statement(code)

    def _decorated_script(
        self,
        settings: input_schema.NodePythonScript,
        var_name: str,
        inputs: list[str],
        outputs: list[str],
        schema_literals: dict[str, str],
    ) -> str | None:
        """The ``@fl.python_script`` form, when regenerating its cells reproduces the stored ones exactly.

        The prelude's lines are independent statements, so a stored prelude in another order (one saved
        when the frame ordered it by bytecode) still counts as reproduced.

        Pure text on the core side: prelude imports become stub modules (each must pass
        ``importlib.util.find_spec``) and constant assignments become literals, so nothing the script imports is loaded;
        only the ``def`` is compiled, then the frame's own ``_notebook_cells`` regenerates the cells.
        """
        import importlib.util
        import linecache
        import types

        from flowfile_frame.python_script import _notebook_cells

        cells = [cell.code for cell in settings.python_script_input.cells]
        parts = _decorator_parts(cells)
        if parts is None or len(parts[1]) != len(inputs):
            return None
        prelude, parameters, candidates, function = parts
        namespace: dict = {"__builtins__": builtins}
        known = getattr(self, "_script_prelude", None) or dict(_SESSION_PRELUDE)
        bound: dict[str, str] = {}
        for line in prelude:
            try:
                statement = ast.parse(line).body
            except SyntaxError:
                return None
            if len(statement) != 1:
                return None
            stmt = statement[0]
            if isinstance(stmt, ast.Import) and len(stmt.names) == 1:
                alias = stmt.names[0]
                if alias.asname is None and "." in alias.name:
                    return None
                if importlib.util.find_spec(alias.name.partition(".")[0]) is None:
                    return None
                name = alias.asname or alias.name
                namespace[name] = types.ModuleType(alias.name)
            elif isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                name = stmt.targets[0].id
                try:
                    namespace[name] = ast.literal_eval(stmt.value)
                except ValueError:
                    return None
            else:
                return None
            if known.get(name, line) != line:
                return None
            bound[name] = line
        function = function or f"_script_{settings.node_id}"
        if keyword.iskeyword(function):
            return None
        option_sets = [list(outputs)] if outputs != ["main"] else [None, ["main"]]
        for docstring, body in candidates:
            source = _script_function_text(function, parameters, body, docstring)
            for option in option_sets:
                filename = f"<python-script-export-{settings.node_id}>"
                linecache.cache[filename] = (len(source), None, source.splitlines(True), filename)
                try:
                    exec(compile(source, filename, "exec", dont_inherit=True), namespace)  # noqa: S102
                    regenerated = _notebook_cells(namespace[function], option)
                except Exception:
                    continue
                finally:
                    linecache.cache.pop(filename, None)
                if regenerated == cells or (
                    prelude and regenerated[1:] == cells[1:] and sorted(regenerated[0].split("\n")) == sorted(prelude)
                ):
                    self._script_prelude = {**known, **bound}
                    return self._decorated_text(
                        settings, var_name, prelude, source, function, option, inputs, schema_literals
                    )
        return None

    def _decorated_text(self, settings, var_name, prelude, source, function, option, inputs, schema_literals) -> str:
        """Prelude, ``@fl.python_script(...)``, the ``def`` and the call."""
        kwargs = []
        if settings.python_script_input.kernel_id:
            kwargs.append(f"kernel={json.dumps(settings.python_script_input.kernel_id)}")
        if option is not None:
            kwargs.append(f"outputs={json.dumps(option)}")
        if schema_literals:
            if len(schema_literals) == 1 and len(settings.output_names or ["main"]) == 1:
                kwargs.append(f"returns={next(iter(schema_literals.values()))}")
            else:
                kwargs.append(f"returns={_nested_literal(schema_literals)}")
        kwargs.append(f"description={self._py_str(settings.description or '')}")
        single = len(settings.output_names or ["main"]) == 1
        invocation = f"{var_name} = {function}{'' if single else '.node'}({', '.join(inputs)})"
        head = "\n".join(prelude) + "\n\n\n" if prelude else ""
        return f"{head}{call('@fl.python_script', kwargs)}\n{source}\n\n\n{invocation}"

    def _handle_user_defined(self, node: FlowNode, var_name: str, input_vars: dict[str, str]) -> None:
        """``fl.custom_nodes.<key>(frame, <component>=value, ..., kernel=...)``; settings drift is refused."""
        import inspect

        from flowfile_frame.custom_node import CustomNodeFactory
        from flowfile_frame.custom_nodes import CustomNodes

        key, settings = node.node_type, self._settings_for(node)
        try:
            factory = CustomNodeFactory(key)
        except Exception as exc:
            return self._refuse(node.node_id, key, f"custom node {key!r} is not available: {exc}")
        by_location = {location: name for name, location in factory.parameters.items()}
        defaults = {name: p.default for name, p in inspect.signature(factory).parameters.items()}
        kwargs = []
        for section, values in (settings.settings or {}).items():
            for component, value in (values or {}).items():
                name = by_location.get((section, component))
                if name is None:
                    return self._refuse(node.node_id, key, f"settings drift: {section}.{component} is not a setting")
                if value == defaults.get(name):
                    continue
                literal = value_literal(value)
                if literal is None:
                    return self._refuse(node.node_id, key, f"setting {name!r} has no literal form")
                kwargs.append(f"{name}={literal}")
        uses_kernel = bool(getattr(factory.node_class(), "uses_kernel", False))
        if settings.kernel_id and not uses_kernel:
            return self._refuse(node.node_id, key, "a local custom node carries a kernel id")
        if uses_kernel and not settings.kernel_id:
            return self._refuse(node.node_id, key, "a kernel custom node with no kernel selected")
        if settings.kernel_id:
            kwargs.append(f"kernel={json.dumps(settings.kernel_id)}")
        kwargs += self._description_args(settings)
        attribute = key.isidentifier() and not keyword.iskeyword(key) and not key.startswith("_")
        func = f"fl.custom_nodes.{key}" if attribute and not hasattr(CustomNodes, key) else f"fl.custom_nodes[{key!r}]"
        inputs = list(input_vars.values())
        if len(factory.output_names) == 1:
            self._add_statement(f"{var_name} = {call(func, inputs + kwargs)}")
        else:
            self._bind_outputs(node.node_id, var_name, [f"[{json.dumps(name)}]" for name in factory.output_names])
            self._add_statement(f"{var_name} = {call(func + '.node', inputs + kwargs)}")
