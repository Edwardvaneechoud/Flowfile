"""Project export: convert a FlowGraph into a structured multi-file Python project.

Builds on the FlowFrame code generator but, instead of inlining everything in a
single script, emits a runnable project tree:

- ``pipeline.py`` — the FlowFrame pipeline (``run_etl_pipeline()``)
- ``main.py`` — entry point
- ``pyproject.toml`` / ``README.md`` — scaffolding
- ``notebooks/node_XX_<slug>.py`` — one module per python_script node, the
  node's code preserved verbatim inside a ``run()`` function
- ``flowfile_ctx.py`` — local shim so notebook modules run without a kernel
- ``custom_nodes/<module>.py`` — verbatim source per user-defined node
"""

import ast
import importlib.metadata
import inspect
import io
import keyword
import re
import zipfile
from pathlib import Path

from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToFlowFrameConverter
from flowfile_core.flowfile.code_generator.param_codegen import (
    SENTINEL_PREFIX,
    apply_param_sentinels,
    codegen_parameters,
    parameter_default_repr,
    resolve_param_sentinels,
    restore_param_sentinels,
    restore_sentinels_to_refs,
)
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.param_types import FlowParameter, coerce_param_value
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.output_model import ProjectExportFile, ProjectExportManifest
from flowfile_core.utils.utils import camel_case_to_snake_case

# ParamType -> ff dtype expression for run-metadata casts (ff re-exports the
# polars datatypes; values mirror subflow._PARAM_TYPE_TO_PL so generated output
# dtypes match runtime output).
_PARAM_TYPE_TO_FF_EXPR = {
    "string": "ff.String",
    "enum": "ff.String",
    "integer": "ff.Int64",
    "float": "ff.Float64",
    "boolean": "ff.Boolean",
}

# ParamType -> Python annotation for generated function signatures.
_PARAM_TYPE_TO_ANNOTATION = {
    "string": "str",
    "enum": "str",
    "integer": "int",
    "float": "float",
    "boolean": "bool",
}


def _param_arg(parameter: FlowParameter) -> str:
    annotation = _PARAM_TYPE_TO_ANNOTATION.get(parameter.type, "str")
    return f"{parameter.name}: {annotation} = {parameter_default_repr(parameter)}"

# flowfile_ctx APIs that talk to a running Flowfile server; the exported shim
# raises NotImplementedError for these, so flag them in the manifest warnings.
_SERVER_ONLY_CTX_APIS = (
    "publish_global",
    "get_global",
    "list_global_artifacts",
    "delete_global_artifact",
    "read_catalog_table",
    "write_catalog_table",
    "list_catalogs",
    "get_catalog",
    "default_schema",
    "list_schemas",
    "list_catalog_tables",
)

_CUSTOM_NODE_FALLBACK_IMPORTS = (
    "from flowfile_core.flowfile.node_designer import (\n"
    "    ColumnActionInput,\n"
    "    ColumnSelector,\n"
    "    CustomNodeBase,\n"
    "    IncomingColumns,\n"
    "    MultiSelect,\n"
    "    NodeSettings,\n"
    "    NumericInput,\n"
    "    Section,\n"
    "    SingleSelect,\n"
    "    SliderInput,\n"
    "    TextInput,\n"
    "    ToggleSwitch,\n"
    ")\n"
)


def _sanitize_identifier(name: str, fallback: str) -> str:
    """Turn an arbitrary string into a safe snake_case Python identifier."""
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()
    if not cleaned:
        return fallback
    if cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    return cleaned[:60]


def _dependency_pin(distribution: str, package_name: str) -> str:
    """Pin a dependency to the version installed on the exporting server."""
    try:
        return f"{package_name}=={importlib.metadata.version(distribution)}"
    except importlib.metadata.PackageNotFoundError:
        return package_name


def _read_shim_source() -> str:
    """Return the ``project_shim.py`` source to ship into exported projects.

    In a source/pip install ``__file__`` resolves to the ``.py`` on disk. In a
    PyInstaller build the module is stored as bytecode, so the sibling source is
    only present when bundled as data (see ``build_backends`` —
    ``get_code_generator_datas``); fall back to ``inspect.getsource`` so a clear
    value is still produced rather than an unhandled error.
    """
    try:
        return Path(__file__).with_name("project_shim.py").read_text(encoding="utf-8")
    except OSError:
        from flowfile_core.flowfile.code_generator import project_shim

        return inspect.getsource(project_shim)


def _insert_flowfile_ctx_import(source: str) -> str:
    """Insert ``import flowfile_ctx`` into a verbatim custom-node module.

    Placed after a leading module docstring and any ``from __future__`` imports
    (which must stay first) and before the first other statement, so the node's
    ``flowfile_ctx.*`` calls resolve against the shipped shim. No-op if the module
    already imports flowfile_ctx.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return _insert_flowfile_ctx_import_fallback(source)
    for stmt in tree.body:
        if isinstance(stmt, ast.Import) and any(alias.name == "flowfile_ctx" for alias in stmt.names):
            return source
        if isinstance(stmt, ast.ImportFrom) and stmt.module == "flowfile_ctx":
            return source
    insert_line = 1
    for stmt in tree.body:
        is_docstring = (
            isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant) and isinstance(stmt.value.value, str)
        )
        is_future = isinstance(stmt, ast.ImportFrom) and stmt.module == "__future__"
        if is_docstring or is_future:
            insert_line = stmt.end_lineno + 1
            continue
        insert_line = stmt.lineno
        break
    lines = source.split("\n")
    lines.insert(max(0, insert_line - 1), "import flowfile_ctx")
    return "\n".join(lines)


def _insert_flowfile_ctx_import_fallback(source: str) -> str:
    """Line-scan fallback for unparseable source: keep leading ``from __future__``
    lines first, then insert the import."""
    lines = source.split("\n")
    idx = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("from __future__"):
            idx = i + 1
        elif stripped and not stripped.startswith("#"):
            break
    lines.insert(idx, "import flowfile_ctx")
    return "\n".join(lines)


class FlowGraphToProjectConverter(FlowGraphToFlowFrameConverter):
    """Generates a multi-file FlowFrame project from a FlowGraph.

    Extends the single-file FlowFrame converter with python_script (notebook)
    support and per-module emission of custom node sources.
    """

    def __init__(self, flow_graph: FlowGraph):
        super().__init__(flow_graph)
        self.module_files: dict[str, str] = {}
        self.has_notebooks = False
        self.warnings: list[str] = []
        # Maps a stable class identity -> the module stem its source was written
        # to, so the same custom-node class reuses one module and two *different*
        # classes never collide onto the same custom_nodes/<stem>.py.
        self._custom_node_modules: dict[str, str] = {}
        # Subflow support: resolved flow path -> module stem (module reuse), the
        # per-module call metadata (input arg names, codegen params), and the
        # generation-time cycle guard (paths currently being generated).
        self._subflow_modules: dict[str, str] = {}
        self._subflow_module_info: dict[str, dict] = {}
        self._subflow_ancestry: set[str] = set()
        # This flow's parameters that become function kwargs (set in convert()).
        self._codegen_params: list[FlowParameter] = []

    # --- flow parameters as function arguments -------------------------------------------

    def convert(self) -> str:
        """Convert with ``${name}`` parameter refs turned into function-argument references."""
        self._codegen_params = codegen_parameters(self.flow_graph.flow_settings.parameters)
        restorations = apply_param_sentinels(
            [node.setting_input for node in self.flow_graph.nodes], self._codegen_params
        )
        try:
            code = super().convert()
        finally:
            restore_param_sentinels(restorations)
        code, leaked = resolve_param_sentinels(code, {p.name for p in self._codegen_params})
        if leaked:
            self.warnings.append(
                f"Parameter reference(s) {sorted(leaked)} appear in places that cannot reference a "
                "function argument (e.g. multi-line strings) and were left as literal ${...} text."
            )
        return code

    def _function_def_line(self) -> str:
        if not self._codegen_params:
            return super()._function_def_line()
        args = ", ".join(_param_arg(p) for p in self._codegen_params)
        return f"def {self.function_name}(*, {args}):"

    # --- python_script (notebook) nodes -------------------------------------------------

    def _handle_python_script(
        self, settings: input_schema.NodePythonScript, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Emit the notebook node as its own module exposing run(), plus the call site."""
        node = self.flow_graph.get_node(settings.node_id)
        module_stem = self._notebook_module_stem(settings)
        bindings, main_items = self._notebook_input_bindings(node)
        self.module_files[f"notebooks/{module_stem}.py"] = self._build_notebook_module(
            settings, bindings, main_items
        )
        self.has_notebooks = True
        self.imports.add(f"from notebooks import {module_stem}")
        self._collect_notebook_warnings(settings)

        output_names = settings.output_names or ["main"]
        # First line only: a multi-line description would put its tail on an
        # uncommented line of the generated pipeline.
        node_label = (settings.description or f"node {settings.node_id}").splitlines()[0]
        outputs_var = f"_nb_{settings.node_id}_outputs"
        call_args = ", ".join(
            f"{b['param']}={b['args'][0]}" if len(b["args"]) == 1 else f"{b['param']}=[{', '.join(b['args'])}]"
            for b in bindings
        )

        self._add_code(f"# Notebook node {settings.node_id}: {node_label}")
        self._add_code(f"{outputs_var} = {module_stem}.run({call_args})")
        for index, name in enumerate(output_names):
            out_var = var_name if index == 0 else f"{var_name}_{name}"
            self._add_code(f'{out_var} = ff.FlowFrame({outputs_var}["{name}"])')
            self.node_handle_var_mapping[(settings.node_id, f"output-{index}")] = out_var
        self._add_code("")

    def _notebook_module_stem(self, settings: input_schema.NodePythonScript) -> str:
        slug = _sanitize_identifier(settings.description or "", "python_script")
        return f"node_{settings.node_id:02d}_{slug}"

    def _notebook_input_bindings(self, node: FlowNode | None) -> tuple[list[dict], list[str]]:
        """Resolve a notebook node's inputs to run() parameter bindings.

        Input names follow the same rule the kernel uses at runtime
        (FlowGraph._resolve_input_names): the source node's ``node_reference``
        when set, otherwise ``df_<node_id>`` — so verbatim ``read_input(name)``
        calls keep working.

        Returns ``(bindings, main_items)``: one binding per unique input name
        in first-seen edge order, ``{"name": <input name>, "param": <safe
        parameter identifier>, "args": [<pipeline exprs like "df_5.data">]}``,
        plus the per-edge expressions (in edge order) used to synthesize the
        ``"main"`` key inside run().
        """
        bindings: list[dict] = []
        if node is None:
            return bindings, []
        by_name: dict[str, dict] = {}
        used_params: set[str] = set()
        edge_refs: list[tuple[dict, int]] = []
        for source_node in node.all_inputs:
            ref = getattr(source_node.setting_input, "node_reference", None)
            name = ref if ref else f"df_{source_node.node_id}"
            upstream_var = self._resolve_upstream_var(node, source_node.node_id, f"df_{source_node.node_id}")
            entry = by_name.get(name)
            if entry is None:
                param = _sanitize_identifier(name, f"df_{source_node.node_id}")
                if keyword.iskeyword(param) or param in used_params:
                    param = f"{param}_{source_node.node_id}"
                used_params.add(param)
                entry = {"name": name, "param": param, "args": []}
                by_name[name] = entry
                bindings.append(entry)
            edge_refs.append((entry, len(entry["args"])))
            entry["args"].append(f"{upstream_var}.data")
        main_items = [
            entry["param"] if len(entry["args"]) == 1 else f"{entry['param']}[{index}]"
            for entry, index in edge_refs
        ]
        return bindings, main_items

    def _build_notebook_module(
        self, settings: input_schema.NodePythonScript, bindings: list[dict], main_items: list[str]
    ) -> str:
        """Build the notebook module: run() exec's the node's code, preserved byte-for-byte.

        The source is embedded as a string constant instead of being indented
        into run(): indentation would change the *content* of multi-line string
        literals inside the user's code (SQL templates, regexes, ...). exec'ing
        the verbatim source with ``flowfile_ctx`` injected and ``__name__`` set
        to ``"__main__"`` mirrors how the kernel runs the node inside Flowfile.
        """
        script_input = settings.python_script_input
        if script_input.cells:
            cell_blocks = [f"# %%\n{cell.code.rstrip()}" for cell in script_input.cells if cell.code.strip()]
            body = "\n\n".join(cell_blocks)
        else:
            body = script_input.code.rstrip()
        if SENTINEL_PREFIX in body:
            # Notebook source ships verbatim and is exec'd at runtime — keep the
            # literal ${name} form instead of a function-argument reference.
            body = restore_sentinels_to_refs(body)
            self.warnings.append(
                f"Notebook node {settings.node_id}: ${{...}} parameter references inside notebook code "
                "are not resolved in the exported project."
            )

        label = (settings.description or f"Notebook node {settings.node_id}").replace('"""', "'''")
        node_label = settings.description or f"node {settings.node_id}"
        output_names = settings.output_names or ["main"]

        params = ", ".join(
            f"{b['param']}: pl.LazyFrame" if len(b["args"]) == 1 else f"{b['param']}: list[pl.LazyFrame]"
            for b in bindings
        )
        entries = [
            f'"{b["name"]}": [{b["param"]}]' if len(b["args"]) == 1 else f'"{b["name"]}": {b["param"]}'
            for b in bindings
        ]
        if bindings and all(b["name"] != "main" for b in bindings):
            entries.append(f'"main": [{", ".join(main_items)}]')
        inputs_literal = "{" + ", ".join(entries) + "}"

        return (
            f'"""{label}\n\n'
            f"Code preserved verbatim from Flowfile notebook node {settings.node_id} (see _NODE_SOURCE).\n"
            '"""\n'
            "import polars as pl\n"
            "\n"
            "import flowfile_ctx\n"
            "\n"
            f"_NODE_SOURCE = {body!r}\n"
            "\n"
            "\n"
            f"def run({params}) -> dict[str, pl.LazyFrame]:\n"
            f'    """Notebook node {settings.node_id}: {label}."""\n'
            "    with flowfile_ctx.node_context(\n"
            f"        inputs={inputs_literal},\n"
            f"        output_names={output_names!r},\n"
            f"        node_name={node_label!r},\n"
            "    ) as ctx:\n"
            "        exec(  # noqa: S102 - the node's own code, preserved byte-for-byte\n"
            f'            compile(_NODE_SOURCE, "<notebook node {settings.node_id}>", "exec"),\n'
            '            {"flowfile_ctx": flowfile_ctx, "pl": pl, "__name__": "__main__"},\n'
            "        )\n"
            "    return ctx.results()\n"
        )

    def _collect_notebook_warnings(self, settings: input_schema.NodePythonScript) -> None:
        script_input = settings.python_script_input
        code = script_input.code or ""
        if script_input.cells:
            code = "\n".join(cell.code for cell in script_input.cells)
        for api_name in _SERVER_ONLY_CTX_APIS:
            if f"{api_name}(" in code:
                self.warnings.append(
                    f"Notebook node {settings.node_id} calls flowfile_ctx.{api_name}(), which requires a "
                    "running Flowfile server; it will raise NotImplementedError in the exported project."
                )

    # --- run_flow (subflow) nodes ---------------------------------------------------------

    def _handle_run_flow(
        self, settings: input_schema.NodeRunFlow, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Emit the referenced flow as a subflows/ module exposing run(), plus the call site."""
        from flowfile_core.flowfile.subflow import SubflowResolutionError, resolve_subflow_path

        try:
            resolved = resolve_subflow_path(settings.flow_reference, settings.user_id)
        except SubflowResolutionError as exc:
            self.unsupported_nodes.append((settings.node_id, "run_flow", str(exc)))
            self._add_comment(f"# WARNING: run_flow node {settings.node_id}: {exc}")
            return
        path_key = str(resolved.path.resolve())
        if path_key in self._subflow_ancestry:
            reason = f"Circular flow reference: '{resolved.name}' is already being generated in this chain"
            self.unsupported_nodes.append((settings.node_id, "run_flow", reason))
            self._add_comment(f"# WARNING: run_flow node {settings.node_id}: {reason}")
            return

        module_stem = self._subflow_modules.get(path_key)
        if module_stem is None:
            module_stem = self._generate_subflow_module(settings.node_id, path_key, resolved)
            if module_stem is None:
                return
        info = self._subflow_module_info[path_key]
        self.imports.add(f"from subflows import {module_stem}")

        node = self.flow_graph.get_node(settings.node_id)
        keyed = (node.node_inputs.keyed_inputs or {}) if node is not None else {}
        source_handles = (node.node_inputs.keyed_source_handles or {}) if node is not None else {}

        def upstream_var(handle: str) -> str | None:
            source = keyed.get(handle)
            if source is None:
                return None
            src_handle = source_handles.get(handle, "output-0")
            per_handle = self.node_handle_var_mapping.get((source.node_id, src_handle))
            return per_handle or self.node_var_mapping.get(source.node_id, f"df_{source.node_id}")

        prefix = f"_sf_{settings.node_id}"
        call_kwargs: list[str] = []
        for index, slot in enumerate(settings.input_slots):
            source_var = upstream_var(f"input-{index + 1}")
            if source_var is not None:
                call_kwargs.append(f"{info['input_args'].get(slot, slot)}={source_var}")

        specs_by_name = {p.name: p for p in settings.parameter_specs}
        specs_by_name = {**{name: p for name, p in info["params"].items()}, **specs_by_name}
        constant_bindings: list[input_schema.RunFlowParameterBinding] = []
        column_bindings: list[input_schema.RunFlowParameterBinding] = []
        for binding in settings.parameter_bindings:
            if binding.parameter_name not in info["params"]:
                continue
            if binding.source == "constant":
                constant_bindings.append(binding)
                call_kwargs.append(
                    f"{binding.parameter_name}={self._constant_literal(binding, specs_by_name)}"
                )
            elif binding.source == "column":
                column_bindings.append(binding)

        param_frame_var = upstream_var("input-0")
        if column_bindings and param_frame_var is None:
            self.warnings.append(
                f"run_flow node {settings.node_id}: column-mapped parameter(s) have no data connected "
                "to the parameter input; the generated code uses defaults/constants."
            )
            column_bindings = []

        append_metadata = settings.iteration_mode == "iterate" and settings.append_run_metadata
        metadata_bindings = constant_bindings + column_bindings if append_metadata else []
        node_label = resolved.name.splitlines()[0]
        self._add_code(f"# Subflow node {settings.node_id}: {node_label}")

        if settings.iteration_mode == "iterate" and column_bindings:
            self._emit_iterate_call(
                settings, module_stem, prefix, var_name, call_kwargs, column_bindings,
                metadata_bindings, specs_by_name, param_frame_var,
            )
            return

        if column_bindings:  # first_value
            self._add_code(f"{prefix}_params = {param_frame_var}.head(1).collect()")
            for binding in column_bindings:
                call_kwargs.append(
                    f"{binding.parameter_name}={prefix}_params[{binding.column_name!r}][0]"
                )

        outputs_var = f"{prefix}_outputs"
        self._add_code(f"{outputs_var} = {module_stem}.run({', '.join(call_kwargs)})")
        if not settings.output_slots:
            self._add_code(f"{var_name} = None  # run_flow node has no outputs")
            self._add_code("")
            return
        metadata_suffix = ""
        if append_metadata:
            exprs = self._metadata_exprs(
                metadata_bindings, specs_by_name, lambda b: self._constant_literal(b, specs_by_name)
            )
            exprs.append('ff.lit(1).cast(ff.UInt32).alias("run_index")')
            metadata_suffix = f".with_columns([{', '.join(exprs)}])"
        for index, name in enumerate(settings.output_slots):
            out_var = var_name if index == 0 else f"{var_name}_{name}"
            self._add_code(f'{out_var} = {outputs_var}["{name}"]{metadata_suffix}')
            self.node_handle_var_mapping[(settings.node_id, f"output-{index}")] = out_var
        self._add_code("")

    def _emit_iterate_call(
        self,
        settings: input_schema.NodeRunFlow,
        module_stem: str,
        prefix: str,
        var_name: str,
        call_kwargs: list[str],
        column_bindings: list,
        metadata_bindings: list,
        specs_by_name: dict,
        param_frame_var: str,
    ) -> None:
        """One subflow call per parameter row, outputs concatenated (mirrors runtime iterate)."""
        row_var = f"{prefix}_row"
        runs_var = f"{prefix}_runs"
        rows_var = f"{prefix}_param_rows"
        loop_kwargs = list(call_kwargs) + [
            f"{binding.parameter_name}={row_var}[{binding.column_name!r}]" for binding in column_bindings
        ]
        self._add_code(f"{rows_var} = {param_frame_var}.collect().to_dicts()")
        self._add_code(f"{runs_var} = [")
        self._add_code(f"    {module_stem}.run({', '.join(loop_kwargs)})")
        self._add_code(f"    for {row_var} in {rows_var}")
        self._add_code("]")
        if not settings.output_slots:
            self._add_code(f"{var_name} = None  # run_flow node has no outputs")
            self._add_code("")
            return

        out_var_ref = f"{prefix}_out"
        index_var = f"{prefix}_i"
        if metadata_bindings:
            def value_expr(binding) -> str:
                if binding.source == "column":
                    return f"{row_var}[{binding.column_name!r}]"
                return self._constant_literal(binding, specs_by_name)

            exprs = self._metadata_exprs(metadata_bindings, specs_by_name, value_expr)
            exprs.append(f'ff.lit({index_var}).cast(ff.UInt32).alias("run_index")')
            item_template = f'{out_var_ref}["{{name}}"].with_columns([{", ".join(exprs)}])'
            iterator = (
                f"for {index_var}, ({row_var}, {out_var_ref}) in "
                f"enumerate(zip({rows_var}, {runs_var}), start=1)"
            )
        else:
            item_template = f'{out_var_ref}["{{name}}"]'
            iterator = f"for {out_var_ref} in {runs_var}"

        for index, name in enumerate(settings.output_slots):
            out_var = var_name if index == 0 else f"{var_name}_{name}"
            item = item_template.format(name=name)
            self._add_code(f"{out_var} = ff.concat([")
            self._add_code(f"    {item}")
            self._add_code(f"    {iterator}")
            self._add_code('], how="diagonal_relaxed")')
            self.node_handle_var_mapping[(settings.node_id, f"output-{index}")] = out_var
        self._add_code("")

    @staticmethod
    def _metadata_exprs(bindings: list, specs_by_name: dict, value_expr) -> list[str]:
        """`param_<name>` metadata column expressions (typed casts mirror the runtime)."""
        exprs = []
        for binding in bindings:
            spec = specs_by_name.get(binding.parameter_name)
            dtype = _PARAM_TYPE_TO_FF_EXPR.get(spec.type if spec else "string", "ff.String")
            exprs.append(
                f'ff.lit({value_expr(binding)}).cast({dtype}).alias("param_{binding.parameter_name}")'
            )
        return exprs

    def _constant_literal(self, binding, specs_by_name: dict) -> str:
        """A constant binding as a Python literal (typed via the parameter's declared type)."""
        raw = binding.constant_value if binding.constant_value is not None else ""
        if SENTINEL_PREFIX in raw:
            # References a parent-flow parameter: emit as a string literal and let
            # the parent's sentinel post-pass turn it into the argument reference.
            return repr(raw)
        spec = specs_by_name.get(binding.parameter_name)
        try:
            return repr(coerce_param_value(spec.type if spec else "string", raw, spec.enum_values if spec else None))
        except ValueError:
            return repr(raw)

    def _generate_subflow_module(self, node_id: int, path_key: str, resolved) -> str | None:
        """Generate subflows/<stem>.py for *resolved* and register its call metadata."""
        from flowfile_core.flowfile.code_generator.code_generator import UnsupportedNodeError
        from flowfile_core.flowfile.manage.io_flowfile import open_flow

        try:
            child_graph = open_flow(resolved.path)
        except Exception as exc:  # noqa: BLE001 - unreadable/incompatible flow file
            reason = f"Could not load referenced flow '{resolved.name}': {exc}"
            self.unsupported_nodes.append((node_id, "run_flow", reason))
            self._add_comment(f"# WARNING: run_flow node {node_id}: {reason}")
            return None

        child = SubflowModuleConverter(child_graph, parent=self)
        self._subflow_ancestry.add(path_key)
        try:
            module_code = child.convert()
        except UnsupportedNodeError as exc:
            reason = f"Referenced flow '{resolved.name}' cannot be exported: {exc.reason}"
            self.unsupported_nodes.append((node_id, "run_flow", reason))
            self._add_comment(f"# WARNING: run_flow node {node_id}: {reason}")
            return None
        finally:
            self._subflow_ancestry.discard(path_key)

        self.has_notebooks = self.has_notebooks or child.has_notebooks
        self._needs_flowfile_ctx = self._needs_flowfile_ctx or child._needs_flowfile_ctx
        module_stem = self._unique_subflow_module_name(resolved.name)
        self.module_files[f"subflows/{module_stem}.py"] = (
            module_code if module_code.endswith("\n") else module_code + "\n"
        )
        self._subflow_modules[path_key] = module_stem
        self._subflow_module_info[path_key] = {
            "input_args": dict(child._input_args),
            "params": {p.name: p for p in child._codegen_params},
        }
        return module_stem

    def _unique_subflow_module_name(self, flow_name: str) -> str:
        base = _sanitize_identifier(flow_name, "subflow")
        used = set(self._subflow_modules.values())
        candidate = base
        suffix = 2
        while candidate in used:
            candidate = f"{base}_{suffix}"
            suffix += 1
        return candidate

    # --- user-defined (custom) nodes -----------------------------------------------------

    def _register_custom_node_source(self, node: FlowNode, custom_node_class: type) -> bool:
        """Ship the custom node's source file as its own module instead of inlining it."""
        class_name = custom_node_class.__name__
        class_key = f"{custom_node_class.__module__}.{custom_node_class.__qualname__}"
        module_name = self._custom_node_modules.get(class_key)
        if module_name is None:
            source = self._read_custom_node_source_file(custom_node_class)
            if source is None:
                try:
                    # Class-only fallback: prepend the node_designer imports the
                    # full source file would otherwise carry.
                    source = _CUSTOM_NODE_FALLBACK_IMPORTS + "\n\n" + inspect.getsource(custom_node_class)
                except (OSError, TypeError) as e:
                    self.unsupported_nodes.append(
                        (node.node_id, node.node_type, f"Could not retrieve source code for user-defined node: {e}")
                    )
                    self._add_comment(
                        f"# Node {node.node_id}: User-defined node '{node.node_type}' - Source code unavailable"
                    )
                    return False
            module_name = self._unique_custom_module_name(class_name)
            if re.search(r"\bflowfile_ctx\b", source):
                # Node code uses the kernel-injected flowfile_ctx global; bind it to
                # the shipped shim so the exported module runs standalone.
                self._needs_flowfile_ctx = True
                source = _insert_flowfile_ctx_import(source)
            self.module_files[f"custom_nodes/{module_name}.py"] = source if source.endswith("\n") else source + "\n"
            self._custom_node_modules[class_key] = module_name
        self.imports.add(f"from custom_nodes.{module_name} import {class_name}")
        return True

    def _unique_custom_module_name(self, class_name: str) -> str:
        """A custom_nodes module stem unique across distinct classes (suffixes on collision)."""
        base = _sanitize_identifier(camel_case_to_snake_case(class_name), "custom_node")
        used = set(self._custom_node_modules.values())
        candidate = base
        suffix = 2
        while candidate in used:
            candidate = f"{base}_{suffix}"
            suffix += 1
        return candidate

    # --- project assembly ----------------------------------------------------------------

    def convert_to_project(self) -> ProjectExportManifest:
        """Convert the flow and assemble the full project file manifest."""
        # Cycle guard root: a subflow chain that leads back to this file is circular.
        parent_path = self.flow_graph.flow_settings.path
        if parent_path:
            self._subflow_ancestry.add(str(Path(parent_path).resolve()))
        pipeline_code = self.convert()
        project_name = _sanitize_identifier(self.flow_graph.__name__, "flowfile_project")

        files: dict[str, str] = {"pipeline.py": pipeline_code if pipeline_code.endswith("\n") else pipeline_code + "\n"}
        if self.has_notebooks or self._needs_flowfile_ctx:
            files["flowfile_ctx.py"] = _read_shim_source()
        if self.has_notebooks:
            files["notebooks/__init__.py"] = ""
        if any(path.startswith("custom_nodes/") for path in self.module_files):
            files["custom_nodes/__init__.py"] = ""
        if any(path.startswith("subflows/") for path in self.module_files):
            files["subflows/__init__.py"] = ""
        files.update(self.module_files)
        files["main.py"] = self._build_main_py()
        files["pyproject.toml"] = self._build_pyproject_toml(project_name)
        files["README.md"] = self._build_readme(project_name)

        return ProjectExportManifest(
            project_name=project_name,
            files=[ProjectExportFile(path=path, content=content) for path, content in files.items()],
            warnings=self.warnings,
        )

    def _build_main_py(self) -> str:
        return (
            '"""Entry point for this exported Flowfile project."""\n'
            "\n"
            "import sys\n"
            "from pathlib import Path\n"
            "\n"
            "sys.path.insert(0, str(Path(__file__).resolve().parent))\n"
            "\n"
            "from pipeline import run_etl_pipeline  # noqa: E402\n"
            "\n"
            "\n"
            "def main() -> None:\n"
            "    result = run_etl_pipeline()\n"
            "    if result is None:\n"
            '        print("Pipeline finished.")\n'
            "    elif isinstance(result, dict):\n"
            "        for name, frame in result.items():\n"
            '            print(f"=== {name} ===")\n'
            "            print(frame.collect())\n"
            "    else:\n"
            "        print(result.collect())\n"
            "\n"
            "\n"
            'if __name__ == "__main__":\n'
            "    main()\n"
        )

    def _build_pyproject_toml(self, project_name: str) -> str:
        dependencies = [_dependency_pin("Flowfile", "flowfile"), _dependency_pin("polars", "polars")]
        deps = "\n".join(f'    "{dep}",' for dep in dependencies)
        # PEP 508 names must start alphanumeric: the identifier sanitizer prefixes
        # an underscore for digit-leading flow names, which the dash-replace would
        # otherwise turn into an invalid leading "-".
        package_name = re.sub(r"^[^A-Za-z0-9]+", "", project_name.replace("_", "-")) or "flowfile-project"
        # TOML basic strings can't hold raw newlines/quotes/backslashes.
        flow_label = " ".join(self.flow_graph.__name__.split()).replace("\\", "\\\\").replace('"', '\\"')
        return (
            "[project]\n"
            f'name = "{package_name}"\n'
            'version = "0.1.0"\n'
            f"description = \"ETL pipeline exported from the Flowfile flow '{flow_label}'\"\n"
            'requires-python = ">=3.10"\n'
            "dependencies = [\n"
            f"{deps}\n"
            "]\n"
        )

    def _build_readme(self, project_name: str) -> str:
        lines = [
            f"# {project_name}",
            "",
            f"ETL pipeline exported from the Flowfile flow **{self.flow_graph.__name__}**.",
            "",
            "## Project structure",
            "",
            "- `pipeline.py` — the pipeline (`run_etl_pipeline()`), built with the "
            "[flowfile](https://pypi.org/project/Flowfile/) FlowFrame API.",
            "- `main.py` — entry point.",
        ]
        if self.has_notebooks:
            lines += [
                "- `notebooks/` — one module per notebook node, the node's code preserved "
                "verbatim inside a `run()` function.",
                "- `flowfile_ctx.py` — local stand-in for the kernel `flowfile_ctx` API so the "
                "notebook code runs without a Flowfile server (each `run()` executes inside "
                "`flowfile_ctx.node_context(...)`). Artifacts are pickled to "
                "`.artifacts/`; `get_shared_location()` resolves into `.shared/`.",
            ]
        if any(path.startswith("custom_nodes/") for path in self.module_files):
            lines += ["- `custom_nodes/` — user-defined node classes, source preserved verbatim."]
            if self._needs_flowfile_ctx and not self.has_notebooks:
                lines += [
                    "- `flowfile_ctx.py` — local stand-in for the kernel `flowfile_ctx` API so "
                    "custom-node code (e.g. `flowfile_ctx.log_info(...)`) runs without a Flowfile server."
                ]
        if any(path.startswith("subflows/") for path in self.module_files):
            lines += [
                "- `subflows/` — referenced flows exported as callable modules: "
                "`run(<inputs>, *, <parameters>)` returns a dict keyed by the flow's output names. "
                "Iterated run-info columns use fixed `param_<name>`/`run_index` names (the in-app "
                "runtime prefixes them with `_` on collision)."
            ]
        lines += [
            "",
            "## How to run",
            "",
            "```bash",
            "pip install .",
            "python main.py",
            "```",
            "",
            "## Nodes in this flow",
            "",
            "| Node | Type | Description |",
            "|------|------|-------------|",
        ]
        for node in sorted(self.flow_graph.nodes, key=lambda n: n.node_id):
            description = (getattr(node.setting_input, "description", "") or "").replace("\n", " ").replace("|", "\\|")
            lines.append(f"| {node.node_id} | {node.node_type} | {description} |")
        if self.warnings:
            lines += ["", "## Limitations", ""]
            lines += [f"- {warning}" for warning in self.warnings]
        lines.append("")
        return "\n".join(lines)


class SubflowModuleConverter(FlowGraphToProjectConverter):
    """Generates a subflow as an importable module exposing ``run(<inputs>, *, <params>)``.

    flow_input nodes become function arguments (their sample data is the
    fallback when an argument is omitted) and flow_output nodes become entries
    of the returned dict, keyed by output name. Module/warning registries are
    shared with the parent converter so nested notebooks, custom nodes, and
    sub-subflows all land in the same project manifest.
    """

    function_name = "run"

    def __init__(self, flow_graph: FlowGraph, parent: FlowGraphToProjectConverter):
        super().__init__(flow_graph)
        self.module_files = parent.module_files
        self.warnings = parent.warnings
        self._custom_node_modules = parent._custom_node_modules
        self._subflow_modules = parent._subflow_modules
        self._subflow_module_info = parent._subflow_module_info
        self._subflow_ancestry = parent._subflow_ancestry
        self._flow_output_names: dict[int, str] = {}
        self._input_args: dict[str, str] = {}
        used = {p.name for p in codegen_parameters(flow_graph.flow_settings.parameters)}
        used |= {"ff", "pl"}
        for node in sorted(flow_graph.nodes, key=lambda n: n.node_id):
            if node.node_type != "flow_input" or not isinstance(node.setting_input, input_schema.NodeFlowInput):
                continue
            name = node.setting_input.input_name
            if keyword.iskeyword(name) or name in used:
                self._input_args[name] = self._uniquify(f"{name}_in", used)
            else:
                used.add(name)
                self._input_args[name] = name

    def _function_def_line(self) -> str:
        input_args = [f"{arg}: ff.FlowFrame | None = None" for arg in self._input_args.values()]
        param_args = [_param_arg(p) for p in self._codegen_params]
        if param_args:
            signature = ", ".join([*input_args, "*", *param_args])
        else:
            signature = ", ".join(input_args)
        return f"def {self.function_name}({signature}) -> dict[str, ff.FlowFrame]:"

    def _append_module_epilogue(self, lines: list[str]) -> None:
        lines.append("")

    def _handle_flow_input(
        self, settings: input_schema.NodeFlowInput, var_name: str, input_vars: dict[str, str]
    ) -> None:
        arg = self._input_args.get(settings.input_name)
        if arg is None:
            super()._handle_flow_input(settings, var_name, input_vars)
            return
        self._add_code(f"# flow_input '{settings.input_name}' — function argument (sample data when omitted)")
        self._add_code(f"{var_name} = {arg} if {arg} is not None else {self._sample_expression(settings)}")
        self._add_code("")

    def _sample_expression(self, settings: input_schema.NodeFlowInput) -> str:
        if settings.raw_data_format is not None and settings.raw_data_format.columns:
            # Public API only: from_raw_data coerces the dict into RawData via pydantic.
            return f"ff.from_raw_data({settings.raw_data_format.model_dump()})"
        return "ff.LazyFrame()"

    def _handle_flow_output(
        self, settings: input_schema.NodeFlowOutput, var_name: str, input_vars: dict[str, str]
    ) -> None:
        super()._handle_flow_output(settings, var_name, input_vars)
        self._flow_output_names[settings.node_id] = settings.output_name
        # output_nodes vars are kept in sync by the chain-fusion rename pass.
        self.output_nodes.append((settings.node_id, var_name))

    def add_return_code(self, lines: list[str]) -> None:
        entries = [
            (self._flow_output_names[node_id], var)
            for node_id, var in self.output_nodes
            if node_id in self._flow_output_names
        ]
        if not entries:
            lines.append("    return {}")
            return
        lines.append("    return {")
        for name, var in entries:
            lines.append(f'        "{name}": {var},')
        lines.append("    }")


def export_flow_to_project(flow_graph: FlowGraph) -> ProjectExportManifest:
    """Export *flow_graph* as a multi-file Python project manifest."""
    converter = FlowGraphToProjectConverter(flow_graph)
    return converter.convert_to_project()


def project_to_zip_bytes(manifest: ProjectExportManifest) -> bytes:
    """Pack a project manifest into a zip archive (files nested under the project name)."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for file in manifest.files:
            archive.writestr(f"{manifest.project_name}/{file.path}", file.content)
    return buffer.getvalue()
