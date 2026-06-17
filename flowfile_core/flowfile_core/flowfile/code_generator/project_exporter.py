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

import importlib.metadata
import inspect
import io
import keyword
import re
import zipfile
from pathlib import Path

from flowfile_core.flowfile.code_generator.code_generator import FlowGraphToFlowFrameConverter
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.output_model import ProjectExportFile, ProjectExportManifest
from flowfile_core.utils.utils import camel_case_to_snake_case

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
        pipeline_code = self.convert()
        project_name = _sanitize_identifier(self.flow_graph.__name__, "flowfile_project")

        files: dict[str, str] = {"pipeline.py": pipeline_code if pipeline_code.endswith("\n") else pipeline_code + "\n"}
        if self.has_notebooks:
            files["flowfile_ctx.py"] = _read_shim_source()
            files["notebooks/__init__.py"] = ""
        if any(path.startswith("custom_nodes/") for path in self.module_files):
            files["custom_nodes/__init__.py"] = ""
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
