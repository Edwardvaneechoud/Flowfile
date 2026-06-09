"""Project export: convert a FlowGraph into a structured multi-file Python project.

Builds on the FlowFrame code generator but, instead of inlining everything in a
single script, emits a runnable project tree:

- ``pipeline.py`` — the FlowFrame pipeline (``run_etl_pipeline()``)
- ``main.py`` — entry point
- ``pyproject.toml`` / ``README.md`` — scaffolding
- ``notebooks/node_XX_<slug>.py`` — one verbatim module per python_script node
- ``flowfile_ctx.py`` — local shim so notebook modules run without a kernel
- ``custom_nodes/<module>.py`` — verbatim source per user-defined node
"""

import importlib.metadata
import inspect
import io
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
    "    ColumnSelector,\n"
    "    CustomNodeBase,\n"
    "    DropdownSelector,\n"
    "    IncomingColumns,\n"
    "    MultiSelect,\n"
    "    NodeSettings,\n"
    "    NumericInput,\n"
    "    Section,\n"
    "    SingleSelect,\n"
    "    TextArea,\n"
    "    TextInput,\n"
    "    Toggle,\n"
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
    return Path(__file__).with_name("project_shim.py").read_text(encoding="utf-8")


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

    # --- python_script (notebook) nodes -------------------------------------------------

    def _handle_python_script(
        self, settings: input_schema.NodePythonScript, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Emit the notebook node as its own module plus a run_node() call site."""
        node = self.flow_graph.get_node(settings.node_id)
        module_stem = self._notebook_module_stem(settings)
        self.module_files[f"notebooks/{module_stem}.py"] = self._build_notebook_module(settings)
        self.has_notebooks = True
        self.imports.add("import flowfile_ctx")
        self._collect_notebook_warnings(settings)

        output_names = settings.output_names or ["main"]
        node_label = settings.description or f"node {settings.node_id}"
        outputs_var = f"_nb_{settings.node_id}_outputs"

        self._add_code(f"# Notebook node {settings.node_id}: {node_label}")
        self._add_code(f"{outputs_var} = flowfile_ctx.run_node(")
        self._add_code(f'    "notebooks.{module_stem}",')
        self._add_code(f"    inputs={self._build_notebook_inputs_literal(node)},")
        self._add_code(f"    output_names={output_names!r},")
        self._add_code(f"    node_name={node_label!r},")
        self._add_code(")")
        for index, name in enumerate(output_names):
            out_var = var_name if index == 0 else f"{var_name}_{name}"
            self._add_code(f'{out_var} = ff.FlowFrame({outputs_var}["{name}"])')
            self.node_handle_var_mapping[(settings.node_id, f"output-{index}")] = out_var
        self._add_code("")

    def _notebook_module_stem(self, settings: input_schema.NodePythonScript) -> str:
        slug = _sanitize_identifier(settings.description or "", "python_script")
        return f"node_{settings.node_id:02d}_{slug}"

    def _build_notebook_module(self, settings: input_schema.NodePythonScript) -> str:
        """Build the notebook module: a small generated header plus the user code verbatim."""
        script_input = settings.python_script_input
        if script_input.cells:
            cell_blocks = [f"# %%\n{cell.code.rstrip()}" for cell in script_input.cells if cell.code.strip()]
            body = "\n\n".join(cell_blocks)
        else:
            body = script_input.code.rstrip()

        label = (settings.description or f"Notebook node {settings.node_id}").replace('"""', "'''")
        header = (
            f'"""{label}\n\n'
            f"Code preserved verbatim from Flowfile notebook node {settings.node_id}.\n"
            '"""\n'
            "import flowfile_ctx  # noqa: F401\n"
        )
        return f"{header}\n{body}\n" if body else header

    def _build_notebook_inputs_literal(self, node: FlowNode | None) -> str:
        """Build the inputs dict literal passed to flowfile_ctx.run_node().

        Input names follow the same rule the kernel uses at runtime
        (FlowGraph._resolve_input_names): the source node's ``node_reference``
        when set, otherwise ``df_<node_id>`` — so verbatim ``read_input(name)``
        calls keep working. A ``"main"`` key always lists all inputs in order.
        """
        if node is None:
            return "{}"
        named: dict[str, list[str]] = {}
        ordered: list[str] = []
        for source_node in node.all_inputs:
            ref = getattr(source_node.setting_input, "node_reference", None)
            name = ref if ref else f"df_{source_node.node_id}"
            upstream_var = self._resolve_upstream_var(node, source_node.node_id, f"df_{source_node.node_id}")
            named.setdefault(name, []).append(f"{upstream_var}.data")
            ordered.append(f"{upstream_var}.data")
        if "main" not in named and ordered:
            named["main"] = ordered
        if not named:
            return "{}"
        entries = ", ".join(f'"{name}": [{", ".join(frames)}]' for name, frames in named.items())
        return "{" + entries + "}"

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
        module_name = _sanitize_identifier(camel_case_to_snake_case(class_name), "custom_node")
        rel_path = f"custom_nodes/{module_name}.py"
        if rel_path not in self.module_files:
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
            self.module_files[rel_path] = source if source.endswith("\n") else source + "\n"
        self.imports.add(f"from custom_nodes.{module_name} import {class_name}")
        return True

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
        return (
            "[project]\n"
            f'name = "{project_name.replace("_", "-")}"\n'
            'version = "0.1.0"\n'
            f'description = "ETL pipeline exported from the Flowfile flow {self.flow_graph.__name__!r}"\n'
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
                "- `notebooks/` — one module per notebook node, code preserved verbatim.",
                "- `flowfile_ctx.py` — local stand-in for the kernel `flowfile_ctx` API so the "
                "notebook modules run without a Flowfile server. Artifacts are pickled to "
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
