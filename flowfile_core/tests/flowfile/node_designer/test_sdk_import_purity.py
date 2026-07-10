"""
Gate tests for the shared/node_designer SDK extraction:

- importing the SDK must not pull in flowfile_core (no DB creation, no Alembic)
- install_import_aliases() must make every legacy import spelling resolve to the
  SDK in a process without flowfile_core, and fail loudly for non-SDK core imports
- inside core (flowfile_core imported for real), the aliases must be a no-op
"""
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]


def run_python(code: str, tmp_path: Path) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["FLOWFILE_STORAGE_DIR"] = str(tmp_path / "storage")
    env["FLOWFILE_USER_DATA_DIR"] = str(tmp_path / "user_data")
    env["FLOWFILE_DB_PATH"] = str(tmp_path / "purity_test.db")
    env["FLOWFILE_SKIP_STARTUP_MIGRATION"] = "1"
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(REPO_ROOT),
        timeout=120,
    )


def test_sdk_import_is_pure(tmp_path):
    result = run_python(
        """
        import json, sys

        import shared.node_designer  # noqa: F401
        from shared.node_designer import CustomNodeBase, NodeSettings, Section, TextInput, Types  # noqa: F401

        loaded = sorted(m for m in sys.modules if m.split(".")[0] in {"flowfile_core", "flowfile", "alembic", "sqlalchemy", "fastapi"})
        print(json.dumps(loaded))
        """,
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    loaded = json.loads(result.stdout.strip().splitlines()[-1])
    assert loaded == [], f"SDK import pulled in forbidden modules: {loaded}"
    assert not (tmp_path / "purity_test.db").exists(), "SDK import created a database file"


LEGACY_NODE_TEMPLATES = {
    "core_package_import": """
        import polars as pl
        from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings, Section, TextInput

        section = Section(title="Main", prefix=TextInput(label="Prefix", default="x"))

        class S(NodeSettings):
            main: Section = section

        class LegacyCoreImportNode(CustomNodeBase):
            node_name: str = "Legacy Core Import"
            settings_schema: S = S()

            def process(self, *inputs):
                return inputs[0]
        """,
    "core_module_import": """
        import polars as pl
        from flowfile_core.flowfile.node_designer.custom_node import CustomNodeBase, NodeSettings
        from flowfile_core.flowfile.node_designer.ui_components import Section, TextInput
        from flowfile_core.types import Types

        class LegacyModuleImportNode(CustomNodeBase):
            node_name: str = "Legacy Module Import"

            def process(self, *inputs):
                return inputs[0]
        """,
    "flowfile_nd_import": """
        import polars as pl
        from flowfile import node_designer as nd

        class NdStyleNode(nd.CustomNodeBase):
            node_name: str = "Nd Style"
            settings_schema: nd.NodeSettings = nd.NodeSettings(
                main=nd.Section(title="Main", col=nd.ColumnSelector(label="Col", data_types=nd.Types.String))
            )

            def process(self, *inputs):
                return inputs[0]
        """,
}


def test_import_aliases_load_legacy_spellings_without_core(tmp_path):
    for name, node_code in LEGACY_NODE_TEMPLATES.items():
        node_file = tmp_path / f"{name}.py"
        node_file.write_text(textwrap.dedent(node_code))
        result = run_python(
            f"""
            import json, sys
            from shared.node_designer.loading import find_custom_node_class, install_import_aliases, load_node_module

            install_import_aliases()
            module = load_node_module(path={str(node_file)!r})
            node_cls = find_custom_node_class(module)
            node = node_cls()
            real_core = getattr(sys.modules.get("flowfile_core"), "__file__", None)
            print(json.dumps({{"node_name": node.node_name, "real_core_file": real_core}}))
            """,
            tmp_path,
        )
        assert result.returncode == 0, f"{name}: {result.stderr}"
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        assert payload["node_name"], name
        assert payload["real_core_file"] is None, f"{name} imported the real flowfile_core"


def test_import_aliases_reject_non_sdk_core_imports(tmp_path):
    node_file = tmp_path / "forbidden.py"
    node_file.write_text(
        textwrap.dedent(
            """
            from flowfile_core.flowfile.flow_graph import FlowGraph

            class Nope:
                pass
            """
        )
    )
    result = run_python(
        f"""
        from shared.node_designer.loading import install_import_aliases, load_node_module

        install_import_aliases()
        try:
            load_node_module(path={str(node_file)!r})
        except ImportError as e:
            print("IMPORT_ERROR:", e)
        else:
            raise SystemExit("expected ImportError")
        """,
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "IMPORT_ERROR:" in result.stdout
    assert "node designer SDK" in result.stdout


def test_install_import_aliases_noop_inside_core():
    import flowfile_core  # noqa: F401 - ensure real core is imported

    from shared.node_designer.loading import install_import_aliases

    before = sys.modules["flowfile_core"]
    install_import_aliases()
    assert sys.modules["flowfile_core"] is before
    assert getattr(sys.modules["flowfile_core"], "__file__", None)


def test_shims_expose_same_objects():
    from flowfile_core.flowfile.node_designer import CustomNodeBase as shim_cnb
    from flowfile_core.flowfile.node_designer.custom_node import CustomNodeBase as shim_module_cnb
    from flowfile_core.flowfile.node_designer.ui_components import Section as shim_section
    from flowfile_core.types import DataType as shim_dt
    from shared.node_designer import CustomNodeBase, DataType, Section

    assert shim_cnb is CustomNodeBase
    assert shim_module_cnb is CustomNodeBase
    assert shim_section is Section
    assert shim_dt is DataType
