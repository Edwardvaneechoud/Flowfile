"""Tests for custom-node execution in the worker (child target + endpoint)."""

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import polars as pl
import pytest
from fastapi.testclient import TestClient

from flowfile_worker import main, models, mp_context
from flowfile_worker.custom_node_runner import execute_custom_node_task
from flowfile_worker.secrets import encrypt_secret

client = TestClient(main.app)

pytestmark = pytest.mark.worker


NODE_SOURCE = '''
import polars as pl
from shared.node_designer import CustomNodeBase, NodeSettings, Section, TextInput


class UpperSettings(NodeSettings):
    main: Section = Section(title="Main", prefix=TextInput(label="Prefix", default="pre"))


class UpperNode(CustomNodeBase):
    node_name: str = "Upper Node"
    settings_schema: UpperSettings = UpperSettings()

    def process(self, *inputs):
        prefix = self.settings_schema.main.prefix.value
        return inputs[0].with_columns(pl.col("name").str.to_uppercase().alias(f"{prefix}_name"))
'''

LEGACY_NODE_SOURCE = '''
import polars as pl
from flowfile_core.flowfile.node_designer import CustomNodeBase


class LegacyNode(CustomNodeBase):
    node_name: str = "Legacy Node"

    def process(self, *inputs):
        return inputs[0]
'''

MULTI_OUTPUT_SOURCE = '''
import polars as pl
from shared.node_designer import CustomNodeBase


class SplitNode(CustomNodeBase):
    node_name: str = "Split Node"
    number_of_outputs: int = 2
    output_names: list[str] = ["high", "low"]

    def process(self, *inputs):
        lf = inputs[0]
        return {"high": lf.filter(pl.col("value") > 10), "low": lf.filter(pl.col("value") <= 10)}
'''

SECRET_NODE_SOURCE = '''
import polars as pl
from shared.node_designer import AvailableSecrets, CustomNodeBase, NodeSettings, SecretSelector, Section


class SecretSettings(NodeSettings):
    main: Section = Section(title="Main", api_key=SecretSelector(label="Key", options=AvailableSecrets))


class SecretNode(CustomNodeBase):
    node_name: str = "Secret Node"
    settings_schema: SecretSettings = SecretSettings()

    def process(self, *inputs):
        key = self.settings_schema.main.api_key.secret_value.get_secret_value()
        return inputs[0].with_columns(pl.lit(key).alias("key"))
'''


@pytest.fixture(autouse=True)
def clean_import_aliases():
    """Direct child-target calls install aliases in this process; undo after each test."""
    yield
    from shared.node_designer.loading import _ForbiddenCoreImportFinder

    sys.meta_path[:] = [f for f in sys.meta_path if not isinstance(f, _ForbiddenCoreImportFinder)]
    for name in list(sys.modules):
        root = name.split(".", 1)[0]
        if root in {"flowfile_core", "flowfile"} and not getattr(sys.modules[name], "__file__", None):
            del sys.modules[name]


def serialize_input(df: pl.DataFrame) -> bytes:
    return df.lazy().serialize()


def run_task(tmp_path, node_source, *, inputs, settings_values=None, secrets=None, output_names=None,
             class_name=None, dry_run=False, row_limit=None):
    progress = mp_context.Value("i", 0)
    error_message = mp_context.Array("c", 1024)
    q = mp_context.Queue(maxsize=1)
    file_path = str(tmp_path / "result.arrow")

    execute_custom_node_task(
        node_source=node_source,
        class_name=class_name,
        settings_values=settings_values or {},
        secrets=secrets or {},
        inputs=inputs,
        output_names=output_names or ["main"],
        dry_run=dry_run,
        row_limit=row_limit,
        user_id=1,
        progress=progress,
        error_message=error_message,
        queue=q,
        file_path=file_path,
        flowfile_flow_id=-1,
        flowfile_node_id=1,
    )
    payload = None
    if progress.value == 100:
        payload = json.loads(q.get(timeout=5))
    return progress.value, error_message.value.decode().rstrip("\x00"), payload, file_path


def test_single_input_with_settings(tmp_path):
    df = pl.DataFrame({"name": ["alice", "bob"]})
    progress, error, payload, file_path = run_task(
        tmp_path, NODE_SOURCE, inputs=[serialize_input(df)],
        settings_values={"main": {"prefix": "up"}},
    )
    assert progress == 100, error
    assert payload["outputs"]["main"]["row_count"] == 2
    result = pl.read_ipc(file_path)
    assert result["up_name"].to_list() == ["ALICE", "BOB"]


def run_in_subprocess(code: str) -> subprocess.CompletedProcess:
    """Run in a fresh interpreter — the real worker-child condition (no flowfile_core imported).

    Needed because other worker tests (test_app.py) import the real flowfile_core into the
    shared pytest process, which correctly turns install_import_aliases into a no-op.
    """
    env = os.environ.copy()
    env.setdefault("TEST_MODE", "1")
    repo_root = Path(__file__).resolve().parents[2]
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(repo_root),
        timeout=120,
    )


def test_legacy_import_spelling_loads_without_core(tmp_path):
    node_file = tmp_path / "legacy_node.py"
    node_file.write_text(LEGACY_NODE_SOURCE)
    result = run_in_subprocess(
        f"""
        import sys
        from shared.node_designer.loading import find_custom_node_class, install_import_aliases, load_node_module

        install_import_aliases()
        module = load_node_module(path={str(node_file)!r})
        node = find_custom_node_class(module)()
        assert node.node_name == "Legacy Node"
        assert not getattr(sys.modules.get("flowfile_core"), "__file__", None)
        print("OK")
        """
    )
    assert result.returncode == 0, result.stderr
    assert "OK" in result.stdout


def test_multi_output(tmp_path):
    df = pl.DataFrame({"value": [5, 15, 25]})
    progress, error, payload, file_path = run_task(
        tmp_path, MULTI_OUTPUT_SOURCE, inputs=[serialize_input(df)], output_names=["high", "low"],
    )
    assert progress == 100, error
    assert set(payload["outputs"]) == {"high", "low"}
    assert payload["outputs"]["high"]["row_count"] == 2
    assert payload["outputs"]["low"]["row_count"] == 1
    assert pl.read_ipc(payload["outputs"]["low"]["path"])["value"].to_list() == [5]


def test_multi_output_missing_declared_name_errors(tmp_path):
    df = pl.DataFrame({"value": [5]})
    progress, error, _, _ = run_task(
        tmp_path, MULTI_OUTPUT_SOURCE, inputs=[serialize_input(df)], output_names=["high", "low", "mid"],
    )
    assert progress == -1
    assert "mid" in error


def test_secret_decryption_via_ffsec(tmp_path):
    df = pl.DataFrame({"a": [1]})
    encrypted = encrypt_secret("s3cret-value", user_id=1)
    progress, error, payload, file_path = run_task(
        tmp_path, SECRET_NODE_SOURCE, inputs=[serialize_input(df)],
        settings_values={"main": {"api_key": "MY_KEY"}},
        secrets={"MY_KEY": encrypted},
    )
    assert progress == 100, error
    assert payload["accessed_secret_count"] == 1
    assert pl.read_ipc(file_path)["key"].to_list() == ["s3cret-value"]


def test_missing_secret_fails_cleanly(tmp_path):
    df = pl.DataFrame({"a": [1]})
    progress, error, _, _ = run_task(
        tmp_path, SECRET_NODE_SOURCE, inputs=[serialize_input(df)],
        settings_values={"main": {"api_key": "MY_KEY"}},
    )
    assert progress == -1
    assert "MY_KEY" in error


def test_bad_class_name_errors(tmp_path):
    df = pl.DataFrame({"a": [1]})
    progress, error, _, _ = run_task(
        tmp_path, NODE_SOURCE, inputs=[serialize_input(df)], class_name="NopeNode",
    )
    assert progress == -1
    assert "NopeNode" in error


def test_forbidden_core_import_errors(tmp_path):
    result = run_in_subprocess(
        """
        from shared.node_designer.loading import install_import_aliases, load_node_module

        install_import_aliases()
        try:
            load_node_module(source="from flowfile_core.flowfile.flow_graph import FlowGraph\\n")
        except ImportError as e:
            assert "node designer SDK" in str(e), e
            print("OK")
        else:
            raise SystemExit("expected ImportError")
        """
    )
    assert result.returncode == 0, result.stderr
    assert "OK" in result.stdout


def test_dry_run_returns_preview_and_logs(tmp_path):
    df = pl.DataFrame({"name": ["alice", "bob", "carol"]})
    progress, error, payload, _ = run_task(
        tmp_path, NODE_SOURCE, inputs=[serialize_input(df)],
        settings_values={"main": {"prefix": "p"}}, dry_run=True, row_limit=2,
    )
    assert progress == 100, error
    preview = payload["preview"]["main"]
    assert preview["truncated"] is True
    assert len(preview["rows"]) == 2
    assert {c["name"] for c in preview["columns"]} == {"name", "p_name"}
    assert payload["duration_ms"] > 0
    assert any("Executing custom node" in line for line in payload["logs"])


def test_dry_run_error_carries_traceback(tmp_path):
    source = '''
from shared.node_designer import CustomNodeBase

class BoomNode(CustomNodeBase):
    node_name: str = "Boom"

    def process(self, *inputs):
        raise RuntimeError("boom goes the node")
'''
    df = pl.DataFrame({"a": [1]})
    progress, error, _, _ = run_task(tmp_path, source, inputs=[serialize_input(df)], dry_run=True)
    assert progress == -1
    assert "boom goes the node" in error
    assert "Traceback" in error


def test_execute_custom_node_endpoint(tmp_path):
    df = pl.DataFrame({"name": ["dana"]})
    body = models.CustomNodeExecuteInput(
        node_source=NODE_SOURCE,
        settings_values={"main": {"prefix": "e2e"}},
        inputs=[serialize_input(df)],
        cache_dir=str(tmp_path),
        flowfile_flow_id=-1,
        flowfile_node_id=99,
        user_id=1,
    )
    response = client.post("/execute_custom_node", json=json.loads(body.model_dump_json()))
    assert response.status_code == 200
    task_id = response.json()["background_task_id"]

    status_response = client.get(f"/status/{task_id}")
    assert status_response.status_code == 200
    status = status_response.json()
    assert status["status"] == "Completed"
    payload = json.loads(status["results"])
    assert payload["outputs"]["main"]["row_count"] == 1
    assert pl.read_ipc(payload["outputs"]["main"]["path"])["e2e_name"].to_list() == ["DANA"]
    client.delete(f"/clear_task/{task_id}")
