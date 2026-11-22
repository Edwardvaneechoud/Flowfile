"""``flowfile-merge check --build`` runs core's build check through the command a pipeline runs.

``shared`` never imports core: the command finds the check as the ``build_check`` entry point of the
``flowfile.flow_merge`` group that the root ``pyproject.toml`` registers. An environment installed before that
registration does not list it yet, so the command runs with a distribution on its path that declares exactly the
entry point ``pyproject.toml`` declares.
"""

import os
import subprocess
import sys
import tomllib
from pathlib import Path

import yaml

from flowfile_core.flowfile.manage.ephemeral_graph import free_flow_id
from shared.flow_merge import cli
from shared.flow_merge.canonical import canonical, emit
from shared.flow_merge.edges import RIGHT
from tests.flow_merge.test_document_check import _base

REPO_ROOT = Path(__file__).resolve().parents[3]


def _declared() -> tuple[dict, str]:
    """The root ``pyproject.toml``'s console scripts and the build check it registers for ``check --build``."""
    poetry = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["tool"]["poetry"]
    return poetry["scripts"], poetry["plugins"][cli.BUILD_CHECK_GROUP][cli.BUILD_CHECK_NAME]


def _run(tmp_path: Path, *files: Path) -> subprocess.CompletedProcess:
    _, target = _declared()
    site = tmp_path / "site"
    dist = site / "flowfile_merge_build_check_test-0.dist-info"
    dist.mkdir(parents=True, exist_ok=True)
    (dist / "METADATA").write_text("Metadata-Version: 2.1\nName: flowfile-merge-build-check-test\nVersion: 0\n")
    (dist / "entry_points.txt").write_text(f"[{cli.BUILD_CHECK_GROUP}]\n{cli.BUILD_CHECK_NAME} = {target}\n")
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join([str(site), str(REPO_ROOT), os.environ.get("PYTHONPATH", "")]),
        "FLOWFILE_STORAGE_DIR": str(tmp_path / "storage"),
        "FLOWFILE_USER_DATA_DIR": str(tmp_path / "user_data"),
        "FLOWFILE_DB_PATH": str(tmp_path / "cli.db"),
    }
    return subprocess.run(
        [sys.executable, "-m", "shared.flow_merge", "check", "--build", *map(str, files)],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(tmp_path),
        timeout=600,
    )


def test_the_command_and_the_module_run_the_same_main():
    scripts, target = _declared()
    assert scripts["flowfile-merge"] == "shared.flow_merge.cli:main"
    assert target == "flowfile_core.flowfile.manage.document_check:build_check"


def test_check_build_runs_the_core_build_check(tmp_path):
    good = _base(tmp_path, free_flow_id())
    flow, _ = canonical(good)
    del flow.inputs[3][RIGHT]
    good_path, bad_path = tmp_path / "good.yaml", tmp_path / "bad.yaml"
    good_path.write_text(yaml.safe_dump(good, sort_keys=False), encoding="utf-8")
    bad_path.write_text(yaml.safe_dump(emit(flow), sort_keys=False), encoding="utf-8")

    result = _run(tmp_path, good_path, bad_path)
    assert result.returncode == 1, result.stderr
    lines = result.stderr.splitlines()
    assert f"{bad_path}: Step 3 join does not have the inputs a join needs." in lines
    assert not [line for line in lines if line.startswith(f"{good_path}:")], result.stderr

    clean = _run(tmp_path, good_path)
    assert clean.returncode == 0, clean.stderr
