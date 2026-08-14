"""Lazy-import contract for two distinct import surfaces.

1. The spawned child (`flowfile_worker.funcs`): every task spawns a fresh 'spawn'-context
   child, so heavy cloud/db deps must never load at import time.
2. The bare packages (`shared`, `flowfile_worker`) that every process boot pays for.

These are NOT the same surface. The child still loads pydantic, pl_fuzzy_frame_match,
shared.sql_utils and shared.delta_utils -- funcs.py pulls all four through its own
module-top imports (and unconditionally via flow_logger -> models). Deferring those is
Phase 2b; nothing here claims the child gets lighter.
"""

import ast
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

# requests is loaded on demand by flow_logger.emit; if a future transitive dep
# legitimately pulls one of these at import time, reassess before relaxing.
FORBIDDEN = ("gcsfs", "boto3", "botocore", "connectorx", "deltalake", "requests")


def _probe_env(tmp_path):
    env = os.environ.copy()
    env["TEST_MODE"] = "1"
    env["FLOWFILE_SKIP_STARTUP_MIGRATION"] = "1"
    env["FLOWFILE_STORAGE_DIR"] = str(tmp_path / "storage")
    env["FLOWFILE_USER_DATA_DIR"] = str(tmp_path / "user_data")
    env["PYTHONPATH"] = os.pathsep.join([str(REPO_ROOT / "flowfile_worker"), str(REPO_ROOT)])
    return env


def _modules_after(stmt: str, tmp_path) -> set[str]:
    """sys.modules of a fresh spawn-shaped child that ran *stmt*."""
    # stmt is spliced at column 0, so build the source by joining rather than dedenting.
    code = "\n".join(
        [
            "import json, multiprocessing, sys",
            'multiprocessing.current_process().name = "Worker-1"',
            stmt,
            "print(json.dumps(sorted(sys.modules)))",
        ]
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=_probe_env(tmp_path),
        # cwd must not be a repo root: '' leads sys.path for -c and would shadow `shared`.
        cwd=str(tmp_path),
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    return set(json.loads(result.stdout.strip().splitlines()[-1]))


# shared/__init__ re-exports the sql/delta/cloud helpers lazily (PEP 562), so importing
# `shared` never pulls the pydantic-backed dialect registry. This is a boot-path win for
# core, the scheduler and the CLI -- it does not reach the spawned child (see docstring).
SHARED_MUST_STAY_LAZY = (
    "shared.sql_utils",
    "shared.db_dialects",
    "shared.delta_utils",
    "pydantic",
)


@pytest.mark.worker
def test_importing_shared_stays_lazy(tmp_path):
    loaded = _modules_after("import shared", tmp_path)
    leaked = sorted(set(SHARED_MUST_STAY_LAZY) & loaded)
    assert leaked == [], f"`import shared` eagerly loaded: {leaked}"


@pytest.mark.worker
def test_shared_storage_config_has_no_module_level_sql_or_delta_import():
    # Static, so it can isolate storage_config as the culprit: an import-time probe cannot,
    # because importing any shared.* submodule runs shared/__init__ first.
    tree = ast.parse((REPO_ROOT / "shared" / "storage_config.py").read_text(encoding="utf-8"))
    banned = {"sql_utils", "delta_utils", "db_dialects"}
    offenders = []
    for node in tree.body:  # module level only -- function-local imports are already lazy
        if isinstance(node, ast.Import):
            offenders += [a.name for a in node.names if a.name.split(".")[-1] in banned]
        elif isinstance(node, ast.ImportFrom) and node.module:
            if node.module.split(".")[-1] in banned:
                offenders.append(node.module)
            offenders += [a.name for a in node.names if a.name in banned]
    assert offenders == [], f"shared/storage_config.py re-eagerizes: {offenders}"


@pytest.mark.worker
def test_worker_package_init_does_not_pull_models(tmp_path):
    # flowfile_worker/__init__ needs models.Status only as a type annotation. This covers
    # `import flowfile_worker` alone -- consumers such as flowfile/web that want mp_context
    # or CACHE_DIR. The spawned child imports funcs, which still pulls all of these.
    loaded = _modules_after("import flowfile_worker", tmp_path)
    leaked = sorted({"flowfile_worker.models", "pydantic", "pl_fuzzy_frame_match"} & loaded)
    assert leaked == [], f"flowfile_worker/__init__ eagerly loaded: {leaked}"


@pytest.mark.worker
def test_worker_funcs_import_is_lean(tmp_path):
    env = os.environ.copy()
    env["TEST_MODE"] = "1"
    env["FLOWFILE_SKIP_STARTUP_MIGRATION"] = "1"
    env["FLOWFILE_STORAGE_DIR"] = str(tmp_path / "storage")
    env["FLOWFILE_USER_DATA_DIR"] = str(tmp_path / "user_data")
    env["PYTHONPATH"] = os.pathsep.join([str(REPO_ROOT / "flowfile_worker"), str(REPO_ROOT)])
    # Mimic a spawn-context child (non-MainProcess): configs.py imports connectorx
    # only in the parent for version logging, so the child path is what we assert on.
    code = textwrap.dedent(
        """
        import json, multiprocessing, sys
        multiprocessing.current_process().name = "Worker-1"
        import flowfile_worker.funcs  # noqa: F401
        forbidden = %r
        leaked = sorted({m.split(".")[0] for m in sys.modules if m.split(".")[0] in forbidden})
        print(json.dumps(leaked))
        """
        % (FORBIDDEN,)
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(tmp_path),
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    leaked = json.loads(result.stdout.strip().splitlines()[-1])
    assert leaked == [], f"heavy modules leaked into worker child import: {leaked}"


# A console-script launch leaves __main__.__spec__ as None, which makes spawn pass
# init_main_from_path and re-execute the launcher (and the whole FastAPI app) in every
# child. flowfile_worker/__init__.py stamps a ".__main__" spec to take spawn's free path.
FORBIDDEN_IN_SPAWNED_CHILD = ("fastapi", "uvicorn", "openpyxl", "faker", "httpx", "sqlalchemy")
# A ratchet, not a win: the child loads 513 modules, unchanged since Phase 1 -- the lazy
# `shared` / `flowfile_worker` exports don't reach it. 700 was just stale headroom.
CHILD_MODULE_CEILING = 550

_PROBE_TARGET = '''
import json
import sys


def report(out_path):
    import flowfile_worker.funcs  # noqa: F401 - the real spawned-child entry surface

    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(sorted(sys.modules), fh)
'''

# Run as `python launcher.py`, so __main__.__spec__ is None exactly like a console script.
_LAUNCHER = '''
import os
import sys

from flowfile_worker.main import run  # noqa: F401 - console-script shape: full app import

from flowfile_worker import mp_context
from probe_target import report

if __name__ == "__main__":
    p = mp_context.Process(target=report, args=(os.environ["PROBE_OUT"],))
    p.start()
    p.join(180)
    sys.exit(p.exitcode if p.exitcode is not None else 1)
'''


@pytest.mark.worker
def test_spawned_child_does_not_reexecute_launcher(tmp_path):
    out_path = tmp_path / "child_modules.json"
    (tmp_path / "probe_target.py").write_text(_PROBE_TARGET, encoding="utf-8")
    launcher = tmp_path / "launcher.py"
    launcher.write_text(_LAUNCHER, encoding="utf-8")

    env = os.environ.copy()
    env["TEST_MODE"] = "1"
    env["FLOWFILE_SKIP_STARTUP_MIGRATION"] = "1"
    env["FLOWFILE_STORAGE_DIR"] = str(tmp_path / "storage")
    env["FLOWFILE_USER_DATA_DIR"] = str(tmp_path / "user_data")
    env["PROBE_OUT"] = str(out_path)
    env["PYTHONPATH"] = os.pathsep.join(
        [str(tmp_path), str(REPO_ROOT / "flowfile_worker"), str(REPO_ROOT)]
    )

    result = subprocess.run(
        [sys.executable, str(launcher)],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(tmp_path),
        timeout=300,
    )
    assert result.returncode == 0, f"launcher failed: {result.stdout}\n{result.stderr}"
    assert out_path.exists(), f"probe wrote no output: {result.stdout}\n{result.stderr}"

    child_modules = json.loads(out_path.read_text(encoding="utf-8"))
    roots = {m.split(".")[0] for m in child_modules}
    leaked = sorted(roots & set(FORBIDDEN_IN_SPAWNED_CHILD))
    assert leaked == [], (
        f"spawned child re-executed the launcher and imported the app: {leaked} "
        f"({len(child_modules)} modules loaded)"
    )
    assert len(child_modules) <= CHILD_MODULE_CEILING, (
        f"spawned child loaded {len(child_modules)} modules, ceiling is {CHILD_MODULE_CEILING}"
    )
