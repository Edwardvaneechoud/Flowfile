"""Lazy-import contract for spawned worker children.

Every task spawns a fresh 'spawn'-context child that imports flowfile_worker.funcs;
heavy cloud/db deps must never load at import time (child startup ~0.95s -> ~0.35s).
"""

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
FORBIDDEN_IN_SPAWNED_CHILD = ("fastapi", "uvicorn", "openpyxl", "faker", "httpx")
CHILD_MODULE_CEILING = 700

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
