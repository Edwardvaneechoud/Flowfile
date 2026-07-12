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
