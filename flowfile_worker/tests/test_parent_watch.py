"""A child ``mp_context`` starts ends itself once its parent is gone, whether that parent ran its shutdown or not."""

import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from shared.run_completion import _pid_is_alive

REPO_ROOT = Path(__file__).resolve().parents[2]

_LINGER = '''
import time


def linger(path):
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("running")
    time.sleep(120)
'''

# The parent waits until its child runs, then ends with no cleanup, as a SIGKILL or a Windows TerminateProcess would.
_PARENT = textwrap.dedent(
    """
    import os, sys, time
    from pathlib import Path

    from flowfile_worker import mp_context
    from linger_target import linger

    marker = Path(sys.argv[1])
    child = mp_context.Process(target=linger, args=(str(marker),))
    child.start()
    deadline = time.monotonic() + 60
    while not marker.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    print(child.pid, flush=True)
    os._exit(0 if marker.exists() else 1)
    """
)


@pytest.mark.worker
def test_a_child_ends_itself_once_its_parent_died_without_cleanup(tmp_path):
    (tmp_path / "linger_target.py").write_text(_LINGER, encoding="utf-8")
    marker = tmp_path / "running"
    env = os.environ.copy()
    env["TEST_MODE"] = "1"
    env["FLOWFILE_STORAGE_DIR"] = str(tmp_path / "storage")
    env["FLOWFILE_USER_DATA_DIR"] = str(tmp_path / "user_data")
    env["PYTHONPATH"] = os.pathsep.join([str(tmp_path), str(REPO_ROOT / "flowfile_worker"), str(REPO_ROOT)])

    result = subprocess.run(
        [sys.executable, "-c", _PARENT, str(marker)],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(tmp_path),
        timeout=120,
    )
    assert result.returncode == 0, f"the child never ran: {result.stdout}\n{result.stderr}"
    child_pid = int(result.stdout.strip().splitlines()[-1])

    deadline = time.monotonic() + 30
    while _pid_is_alive(child_pid) and time.monotonic() < deadline:
        time.sleep(0.2)
    assert not _pid_is_alive(child_pid), "the child outlived its parent"
