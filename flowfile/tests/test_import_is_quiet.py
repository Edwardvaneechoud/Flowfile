"""``import flowfile`` as a library prints nothing and leaves the process's logging unconfigured."""

import os
import subprocess
import sys

_PROBE = (
    "import logging\n"
    # passlib warns about the installed bcrypt at import; that is its noise, not flowfile's.
    "logging.getLogger('passlib').setLevel(logging.ERROR)\n"
    "import flowfile\n"
    "root = logging.getLogger()\n"
    "print(len(root.handlers), root.level, root.manager.disable)\n"
)


def test_importing_flowfile_prints_nothing_and_configures_no_logging(tmp_path):
    env = {
        **os.environ,
        "FLOWFILE_STORAGE_DIR": str(tmp_path / "storage"),
        "FLOWFILE_USER_DATA_DIR": str(tmp_path / "user"),
        "FLOWFILE_DB_PATH": str(tmp_path / "catalog.db"),
        "FLOWFILE_SECURE_STORAGE_PATH": str(tmp_path / "secure"),
        "FLOWFILE_TELEMETRY": "0",
        "TESTING": "True",
    }
    result = subprocess.run([sys.executable, "-c", _PROBE], capture_output=True, text=True, env=env, timeout=600)
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    assert result.stdout.strip() == "0 30 0", result.stdout  # no handlers, WARNING, nothing disabled
