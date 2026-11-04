"""``import flowfile`` in a program leaves that program's own command line to it: core and the worker read their join
flags only when they are the process being started."""

import os
import subprocess
import sys

_PROGRAM = (
    "import logging\n"
    "logging.getLogger('passlib').setLevel(logging.ERROR)\n"
    "import flowfile\n"
    "from flowfile_core.configs import settings\n"
    "from flowfile_worker import configs\n"
    "print(settings.JOIN_TOKEN, settings.WORKER_PORT, configs.JOIN_TOKEN, configs.SERVICE_HOST)\n"
)


def test_a_program_given_join_flags_imports_flowfile(tmp_path):
    program = tmp_path / "etl.py"
    program.write_text(_PROGRAM, encoding="utf-8")
    env = {
        **os.environ,
        "FLOWFILE_STORAGE_DIR": str(tmp_path / "storage"),
        "FLOWFILE_USER_DATA_DIR": str(tmp_path / "user"),
        "FLOWFILE_DB_PATH": str(tmp_path / "catalog.db"),
        "FLOWFILE_SECURE_STORAGE_PATH": str(tmp_path / "secure"),
        "FLOWFILE_TELEMETRY": "0",
        "TESTING": "True",
    }
    for name in ("FLOWFILE_HUB_JOIN_TOKEN", "FLOWFILE_HUB_URL", "FLOWFILE_NODE_NAME", "FLOWFILE_WORKER_PORT"):
        env.pop(name, None)
    flags = ["--join", "x", "--joi", "y", "--worker", "5", "--name", "n"]

    result = subprocess.run(
        [sys.executable, str(program), *flags], capture_output=True, text=True, env=env, cwd=tmp_path, timeout=600
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == ["None", "5", "None", "127.0.0.1"]
