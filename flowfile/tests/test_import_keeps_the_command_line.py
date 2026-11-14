"""``import flowfile`` in a program leaves that program's own command line to it: flags core and the worker do not
know are the program's, and none of them stops the import."""

import os
import subprocess
import sys

_PROGRAM = (
    "import logging\n"
    "logging.getLogger('passlib').setLevel(logging.ERROR)\n"
    "import flowfile\n"
    "from flowfile_core.configs import settings\n"
    "from flowfile_worker import configs\n"
    "print(settings.WORKER_PORT, configs.SERVICE_HOST)\n"
)


def test_a_program_given_flags_of_its_own_imports_flowfile(tmp_path):
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
    env.pop("FLOWFILE_WORKER_PORT", None)
    flags = ["--target", "x", "--targ", "y", "--worker", "5", "--name", "n"]

    result = subprocess.run(
        [sys.executable, str(program), *flags], capture_output=True, text=True, env=env, cwd=tmp_path, timeout=600
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == ["5", "127.0.0.1"]
