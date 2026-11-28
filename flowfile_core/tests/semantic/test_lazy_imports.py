"""Importing the semantic modules, and core's app, loads neither glosslayer, its Flowfile plugin nor litellm.

A fresh interpreter, since this test process has long imported all three.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

MODULES = [
    "flowfile_core.catalog.semantic.host",
    "flowfile_core.catalog.semantic.service",
    "flowfile_core.catalog.semantic.confidentiality",
    "flowfile_core.catalog.semantic.activity",
    "flowfile_core.catalog.semantic.data_rules",
    "flowfile_core.routes.catalog_semantic",
    "flowfile_core.schemas.semantic_schema",
    "flowfile_core.ai.semantic_routes",
    "flowfile_core.ai.semantic_draft",
    "flowfile_core.ai.semantic_ask",
    "flowfile_core.ai.semantic_suggest",
    "flowfile_core.ai.semantic_answer",
    "flowfile_core.ai.chart_profile",
    "flowfile_core.ai.dashboard_story",
    "flowfile_core.main",
]


def test_imports_load_no_glosslayer_and_no_litellm(tmp_path: Path) -> None:
    code = f"""
        import importlib, json, sys

        for name in {MODULES!r}:
            importlib.import_module(name)
        roots = {{"glosslayer", "glosslayer_flowfile", "litellm"}}
        print(json.dumps(sorted({{m.split(".")[0] for m in sys.modules}} & roots)))
    """
    env = {
        **os.environ,
        "FLOWFILE_STORAGE_DIR": str(tmp_path / "storage"),
        "FLOWFILE_USER_DATA_DIR": str(tmp_path / "user"),
        "FLOWFILE_DB_PATH": str(tmp_path / "lazy.db"),
        "FLOWFILE_SKIP_STARTUP_MIGRATION": "1",
        "FLOWFILE_SKIP_INIT_DB": "1",
    }
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(REPO_ROOT),
        timeout=180,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    assert json.loads(result.stdout.strip().splitlines()[-1]) == []
