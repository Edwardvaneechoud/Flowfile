"""Local CI dry-run for a community-node bundle.

Executes ``node.py``'s ``process()`` against its own ``example_inputs`` /
``example_settings`` in a **child process that never imports flowfile_core**
(importing core runs DB migrations and is unavailable on the CI/fork runner).
The child imports only ``shared.node_designer.loading`` + polars, mirroring how
the worker loads a node from shipped source text.

Row/timeout caps mirror ``flowfile_core/flowfile/user_defined/dry_run.py``.
"""

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from pydantic import BaseModel

from flowfile_core.flowfile.community_nodes.validation import _extract_example_settings
from flowfile_core.flowfile.node_designer.parsing import extract_example_inputs, extract_manifest

_MAX_ROW_LIMIT = 1000
_MAX_TIMEOUT = 120

# Runs in a child `python -c` with cwd inheriting the parent's sys.path (the poetry
# venv), so `import shared` resolves. Must NOT import flowfile_core. argv: folder, config.
_RUNNER = r"""
import contextlib
import json
import logging
import sys

import polars as pl

from shared.node_designer.loading import find_custom_node_class, install_import_aliases, load_node_module


def _normalize(result, output_names):
    if isinstance(result, dict):
        unknown = sorted(set(result) - set(output_names))
        if unknown:
            raise ValueError("process() returned unknown outputs %s; declared %s" % (unknown, output_names))
        missing = [n for n in output_names if n not in result]
        if missing:
            raise ValueError("process() did not return declared outputs %s" % missing)
        frames = result
    elif isinstance(result, (pl.LazyFrame, pl.DataFrame)):
        if len(output_names) > 1:
            raise ValueError(
                "node declares %d outputs %s but process() returned a single frame" % (len(output_names), output_names)
            )
        frames = {output_names[0]: result}
    elif result is None:
        raise ValueError("process() returned None; return a DataFrame or LazyFrame")
    else:
        raise TypeError("process() must return a DataFrame, LazyFrame or dict, got %s" % type(result).__name__)
    return {name: (f.lazy() if isinstance(f, pl.DataFrame) else f) for name, f in frames.items()}


def _run(folder, config):
    install_import_aliases()
    with open(folder + "/node.py", encoding="utf-8") as f:
        source = f.read()
    module = load_node_module(source=source)
    module.__dict__.setdefault("logging", logging)
    node = find_custom_node_class(module, config["class_name"])()
    settings = config.get("example_settings") or {}
    if settings and node.settings_schema:
        node.settings_schema.populate_values(settings)
    node.set_execution_context(-1, resolver=None)
    lazy_inputs = [pl.LazyFrame(d) for d in config.get("example_inputs") or []]
    outputs = _normalize(node.process(*lazy_inputs), config["output_names"])
    names = []
    for name, lf in outputs.items():
        lf.head(config["row_limit"]).collect()
        names.append(name)
    return {"success": True, "error": None, "output_names": names}


def main():
    folder = sys.argv[1]
    with open(sys.argv[2], encoding="utf-8") as f:
        config = json.load(f)
    real_stdout = sys.stdout
    verdict = {"success": False, "error": "dry-run runner did not complete", "output_names": []}
    try:
        with contextlib.redirect_stdout(sys.stderr):
            verdict = _run(folder, config)
    except Exception as e:
        verdict = {"success": False, "error": str(e), "output_names": []}
    real_stdout.write(json.dumps(verdict))
    real_stdout.flush()


main()
"""


class DryRunOutcome(BaseModel):
    success: bool = False
    error: str | None = None
    duration_ms: float = 0.0
    output_names: list[str] = []


def run_bundle_dry_run(folder: Path, *, timeout_seconds: int = 120, row_limit: int = 1000) -> DryRunOutcome:
    """Execute a bundle's ``process()`` against its example data in a child process.

    Returns a typed verdict; never raises for user-code failures (they surface in
    ``error``). The child is killed if it exceeds ``timeout_seconds``.
    """
    folder = Path(folder)
    node_py = folder / "node.py"
    if not node_py.is_file():
        return DryRunOutcome(success=False, error="node.py not found")

    row_limit = max(1, min(row_limit, _MAX_ROW_LIMIT))
    timeout_seconds = max(1, min(timeout_seconds, _MAX_TIMEOUT))

    source = node_py.read_text(encoding="utf-8")
    manifest = extract_manifest(source)
    if not manifest.class_name:
        return DryRunOutcome(success=False, error="No CustomNodeBase subclass found in node.py")

    config = {
        "class_name": manifest.class_name,
        "output_names": manifest.output_names,
        "number_of_inputs": manifest.number_of_inputs,
        "row_limit": row_limit,
        "example_inputs": extract_example_inputs(source) or [],
        "example_settings": _extract_example_settings(source) or {},
    }

    started = time.monotonic()
    with tempfile.NamedTemporaryFile("w", suffix=".json", encoding="utf-8", delete=True) as cfg:
        json.dump(config, cfg)
        cfg.flush()
        try:
            completed = subprocess.run(
                [sys.executable, "-c", _RUNNER, str(folder), cfg.name],
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            return DryRunOutcome(
                success=False,
                error=f"Dry run exceeded the {timeout_seconds}s time limit and was killed.",
                duration_ms=(time.monotonic() - started) * 1000,
            )

    duration_ms = (time.monotonic() - started) * 1000
    verdict = _parse_verdict(completed.stdout)
    if verdict is None:
        detail = (completed.stderr or completed.stdout or "no output").strip()[-500:]
        return DryRunOutcome(success=False, error=f"Dry-run runner failed: {detail}", duration_ms=duration_ms)
    return DryRunOutcome(
        success=bool(verdict.get("success")),
        error=verdict.get("error"),
        duration_ms=duration_ms,
        output_names=list(verdict.get("output_names") or []),
    )


def _parse_verdict(stdout: str) -> dict | None:
    stdout = (stdout or "").strip()
    if not stdout:
        return None
    try:
        parsed = json.loads(stdout)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None
