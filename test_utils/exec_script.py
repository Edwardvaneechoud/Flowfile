"""Run generated code the way a script file runs it."""

import hashlib
import linecache
from typing import Any


def exec_script(code: str, namespace: dict | None = None) -> dict:
    """Exec generated code the way a script file runs it: with its source readable by ``inspect``."""
    filename = f"<flowfile-export-{hashlib.sha1(code.encode()).hexdigest()[:12]}>"
    linecache.cache[filename] = (len(code), None, code.splitlines(True), filename)
    namespace = {} if namespace is None else namespace
    exec(compile(code, filename, "exec"), namespace)
    return namespace


def run_exported(code: str) -> Any:
    """Run an exported flow script and collect what its ``run_etl_pipeline()`` returns."""
    result = exec_script(code)["run_etl_pipeline"]()
    return result.collect() if hasattr(result, "collect") else result
