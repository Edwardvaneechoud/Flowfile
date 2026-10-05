"""The notebook engine's copies of flowfile_core stay what core has.

``notebook_fusion.py`` is core's ``chain_fusion.py`` byte for byte, and ``notebook_allowlist.py`` is a
subset of core's allowlist: a cell the browser reads is one the full app reads the same way.
"""

import importlib.util
from pathlib import Path

from engine import notebook_allowlist as browser

REPO = Path(__file__).resolve().parents[3]
ENGINE = REPO / "flowfile_wasm" / "src" / "pyodide" / "engine"
CORE = REPO / "flowfile_core" / "flowfile_core"


def _core_allowlist():
    spec = importlib.util.spec_from_file_location("core_notebook_allowlist", CORE / "notebook" / "allowlist.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_chain_fusion_is_flowfile_cores_file():
    core = CORE / "flowfile" / "code_generator" / "chain_fusion.py"
    assert (ENGINE / "notebook_fusion.py").read_bytes() == core.read_bytes()


def test_every_ff_name_has_flowfile_cores_verdict():
    core = _core_allowlist()
    for name, verdict in browser.FL_VERDICTS.items():
        assert core.FL_VERDICTS.get(name) == verdict, name


def test_every_attribute_has_flowfile_cores_usage():
    core = _core_allowlist()
    for kind, attributes in browser.ALLOWLIST.items():
        for attribute, usage in attributes.items():
            assert core.ALLOWLIST.get(kind, {}).get(attribute) == usage, f"{kind}.{attribute}"


def test_what_only_a_sync_reads_is_what_flowfile_core_reads():
    core = _core_allowlist()
    for kind, attributes in browser.INPUT_ONLY.items():
        for attribute, usage in attributes.items():
            assert core.INPUT_ONLY.get(kind, {}).get(attribute) == usage, f"{kind}.{attribute}"
    for key, keywords in browser.DATA_ARGUMENTS.items():
        assert core.DATA_ARGUMENTS.get(key) == keywords, key


def test_the_expression_tables_are_flowfile_cores_own():
    core = _core_allowlist()
    for kind in ("Expr", "StringNS", "DateTimeNS"):
        assert browser.ALLOWLIST[kind] == core.ALLOWLIST[kind], kind


def test_imports_names_operators_and_bounds_are_flowfile_cores_own():
    core = _core_allowlist()
    for key, bound in browser.IMPORTS.items():
        assert core.IMPORTS.get(key) == bound, key
    assert set(browser.GENERATED_NAMES) <= set(core.GENERATED_NAMES)
    assert browser.USER_NAME == core.USER_NAME
    assert browser.RESERVED_NAMES == core.RESERVED_NAMES
    assert browser.BINARY_OPERATORS == core.BINARY_OPERATORS
    assert browser.COMPARE_OPERATORS == core.COMPARE_OPERATORS
    for name, bound in browser.BOUNDS.items():
        assert core.BOUNDS.get(name) == bound, name
