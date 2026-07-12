"""Tests for the community-node AST security scanner.

The corpus lives under ``corpus_malicious/``:
  - ``deny/``   one file per hard-deny idiom, mapped to the rule id it must trip.
  - ``benign/`` capable-but-safe nodes that must pass with an exact capability set.
  - ``evade/``  known static-analysis blind spots, marked xfail(strict) so that
                hardening the scanner surfaces as an XPASS to prune.
"""

import ast
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from flowfile_core.flowfile.community_nodes.security_scan import (
    CAPABILITIES,
    SCANNER_VERSION,
    scan_source,
)

CORPUS = Path(__file__).parent / "corpus_malicious"
SCANNER_PATH = (
    Path(__file__).resolve().parents[4]
    / "flowfile_core"
    / "flowfile_core"
    / "flowfile"
    / "community_nodes"
    / "security_scan.py"
)

DENY_EXPECTATIONS = {
    "exec_dynamic": "FF-SEC-001",
    "importlib_indirection": "FF-SEC-002",
    "base64_to_exec": "FF-SEC-003",
    "zlib_to_exec": "FF-SEC-003",
    "marshal_to_exec": "FF-SEC-003",
    "getattr_builtin_chain": "FF-SEC-004",
    "globals_index": "FF-SEC-004",
    "ctypes_ffi": "FF-SEC-005",
    "os_system": "FF-SEC-006",
    "subprocess_shell_true": "FF-SEC-007",
    "pty_socket_reverse_shell": "FF-SEC-008",
    "sys_getframe": "FF-SEC-009",
    "runtime_pip": "FF-SEC-010",
    "opaque_blob": "FF-SEC-011",
    "env_enumeration": "FF-SEC-012",
}

BENIGN_EXPECTATIONS = {
    "network_httpx_connector": ["network"],
    "filesystem_writer": ["fs_write"],
    "secret_selector_node": ["secrets"],
    "env_getenv_node": ["env_read"],
}


def _read(rel: str) -> str:
    return (CORPUS / rel).read_text()


def _stems(subdir: str) -> set[str]:
    return {p.stem for p in (CORPUS / subdir).glob("*.pytxt")}


@pytest.mark.parametrize("stem, expected_rule", sorted(DENY_EXPECTATIONS.items()))
def test_deny_idiom_fails(stem, expected_rule):
    report = scan_source(_read(f"deny/{stem}.pytxt"))
    rule_ids = [f.rule_id for f in report.findings]
    assert report.passed is False, f"{stem} should be denied"
    assert report.deny_count >= 1
    assert expected_rule in rule_ids, f"{stem} expected {expected_rule}, got {sorted(set(rule_ids))}"


def test_every_deny_fixture_is_mapped():
    assert _stems("deny") == set(DENY_EXPECTATIONS), "unmapped deny fixture(s) present"


@pytest.mark.parametrize("stem, expected_caps", sorted(BENIGN_EXPECTATIONS.items()))
def test_benign_capable_node_passes(stem, expected_caps):
    report = scan_source(_read(f"benign/{stem}.pytxt"))
    assert report.passed is True, f"{stem} falsely denied: {[f.rule_id for f in report.findings]}"
    assert report.deny_count == 0
    assert report.capabilities == expected_caps


def test_every_benign_fixture_is_mapped():
    assert _stems("benign") == set(BENIGN_EXPECTATIONS), "unmapped benign fixture(s) present"


def test_all_corpus_fixtures_are_valid_python():
    for path in CORPUS.rglob("*.pytxt"):
        ast.parse(path.read_text())  # must not raise; fixtures are real code, not gibberish


@pytest.mark.xfail(
    strict=True,
    reason=(
        "FF-SEC-003 taint tracking is same-function and simple-name only. A payload decoded into "
        "self.code in one method and exec'd in another evades the decode->exec dataflow rule (the "
        "literal exec is still caught by FF-SEC-001, but the decode provenance is lost). Documents "
        "a known static-analysis limit; the reviewed PR gate is the real control."
    ),
)
def test_evasion_attribute_stored_payload_crossing_functions():
    report = scan_source(_read("evade/attribute_stored_payload.pytxt"))
    assert any(f.rule_id == "FF-SEC-003" for f in report.findings)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "operator.attrgetter('system')(os) resolves os.system without a static attribute chain, so "
        "the reflective trampoline slips past AST scanning entirely. Documents a known limit; the "
        "reviewed PR gate is the real control."
    ),
)
def test_evasion_attrgetter_trampoline():
    report = scan_source(_read("evade/attrgetter_trampoline.pytxt"))
    assert report.passed is False


def test_syntax_error_is_single_deny():
    report = scan_source("def broken(:\n    pass\n")
    assert report.passed is False
    assert report.deny_count == 1
    assert len(report.findings) == 1
    assert report.findings[0].rule_id == "FF-SEC-000"
    assert "does not parse" in report.findings[0].message


def test_compile_of_bad_source_never_raises():
    # scan_source must swallow every SyntaxError shape, not just the common one.
    for bad in ("(", "class", "def f(", "import", "x = ="):
        report = scan_source(bad)
        assert report.findings[0].rule_id == "FF-SEC-000"
        assert report.passed is False


def test_empty_and_trivial_sources_pass():
    for src in ("", "\n\n", "x = 1\n", "import polars as pl\n"):
        report = scan_source(src)
        assert report.passed is True
        assert report.capabilities == []
        assert report.deny_count == 0


def test_clean_node_passes_with_no_capabilities():
    src = (
        "import polars as pl\n\n\n"
        "def process(self, df: pl.LazyFrame) -> pl.LazyFrame:\n"
        "    return df.filter(pl.col('x') > 0).with_columns(y=pl.col('x') * 2)\n"
    )
    report = scan_source(src)
    assert report.passed is True
    assert report.capabilities == []


def test_scanner_version_and_capability_vocabulary():
    assert SCANNER_VERSION == "1"
    report = scan_source("x = 1")
    assert report.scanner_version == "1"
    assert set(CAPABILITIES) == {
        "network",
        "fs_read",
        "fs_write",
        "subprocess",
        "env_read",
        "dynamic_code",
        "native_ffi",
        "secrets",
        "serialization",
    }


def test_capabilities_are_sorted_and_deduped():
    src = (
        "import socket\n"
        "import requests\n\n\n"
        "def process(self, df):\n"
        "    open('a.txt', 'w').write('1')\n"
        "    open('b.txt', 'w').write('2')\n"
        "    token = os.getenv('T')\n"
        "    return df\n"
    )
    report = scan_source(src)
    assert report.passed is True
    assert report.capabilities == sorted(set(report.capabilities))
    assert report.capabilities == ["env_read", "fs_write", "network"]


def test_subprocess_literal_args_flag_not_deny():
    src = "import subprocess\n\n\ndef process(self, df):\n    subprocess.run(['ls', '-la'])\n    return df\n"
    report = scan_source(src)
    assert report.passed is True
    assert report.capabilities == ["subprocess"]
    assert any(f.rule_id == "FF-SEC-102" for f in report.findings)


def test_env_read_without_secrets_emits_lint():
    report = scan_source("import os\n\n\ndef process(self, df):\n    return os.getenv('X')\n")
    assert report.passed is True
    assert any(f.rule_id == "FF-SEC-201" and f.severity == "lint" for f in report.findings)


def test_env_read_with_declared_secret_suppresses_lint():
    src = (
        "import os\n\n\n"
        "def process(self, df):\n"
        "    key = self.settings.token.secret_value\n"
        "    return os.getenv('X')\n"
    )
    report = scan_source(src)
    assert report.passed is True
    assert set(report.capabilities) == {"env_read", "secrets"}
    assert not any(f.rule_id == "FF-SEC-201" for f in report.findings)


def test_serialization_load_not_reaching_exec_is_flag():
    src = "import pickle\n\n\ndef process(self, df):\n    obj = pickle.loads(df.head())\n    return obj\n"
    report = scan_source(src)
    assert report.passed is True
    assert report.capabilities == ["serialization"]


def test_yaml_safe_loader_is_not_flagged():
    safe = "import yaml\n\n\ndef process(self, df):\n    return yaml.load(df, Loader=yaml.SafeLoader)\n"
    assert scan_source(safe).capabilities == []
    unsafe = "import yaml\n\n\ndef process(self, df):\n    return yaml.load(df)\n"
    assert scan_source(unsafe).capabilities == ["serialization"]


def test_getattr_with_safe_constant_is_allowed():
    report = scan_source("def process(self, row):\n    return getattr(row, 'value')\n")
    assert report.passed is True
    assert not any(f.rule_id == "FF-SEC-004" for f in report.findings)


def test_scanner_module_import_is_light(tmp_path):
    # Loaded by file path under a synthetic name so no flowfile_core __init__ (and no
    # fastapi/litellm) is pulled: the scanner must stay stdlib + pydantic only.
    env = os.environ.copy()
    env["TESTING"] = "True"
    env["FLOWFILE_SKIP_STARTUP_MIGRATION"] = "1"
    env["FLOWFILE_DB_PATH"] = str(tmp_path / "scan.db")
    code = (
        "import importlib.util, json, sys\n"
        f"spec = importlib.util.spec_from_file_location('ff_security_scan_isolated', {str(SCANNER_PATH)!r})\n"
        "mod = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(mod)\n"
        "report = mod.scan_source(\"import os\\nos.system('id')\")\n"
        "print(json.dumps({\n"
        "    'fastapi': 'fastapi' in sys.modules,\n"
        "    'litellm': 'litellm' in sys.modules,\n"
        "    'passed': report.passed,\n"
        "    'version': mod.SCANNER_VERSION,\n"
        "}))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, env=env, timeout=60
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    assert payload["fastapi"] is False, "importing the scanner pulled in fastapi"
    assert payload["litellm"] is False, "importing the scanner pulled in litellm"
    assert payload["passed"] is False
    assert payload["version"] == "1"
