"""Tests for CLI 'flowfile import alteryx'.

Run with:
    pytest flowfile/tests/test_cli_import.py -v

Never in the same pytest run as flowfile_core/tests — both trees are packages named `tests`.
"""

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from flowfile.__main__ import main

FIXTURES = Path(__file__).resolve().parents[2] / "flowfile_core" / "tests" / "flowfile" / "converters" / "fixtures"
LEARNING = Path(__file__).resolve().parents[3]
CORPUS = LEARNING / "alteryx_nodes"
CENSUS_SCRIPT = LEARNING / "tools" / "alteryx_census.py"


def run_cli(*argv: str) -> int:
    with patch("sys.argv", ["flowfile", *argv]), pytest.raises(SystemExit) as exit_info:
        main()
    return exit_info.value.code


@pytest.fixture()
def workflow(tmp_path: Path) -> Path:
    target = tmp_path / "out_of_scope.yxmd"
    target.write_bytes((FIXTURES / "out_of_scope.yxmd").read_bytes())
    return target


def test_inspect_prints_the_markdown_table(workflow: Path, capsys):
    assert run_cli("import", "alteryx", str(workflow), "--inspect") == 0

    out = capsys.readouterr().out
    assert "# Out Of Scope Tools" in out
    assert "| Tool | Key | Instances | Status | Reason | Message |" in out
    assert "| Buffer | Buffer | 1 | out_of_scope | scope:spatial |" in out
    assert "| Message | Message | 1 | no_op | no_op |" in out
    assert "of the 2 tools Flowfile aims to convert, 50%" in out
    # --inspect writes nothing.
    assert not list(workflow.parent.glob("*.yaml"))


def test_inspect_is_deterministic(workflow: Path, capsys):
    assert run_cli("import", "alteryx", str(workflow), "--inspect") == 0
    first = capsys.readouterr().out
    assert run_cli("import", "alteryx", str(workflow), "--inspect") == 0
    assert capsys.readouterr().out == first


def test_inspect_json_carries_every_row(workflow: Path, capsys):
    assert run_cli("import", "alteryx", str(workflow), "--inspect", "--format", "json") == 0

    report = json.loads(capsys.readouterr().out)
    assert report["out_of_scope"] == 4
    assert report["no_op"] == 2
    assert report["coverage"]["in_scope"] == 2
    reasons = {row["census_name"]: row["reason"] for row in report["rows"]}
    assert reasons["Precision_Match.yxmc"] == "scope:genai"


def test_inspect_writes_to_a_file(workflow: Path, tmp_path: Path, capsys):
    destination = tmp_path / "coverage.md"
    assert run_cli("import", "alteryx", str(workflow), "--inspect", "--out", str(destination)) == 0

    assert "| Tool | Key |" in destination.read_text()
    assert "1 workflow(s) inspected" in capsys.readouterr().out


def test_a_directory_is_pooled_into_one_report(tmp_path: Path, capsys):
    nested = tmp_path / "corpus" / "01 In Out"
    nested.mkdir(parents=True)
    for name in ("out_of_scope.yxmd", "simple_filter.yxmd"):
        (nested / name).write_bytes((FIXTURES / name).read_bytes())

    assert run_cli("import", "alteryx", str(tmp_path / "corpus"), "--inspect") == 0

    out = capsys.readouterr().out
    assert "# corpus" in out
    assert "13 tools:" in out
    assert "| Filter |" in out and "| Buffer |" in out


def test_without_inspect_a_flow_is_written_beside_the_source(workflow: Path, capsys):
    assert run_cli("import", "alteryx", str(workflow)) == 0

    written = workflow.with_suffix(".yaml")
    flow = yaml.safe_load(written.read_text())
    assert flow["flowfile_name"] == "Out Of Scope Tools"
    assert len(flow["nodes"]) == 8
    assert f"{workflow} -> {written}" in capsys.readouterr().out


def test_an_existing_flow_is_kept_unless_overwrite_is_given(workflow: Path, capsys):
    written = workflow.with_suffix(".yaml")
    written.write_text("hand-edited: true\n")

    assert run_cli("import", "alteryx", str(workflow)) == 1

    assert written.read_text() == "hand-edited: true\n"
    assert "already exists (use --overwrite)" in capsys.readouterr().out


def test_overwrite_replaces_an_existing_flow(workflow: Path):
    written = workflow.with_suffix(".yaml")
    written.write_text("hand-edited: true\n")

    assert run_cli("import", "alteryx", str(workflow), "--overwrite") == 0

    assert yaml.safe_load(written.read_text())["flowfile_name"] == "Out Of Scope Tools"


def test_out_sends_one_flow_to_that_directory(workflow: Path, tmp_path: Path):
    destination = tmp_path / "flows"

    assert run_cli("import", "alteryx", str(workflow), "--out", str(destination)) == 0

    assert (destination / "out_of_scope.yaml").exists()
    assert not workflow.with_suffix(".yaml").exists()


def test_out_mirrors_the_source_tree_for_a_directory(tmp_path: Path):
    nested = tmp_path / "corpus" / "01 In Out"
    nested.mkdir(parents=True)
    for name in ("out_of_scope.yxmd", "simple_filter.yxmd"):
        (nested / name).write_bytes((FIXTURES / name).read_bytes())
    destination = tmp_path / "flows"

    assert run_cli("import", "alteryx", str(tmp_path / "corpus"), "--out", str(destination)) == 0

    assert (destination / "01 In Out" / "out_of_scope.yaml").exists()
    assert (destination / "01 In Out" / "simple_filter.yaml").exists()
    assert not list(nested.glob("*.yaml"))


def test_a_single_unparseable_workflow_reports_its_parse_error(tmp_path: Path, capsys):
    broken = tmp_path / "broken.yxmd"
    broken.write_bytes(b"<nonsense/>")

    assert run_cli("import", "alteryx", str(broken)) == 1

    err = capsys.readouterr().err
    assert f"FAILED  {broken}: " in err
    assert "No .yxmd workflows found" not in err


def test_an_unparseable_workflow_fails_the_command_but_not_the_walk(tmp_path: Path, capsys):
    (tmp_path / "broken.yxmd").write_bytes(b"<nonsense/>")
    (tmp_path / "good.yxmd").write_bytes((FIXTURES / "simple_filter.yxmd").read_bytes())

    assert run_cli("import", "alteryx", str(tmp_path), "--inspect") == 1

    assert "FAILED" in capsys.readouterr().err


def test_a_missing_path_and_a_missing_format_report_usage(capsys):
    assert run_cli("import", "alteryx") == 1
    assert "Usage: flowfile import alteryx" in capsys.readouterr().err
    assert run_cli("import", "yxdb", "x") == 1
    assert "Usage: flowfile import alteryx" in capsys.readouterr().err


def test_import_appears_in_help(capsys):
    with patch("sys.argv", ["flowfile"]):
        main()
    assert "flowfile import alteryx" in capsys.readouterr().out


CENSUS_PARITY_FILES = [
    "02 Preparation/Filter.yxmd",
    "05 Transform/Cross_Tab.yxmd",
    "03 Join/Join.yxmd",
]


@pytest.mark.skipif(not CENSUS_SCRIPT.exists(), reason=f"corpus census script not present at {CENSUS_SCRIPT}")
@pytest.mark.parametrize("name", CENSUS_PARITY_FILES)
def test_the_generated_table_counts_what_the_census_counts(name: str, tmp_path: Path, monkeypatch, capsys):
    """The published table and the private census must not be able to disagree."""
    from collections import Counter

    from flowfile_core.flowfile.converters.alteryx import convert_yxmd
    from flowfile_core.flowfile.converters.alteryx.yxmd_parser import parse_yxmd

    source = CORPUS / name
    if not source.exists():
        pytest.skip(f"corpus workflow not present: {source}")
    target = tmp_path / source.name
    target.write_bytes(source.read_bytes())

    monkeypatch.setattr(sys, "argv", ["census", str(tmp_path), str(tmp_path)])
    census = _load_census_module()

    workflow = parse_yxmd(target.read_bytes())
    result = convert_yxmd(target.read_bytes(), source_name=target.name)
    rows = {row.alteryx_tool_id: row for row in result.report.rows}
    expected = Counter((census.tool_key(tool), rows[tool.tool_id].status) for tool in workflow.tools)

    assert run_cli("import", "alteryx", str(target), "--inspect") == 0
    assert _table_counts(capsys.readouterr().out) == dict(expected)


def _load_census_module():
    """Import `tools/alteryx_census.py` by path; it reads sys.argv[2] at import time."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("alteryx_census_for_test", CENSUS_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _table_counts(markdown: str) -> dict[tuple[str, str], int]:
    counts = {}
    for line in markdown.splitlines():
        cells = [cell.strip() for cell in line.split("|")[1:-1]]
        if len(cells) == 6 and cells[2].isdigit():
            counts[(cells[0], cells[3])] = int(cells[2])
    return counts
