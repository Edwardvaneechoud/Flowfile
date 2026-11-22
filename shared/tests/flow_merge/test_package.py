"""``shared.flow_merge``: importable without core, and the id rewrites a merge needs beyond node ids."""

import ast
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from shared.flow_merge import cli
from shared.flow_merge.relabel import relabel, relabel_comments, relabel_groups

REPO_ROOT = Path(__file__).resolve().parents[3]
PACKAGE_DIR = REPO_ROOT / "shared" / "flow_merge"
FORBIDDEN = {"flowfile_core", "flowfile", "flowfile_frame", "flowfile_worker", "alembic", "sqlalchemy", "fastapi"}


def test_flow_merge_import_is_pure(tmp_path):
    """A git merge driver imports the package once per merged file: it must not start core or touch a database.

    Every module of the package is imported, found on disk rather than listed, and a plain ``check`` runs too.
    """
    flow = tmp_path / "one.yaml"
    flow.write_text("nodes: []\n", encoding="utf-8")
    env = {
        **os.environ,
        "FLOWFILE_STORAGE_DIR": str(tmp_path / "storage"),
        "FLOWFILE_USER_DATA_DIR": str(tmp_path / "user_data"),
        "FLOWFILE_DB_PATH": str(tmp_path / "purity.db"),
    }
    code = f"""
        import importlib, json, pkgutil, sys
        import shared.flow_merge as package
        names = sorted(m.name for m in pkgutil.iter_modules(package.__path__))
        for name in names:
            importlib.import_module(f"shared.flow_merge.{{name}}")
        from shared.flow_merge.cli import main
        rc = main(["check", {str(flow)!r}])
        forbidden = sorted(m for m in sys.modules if m.split(".")[0] in {sorted(FORBIDDEN)!r})
        print(json.dumps({{"modules": names, "check": rc, "forbidden": forbidden}}))
    """
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(REPO_ROOT),
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    out = json.loads(result.stdout.strip().splitlines()[-1])
    assert out["modules"] == sorted(p.stem for p in PACKAGE_DIR.glob("*.py") if p.stem != "__init__")
    assert {"__main__", "cli"} <= set(out["modules"])
    assert out["check"] == 0
    assert out["forbidden"] == []
    assert not (tmp_path / "purity.db").exists()
    assert not (tmp_path / "storage").exists()


def _imported_names(tree: ast.AST) -> list[str]:
    """Every module a source imports, function-local ones included, and every literal handed to an import call."""
    names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names += [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.append(node.module)
        elif isinstance(node, ast.Call):
            func = node.func
            called = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
            if called in ("import_module", "__import__"):
                names += [a.value for a in node.args if isinstance(a, ast.Constant) and isinstance(a.value, str)]
    return names


@pytest.mark.parametrize("path", sorted(PACKAGE_DIR.glob("*.py")), ids=lambda p: p.name)
def test_no_module_imports_above_shared(path):
    """Not at module level, not inside a function, not through ``importlib``: core reaches the CLI as an entry point."""
    names = _imported_names(ast.parse(path.read_text(encoding="utf-8")))
    assert [n for n in names if n.split(".")[0] in FORBIDDEN] == []


class _EntryPoint:
    def __init__(self, load):
        self.load = load


def _flow(tmp_path) -> str:
    path = tmp_path / "one.yaml"
    path.write_text("nodes: []\n", encoding="utf-8")
    return str(path)


def test_check_build_without_a_registered_check_fails_in_one_sentence(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(cli, "entry_points", lambda **kw: ())
    assert cli.main(["check", "--build", _flow(tmp_path)]) == 2
    err = capsys.readouterr().err
    assert err == f"flowfile-merge: {cli.NO_BUILD_CHECK}\n"
    assert "Traceback" not in err


def test_check_build_whose_check_is_not_installed_fails_in_one_sentence(tmp_path, monkeypatch, capsys):
    def missing():
        raise ModuleNotFoundError("No module named 'flowfile_core'")

    monkeypatch.setattr(cli, "entry_points", lambda **kw: (_EntryPoint(missing),))
    assert cli.main(["check", "--build", _flow(tmp_path)]) == 2
    assert capsys.readouterr().err == f"flowfile-merge: {cli.NO_BUILD_CHECK}\n"


def test_check_build_reports_what_the_registered_check_finds(tmp_path, monkeypatch, capsys):
    asked = []

    def lookup(**kw):
        asked.append(kw)
        return (_EntryPoint(lambda: lambda document: ["Step 9 is wrong."]),)

    monkeypatch.setattr(cli, "entry_points", lookup)
    path = _flow(tmp_path)
    assert cli.main(["check", "--build", path]) == 1
    assert capsys.readouterr().err == f"{path}: Step 9 is wrong.\n"
    assert asked == [{"group": cli.BUILD_CHECK_GROUP, "name": cli.BUILD_CHECK_NAME}]


def test_plain_check_never_looks_up_the_build_check(tmp_path, monkeypatch):
    def lookup(**kw):
        raise AssertionError("plain check looked up the build check")

    monkeypatch.setattr(cli, "entry_points", lookup)
    assert cli.main(["check", _flow(tmp_path)]) == 0


def _document() -> dict:
    return {
        "nodes": [
            {"id": 1, "type": "manual_input", "group_id": 2, "input_ids": [], "outputs": [3]},
            {"id": 3, "type": "filter", "group_id": 5, "input_ids": [1], "outputs": []},
            {"id": 4, "type": "sort", "group_id": None, "input_ids": [], "outputs": []},
        ],
        "groups": [
            {"id": 2, "name": "outer", "parent_group_id": None},
            {"id": 5, "name": "inner", "parent_group_id": 2},
        ],
        "comments": [{"id": 1, "text": "a"}, {"id": 2, "text": "b"}],
    }


def test_relabel_groups_rewrites_group_and_parent_ids():
    data = _document()
    out = relabel_groups(data, {2: 20, 5: 50})
    assert [(g["id"], g["parent_group_id"]) for g in out["groups"]] == [(20, None), (50, 20)]
    assert [n["group_id"] for n in out["nodes"]] == [20, 50, None]
    assert data["groups"][0]["id"] == 2, "the input is left as it was"


def test_relabel_groups_leaves_node_ids_alone():
    out = relabel_groups(_document(), {1: 10, 3: 30})
    assert [n["id"] for n in out["nodes"]] == [1, 3, 4]
    assert [n["group_id"] for n in out["nodes"]] == [2, 5, None]


def test_relabel_comments_rewrites_comment_ids_only():
    out = relabel_comments(_document(), {1: 7})
    assert [c["id"] for c in out["comments"]] == [7, 2]
    assert [n["id"] for n in out["nodes"]] == [1, 3, 4]


def test_relabel_still_rewrites_node_ids_and_not_groups():
    out = relabel(_document(), {1: 11, 3: 13})
    assert [n["id"] for n in out["nodes"]] == [11, 13, 4]
    assert out["nodes"][1]["input_ids"] == [11]
    assert out["nodes"][0]["outputs"] == [13]
    assert [n["group_id"] for n in out["nodes"]] == [2, 5, None]
