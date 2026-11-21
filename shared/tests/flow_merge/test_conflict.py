"""A conflicted flow as a file, settled per conflict, and ``flowfile-merge`` as git's merge driver in a real repo."""

import json
import os
import shutil
import subprocess
import sys

import pytest

from shared.flow_merge.cli import keep_ids_for
from shared.flow_merge.conflict import (
    UnresolvedMergeError,
    conflict_document,
    is_conflicted,
    open_conflicts,
    refuse_conflicted,
    resolve,
)
from shared.flow_merge.edges import MAIN
from shared.flow_merge.merge import merge
from shared.flow_merge.yaml_io import dump, load

from .editing import add_step, copied, remove_step, sales, settings, step
from .fixtures import REPO_ROOT


def _two_conflicts():
    base = sales()
    ours, theirs = copied(base), copied(base)
    settings(ours, 6)["join_input"]["how"] = "inner"
    settings(theirs, 6)["join_input"]["how"] = "outer"
    ours = remove_step(ours, 10)
    settings(theirs, 10)["pivot_input"]["aggregations"] = ["mean"]
    return base, ours, theirs


def test_a_clean_merge_writes_the_plain_flow():
    base = sales()
    ours, theirs = copied(base), copied(base)
    settings(theirs, 6)["join_input"]["how"] = "inner"
    document = conflict_document(merge(base, ours, theirs))
    assert not is_conflicted(document)
    assert settings(document, 6)["join_input"]["how"] == "inner"


def test_conflict_document_round_trips():
    base, ours, theirs = _two_conflicts()
    document = conflict_document(merge(base, ours, theirs), {"base": "b", "ours": "main", "theirs": "feature/x"})
    assert is_conflicted(document)
    assert "nodes" not in document, "no loader may read a conflicted flow as a flow"
    for text in (dump(document), json.dumps(document)):
        again = load(text)
        assert [c.key for c in open_conflicts(again)] == ["step:6", "step:10"]
        assert again["sides"]["theirs"] == "feature/x"


def test_refuse_conflicted_names_the_count():
    base, ours, theirs = _two_conflicts()
    document = conflict_document(merge(base, ours, theirs))
    with pytest.raises(UnresolvedMergeError, match="2 merge conflicts"):
        refuse_conflicted(document, "The flow file x.yaml")
    refuse_conflicted(sales(), "a plain flow")


def test_resolve_all_ours():
    base, ours, theirs = _two_conflicts()
    document = conflict_document(merge(base, ours, theirs))
    settled = resolve(document, {"step:6": "ours", "step:10": "ours"})
    assert not is_conflicted(settled)
    assert settings(settled, 6)["join_input"]["how"] == "inner"
    assert 10 not in [n["id"] for n in settled["nodes"]]


def test_resolve_theirs_brings_back_a_removed_step_and_its_readers():
    base, ours, theirs = _two_conflicts()
    document = conflict_document(merge(base, ours, theirs))
    settled = resolve(document, {"step:6": "theirs", "step:10": "theirs"})
    assert settings(settled, 6)["join_input"]["how"] == "outer"
    assert settings(settled, 10)["pivot_input"]["aggregations"] == ["mean"]
    assert step(settled, 11)["input_ids"] == [10], "the step that read the removed one reads it again"
    assert 11 in step(settled, 10)["outputs"]


def test_resolve_last_conflict_gives_plain_document():
    base, ours, theirs = _two_conflicts()
    document = conflict_document(merge(base, ours, theirs))
    half = resolve(document, {"step:6": "theirs"})
    assert is_conflicted(half) and [c.key for c in open_conflicts(half)] == ["step:10"]
    assert settings(half["merged"], 6)["join_input"]["how"] == "outer"
    done = resolve(half, {"step:10": "ours"})
    assert not is_conflicted(done) and "nodes" in done


def test_resolve_refuses_an_unknown_key():
    base, ours, theirs = _two_conflicts()
    document = conflict_document(merge(base, ours, theirs))
    with pytest.raises(KeyError, match="step:99"):
        resolve(document, {"step:99": "ours"})


def test_keep_ids_auto_keeps_the_production_branch(monkeypatch):
    monkeypatch.setattr("shared.flow_merge.cli._production_branch", lambda: "main")
    assert keep_ids_for("auto", "feature/x", "main") == "theirs"
    assert keep_ids_for("auto", "feature/x", "origin/main") == "theirs"
    assert keep_ids_for("auto", "main", "feature/x") == "ours"
    assert keep_ids_for("ours", "feature/x", "main") == "ours"


def _git(repo, *args, check=True):
    return subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True, check=check)


@pytest.fixture
def repo(tmp_path):
    if shutil.which("git") is None:
        pytest.skip("git not available")
    path = tmp_path / "repo"
    path.mkdir()
    _git(path, "init", "-q", "-b", "main")
    _git(path, "config", "user.email", "test@example.invalid")
    _git(path, "config", "user.name", "Test")
    driver = (
        f"{sys.executable} -m shared.flow_merge merge-file %O %A %B -P %P --base-label %S --ours-label %X --theirs-label %Y"
    )
    _git(path, "config", "merge.flowfile.driver", driver)
    (path / ".gitattributes").write_text("flows/*.flow.yaml merge=flowfile diff=flowfile\n")
    (path / "flows").mkdir()
    (path / "flows" / "sales.flow.yaml").write_text(dump(sales()))
    _git(path, "add", ".")
    _git(path, "commit", "-q", "-m", "base")
    return path


def _edit(repo, branch, change, *, new=False):
    _git(repo, "checkout", "-q", *(["-b"] if new else []), branch)
    flow_file = repo / "flows" / "sales.flow.yaml"
    flow_file.write_text(dump(change(load(flow_file.read_text()))))
    _git(repo, "commit", "-q", "-am", f"edit on {branch}")


def _env():
    return {**os.environ, "PYTHONPATH": str(REPO_ROOT) + os.pathsep + os.environ.get("PYTHONPATH", "")}


def test_git_merge_driver_end_to_end(repo, monkeypatch):
    """Two branches each add a step under id 16 and change another step: git merges the flow, then a conflict."""
    for key, value in _env().items():
        monkeypatch.setenv(key, value)

    def feature(doc):
        settings(doc, 6)["join_input"]["how"] = "inner"
        return add_step(doc, 16, "sort", {MAIN: [8]})

    def production(doc):
        settings(doc, 10)["pivot_input"]["aggregations"] = ["mean"]
        return add_step(doc, 16, "filter", {MAIN: [14]})

    _edit(repo, "feature", feature, new=True)
    _edit(repo, "main", production)
    merged = _git(repo, "merge", "-q", "--no-edit", "feature", check=False)
    assert merged.returncode == 0, merged.stdout + merged.stderr
    flow = load((repo / "flows" / "sales.flow.yaml").read_text())
    assert settings(flow, 6)["join_input"]["how"] == "inner"
    assert settings(flow, 10)["pivot_input"]["aggregations"] == ["mean"]
    assert step(flow, 16)["type"] == "filter", "main's step keeps its id"
    assert step(flow, 17)["type"] == "sort", "the feature's step moves"

    def other(doc):
        settings(doc, 6)["join_input"]["how"] = "outer"
        return doc

    _git(repo, "checkout", "-q", "-b", "second", "HEAD~1")
    _edit(repo, "second", other)
    _git(repo, "checkout", "-q", "main")
    clash = _git(repo, "merge", "-q", "--no-edit", "second", check=False)
    assert clash.returncode != 0
    assert "step:6" in clash.stdout + clash.stderr
    conflicted = load((repo / "flows" / "sales.flow.yaml").read_text())
    assert is_conflicted(conflicted)
    assert conflicted["sides"]["ours"] == "HEAD"
    settled = subprocess.run(
        [sys.executable, "-m", "shared.flow_merge", "resolve", "flows/sales.flow.yaml", "--take", "theirs"],
        cwd=repo,
        capture_output=True,
        text=True,
    )
    assert settled.returncode == 0, settled.stderr
    flow = load((repo / "flows" / "sales.flow.yaml").read_text())
    assert settings(flow, 6)["join_input"]["how"] == "outer"
    _git(repo, "commit", "-q", "-am", "merge second")


def test_check_fails_on_a_conflicted_file(tmp_path):
    base, ours, theirs = _two_conflicts()
    path = tmp_path / "x.flow.yaml"
    path.write_text(dump(conflict_document(merge(base, ours, theirs))))
    result = subprocess.run(
        [sys.executable, "-m", "shared.flow_merge", "check", str(path)],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
    )
    assert result.returncode == 1 and "2 merge conflicts" in result.stderr
    path.write_text(dump(sales()))
    assert (
        subprocess.run(
            [sys.executable, "-m", "shared.flow_merge", "check", str(path)], cwd=str(REPO_ROOT), capture_output=True
        ).returncode
        == 0
    )
