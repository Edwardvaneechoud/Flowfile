"""
Tests for the kernel release guard (a kernel image change must not ship under a published version).
"""

import subprocess
import urllib.error

import pytest

from tools import check_kernel_release_needed as guard

PYPROJECT = '[tool.poetry]\nname = "kernel_runtime"\nversion = "{version}"\n\n[build-system]\nrequires = []\n'


def _git(repo, *args):
    return subprocess.run(
        ["git", "-c", "user.name=test", "-c", "user.email=test@example.com", "-c", "commit.gpgsign=false", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _commit(repo, files, message="change"):
    for rel, content in files.items():
        path = repo / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", message)


@pytest.fixture
def repo(tmp_path):
    """A repo whose first commit is the base: kernel 0.6.1 with a lock, source and tests."""
    _git(tmp_path, "init", "-q")
    _commit(
        tmp_path,
        {
            "kernel_runtime/pyproject.toml": PYPROJECT.format(version="0.6.1"),
            "kernel_runtime/poetry.lock": "fastapi 0.142.1\n",
            "kernel_runtime/kernel_runtime/main.py": "print('v1')\n",
            "kernel_runtime/tests/test_main.py": "def test_x():\n    pass\n",
        },
        "base",
    )
    return tmp_path


@pytest.fixture
def hub(monkeypatch):
    """Replace the Docker Hub check; set ``hub.published`` and inspect ``hub.calls``."""

    class Hub:
        def __init__(self):
            self.published = None
            self.calls = []

        def __call__(self, org, image, tag):
            self.calls.append((org, image, tag))
            if self.published is None:
                raise AssertionError("Docker Hub must not be queried")
            return self.published

    fake = Hub()
    monkeypatch.delenv("DOCKERHUB_ORG", raising=False)
    monkeypatch.setattr(guard, "_tag_exists", fake)
    return fake


def _run(repo, base):
    return guard.main(["--base", base, "--repo", str(repo)])


def test_no_inputs_changed_passes(repo, hub, capsys):
    base = _git(repo, "rev-parse", "HEAD")
    _commit(repo, {"kernel_runtime/tests/test_main.py": "def test_y():\n    pass\n", "kernel_runtime/README.md": "x\n"})

    assert _run(repo, base) == 0
    assert "no kernel image inputs changed" in capsys.readouterr().out
    assert hub.calls == []


def test_inputs_changed_with_version_bump_passes(repo, hub, capsys):
    base = _git(repo, "rev-parse", "HEAD")
    _commit(
        repo,
        {
            "kernel_runtime/poetry.lock": "fastapi 0.142.2\n",
            "kernel_runtime/pyproject.toml": PYPROJECT.format(version="0.6.2"),
        },
    )

    assert _run(repo, base) == 0
    assert "kernel version bumped 0.6.1 -> 0.6.2" in capsys.readouterr().out
    assert hub.calls == []


@pytest.mark.parametrize("changed", ["kernel_runtime/poetry.lock", "kernel_runtime/kernel_runtime/main.py"])
def test_inputs_changed_on_published_version_fails(repo, hub, capsys, changed):
    base = _git(repo, "rev-parse", "HEAD")
    _commit(repo, {changed: "changed\n"})
    hub.published = True

    assert _run(repo, base) == 1
    err = capsys.readouterr().err
    assert changed in err
    assert "make bump-version-kernel VERSION=0.6.2" in err
    assert "_KERNEL_IMAGE_*_DEFAULT" in err
    assert hub.calls == [("edwardvaneechoud", "flowfile-kernel-base", "0.6.1")]


def test_inputs_changed_on_unpublished_version_passes(repo, hub, capsys, monkeypatch):
    base = _git(repo, "rev-parse", "HEAD")
    _commit(repo, {"kernel_runtime/poetry.lock": "fastapi 0.142.2\n"})
    hub.published = False
    monkeypatch.setenv("DOCKERHUB_ORG", "someorg")

    assert _run(repo, base) == 0
    assert "0.6.1 is not published yet; it publishes on merge" in capsys.readouterr().out
    assert hub.calls == [("someorg", "flowfile-kernel-base", "0.6.1")]


def test_zero_sha_base_passes(repo, hub, capsys):
    _commit(repo, {"kernel_runtime/poetry.lock": "fastapi 0.142.2\n"})

    assert _run(repo, "0" * 40) == 0
    assert "all-zeros sha" in capsys.readouterr().out
    assert hub.calls == []


def test_unreachable_hub_fails_after_retries(monkeypatch):
    attempts = []

    def unreachable(url, timeout):
        attempts.append(url)
        raise urllib.error.URLError("network down")

    monkeypatch.setattr(guard.urllib.request, "urlopen", unreachable)
    monkeypatch.setattr(guard.time, "sleep", lambda _seconds: None)

    with pytest.raises(SystemExit, match="could not verify.*re-run"):
        guard._tag_exists("edwardvaneechoud", "flowfile-kernel-base", "0.6.1")
    assert len(attempts) == 3
