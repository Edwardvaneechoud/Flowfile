"""
Tests for the docker-publish matrices: which images a run publishes, under which tags, from which inputs.
"""

import json

import pytest

from tools import docker_publish_matrix as matrix

PYPROJECT = '[tool.poetry]\nname = "{name}"\nversion = "{version}"\n'


def _outputs(path) -> dict[str, str]:
    pairs = (line.split("=", 1) for line in path.read_text(encoding="utf-8").splitlines() if line)
    return dict(pairs)


@pytest.fixture
def run(tmp_path, monkeypatch):
    """Run ``main()`` against an app 0.22.1 / kernel 0.6.2 checkout with Docker Hub answered by ``published``."""
    (tmp_path / "kernel_runtime").mkdir()
    (tmp_path / "pyproject.toml").write_text(PYPROJECT.format(name="flowfile", version="0.22.1"))
    (tmp_path / "kernel_runtime" / "pyproject.toml").write_text(PYPROJECT.format(name="kernel_runtime", version="0.6.2"))
    monkeypatch.setattr(matrix, "ROOT", tmp_path)
    monkeypatch.setenv("DOCKERHUB_ORG", "someorg")
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    output = tmp_path / "github_output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))

    def _run(*, publish_app: bool, published: bool, force_kernel: bool = False):
        monkeypatch.setenv("PUBLISH_APP", "true" if publish_app else "false")
        monkeypatch.setenv("FORCE_KERNEL", "true" if force_kernel else "false")
        monkeypatch.setattr(matrix, "_tag_exists", lambda org, image, tag: published)
        assert matrix.main() == 0
        out = _outputs(output)
        return {key: json.loads(out[key])["include"] if key.endswith("matrix") else out[key] for key in out}

    return _run


def test_tag_push_publishes_app_images_and_the_notebook_image(run):
    out = run(publish_app=True, published=True)

    assert {c["image"] for c in out["build_matrix"]} == {"flowfile-core", "flowfile-frontend", "flowfile-worker"}
    assert {c["version"] for c in out["build_matrix"]} == {"0.22.1"}
    assert out["has_work"] == "true" and out["has_notebook_work"] == "true"

    notebook = out["notebook_build_matrix"]
    assert {c["image"] for c in notebook} == {"flowfile-kernel-notebook"}
    assert {c["platform"] for c in notebook} == {"linux/amd64", "linux/arm64"}
    cell = notebook[0]
    assert cell["dockerfile"] == "./kernel_runtime/Dockerfile.notebook"
    assert cell["context"] == "./build/notebook_kernel"
    assert cell["version"] == "0.22.1"
    # No org in any output: GitHub drops outputs that contain a secret's value.
    assert cell["base_image"] == "flowfile-kernel-lite:0.6.2"
    assert out["notebook_merge_matrix"] == [
        {"image": "flowfile-kernel-notebook", "version": "0.22.1", "tags": "0.22.1 latest"}
    ]


def test_kernel_path_push_publishes_only_missing_kernel_images(run):
    out = run(publish_app=False, published=False)

    assert {c["image"] for c in out["build_matrix"]} == {
        "flowfile-kernel-base",
        "flowfile-kernel-ml",
        "flowfile-kernel-lite",
    }
    assert {c["version"] for c in out["build_matrix"]} == {"0.6.2"}
    lite = next(c for c in out["build_matrix"] if c["image"] == "flowfile-kernel-lite")
    assert lite["slim_constraints"] == "true" and lite["extras"] == ""
    assert out["notebook_build_matrix"] == [] and out["has_notebook_work"] == "false"


def test_nothing_to_publish_when_kernels_exist_and_no_app_release(run):
    out = run(publish_app=False, published=True)

    assert out["build_matrix"] == [] and out["has_work"] == "false"
    assert out["notebook_build_matrix"] == [] and out["has_notebook_work"] == "false"


def test_force_kernel_republishes_existing_tags(run):
    out = run(publish_app=False, published=True, force_kernel=True)

    assert len(out["build_matrix"]) == 3 * len(matrix.PLATFORMS)


def test_prerelease_versions_get_no_latest_tag():
    assert matrix._tags_for("0.23.0-rc1") == ["0.23.0-rc1"]
    assert matrix._tags_for("0.23.0") == ["0.23.0", "latest"]
