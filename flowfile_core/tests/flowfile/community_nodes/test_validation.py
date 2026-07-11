"""Bundle validation ladder tests: folder contract, identity, semver/authorization
gates, examples, security, media, and dependency policy.

These import the ``community_nodes`` package, which requires ``models.py`` to
import cleanly under pydantic v2.
"""

import json
from pathlib import Path

import pytest

from flowfile_core.flowfile.community_nodes.index_build import build_index
from flowfile_core.flowfile.community_nodes.validation import (
    _valid_dependency_spec,
    png_dimensions,
    validate_bundle,
)

from .conftest import VALID_ID, copy_valid_bundle, make_png


def codes(issues) -> set[str]:
    return {issue.code for issue in issues}


def edit_manifest(folder: Path, **changes) -> None:
    manifest = json.loads((folder / "manifest.json").read_text())
    manifest.update(changes)
    (folder / "manifest.json").write_text(json.dumps(manifest))


def edit_node(folder: Path, old: str, new: str) -> None:
    src = (folder / "node.py").read_text()
    assert old in src, f"pattern not found in node.py: {old!r}"
    (folder / "node.py").write_text(src.replace(old, new))


def index_at(tmp_path: Path, version: str = "1.0.0") -> object:
    """Build a CommunityIndex containing the valid node at ``version``."""
    repo = tmp_path / "reg_repo"
    folder = copy_valid_bundle(repo / "nodes", name=VALID_ID)
    if version != "1.0.0":
        edit_manifest(folder, version=version)
        edit_node(folder, 'version: str = "1.0.0"', f'version: str = "{version}"')
    return build_index(repo, previous=None, commit="c0", generated_at="2026-01-01T00:00:00Z")


# -- happy path ------------------------------------------------------------


def test_valid_bundle_passes(valid_bundle):
    report = validate_bundle(valid_bundle, categories=["Text", "Fun"], pr_author="edwardvaneechoud")
    assert report.ok, [(i.code, i.message) for i in report.errors]
    assert report.errors == []
    assert report.parse_mode == "designer"
    assert report.manifest is not None and report.manifest.id == VALID_ID
    assert report.capabilities == []
    assert report.scan is not None and report.scan.passed


# -- folder contract -------------------------------------------------------


def test_unexpected_file(valid_bundle):
    (valid_bundle / "extra.txt").write_text("nope")
    report = validate_bundle(valid_bundle)
    assert not report.ok
    assert "UNEXPECTED_FILE" in codes(report.errors)


def test_missing_node_py(valid_bundle):
    (valid_bundle / "node.py").unlink()
    report = validate_bundle(valid_bundle)
    assert "NODE_PY_MISSING" in codes(report.errors)


def test_missing_manifest(valid_bundle):
    (valid_bundle / "manifest.json").unlink()
    report = validate_bundle(valid_bundle)
    assert "MANIFEST_MISSING" in codes(report.errors)


def test_node_py_too_large(valid_bundle):
    edit_node(valid_bundle, "import polars as pl", "import polars as pl\n# " + "x" * 210_000)
    report = validate_bundle(valid_bundle)
    assert "NODE_PY_TOO_LARGE" in codes(report.errors)


# -- manifest --------------------------------------------------------------


def test_bad_manifest_invalid_license(valid_bundle):
    edit_manifest(valid_bundle, license="GPL-3.0")
    report = validate_bundle(valid_bundle)
    assert "MANIFEST_INVALID" in codes(report.errors)
    assert report.manifest is None


# -- identity --------------------------------------------------------------


def test_id_mismatch_folder_name(tmp_path):
    folder = copy_valid_bundle(tmp_path, name="wrong_folder")
    report = validate_bundle(folder)
    assert "ID_MISMATCH" in codes(report.errors)


def test_node_name_mismatch(valid_bundle):
    edit_manifest(valid_bundle, node_name="Different Name")
    report = validate_bundle(valid_bundle)
    assert "NODE_NAME_MISMATCH" in codes(report.errors)


def test_category_unknown(valid_bundle):
    report = validate_bundle(valid_bundle, categories=["Fun", "Other"])
    assert "CATEGORY_UNKNOWN" in codes(report.errors)


def test_blocklisted_id(valid_bundle):
    blocklist = {"blocked": [{"id": VALID_ID, "reason": "retired"}]}
    report = validate_bundle(valid_bundle, blocklist=blocklist)
    assert "ID_BLOCKED" in codes(report.errors)


# -- version / authorization rules -----------------------------------------


def test_version_not_incremented(valid_bundle, tmp_path):
    index = index_at(tmp_path, "1.0.0")
    report = validate_bundle(valid_bundle, index=index, pr_author="edwardvaneechoud")
    assert "VERSION_NOT_INCREMENTED" in codes(report.errors)


def test_version_bumped_authorized(valid_bundle, tmp_path):
    index = index_at(tmp_path, "1.0.0")
    edit_manifest(valid_bundle, version="1.1.0")
    edit_node(valid_bundle, 'version: str = "1.0.0"', 'version: str = "1.1.0"')
    report = validate_bundle(valid_bundle, index=index, pr_author="edwardvaneechoud")
    assert report.ok, [(i.code, i.message) for i in report.errors]


def test_unauthorized_update(valid_bundle, tmp_path):
    index = index_at(tmp_path, "1.0.0")
    edit_manifest(valid_bundle, version="1.1.0")
    edit_node(valid_bundle, 'version: str = "1.0.0"', 'version: str = "1.1.0"')
    report = validate_bundle(valid_bundle, index=index, pr_author="randomhacker")
    assert "UNAUTHORIZED_UPDATE" in codes(report.errors)


def test_unauthorized_new_node(valid_bundle):
    from flowfile_core.flowfile.community_nodes.models import CommunityIndex

    report = validate_bundle(valid_bundle, index=CommunityIndex(), pr_author="randomhacker")
    assert "UNAUTHORIZED_NEW_NODE" in codes(report.errors)


def test_new_node_author_matches(valid_bundle):
    from flowfile_core.flowfile.community_nodes.models import CommunityIndex

    report = validate_bundle(valid_bundle, index=CommunityIndex(), pr_author="edwardvaneechoud")
    assert report.ok, [(i.code, i.message) for i in report.errors]


# -- cross-checks (.py vs manifest) ----------------------------------------


def test_cross_version_mismatch(valid_bundle):
    edit_node(valid_bundle, 'version: str = "1.0.0"', 'version: str = "9.9.9"')
    report = validate_bundle(valid_bundle)
    assert "CROSS_VERSION_MISMATCH" in codes(report.errors)


def test_cross_author_mismatch_is_warning(valid_bundle):
    edit_node(valid_bundle, 'author: str = "edwardvaneechoud"', 'author: str = "someone-else"')
    report = validate_bundle(valid_bundle)
    assert report.ok  # author mismatch is a warning, not an error
    assert "CROSS_AUTHOR_MISMATCH" in codes(report.warnings)


# -- examples --------------------------------------------------------------


def test_missing_examples(valid_bundle):
    edit_node(
        valid_bundle,
        "    example_inputs: list[dict[str, list]] = [\n"
        '        {"city": ["  amsterdam ", "berlin  "], "country": [" nl", "de "]},\n'
        "    ]\n"
        "    example_settings: dict[str, dict] = {}\n",
        "",
    )
    report = validate_bundle(valid_bundle)
    assert "EXAMPLES_REQUIRED" in codes(report.errors)


# -- security --------------------------------------------------------------


def test_denied_scan(valid_bundle):
    edit_node(
        valid_bundle,
        "return inputs[0].with_columns(pl.col(pl.String).str.strip_chars())",
        "eval('1 + 1')\n        return inputs[0]",
    )
    report = validate_bundle(valid_bundle)
    assert "SECURITY_DENIED" in codes(report.errors)


# -- media -----------------------------------------------------------------


def test_svg_icon(valid_bundle):
    (valid_bundle / "icon.png").write_bytes(b'<svg xmlns="http://www.w3.org/2000/svg"></svg>')
    report = validate_bundle(valid_bundle)
    assert "SVG_FORBIDDEN" in codes(report.errors)


def test_icon_dimensions_too_large(valid_bundle):
    (valid_bundle / "icon.png").write_bytes(make_png(600, 600))
    report = validate_bundle(valid_bundle)
    assert "MEDIA_DIMENSIONS" in codes(report.errors)


def test_corrupt_png(valid_bundle):
    (valid_bundle / "icon.png").write_bytes(b"not a png at all")
    report = validate_bundle(valid_bundle)
    assert "INVALID_PNG" in codes(report.errors)


# -- dependency policy -----------------------------------------------------


def test_dependency_not_allowed_local(valid_bundle):
    edit_node(
        valid_bundle,
        'node_category: str = "Text"',
        'node_category: str = "Text"\n    dependencies: list[str] = ["numpy"]',
    )
    report = validate_bundle(valid_bundle)
    assert "DEPENDENCY_NOT_ALLOWED" in codes(report.errors)


def test_bad_dependency_spec_kernel(valid_bundle):
    edit_node(
        valid_bundle,
        'node_category: str = "Text"',
        'node_category: str = "Text"\n    environment: str = "kernel"\n'
        '    dependencies: list[str] = ["git+https://evil/pkg", "numpy>=1.0"]',
    )
    report = validate_bundle(valid_bundle)
    assert "INVALID_DEPENDENCY_SPEC" in codes(report.errors)
    assert "DEPENDENCY_NOT_ALLOWED" not in codes(report.errors)  # kernel env allows deps


# -- helpers ---------------------------------------------------------------


def test_png_dimensions_helper():
    assert png_dimensions(make_png(16, 24)) == (16, 24)
    assert png_dimensions(b"garbage") is None
    assert png_dimensions(b"") is None


@pytest.mark.parametrize(
    "spec,valid",
    [
        ("numpy", True),
        ("numpy==1.26.0", True),
        ("numpy>=1.0", True),
        ("scikit-learn", True),
        ("requests[security]", True),
        ("git+https://github.com/x/y", False),
        ("https://example.com/pkg.whl", False),
        ("--index-url=http://evil", False),
        ("./local_pkg", False),
        ("_privatepkg", False),
        (".hidden", False),
        ("numpy >= 1.0 evil", False),
    ],
)
def test_valid_dependency_spec(spec, valid):
    assert _valid_dependency_spec(spec) is valid


# -- dry-run (spawns a child python process; only shared + polars in the child) --


def test_bundle_dry_run_executes(valid_bundle):
    # NOTE: this spawns a real child `python -c` that imports only
    # shared.node_designer.loading + polars (never flowfile_core) and runs process().
    from flowfile_core.flowfile.community_nodes.dry_run_local import run_bundle_dry_run

    outcome = run_bundle_dry_run(valid_bundle, timeout_seconds=60)
    assert outcome.success, outcome.error
    assert outcome.output_names == ["main"]
    assert outcome.duration_ms > 0
