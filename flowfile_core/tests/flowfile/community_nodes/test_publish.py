"""Publish-bundle completeness + export tests.

Storage is redirected to tmp so the singleton registry / icons / screenshots never
touch the developer's ~/.flowfile. Nodes are written into the default nodes dir and
loaded through the real registry, exactly as the designer save path leaves them.

``_write_node`` / ``_valid_png`` / ``_add_icon`` / ``_add_screenshot`` and the
``isolated_storage`` fixture live in ``conftest`` so the community-github route
suite reuses the same designed-node setup.
"""
import io
import json
import zipfile

import pytest
from fastapi import HTTPException

from flowfile_core.flowfile.community_nodes import publish, validation
from flowfile_core.flowfile.community_nodes.models import CommunityManifest
from flowfile_core.routes import community_nodes as route_mod

from .conftest import _add_icon, _add_screenshot, _valid_png, _write_node


def _errors(issues) -> list[str]:
    return [i.code for i in issues if i.code not in publish.WARNING_CODES]


def _warnings(issues) -> list[str]:
    return [i.code for i in issues if i.code in publish.WARNING_CODES]


# ---------------------------------------------------------------- completeness


def test_healthy_complete_node_passes(isolated_storage):
    entry = _write_node()
    issues = publish.completeness_check(entry, license="MIT", repository="")
    assert _errors(issues) == []
    # Default icon + no screenshots surface as advisory warnings only.
    assert "ICON_DEFAULT" in _warnings(issues)
    assert "NO_SCREENSHOTS" in _warnings(issues)


def test_missing_author_fails(isolated_storage):
    entry = _write_node(author="")
    assert "AUTHOR_MISSING" in _errors(publish.completeness_check(entry, license="MIT", repository=""))


def test_bad_version_fails(isolated_storage):
    entry = _write_node(version="1.2")
    assert "VERSION_INVALID" in _errors(publish.completeness_check(entry, license="MIT", repository=""))


def test_bad_license_fails(isolated_storage):
    entry = _write_node()
    assert "LICENSE_INVALID" in _errors(publish.completeness_check(entry, license="GPL-3.0", repository=""))
    assert "LICENSE_INVALID" in _errors(publish.completeness_check(entry, license=None, repository=""))


def test_short_description_fails(isolated_storage):
    entry = _write_node(intro="too short")
    assert "DESCRIPTION_MISSING" in _errors(publish.completeness_check(entry, license="MIT", repository=""))


def test_missing_examples_blocks_publish(isolated_storage):
    # Community CI dry-runs persisted examples, so their absence must block export.
    entry = _write_node(with_examples=False)
    assert "EXAMPLES_MISSING" in _errors(publish.completeness_check(entry, license="MIT", repository=""))


def test_non_png_icon_is_error(isolated_storage):
    entry = _write_node(node_icon="my_icon.svg")
    assert "ICON_NOT_PNG" in _errors(publish.completeness_check(entry, license="MIT", repository=""))


def test_valid_png_icon_passes(isolated_storage):
    _add_icon("custom.png", _valid_png())
    entry = _write_node(node_icon="custom.png")
    issues = publish.completeness_check(entry, license="MIT", repository="")
    assert _errors(issues) == []
    assert "ICON_DEFAULT" not in _warnings(issues)


def test_screenshot_present_clears_warning(isolated_storage):
    entry = _write_node()
    _add_screenshot("mood_emoji", "shot.png", _valid_png())
    assert "NO_SCREENSHOTS" not in _warnings(publish.completeness_check(entry, license="MIT", repository=""))


# ---------------------------------------------------------------- publish file set


def _by_name(pf: publish.PublishFiles) -> dict[str, bytes]:
    return dict(pf.files)


def test_publish_files_excludes_how_to_publish(isolated_storage):
    _add_icon("mymood.png", _valid_png(48, 48))
    entry = _write_node(node_icon="mymood.png", author="octocat", version="2.1.0")
    _add_screenshot("mood_emoji", "a.png", _valid_png())
    _add_screenshot("mood_emoji", "b.png", _valid_png())

    pf = publish.build_publish_files(
        entry,
        license="Apache-2.0",
        repository="https://github.com/octocat/mood",
        description="A friendly mood emoji tagging node",
        category="Fun",
        screenshots=publish.png_screenshots(entry),
    )

    names = [name for name, _ in pf.files]
    assert names == [
        "nodes/mood_emoji/node.py",
        "nodes/mood_emoji/manifest.json",
        "nodes/mood_emoji/README.md",
        "nodes/mood_emoji/icon.png",
        "nodes/mood_emoji/screenshots/1.png",
        "nodes/mood_emoji/screenshots/2.png",
    ]
    assert all("HOW_TO_PUBLISH" not in name for name in names)
    assert pf.node_id == "mood_emoji"
    assert pf.node_name == "Mood Emoji"
    assert pf.version == "2.1.0"
    assert pf.used_placeholder is False

    files = _by_name(pf)
    # node.py is the verbatim on-disk bytes.
    assert files["nodes/mood_emoji/node.py"] == entry.file_path.read_bytes()
    # manifest.json is indent=2 + trailing newline + exclude_none.
    manifest_bytes = files["nodes/mood_emoji/manifest.json"]
    assert manifest_bytes.endswith(b"\n")
    assert b'\n  "id":' in manifest_bytes
    manifest = CommunityManifest.model_validate_json(manifest_bytes)
    assert manifest.id == "mood_emoji"
    assert manifest.version == "2.1.0"
    assert manifest.license == "Apache-2.0"
    assert manifest.category == "Fun"
    assert manifest.author.github == "octocat"
    assert manifest.icon == "icon.png"
    assert manifest.screenshots == ["screenshots/1.png", "screenshots/2.png"]
    readme = files["nodes/mood_emoji/README.md"].decode("utf-8")
    assert "# Mood Emoji" in readme
    assert "## What it does" in readme


def test_publish_files_default_icon_omitted(isolated_storage):
    entry = _write_node()  # default icon
    pf = publish.build_publish_files(
        entry, license="MIT", repository="", description="A mood emoji node for tables", category="Fun", screenshots=[]
    )
    names = [name for name, _ in pf.files]
    assert "nodes/mood_emoji/icon.png" not in names
    manifest = json.loads(_by_name(pf)["nodes/mood_emoji/manifest.json"])
    assert "icon" not in manifest  # exclude_none drops the absent icon


def test_publish_files_non_login_author_uses_placeholder(isolated_storage):
    entry = _write_node(author="Jane Doe")  # not a valid github login
    pf = publish.build_publish_files(
        entry, license="MIT", repository="", description="A mood emoji node for tables", category="Fun", screenshots=[]
    )
    assert pf.used_placeholder is True
    manifest = CommunityManifest.model_validate_json(_by_name(pf)["nodes/mood_emoji/manifest.json"])
    assert manifest.author.github == publish.GITHUB_PLACEHOLDER
    assert manifest.author.name == "Jane Doe"


def test_publish_files_author_github_override_kills_placeholder(isolated_storage):
    # A non-login node author would placeholder on the bundle path; the PR path
    # supplies the token login and keeps the raw author only as the display name.
    entry = _write_node(author="Jane Doe")
    pf = publish.build_publish_files(
        entry,
        license="MIT",
        repository="",
        description="A mood emoji node for tables",
        category="Fun",
        screenshots=[],
        author_github="jane-real",
    )
    assert pf.used_placeholder is False
    manifest = CommunityManifest.model_validate_json(_by_name(pf)["nodes/mood_emoji/manifest.json"])
    assert manifest.author.github == "jane-real"
    assert manifest.author.name == "Jane Doe"


def test_publish_files_screenshot_cap_honored(isolated_storage):
    entry = _write_node()
    for i in range(validation.SCREENSHOT_COUNT_MAX + 3):
        _add_screenshot("mood_emoji", f"s{i}.png", _valid_png())
    pf = publish.build_publish_files(
        entry,
        license="MIT",
        repository="",
        description="A mood emoji node for tables",
        category="Fun",
        screenshots=publish.png_screenshots(entry),
    )
    shots = [name for name, _ in pf.files if "/screenshots/" in name]
    assert shots == [
        f"nodes/mood_emoji/screenshots/{i + 1}.png" for i in range(validation.SCREENSHOT_COUNT_MAX)
    ]


# ---------------------------------------------------------------- bundle content


def test_bundle_content(isolated_storage):
    _add_icon("mymood.png", _valid_png(48, 48))
    entry = _write_node(node_icon="mymood.png", author="octocat", version="2.1.0")
    _add_screenshot("mood_emoji", "a.png", _valid_png())
    _add_screenshot("mood_emoji", "b.png", _valid_png())

    data = publish.build_bundle(
        entry,
        license="Apache-2.0",
        repository="https://github.com/octocat/mood",
        description="A friendly mood emoji tagging node",
        category="Fun",
        screenshots=publish.png_screenshots(entry),
    )

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        names = set(zf.namelist())
        assert "HOW_TO_PUBLISH.md" in names
        assert "nodes/mood_emoji/node.py" in names
        assert "nodes/mood_emoji/manifest.json" in names
        assert "nodes/mood_emoji/README.md" in names
        assert "nodes/mood_emoji/icon.png" in names
        assert "nodes/mood_emoji/screenshots/1.png" in names
        assert "nodes/mood_emoji/screenshots/2.png" in names

        node_py = zf.read("nodes/mood_emoji/node.py")
        assert node_py == entry.file_path.read_bytes()

        manifest = CommunityManifest.model_validate_json(zf.read("nodes/mood_emoji/manifest.json"))
        assert manifest.id == "mood_emoji"
        assert manifest.node_name == "Mood Emoji"
        assert manifest.version == "2.1.0"
        assert manifest.license == "Apache-2.0"
        assert manifest.category == "Fun"
        assert manifest.author.github == "octocat"
        assert manifest.icon == "icon.png"
        assert manifest.screenshots == ["screenshots/1.png", "screenshots/2.png"]

        readme = zf.read("nodes/mood_emoji/README.md").decode("utf-8")
        assert "# Mood Emoji" in readme
        assert "## What it does" in readme


def test_build_bundle_is_how_to_publish_plus_files(isolated_storage):
    # Zip parity: the download is exactly HOW_TO_PUBLISH.md prepended to build_publish_files.
    _add_icon("mymood.png", _valid_png(48, 48))
    entry = _write_node(node_icon="mymood.png", author="octocat", version="2.1.0")
    _add_screenshot("mood_emoji", "a.png", _valid_png())

    kwargs = dict(
        license="Apache-2.0",
        repository="https://github.com/octocat/mood",
        description="A friendly mood emoji tagging node",
        category="Fun",
        screenshots=publish.png_screenshots(entry),
    )
    pf = publish.build_publish_files(entry, **kwargs)
    data = publish.build_bundle(entry, **kwargs)

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        assert zf.namelist() == ["HOW_TO_PUBLISH.md"] + [name for name, _ in pf.files]
        for name, payload in pf.files:
            assert zf.read(name) == payload
        assert zf.read("HOW_TO_PUBLISH.md").decode("utf-8") == publish._how_to_publish(pf.node_id, pf.used_placeholder)


def test_bundle_default_icon_omits_icon(isolated_storage):
    entry = _write_node()  # default icon
    data = publish.build_bundle(
        entry, license="MIT", repository="", description="A mood emoji node for tables", category="Fun", screenshots=[]
    )
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        assert "nodes/mood_emoji/icon.png" not in zf.namelist()
        manifest = json.loads(zf.read("nodes/mood_emoji/manifest.json"))
        assert "icon" not in manifest  # exclude_none drops the absent icon


def test_bundle_non_login_author_uses_placeholder(isolated_storage):
    entry = _write_node(author="Jane Doe")  # not a valid github login
    data = publish.build_bundle(
        entry, license="MIT", repository="", description="A mood emoji node for tables", category="Fun", screenshots=[]
    )
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        manifest = CommunityManifest.model_validate_json(zf.read("nodes/mood_emoji/manifest.json"))
        assert manifest.author.github == publish.GITHUB_PLACEHOLDER
        assert manifest.author.name == "Jane Doe"
        how_to = zf.read("HOW_TO_PUBLISH.md").decode("utf-8")
        assert publish.GITHUB_PLACEHOLDER in how_to


# ---------------------------------------------------------------- PR text helpers


def test_pr_branch_name():
    assert publish.pr_branch_name("mood_emoji", "1.2.3") == "node/mood_emoji-v1.2.3"


def test_pr_title_new_vs_update():
    assert publish.pr_title("mood_emoji", "Mood Emoji", "1.0.0", is_update=False) == (
        "Add Mood Emoji (mood_emoji) v1.0.0"
    )
    assert publish.pr_title("mood_emoji", "Mood Emoji", "1.1.0", is_update=True) == (
        "Update Mood Emoji (mood_emoji) v1.1.0"
    )


def test_build_pr_body_new_node():
    body = publish.build_pr_body("mood_emoji", "1.0.0", is_update=False, description="Adds a mood emoji column")
    assert "`nodes/mood_emoji/`" in body
    assert "New node" in body
    assert "**What does it do?** Adds a mood emoji column" in body
    assert publish.SCAFFOLD_PR_TEMPLATE in body  # drift pointer for review
    # mechanical + honesty items pre-checked; the update-only item stays open on a new node.
    assert "- [x] I built and **dry-ran this node" in body
    assert "- [x] The manifest `author.github` is **my** GitHub login" in body
    assert "- [x] This PR adds/edits **only** `nodes/mood_emoji/`" in body
    assert "- [x] I added at least one **PNG** screenshot" in body
    assert "- [x] The node makes **no hidden network calls**" in body
    assert "- [x] I understand community nodes are **not sandboxed**" in body
    assert "- [ ] For an update:" in body


def test_build_pr_body_update():
    body = publish.build_pr_body("mood_emoji", "2.0.0", is_update=True, description="Adds a mood emoji column")
    assert "Update — bumped to v2.0.0" in body
    assert "- [x] For an update:" in body
    # Every box is pre-checked on the update path — no open items remain.
    assert "- [ ]" not in body


# ---------------------------------------------------------------- route


def test_route_unknown_file_404(isolated_storage):
    request = route_mod.PublishBundleRequest(file_name="does_not_exist.py", license="MIT")
    with pytest.raises(HTTPException) as exc:
        route_mod.publish_bundle(request, current_user=None)
    assert exc.value.status_code == 404


def test_route_incomplete_returns_422_payload(isolated_storage):
    _write_node(author="")
    request = route_mod.PublishBundleRequest(file_name="mood_emoji.py", license="MIT")
    with pytest.raises(HTTPException) as exc:
        route_mod.publish_bundle(request, current_user=None)
    assert exc.value.status_code == 422
    detail = exc.value.detail
    assert detail["error_code"] == "INCOMPLETE"
    codes = {issue["code"] for issue in detail["issues"]}
    assert "AUTHOR_MISSING" in codes
    assert all("severity" in issue for issue in detail["issues"])


def test_route_check_only_reports_without_zip(isolated_storage):
    _write_node(author="")
    incomplete = route_mod.publish_bundle(
        route_mod.PublishBundleRequest(file_name="mood_emoji.py", license="MIT", check_only=True),
        current_user=None,
    )
    assert incomplete["ok"] is False
    assert any(i["code"] == "AUTHOR_MISSING" and i["severity"] == "error" for i in incomplete["issues"])

    _write_node(author="octocat")  # overwrite healthy + complete
    complete = route_mod.publish_bundle(
        route_mod.PublishBundleRequest(file_name="mood_emoji.py", license="MIT", check_only=True),
        current_user=None,
    )
    assert complete["ok"] is True


def test_route_complete_returns_zip(isolated_storage):
    _write_node(author="octocat")
    response = route_mod.publish_bundle(
        route_mod.PublishBundleRequest(file_name="mood_emoji.py", license="MIT", category="Fun"),
        current_user=None,
    )
    assert response.media_type == "application/zip"
    assert "mood_emoji-bundle.zip" in response.headers["Content-Disposition"]
    with zipfile.ZipFile(io.BytesIO(response.body)) as zf:
        assert "nodes/mood_emoji/node.py" in zf.namelist()
