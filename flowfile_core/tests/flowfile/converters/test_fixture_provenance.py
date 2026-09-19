"""Every committed `.yxmd` fixture must be hand-written, never a workflow saved by Alteryx Designer.

Designer's samples are proprietary and this repo is MIT (see fixtures/README.md). A real Designer save
carries markers no hand-written fixture needs; any of them in a committed fixture fails this test.
"""

from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"

# Strings a Designer-saved workflow writes and a hand-written fixture never needs.
DESIGNER_MARKERS = (
    "Documents and Settings",  # an Alteryx employee's machine path in the sample tree
    "SampleData\\",  # Designer's install-relative sample data tree
    "WorkflowId value",  # Designer-generated workflow GUID
    "OriginWorkflowId",  # Designer telemetry lineage
    "Program Files\\Alteryx\\Samples",
    "One Tool Example",
)


@pytest.mark.parametrize("fixture", sorted(FIXTURES.glob("*.yxmd")), ids=lambda p: p.name)
def test_fixture_is_not_a_designer_sample(fixture: Path):
    text = fixture.read_text(encoding="utf-8", errors="replace")
    found = [marker for marker in DESIGNER_MARKERS if marker in text]
    assert not found, (
        f"{fixture.name} looks like an Alteryx Designer save (markers: {found}); replace it with a hand-written fixture"
    )


def test_no_alteryx_binary_or_package_files_committed():
    repo_root = Path(__file__).resolve().parents[4]
    stray = [
        p.relative_to(repo_root)
        for ext in ("yxmc", "yxwz", "yxdb", "yxzp", "yxi")
        for p in repo_root.rglob(f"*.{ext}")
        if "node_modules" not in p.parts and ".venv" not in p.parts
    ]
    assert not stray, f"Alteryx macro/data/package files must not be committed: {stray}"
