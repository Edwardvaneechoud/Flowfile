"""The 64-bit Polars runtime ships in the built artifacts only, at the locked Polars version.

The package takes a Polars range, so a ``pip install flowfile`` sits beside the user's own Polars. The desktop bundle
and the Docker images install the ``runtime64`` group, whose exact ``polars-runtime-64`` pin must be the locked
``polars``: a runtime of another version is skipped with only a warning and Polars loads the 32-bit one.
"""

import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _pyproject() -> dict:
    return tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["tool"]["poetry"]


def _locked_versions() -> dict[str, str]:
    lock = tomllib.loads((ROOT / "poetry.lock").read_text(encoding="utf-8"))
    return {package["name"]: package["version"] for package in lock["package"]}


def test_package_metadata_carries_no_polars_runtime():
    dependencies = _pyproject()["dependencies"]
    assert isinstance(dependencies["polars"], str), "polars takes a plain range, with no extras such as rt64"
    assert "==" not in dependencies["polars"]
    assert "polars-runtime-64" not in dependencies


def test_runtime64_group_pins_the_locked_polars():
    group = _pyproject()["group"]["runtime64"]
    assert group.get("optional") is True
    pinned = group["dependencies"]["polars-runtime-64"]
    locked = _locked_versions()
    assert (
        pinned == f"=={locked['polars']}"
    ), f"runtime64 pins polars-runtime-64 {pinned}, but the lock has polars {locked['polars']}; move them together"
    assert locked["polars-runtime-64"] == locked["polars"]
