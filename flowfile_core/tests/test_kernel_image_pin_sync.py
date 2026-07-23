"""The pinned kernel image tags in manager.py must match the kernel runtime
version (kernel_runtime/pyproject.toml). docker-publish.yml publishes images at
that version on merge to main, so the tags core pulls should track it — bump the
pins in the same change as `make bump-version-kernel`.
"""
import re
from pathlib import Path

from flowfile_core.kernel import manager as kernel_manager

KERNEL_PYPROJECT = Path(__file__).resolve().parents[2] / "kernel_runtime" / "pyproject.toml"


def _kernel_version() -> str:
    text = KERNEL_PYPROJECT.read_text(encoding="utf-8")
    section = re.search(r"^\[tool\.poetry\](.*?)(?=^\[|\Z)", text, re.MULTILINE | re.DOTALL)
    assert section, "no [tool.poetry] section in kernel_runtime/pyproject.toml"
    match = re.search(r'^version\s*=\s*"([^"]+)"', section.group(1), re.MULTILINE)
    assert match, "no version in [tool.poetry]"
    return match.group(1)


def test_pinned_kernel_images_match_kernel_version():
    version = _kernel_version()
    pins = {
        "base": kernel_manager._KERNEL_IMAGE_BASE_DEFAULT,
        "ml": kernel_manager._KERNEL_IMAGE_ML_DEFAULT,
        "lite": kernel_manager._KERNEL_IMAGE_LITE_DEFAULT,
    }
    mismatched = {flavour: tag for flavour, tag in pins.items() if not tag.endswith(f":{version}")}
    assert not mismatched, (
        f"kernel image pins in manager.py are out of sync with kernel version {version}: {mismatched}. "
        "Bump the pins when you bump the kernel version (docker-publish.yml publishes images at that version)."
    )
