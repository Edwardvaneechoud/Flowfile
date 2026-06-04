"""Resolve the locked dependency versions for each kernel image flavour.

The truth is ``kernel_runtime/poetry.lock`` (which is the same source the
Docker build uses to install pinned dependencies). Reading it at startup
keeps the API free of duplicated, drift-prone constants.
"""

import logging
import re
from pathlib import Path

from flowfile_core.kernel.models import ImageFlavour

logger = logging.getLogger(__name__)

# kernel/ -> flowfile_core/ -> flowfile_core/ -> repo root -> kernel_runtime/poetry.lock
_KERNEL_LOCK_PATH = (
    Path(__file__).resolve().parents[3] / "kernel_runtime" / "poetry.lock"
)

# Packages we surface to the user. Order matters — it drives display order.
_BASE_PACKAGE_NAMES: tuple[str, ...] = (
    "polars",
    "pyarrow",
    "numpy",
    "fastapi",
    "uvicorn",
    "httpx",
    "cloudpickle",
    "joblib",
)
_ML_EXTRA_PACKAGE_NAMES: tuple[str, ...] = (
    "scikit-learn",
    "xgboost",
    "lightgbm",
    "statsmodels",
    "polars-ds",
)
# Only the packages users actually import in flow code. The kernel image
# also has fastapi/uvicorn/httpx (to serve the runtime HTTP API) and
# cloudpickle/joblib (for artifact persistence) baked and pinned via the
# SLIM_CONSTRAINTS whitelist in kernel_runtime/Dockerfile — but those are
# kernel plumbing, not something users build on top of, so we don't surface
# them as part of the lite "guarantee".
_LITE_PACKAGE_NAMES: tuple[str, ...] = (
    "polars",
)

_NAME_RE = re.compile(r'^name = "([^"]+)"', re.MULTILINE)
_VERSION_RE = re.compile(r'^version = "([^"]+)"', re.MULTILINE)


def _load_locked_versions() -> dict[str, str]:
    """Parse ``poetry.lock`` and return ``{lowercase-name: version}``.

    Returns an empty dict if the lockfile cannot be read; callers fall back to
    showing package names without version numbers.
    """
    if not _KERNEL_LOCK_PATH.exists():
        logger.warning("Kernel poetry.lock not found at %s", _KERNEL_LOCK_PATH)
        return {}
    try:
        text = _KERNEL_LOCK_PATH.read_text(encoding="utf-8")
    except OSError as exc:
        logger.warning("Could not read kernel poetry.lock: %s", exc)
        return {}

    versions: dict[str, str] = {}
    for block in text.split("[[package]]"):
        name_match = _NAME_RE.search(block)
        version_match = _VERSION_RE.search(block)
        if name_match and version_match:
            versions[name_match.group(1).lower()] = version_match.group(1)
    return versions


def get_flavour_packages() -> dict[ImageFlavour, list[tuple[str, str]]]:
    """Return ``{flavour: [(name, version), ...]}`` for each image flavour."""
    versions = _load_locked_versions()
    base = [
        (name, versions.get(name.lower(), "—"))
        for name in _BASE_PACKAGE_NAMES
    ]
    ml = base + [
        (name, versions.get(name.lower(), "—"))
        for name in _ML_EXTRA_PACKAGE_NAMES
    ]
    # Lite bakes the same packages as base but only ``_LITE_PACKAGE_NAMES``
    # are pinned in /opt/constraints.txt — pyarrow / numpy float so user
    # installs aren't blocked by transitive pins.
    lite = [
        (name, versions.get(name.lower(), "—"))
        for name in _LITE_PACKAGE_NAMES
    ]
    return {
        ImageFlavour.BASE: base,
        ImageFlavour.ML: ml,
        ImageFlavour.LITE: lite,
        ImageFlavour.CUSTOM: [],
    }
