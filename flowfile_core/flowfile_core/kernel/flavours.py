"""Report what each kernel image flavour contains.

The truth is ``kernel_image_manifest.json``, generated from
``kernel_runtime/{pyproject.toml,poetry.lock,Dockerfile}`` by
``tools/generate_kernel_manifest.py`` and committed as package data next to this
module. It is read package-relative on purpose: the previous implementation
walked up to the repo root for ``kernel_runtime/poetry.lock``, which exists only
in a checkout, so the wheel, the Docker image and the PyInstaller sidecar all
silently lost their baseline and reported every dependency as satisfied.

Run ``make kernel_manifest`` after touching kernel_runtime's dependencies;
``make check_kernel_manifest`` fails CI when the committed file drifts.
"""

import functools
import json
import logging
import sys
from pathlib import Path

from flowfile_core.kernel.images import _flavour_images, parse_image_version
from flowfile_core.kernel.models import ImageFlavour

logger = logging.getLogger(__name__)

_MANIFEST_NAME = "kernel_image_manifest.json"


def _manifest_path() -> Path:
    """Locate the manifest in a checkout, a wheel, a Docker image or a frozen bundle.

    The package-relative path covers all four; the ``sys.frozen`` branch mirrors
    ``database/migration.py``'s handling for builds where ``__file__`` is not
    usable on its own.
    """
    if getattr(sys, "frozen", False):
        frozen = Path(getattr(sys, "_MEIPASS", "")) / "flowfile_core" / "kernel" / _MANIFEST_NAME
        if frozen.is_file():
            return frozen
    return Path(__file__).resolve().parent / _MANIFEST_NAME


_MANIFEST_PATH = _manifest_path()

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

_WARNED_TAGS: set[str] = set()

_FLAVOUR_KEYS: dict[str, ImageFlavour] = {
    "base": ImageFlavour.BASE,
    "ml": ImageFlavour.ML,
    "lite": ImageFlavour.LITE,
}


@functools.lru_cache(maxsize=1)
def load_manifest() -> dict | None:
    """Parse the shipped manifest, or ``None`` when it is missing or malformed.

    ``None`` is the honest "we have no baseline" answer — callers must treat it
    as unknowable, never as an empty image.
    """
    if not _MANIFEST_PATH.is_file():
        logger.warning(
            "Kernel image manifest not found at %s — kernel dependency checks cannot prove "
            "a package is missing and will report every dependency as unknown. "
            "Run 'make kernel_manifest' and make sure the file ships with the package.",
            _MANIFEST_PATH,
        )
        return None
    try:
        manifest = json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        logger.warning("Kernel image manifest at %s is unreadable (%s)", _MANIFEST_PATH, exc)
        return None
    if not isinstance(manifest, dict) or not isinstance(manifest.get("flavours"), dict):
        logger.warning("Kernel image manifest at %s has an unexpected shape", _MANIFEST_PATH)
        return None
    return manifest


def _describes_configured_image(flavour: ImageFlavour, manifest_version: str) -> bool:
    """Whether the manifest describes the image tag this flavour actually launches.

    An operator can repoint a flavour with ``FLOWFILE_KERNEL_IMAGE_*``. Only a
    tag carrying a *different* dotted-numeric version disproves the manifest —
    a ``:local`` or digest tag is unparseable and stays trusted, since that is
    the dev-built image from this same checkout.
    """
    tag = _flavour_images().get(flavour)
    if not tag:
        return False
    tag_version = parse_image_version(tag)
    if tag_version is None:
        return True
    expected = parse_image_version(f"kernel:{manifest_version}")
    if expected is None:
        return True
    if tag_version != expected:
        # flavour_contents() is uncached (env overrides are read per call), so
        # dedupe or the /kernels/flavours route logs this on every request.
        if tag not in _WARNED_TAGS:
            _WARNED_TAGS.add(tag)
            logger.warning(
                "Kernel image %s is version %s but the shipped manifest describes %s; "
                "treating that flavour's contents as unknown.",
                tag,
                ".".join(str(part) for part in tag_version),
                manifest_version,
            )
        return False
    return True


def flavour_contents() -> dict[ImageFlavour, dict[str, str | None]] | None:
    """``{flavour: {canonical_name: version | None}}`` for the baked images.

    ``None`` means no manifest at all. A flavour *absent* from the returned dict
    is one we cannot vouch for (its configured image is a different release), so
    absence must never be read as "that flavour installs nothing".
    """
    manifest = load_manifest()
    if manifest is None:
        return None
    version = str(manifest.get("kernel_version", ""))
    contents: dict[ImageFlavour, dict[str, str | None]] = {}
    for key, flavour in _FLAVOUR_KEYS.items():
        packages = manifest["flavours"].get(key)
        if not isinstance(packages, dict):
            continue
        if not _describes_configured_image(flavour, version):
            continue
        contents[flavour] = dict(packages)
    return contents


def get_flavour_packages() -> dict[ImageFlavour, list[tuple[str, str]]]:
    """Return ``{flavour: [(name, version), ...]}`` for each image flavour."""
    contents = flavour_contents() or {}

    def curated(flavour: ImageFlavour, names: tuple[str, ...]) -> list[tuple[str, str]]:
        versions = contents.get(flavour, {})
        return [(name, versions.get(name) or "—") for name in names]

    base = curated(ImageFlavour.BASE, _BASE_PACKAGE_NAMES)
    ml = curated(ImageFlavour.ML, _BASE_PACKAGE_NAMES + _ML_EXTRA_PACKAGE_NAMES)
    lite = curated(ImageFlavour.LITE, _LITE_PACKAGE_NAMES)
    return {
        ImageFlavour.BASE: base,
        ImageFlavour.ML: ml,
        ImageFlavour.LITE: lite,
        ImageFlavour.CUSTOM: [],
    }
