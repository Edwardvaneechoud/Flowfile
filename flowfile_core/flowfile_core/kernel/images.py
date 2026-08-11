"""Kernel image tags and their env overrides — pure, Docker-free.

Split out of ``manager.py`` so ``matching.py`` can ask which image tag a flavour
resolves to without importing the docker SDK. ``manager.py`` re-exports every
name here, so it stays the place to look for image *resolution* (which needs a
docker client); this module only knows the configured tags.
"""

import os

from flowfile_core.kernel.models import ImageFlavour

_KERNEL_IMAGE_BASE_DEFAULT = "edwardvaneechoud/flowfile-kernel-base:0.5.4"
_KERNEL_IMAGE_ML_DEFAULT = "edwardvaneechoud/flowfile-kernel-ml:0.5.4"
_KERNEL_IMAGE_LITE_DEFAULT = "edwardvaneechoud/flowfile-kernel-lite:0.5.4"


def _envvar_or_default(name: str, default: str) -> str:
    """Read an env var, treating unset OR empty/whitespace as 'use default'.

    Compose's ``${VAR:-}`` writes an empty string into the container when the
    host hasn't set the var; treat that the same as 'unset' so we fall back to
    the registry default instead of trying to ``docker run ""``.
    """
    return (os.environ.get(name) or "").strip() or default


# FLOWFILE_KERNEL_IMAGE is the legacy override for the base image (kept for
# backwards compatibility). FLOWFILE_KERNEL_IMAGE_BASE / _ML let an operator
# pin each flavour to a specific tag (or their own registry). Reads happen at
# lookup time, not module-import time, so the env var can be set after Python
# starts (e.g. by a container entrypoint, or a pytest step env block) without
# poisoning the rest of the process with the default value.
def _kernel_image_base() -> str:
    return _envvar_or_default(
        "FLOWFILE_KERNEL_IMAGE_BASE",
        _envvar_or_default("FLOWFILE_KERNEL_IMAGE", _KERNEL_IMAGE_BASE_DEFAULT),
    )


def _kernel_image_ml() -> str:
    return _envvar_or_default("FLOWFILE_KERNEL_IMAGE_ML", _KERNEL_IMAGE_ML_DEFAULT)


def _kernel_image_lite() -> str:
    return _envvar_or_default("FLOWFILE_KERNEL_IMAGE_LITE", _KERNEL_IMAGE_LITE_DEFAULT)


def _flavour_images() -> dict[ImageFlavour, str]:
    return {
        ImageFlavour.BASE: _kernel_image_base(),
        ImageFlavour.ML: _kernel_image_ml(),
        ImageFlavour.LITE: _kernel_image_lite(),
    }


def parse_image_version(image_tag: str) -> tuple[int, ...] | None:
    """Parse a numeric version tuple from an image tag's ``:version`` suffix.

    Returns None for tags without a dotted-numeric version (e.g. ``:local``,
    digests), so callers can skip update comparisons for non-release images.
    """
    if ":" not in image_tag:
        return None
    tag = image_tag.rsplit(":", 1)[1]
    parts = tag.split(".")
    try:
        return tuple(int(p) for p in parts)
    except ValueError:
        return None
