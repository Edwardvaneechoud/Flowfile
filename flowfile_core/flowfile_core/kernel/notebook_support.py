"""What makes a kernel able to run the canvas notebook: the ``flowfile`` package among its packages."""

from __future__ import annotations

import re

from packaging.utils import canonicalize_name

_REQUIREMENT_NAME = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)")


def _package_name(spec: str) -> str:
    match = _REQUIREMENT_NAME.match(spec)
    return canonicalize_name(match.group(1)) if match else ""


def is_notebook_kernel_config(kernel) -> bool:
    """Whether a ``KernelConfig``/``KernelInfo`` can run the notebook: a pip spec in its packages installs ``flowfile``.

    A custom image cannot list ``flowfile`` in its packages when it bakes it in (the dev image from
    ``make notebook_kernel_dev``), so an image tag containing ``notebook`` counts too.
    """
    if any(_package_name(spec) == "flowfile" for spec in getattr(kernel, "packages", None) or []):
        return True
    return "notebook" in (getattr(kernel, "custom_image", None) or "").lower()
