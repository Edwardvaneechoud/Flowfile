"""What makes a kernel able to run the canvas notebook: the ``flowfile`` package among its packages."""

from __future__ import annotations

import re

_REQUIREMENT_NAME = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)")


def _package_name(spec: str) -> str:
    match = _REQUIREMENT_NAME.match(spec)
    return re.sub(r"[-_.]+", "-", match.group(1)).lower() if match else ""


def is_notebook_kernel(packages: list[str] | None) -> bool:
    """Whether ``packages`` (pip specs, as ``KernelConfig.packages`` holds them) install ``flowfile``."""
    return any(_package_name(spec) == "flowfile" for spec in packages or [])


def is_notebook_kernel_config(kernel) -> bool:
    """Whether a ``KernelConfig``/``KernelInfo`` can run the notebook.

    A custom image cannot list ``flowfile`` in its packages when it bakes it in (the dev image from
    ``make notebook_kernel_dev``), so an image tag containing ``notebook`` counts too.
    """
    if is_notebook_kernel(getattr(kernel, "packages", None)):
        return True
    return "notebook" in (getattr(kernel, "custom_image", None) or "").lower()
