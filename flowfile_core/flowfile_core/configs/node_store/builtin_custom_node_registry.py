"""Discovery of built-in ``CustomNodeBase`` subclasses.

Built-in custom nodes ship with the Flowfile package itself (under
``flowfile_core.flowfile.builtin_custom_nodes``) rather than from the user's
data directory. We discover them by importing the package and walking its
submodules — no filesystem scan or icon lookup needed (icons are bundled
via the standard frontend assets).
"""

from __future__ import annotations

import importlib
import inspect
import logging
import pkgutil

from flowfile_core.flowfile import builtin_custom_nodes
from flowfile_core.flowfile.node_designer.custom_node import CustomNodeBase

logger = logging.getLogger(__name__)


def get_all_builtin_custom_nodes() -> dict[str, type[CustomNodeBase]]:
    """Return ``{item: NodeClass}`` for every CustomNodeBase under the package."""
    discovered: dict[str, type[CustomNodeBase]] = {}

    for module_info in pkgutil.walk_packages(
        builtin_custom_nodes.__path__,
        prefix=builtin_custom_nodes.__name__ + ".",
    ):
        try:
            module = importlib.import_module(module_info.name)
        except Exception as exc:
            logger.warning("Failed to import builtin custom node module %s: %s", module_info.name, exc)
            continue

        for _, obj in inspect.getmembers(module, inspect.isclass):
            if obj is CustomNodeBase or not issubclass(obj, CustomNodeBase):
                continue
            if obj.__module__ != module.__name__:
                continue
            try:
                key = obj().item
            except Exception as exc:
                logger.warning("Failed to instantiate builtin custom node %s: %s", obj.__name__, exc)
                continue
            discovered[key] = obj

    return discovered
