"""Keep a package unimportable for a block, so a test can show that the code it runs never imports it."""

from __future__ import annotations

import importlib.abc
import sys
from collections.abc import Iterator
from contextlib import contextmanager


class _Refuse(importlib.abc.MetaPathFinder):
    def __init__(self, package: str) -> None:
        self.package = package
        self.attempts: list[str] = []

    def find_spec(self, fullname, path=None, target=None):
        if fullname == self.package or fullname.startswith(f"{self.package}."):
            self.attempts.append(fullname)
            raise ImportError(f"{fullname} must not be imported here")
        return None


def _loaded(package: str) -> list[str]:
    return [name for name in sys.modules if name == package or name.startswith(f"{package}.")]


@contextmanager
def unimportable(package: str) -> Iterator[list[str]]:
    """Take ``package`` and its submodules out of ``sys.modules`` and refuse importing them until the block ends.

    Yields the list of refused imports (a caught ``ImportError`` still lands there). The modules
    are put back afterwards, so objects taken from them before the block stay valid.
    """
    saved = {name: sys.modules.pop(name) for name in _loaded(package)}
    finder = _Refuse(package)
    sys.meta_path.insert(0, finder)
    try:
        yield finder.attempts
    finally:
        sys.meta_path.remove(finder)
        for name in _loaded(package):
            del sys.modules[name]
        sys.modules.update(saved)
