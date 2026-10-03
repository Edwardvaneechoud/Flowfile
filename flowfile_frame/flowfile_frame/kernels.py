"""``ff.kernels``: the kernels you created in the Designer, by id, to pass as ``kernel=``."""

from __future__ import annotations

import builtins
from collections.abc import Iterator
from typing import NamedTuple

from flowfile_frame import _metadata
from flowfile_frame.native import NativeNodeError


class KernelLookupError(NativeNodeError, KeyError):
    """An ``ff.kernels[id]`` lookup of an id the current user has no kernel for; also a ``KeyError``."""

    def __str__(self) -> str:
        # KeyError.__str__ would wrap the message in quotes.
        return Exception.__str__(self)


class KernelInfo(NamedTuple):
    """One saved kernel definition; as ``kernel=`` it places the node on that kernel (its ``id`` is stored)."""

    id: str
    name: str
    flavour: str
    packages: list[str]


def _saved_kernels() -> builtins.list[KernelInfo]:
    """The current user's saved kernels, sorted by id; no container state, so Docker need not run.

    Read from the catalog database, or in a notebook kernel session from core (``_metadata.kernels``).
    """
    infos = [KernelInfo(k.id, k.name, k.flavour, builtins.list(k.packages)) for k in _metadata.kernels()]
    return sorted(infos, key=lambda info: info.id)


def _unknown_kernel_message(kernel_id: object, known: builtins.list[str]) -> str:
    have = f"your kernels are {', '.join(map(repr, known))}" if known else "you have no kernels yet"
    return f"No kernel {kernel_id!r}; {have}. Kernels are created in the Designer, on the Python Kernels page."


class Kernels:
    """The kernels you created in the Designer (``ff.kernels``): saved definitions by id, not running state.

    Look one up with ``ff.kernels["ml-kernel"]``; there is no attribute access because kernel ids
    can contain hyphens. Every call reads the saved kernels again (the catalog DB, or the app in a
    notebook kernel session), so a kernel created after import is listed and Docker need not run. A
    :class:`KernelInfo` works as any ``kernel=`` argument.
    """

    def list(self) -> builtins.list[KernelInfo]:
        """Your saved kernels, sorted by id."""
        return _saved_kernels()

    def get(self, kernel_id: str) -> KernelInfo:
        """The saved kernel with this id; an unknown id raises :class:`KernelLookupError` naming your ids."""
        kernels = {info.id: info for info in self.list()}
        if isinstance(kernel_id, str) and kernel_id in kernels:
            return kernels[kernel_id]
        raise KernelLookupError(_unknown_kernel_message(kernel_id, builtins.list(kernels)))

    def __getitem__(self, kernel_id: str) -> KernelInfo:
        return self.get(kernel_id)

    def __contains__(self, kernel_id: object) -> bool:
        return isinstance(kernel_id, str) and any(info.id == kernel_id for info in self.list())

    def __iter__(self) -> Iterator[str]:
        return iter([info.id for info in self.list()])

    def __len__(self) -> int:
        return len(self.list())

    def __repr__(self) -> str:
        return f"ff.kernels({builtins.list(self)})"


kernels: Kernels = Kernels()
