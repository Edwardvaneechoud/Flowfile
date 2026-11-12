"""The kinds of object a project holds, as one registry.

Each kind names the files it owns as one-level patterns (``flows/*.flow.yaml``, ``tables.yaml``), its label in
a change summary, the kinds it comes after, its entry cap and its export, ``export(ctx) -> {path: text}``. What
used to be listed by hand in several modules (the managed files git stages, the importer's file kinds and caps,
the change-summary labels, the files a full export writes and sweeps) is derived from here.

The module stays import-light: core's kinds live in ``core_kinds`` and are imported on the first call that
needs the registry, so ``manifest`` and ``git_ops`` can use it without importing the export code.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy.orm import Session

from flowfile_core.project.normalize import atomic_write


@dataclass
class ExportContext:
    """What a kind's export reads: the session, the project's owner and the kinds the project lists.

    ``kinds`` of ``None`` exports every registered kind. ``scope`` is carried for the kinds and read by none of
    core's. ``memo`` is shared by the kinds of one snapshot, so the schedule kind reuses the flow kind's file
    names."""

    db: Session
    owner_id: int
    kinds: frozenset[str] | None = None
    scope: object = None
    memo: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Kind:
    name: str
    label: str
    paths: tuple[str, ...]
    export: Callable[[ExportContext], dict[str, str]]
    after: tuple[str, ...] = ()
    cap: int | None = None
    entries_key: str | None = None
    tracked_by_data_flag: bool = False


_KINDS: dict[str, Kind] = {}
_core_loaded = False


def register(kind: Kind) -> None:
    _KINDS[kind.name] = kind


def registered() -> list[Kind]:
    global _core_loaded
    if not _core_loaded:
        from flowfile_core.project import core_kinds  # noqa: F401

        _core_loaded = True
    return list(_KINDS.values())


def get(name: str) -> Kind:
    for kind in registered():
        if kind.name == name:
            return kind
    raise KeyError(name)


def ordered() -> list[Kind]:
    """The kinds in registration order, each moved after the kinds its ``after`` names."""
    pending = registered()
    names = {k.name for k in pending}
    placed: list[Kind] = []
    done: set[str] = set()
    while pending:
        for kind in pending:
            if all(a in done or a not in names for a in kind.after):
                placed.append(kind)
                done.add(kind.name)
                pending.remove(kind)
                break
        else:
            raise ValueError(f"Project kinds order in a cycle: {sorted(k.name for k in pending)}")
    return placed


def _stem(pattern: str, rel: str) -> str | None:
    folder, _, name = pattern.rpartition("/")
    if "*" not in name:
        return name.split(".", 1)[0] if rel == pattern else None
    rel_folder, _, rel_name = rel.rpartition("/")
    suffix = name[1:]
    if rel_folder != folder or not rel_name.endswith(suffix):
        return None
    return rel_name[: -len(suffix)]


def kind_for_path(rel: str) -> tuple[Kind, str] | None:
    """The kind owning a repo-relative path and the path's stem, or ``None`` for a file no kind owns."""
    for kind in registered():
        for pattern in kind.paths:
            stem = _stem(pattern, rel)
            if stem is not None:
                return kind, stem
    return None


def unknown(names) -> list[str]:
    """The names no registered kind has, in their order."""
    known = {k.name for k in registered()}
    return [n for n in names if n not in known]


def managed_patterns() -> list[str]:
    return [p for kind in ordered() for p in kind.paths]


def snapshot(ctx: ExportContext) -> dict[str, str]:
    """Every kind's export as one ``{path: text}`` tree, nothing written."""
    tree: dict[str, str] = {}
    for kind in ordered():
        if ctx.kinds is not None and kind.name not in ctx.kinds:
            continue
        tree.update(kind.export(ctx))
    return tree


def owned_paths(root: Path) -> list[str]:
    """The repo-relative paths on disk that a registered pattern matches."""
    out: list[str] = []
    for pattern in managed_patterns():
        folder, _, name = pattern.rpartition("/")
        if "*" not in name:
            if (root / pattern).is_file():
                out.append(pattern)
            continue
        d = root / folder
        if d.is_dir():
            out += sorted(p.relative_to(root).as_posix() for p in d.glob(name) if p.is_file())
    return out


def read_tree(root: Path) -> dict[str, str]:
    """The files on disk that a registered pattern matches, as ``{path: text}``."""
    return {rel: (root / rel).read_text(encoding="utf-8") for rel in owned_paths(root)}


def write_tree(root: Path, tree: dict[str, str]) -> None:
    """Write every path of ``tree``, then remove each file a registered pattern matches that it does not hold.

    A file whose name differs only in letter case from one the tree holds is kept only when it is the same file
    as the one just written, as on a case-insensitive filesystem; on a case-sensitive one it is removed."""
    for rel, text in tree.items():
        atomic_write(root / rel, text)
    by_case = {rel.casefold(): rel for rel in tree}
    for rel in owned_paths(root):
        if rel in tree:
            continue
        twin = by_case.get(rel.casefold())
        if twin is not None and (root / rel).samefile(root / twin):
            continue
        (root / rel).unlink(missing_ok=True)
