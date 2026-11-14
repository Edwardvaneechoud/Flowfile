"""The kinds of object a project holds, as one registry.

Each kind names the files it owns as one-level patterns (``flows/*.flow.yaml``, ``tables.yaml``), its label in
a change summary, the kinds it comes after, its entry cap and its export, ``export(ctx) -> {path: text}``. What
used to be listed by hand in several modules (the managed files git stages, the importer's file kinds and caps,
the change-summary labels, the files a full export writes and sweeps) is derived from here.

The module stays import-light: core's kinds live in ``core_kinds`` and are imported on the first call that
needs the registry, so ``manifest`` and ``git_ops`` can use it without importing the export code. A package adds a
kind through the ``flowfile.project_kinds`` entry-point group.
"""

from __future__ import annotations

import importlib.metadata
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy.orm import Session

from flowfile_core.project.normalize import atomic_write


@dataclass
class ExportContext:
    """What a kind's export reads: the session, the project's owner and the kinds the project lists.

    ``kinds`` of ``None`` exports every registered kind. ``memo`` lives as long as the context: the kinds of one
    snapshot share it, so the schedule kind reuses the flow kind's file names, and the deletes of one kind share
    what they looked up."""

    db: Session
    owner_id: int
    kinds: frozenset[str] | None = None
    memo: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Entry:
    """One object of a kind as a comparison sees it: its label in a summary, the file it lives in and the
    canonical text it is compared by."""

    label: str
    path: str
    text: str


@dataclass(frozen=True)
class Kind:
    """``entries(files) -> {key: Entry}`` splits the kind's files into objects; ``delete_label`` is what a preview
    says a delete does, ``None`` for a kind that is never deleted, whose removals are then not reported;
    ``delete(ctx, keys)`` deletes the objects of those ``entries`` keys from the database. ``before`` names kinds
    that come after this one; ``apply(ctx, entries)`` writes a kind the importer has no code for; ``same(a, b)``
    says whether two entry texts that differ still hold the same object."""

    name: str
    label: str
    paths: tuple[str, ...]
    export: Callable[[ExportContext], dict[str, str]]
    after: tuple[str, ...] = ()
    before: tuple[str, ...] = ()
    cap: int | None = None
    entries_key: str | None = None
    tracked_by_data_flag: bool = False
    entries: Callable[[dict[str, str]], dict[object, Entry]] | None = None
    delete_label: str | None = None
    delete: Callable[[ExportContext, list], None] | None = None
    apply: Callable[[ExportContext, dict], None] | None = None
    same: Callable[[str, str], bool] | None = None


@dataclass(frozen=True)
class Change:
    kind: str
    key: object
    change: str
    label: str
    path: str
    consequence: str | None = None


ENTRY_POINT_GROUP = "flowfile.project_kinds"
logger = logging.getLogger(__name__)
_KINDS: dict[str, Kind] = {}
_REJECTED: dict[str, str] = {}
_core_loaded = False


def register(kind: Kind) -> None:
    _KINDS[kind.name] = kind


def _refusal(kind: Kind) -> str | None:
    """Why a package's kind cannot join the registry, or ``None`` when it can."""
    if kind.name in _KINDS:
        return f"the name {kind.name!r} is already registered"
    for other in _KINDS.values():
        for p in kind.paths:
            for q in other.paths:
                if _stem(p, q) is not None or _stem(q, p) is not None:
                    return f"its path {p!r} clashes with {q!r} of kind {other.name!r}"
    try:
        _order([*_KINDS.values(), kind])
    except ValueError as e:
        return str(e)
    return None


def _scan_entry_points() -> None:
    for entry_point in importlib.metadata.entry_points(group=ENTRY_POINT_GROUP):
        try:
            kind = entry_point.load()
        except Exception as e:
            kind, reason = None, f"it did not load: {e}"
        else:
            reason = _refusal(kind) if isinstance(kind, Kind) else "it is not a Kind"
        name = kind.name if isinstance(kind, Kind) else entry_point.name
        if reason is None:
            register(kind)
        else:
            logger.warning("Project kind %r from %r is not registered: %s", name, entry_point.value, reason)
            _REJECTED[name] = reason


def registered() -> list[Kind]:
    """Core's kinds, then those the ``flowfile.project_kinds`` entry points add, scanned on the first call."""
    global _core_loaded
    if not _core_loaded:
        from flowfile_core.project import core_kinds  # noqa: F401

        _core_loaded = True
        _scan_entry_points()
    return list(_KINDS.values())


def rejected() -> dict[str, str]:
    """The kinds a package offered that were left out, each with its reason."""
    registered()
    return dict(_REJECTED)


def get(name: str) -> Kind:
    for kind in registered():
        if kind.name == name:
            return kind
    raise KeyError(name)


def ordered() -> list[Kind]:
    """The kinds in registration order, each moved after the kinds its ``after`` names and before those its
    ``before`` names."""
    return _order(registered())


def _order(pending: list[Kind]) -> list[Kind]:
    pending = list(pending)
    names = {k.name for k in pending}
    after = {k.name: set(k.after) for k in pending}
    for kind in pending:
        for b in kind.before:
            if b in after:
                after[b].add(kind.name)
    placed: list[Kind] = []
    done: set[str] = set()
    while pending:
        for kind in pending:
            if all(a in done or a not in names for a in after[kind.name]):
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


def _by_kind(tree: dict[str, str]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for rel, text in tree.items():
        owned = kind_for_path(rel)
        if owned is not None:
            out.setdefault(owned[0].name, {})[rel] = text
    return out


def _file_entries(files: dict[str, str]) -> dict[str, Entry]:
    return {path: Entry(path.rsplit("/", 1)[-1], path, text) for path, text in files.items()}


def compare(a: dict[str, str], b: dict[str, str], *, kinds=None, strict=False) -> list[Change]:
    """What turns tree ``a`` into tree ``b``, per kind and key: ``added`` (in ``b`` only), ``modified`` (in both,
    different text that the kind's ``same`` does not call the same object) and ``removed`` (in ``a`` only, reported
    only for a kind with a ``delete_label``).

    Kinds come in ``ordered()`` order, keys sorted within a kind; ``kinds`` restricts it to those names. A kind
    whose files do not parse (a version committed by hand) is compared per file instead, so status still answers;
    with ``strict`` it is a ``ValueError`` naming the kind, since per-file keys are not keys a delete can use."""
    split_a, split_b = _by_kind(a), _by_kind(b)
    changes: list[Change] = []
    for kind in ordered():
        if kind.entries is None or (kinds is not None and kind.name not in kinds):
            continue
        files_a, files_b = split_a.get(kind.name, {}), split_b.get(kind.name, {})
        if files_a == files_b:
            continue
        try:
            old_entries, new_entries = kind.entries(files_a), kind.entries(files_b)
        except Exception as e:
            if strict:
                raise ValueError(f"The {kind.label} files cannot be compared: {e}. Nothing was changed.") from e
            old_entries, new_entries = _file_entries(files_a), _file_entries(files_b)
        for key in sorted(old_entries.keys() | new_entries.keys(), key=str):
            old, new = old_entries.get(key), new_entries.get(key)
            if old is None:
                changes.append(Change(kind.name, key, "added", new.label, new.path))
            elif new is None:
                if kind.delete_label is not None:
                    changes.append(Change(kind.name, key, "removed", old.label, old.path, kind.delete_label))
            elif old.text != new.text and not (kind.same is not None and kind.same(old.text, new.text)):
                changes.append(Change(kind.name, key, "modified", new.label, new.path))
    return changes


def kind_files(root: Path, kind: Kind) -> list[str]:
    """The repo-relative paths on disk that one kind's patterns match, sorted."""
    out: list[str] = []
    for pattern in kind.paths:
        folder, _, name = pattern.rpartition("/")
        if "*" not in name:
            if (root / pattern).is_file():
                out.append(pattern)
            continue
        d = root / folder
        if d.is_dir():
            out += [p.relative_to(root).as_posix() for p in d.glob(name) if p.is_file()]
    return sorted(out)


def owned_paths(root: Path) -> list[str]:
    """The repo-relative paths on disk that a registered pattern matches, kind by kind."""
    return [rel for kind in ordered() for rel in kind_files(root, kind)]


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
