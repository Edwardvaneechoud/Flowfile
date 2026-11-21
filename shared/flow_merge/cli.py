"""``flowfile-merge``: the step-level diff and merge of flow files from a shell, a git hook or a pipeline.

It imports nothing of core, so git can run it once per file it merges. Exit codes: 0 done, 1 a difference or a
conflict that the caller asked to be told about, 2 a file that cannot be read.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from shared.flow_merge.conflict import conflict_document, is_conflicted, open_conflicts, resolve
from shared.flow_merge.diff import diff
from shared.flow_merge.integrity import integrity
from shared.flow_merge.merge import merge
from shared.flow_merge.records import is_records, merge_records
from shared.flow_merge.render import render_diff, show
from shared.flow_merge.yaml_io import dump, load


class Unreadable(Exception):
    pass


def _read(path: str) -> dict:
    try:
        data = load(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as e:
        raise Unreadable(f"{path}: {e}") from None
    if data is None:
        return {"nodes": []}
    if not isinstance(data, dict):
        raise Unreadable(f"{path}: not a flow document")
    return data


def _diff(args: argparse.Namespace) -> int:
    result = diff(_read(args.a), _read(args.b))
    name = args.name or Path(args.b).stem.removesuffix(".flow")
    sys.stdout.write(render_diff(result, args.format, name=name, layout=args.layout))
    return 1 if args.exit_code and not result.empty else 0


def _show(args: argparse.Namespace) -> int:
    sys.stdout.write(show(_read(args.file)))
    return 0


def _write(path: str, data: dict, like: str) -> None:
    """Write ``data`` over ``path`` in the format ``like`` (the file's old text) had, through a temporary file."""
    if like.lstrip().startswith("{"):
        text = json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    else:
        text = dump(data, "project" if "flow_uuid" in data else "flow")
    target = Path(path)
    fd, tmp = tempfile.mkstemp(dir=str(target.parent), prefix=".flowfile-merge-")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, target)


def _production_branch() -> str:
    try:
        out = subprocess.run(
            ["git", "config", "--get", "flowfile.production"], capture_output=True, text=True, timeout=10
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        out = ""
    return out or "main"


def _names_branch(label: str | None, branch: str) -> bool:
    return bool(label) and (label == branch or label.endswith(f"/{branch}"))


def keep_ids_for(keep_ids: str, ours_label: str | None, theirs_label: str | None) -> str:
    """``auto``: keep the production branch's ids (``git config flowfile.production``, ``main`` without it)."""
    if keep_ids != "auto":
        return keep_ids
    branch = _production_branch()
    if _names_branch(theirs_label, branch) and not _names_branch(ours_label, branch):
        return "theirs"
    return "ours"


def _merge_file(args: argparse.Namespace) -> int:
    texts = {}
    for role, path in (("base", args.base), ("ours", args.ours), ("theirs", args.theirs)):
        texts[role] = Path(path).read_text(encoding="utf-8") if Path(path).exists() else ""
    base, ours, theirs = _read(args.base), _read(args.ours), _read(args.theirs)
    for role, data in (("base", base), ("ours", ours), ("theirs", theirs)):
        if is_conflicted(data):
            raise Unreadable(f"{role}: it still holds merge conflicts; settle them first")
    if is_records(ours) or is_records(theirs):
        result = merge_records(base, ours, theirs)
    else:
        result = merge(base, ours, theirs, keep_ids=keep_ids_for(args.keep_ids, args.ours_label, args.theirs_label))
    sides = {
        "base": args.base_label or "base",
        "ours": args.ours_label or "ours",
        "theirs": args.theirs_label or "theirs",
    }
    _write(args.ours, conflict_document(result, sides), texts["ours"])
    name = args.path or args.ours
    for warning in result.warnings:
        print(f"flowfile-merge: {name}: {warning.message}", file=sys.stderr)
    if result.clean:
        return 0
    print(
        f"flowfile-merge: {name}: {len(result.conflicts)} conflict{'s' if len(result.conflicts) != 1 else ''}:",
        file=sys.stderr,
    )
    for conflict in result.conflicts:
        print(f"  {conflict.key}: {conflict.message}", file=sys.stderr)
    return 1


def _resolve(args: argparse.Namespace) -> int:
    path = Path(args.file)
    text = path.read_text(encoding="utf-8")
    data = _read(args.file)
    if not is_conflicted(data):
        print(f"flowfile-merge: {args.file} holds no merge conflict.", file=sys.stderr)
        return 0
    keys = args.key or [c.key for c in open_conflicts(data)]
    try:
        settled = resolve(data, {key: args.take for key in keys})
    except KeyError as e:
        print(f"flowfile-merge: {e.args[0]}", file=sys.stderr)
        return 2
    _write(args.file, settled, text)
    left = open_conflicts(settled)
    if left:
        print(
            f"flowfile-merge: {args.file}: {len(left)} conflict{'s' if len(left) != 1 else ''} left.", file=sys.stderr
        )
        return 1
    return 0


def _check(args: argparse.Namespace) -> int:
    failed = False
    for path in args.files:
        data = _read(path)
        if is_conflicted(data):
            print(f"{path}: {len(open_conflicts(data))} merge conflicts not settled", file=sys.stderr)
            failed = True
            continue
        issues = [] if is_records(data) else integrity(data)
        for issue in issues:
            print(f"{path}: {issue.message}", file=sys.stderr)
        failed = failed or bool(issues)
        if args.build and not is_records(data):
            from shared.flow_merge.core_check import check_in_core

            for line in check_in_core(data):
                print(f"{path}: {line}", file=sys.stderr)
                failed = True
    return 1 if failed else 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="flowfile-merge", description="Step-level diff and merge of Flowfile flows.")
    commands = root.add_subparsers(dest="command", required=True)

    cmd = commands.add_parser("diff", help="what changed between two versions of a flow, step by step")
    cmd.add_argument("a", help="the version before")
    cmd.add_argument("b", help="the version after")
    cmd.add_argument("--format", choices=("text", "md", "json"), default="text")
    cmd.add_argument("--layout", action="store_true", help="also list steps, groups and comments that moved")
    cmd.add_argument("--name", help="the flow's name in the summary line (default: the file name)")
    cmd.add_argument("--exit-code", action="store_true", help="exit 1 when anything changed")
    cmd.set_defaults(run=_diff)

    cmd = commands.add_parser("show", help="one flow as review text (git textconv)")
    cmd.add_argument("file")
    cmd.set_defaults(run=_show)

    cmd = commands.add_parser("merge-file", help="three-way merge of one flow file, as a git merge driver")
    cmd.add_argument("base", help="the common ancestor (git's %%O)")
    cmd.add_argument("ours", help="this side, overwritten with the result (git's %%A)")
    cmd.add_argument("theirs", help="the other side (git's %%B)")
    cmd.add_argument("-P", "--path", help="the file's path in the repository, for messages (git's %%P)")
    cmd.add_argument("--base-label", help="git's %%S")
    cmd.add_argument("--ours-label", help="git's %%X")
    cmd.add_argument("--theirs-label", help="git's %%Y")
    cmd.add_argument(
        "--keep-ids",
        choices=("ours", "theirs", "auto"),
        default="auto",
        help="whose step ids stay; auto keeps the production branch's (git config flowfile.production, else main)",
    )
    cmd.set_defaults(run=_merge_file)

    cmd = commands.add_parser("resolve", help="settle the conflicts of a flow file on one side")
    cmd.add_argument("file")
    cmd.add_argument("--take", choices=("ours", "theirs"), required=True)
    cmd.add_argument("--key", action="append", help="one conflict to settle (step:7); every open one without it")
    cmd.set_defaults(run=_resolve)

    cmd = commands.add_parser("check", help="fail on a flow file that holds conflicts or edges that do not hold")
    cmd.add_argument("files", nargs="+")
    cmd.add_argument("--build", action="store_true", help="also build each flow in Flowfile and check its steps")
    cmd.set_defaults(run=_check)
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        return args.run(args)
    except Unreadable as e:
        print(f"flowfile-merge: cannot read {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
