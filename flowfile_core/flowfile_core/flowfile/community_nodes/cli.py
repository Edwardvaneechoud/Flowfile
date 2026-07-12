"""Command-line entry for community-node CI and local checks.

    python -m flowfile_core.flowfile.community_nodes.cli <command> ...

Commands: ``validate``, ``validate-all``, ``dry-run``, ``build-index``, ``schema``.

This CLI is DB-free (validate/scan/dry-run/build-index need no catalog DB), but
importing ``flowfile_core`` otherwise seeds and migrates one on import. When running
outside a configured install (community-repo CI, a fork runner), set these first so
the import creates and touches no database:

    FLOWFILE_SKIP_INIT_DB=1 FLOWFILE_SKIP_STARTUP_MIGRATION=1

Exit code is 1 when any validation error or dry-run failure occurs.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

from flowfile_core.flowfile.community_nodes.dry_run_local import DryRunOutcome, run_bundle_dry_run
from flowfile_core.flowfile.community_nodes.index_build import build_index
from flowfile_core.flowfile.community_nodes.models import CommunityIndex, manifest_json_schema
from flowfile_core.flowfile.community_nodes.validation import ValidationReport, _valid_dependency_spec, validate_bundle
from flowfile_core.flowfile.node_designer.parsing import extract_manifest


def _load_index(path: str | None) -> CommunityIndex | None:
    if not path or not Path(path).is_file():
        return None
    try:
        return CommunityIndex.model_validate_json(Path(path).read_text(encoding="utf-8"))
    except Exception as e:
        print(f"warning: could not load index {path}: {e}", file=sys.stderr)
        return None


def _load_blocklist(path: str | None) -> dict | None:
    if not path or not Path(path).is_file():
        return None
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        print(f"warning: could not load blocklist {path}: {e}", file=sys.stderr)
        return None
    return data if isinstance(data, dict) else None


def _load_categories(path: str | None) -> list[str] | None:
    if not path or not Path(path).is_file():
        return None
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if isinstance(data, dict):
        data = data.get("categories", [])
    return [c for c in data if isinstance(c, str)] if isinstance(data, list) else None


def _print_report(name: str, report: ValidationReport) -> None:
    status = "PASS" if report.ok else "FAIL"
    print(f"[{status}] {name}")
    for issue in report.errors:
        print(f"  error   {issue.code}: {issue.message}")
    for issue in report.warnings:
        print(f"  warning {issue.code}: {issue.message}")
    if report.capabilities:
        print(f"  capabilities: {', '.join(report.capabilities)}")


def _print_dry_run(name: str, outcome: DryRunOutcome) -> None:
    status = "PASS" if outcome.success else "FAIL"
    detail = f" outputs={outcome.output_names}" if outcome.success else f" error={outcome.error}"
    print(f"[dry-run {status}] {name} ({outcome.duration_ms:.0f}ms){detail}")


def _report_markdown(name: str, report: ValidationReport) -> str:
    lines = [f"### `{name}` — {'✅ passed' if report.ok else '❌ failed'}"]
    for issue in report.errors:
        lines.append(f"- ❌ **{issue.code}**: {issue.message}")
    for issue in report.warnings:
        lines.append(f"- ⚠️ **{issue.code}**: {issue.message}")
    if report.capabilities:
        lines.append(f"- 🔑 capabilities: {', '.join(report.capabilities)}")
    return "\n".join(lines) + "\n"


def _dry_run_markdown(name: str, outcome: DryRunOutcome) -> str:
    head = f"- {'✅' if outcome.success else '❌'} dry-run `{name}` ({outcome.duration_ms:.0f}ms)"
    if not outcome.success:
        head += f": {outcome.error}"
    return head + "\n"


def _append_summary(path: str, markdown: str) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(markdown)


def _cmd_validate(args: argparse.Namespace) -> int:
    report = validate_bundle(
        Path(args.folder),
        index=_load_index(args.index),
        blocklist=_load_blocklist(args.blocklist),
        categories=_load_categories(args.categories),
        pr_author=args.pr_author,
    )
    _print_report(args.folder, report)
    if args.summary:
        _append_summary(args.summary, _report_markdown(args.folder, report))
    return 0 if report.ok else 1


def _cmd_validate_all(args: argparse.Namespace) -> int:
    repo_root = Path(args.repo_root)
    blocklist = _load_blocklist(str(repo_root / "registry" / "blocklist.json"))
    categories = _load_categories(str(repo_root / "registry" / "categories.json"))
    nodes_dir = repo_root / "nodes"

    failed = False
    sections: list[str] = []
    node_dirs = (
        sorted((p for p in nodes_dir.iterdir() if p.is_dir()), key=lambda p: p.name) if nodes_dir.is_dir() else []
    )
    for node_dir in node_dirs:
        # Re-validating the already-merged tree: pass no index so the PR-time version-bump
        # and new-node-authorization rules don't fire against each node's own published entry.
        report = validate_bundle(
            node_dir, index=None, blocklist=blocklist, categories=categories, pr_author=args.pr_author
        )
        _print_report(node_dir.name, report)
        sections.append(_report_markdown(node_dir.name, report))
        failed = failed or not report.ok
        # Never execute a bundle that failed validation (a denied scan must not run).
        if not args.quick and report.ok:
            outcome = run_bundle_dry_run(node_dir, timeout_seconds=args.timeout, row_limit=args.row_limit)
            _print_dry_run(node_dir.name, outcome)
            sections.append(_dry_run_markdown(node_dir.name, outcome))
            failed = failed or not outcome.success

    if args.summary:
        _append_summary(args.summary, "".join(sections))
    return 1 if failed else 0


def _install_node_deps(folder: Path) -> None:
    """pip-install a kernel-env node's declared dependencies so its process() can import them.

    Only validated specs are installed (plain name / name==version — no URLs/VCS/flags),
    into the current interpreter so the dry-run child (same sys.executable) sees them.
    """
    try:
        source = (folder / "node.py").read_text(encoding="utf-8")
    except OSError:
        return
    deps = [d for d in extract_manifest(source).environment.dependencies if _valid_dependency_spec(d)]
    if not deps:
        return
    print(f"Installing node dependencies: {', '.join(deps)}")
    subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", *deps], check=True)


def _cmd_dry_run(args: argparse.Namespace) -> int:
    if args.install_deps:
        _install_node_deps(Path(args.folder))
    outcome = run_bundle_dry_run(Path(args.folder), timeout_seconds=args.timeout, row_limit=args.row_limit)
    _print_dry_run(args.folder, outcome)
    if args.summary:
        _append_summary(args.summary, _dry_run_markdown(args.folder, outcome))
    return 0 if outcome.success else 1


def _cmd_build_index(args: argparse.Namespace) -> int:
    index = build_index(
        Path(args.repo_root),
        previous=_load_index(args.previous),
        commit=args.commit,
        generated_at=args.generated_at,
    )
    rendered = json.dumps(index.model_dump(), indent=2, ensure_ascii=False)
    if args.out:
        Path(args.out).write_text(rendered + "\n", encoding="utf-8")
        print(f"wrote {args.out} ({len(index.nodes)} nodes)")
    else:
        print(rendered)
    return 0


def _cmd_schema(_args: argparse.Namespace) -> int:
    print(json.dumps(manifest_json_schema(), indent=2, ensure_ascii=False))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="community_nodes.cli", description="Community-node validation and index CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    validate = sub.add_parser("validate", help="validate a single node bundle folder")
    validate.add_argument("folder")
    validate.add_argument("--index", help="path to index.json (enables version/authorization rules)")
    validate.add_argument("--blocklist", help="path to registry/blocklist.json")
    validate.add_argument("--categories", help="path to registry/categories.json")
    validate.add_argument("--pr-author", help="GitHub login of the PR author")
    validate.add_argument("--summary", help="append GitHub-flavored markdown to this file")
    validate.set_defaults(func=_cmd_validate)

    validate_all = sub.add_parser("validate-all", help="validate every nodes/* folder in a repo")
    validate_all.add_argument("--repo-root", default=".")
    validate_all.add_argument("--pr-author")
    validate_all.add_argument("--summary")
    validate_all.add_argument("--quick", action="store_true", help="validation only, skip the dry-run")
    validate_all.add_argument("--timeout", type=int, default=120)
    validate_all.add_argument("--row-limit", type=int, default=1000)
    validate_all.set_defaults(func=_cmd_validate_all)

    dry_run = sub.add_parser("dry-run", help="execute a bundle's process() against its example data")
    dry_run.add_argument("folder")
    dry_run.add_argument("--timeout", type=int, default=120)
    dry_run.add_argument("--row-limit", type=int, default=1000)
    dry_run.add_argument(
        "--install-deps",
        action="store_true",
        help="pip-install the node's declared dependencies first (kernel-env nodes)",
    )
    dry_run.add_argument("--summary")
    dry_run.set_defaults(func=_cmd_dry_run)

    build = sub.add_parser("build-index", help="build a deterministic index.json")
    build.add_argument("--repo-root", default=".")
    build.add_argument("--previous", help="path to the existing index.json (carries timestamps forward)")
    build.add_argument("--commit", default="", help="commit sha to stamp into registry.commit")
    build.add_argument("--out", help="write index.json here (default: stdout)")
    build.add_argument("--generated-at", help="override the generated_at timestamp (for reproducible builds)")
    build.set_defaults(func=_cmd_build_index)

    schema = sub.add_parser("schema", help="print the manifest JSON schema to stdout")
    schema.set_defaults(func=_cmd_schema)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
