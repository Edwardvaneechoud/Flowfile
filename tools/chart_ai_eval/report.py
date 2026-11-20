"""``report.json`` and ``report.md`` from the scored steps."""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from tools.chart_ai_eval.scoring import StepResult, describe_chart, summarize, summarize_by


def build_report(results: Sequence[StepResult], meta: dict[str, Any]) -> dict[str, Any]:
    return {
        "meta": meta,
        "summary": summarize(results),
        "by_table": summarize_by(results, "table"),
        "by_kind": summarize_by(results, "kind"),
        "steps": [r.as_dict() for r in results],
    }


def _pct(part: int, whole: int) -> str:
    return f"{part}/{whole} ({100 * part / whole:.0f}%)" if whole else f"{part}/0"


def _ms(value: float | None) -> str:
    return "–" if value is None else f"{value / 1000:.1f} s"


def summary_lines(s: dict[str, Any]) -> list[str]:
    """The summary as plain lines, shared by the terminal and ``report.md``."""
    charts = s["valid_charts"]
    lines = [
        f"cases run: {s['cases']} ({s['calls']} calls"
        + (f", {s['cases_partial']} stopped before their last step" if s["cases_partial"] else "")
        + f"); cases meeting every expectation: {_pct(s['cases_met'], s['cases'])}; "
        f"calls meeting theirs: {_pct(s['calls_met'], s['calls'])}",
        f"HTTP answers: {', '.join(f'{k}: {v}' for k, v in s['status_counts'].items()) or 'none'}"
        f" ({s['http_errors']} not 200)",
        f"valid charts: {_pct(charts, s['calls'])}",
        f"  accepted on the first call: {s['first_call']}; after the correction round: {s['after_correction']}"
        + (f"; attempts unknown (core does not send them): {s['attempts_unknown']}" if s["attempts_unknown"] else ""),
        f"  with silent repairs: {s['with_repairs']}"
        + (f" ({', '.join(f'{k} {v}' for k, v in s['repairs_by_code'].items())})" if s["repairs_by_code"] else "")
        + (f"; repairs unknown: {s['repairs_unknown']}" if s["repairs_unknown"] else ""),
        "degraded by reason: " + (", ".join(f"{k} {v}" for k, v in s["degraded_by_reason"].items()) or "none"),
        f"declines: {s['declines']} (right {s['declines_right']}, wrong {s['declines_wrong']}); "
        f"charts where a decline was expected: {s['declines_missed']}",
        f"expectations met: {_pct(s['expectations_met'], s['expectations'])}",
        f"latency: median {_ms(s['latency_ms_median'])}, p90 {_ms(s['latency_ms_p90'])}",
    ]
    return lines


def _table_rows(groups: dict[str, dict[str, Any]], label: str) -> list[str]:
    columns = [label, "cases met", "calls", "valid charts", "first call", "after correction", "repaired"]
    columns += ["declines right/wrong", "expectations met", "median", "p90"]
    rows = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    for name, s in groups.items():
        rows.append(
            f"| {name} | {s['cases_met']}/{s['cases']} | {s['calls']} | {s['valid_charts']} | {s['first_call']} | "
            f"{s['after_correction']} | {s['with_repairs']} | {s['declines_right']}/{s['declines_wrong']} | "
            f"{s['expectations_met']}/{s['expectations']} | "
            f"{_ms(s['latency_ms_median'])} | {_ms(s['latency_ms_p90'])} |"
        )
    return rows


def _attempts(value: int | None) -> str:
    return "unknown" if value is None else str(value)


def render_markdown(report: dict[str, Any], results: Sequence[StepResult]) -> str:
    meta = report["meta"]
    out = [
        "# Chart AI evaluation",
        "",
        f"Core `{meta['api']}` · provider `{meta.get('provider') or 'default'}` · model "
        f"`{meta.get('model') or 'default'}` · {meta['started']} → {meta['finished']}"
        + (" · **interrupted**" if meta.get("interrupted") else ""),
        "",
        "## Summary",
        "",
        *[f"- {line}" for line in summary_lines(report["summary"])],
        "",
        "## Per table",
        "",
        *_table_rows(report["by_table"], "table"),
        "",
        "## Per kind",
        "",
        *_table_rows(report["by_kind"], "kind"),
        "",
        "## Every call",
        "",
        "| case | step | outcome | reason | attempts | repairs | latency | met |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in results:
        a = r.answer
        repairs = "unknown" if a.repairs is None else (", ".join(a.repairs) or "–")
        out.append(
            f"| {r.case_id} | {r.step} | {a.outcome} | {a.reason or '–'} | {_attempts(a.attempts)} | {repairs} | "
            f"{_ms(a.latency_ms)} | {'yes' if r.score.met else 'no'} |"
        )
    failed = [r for r in results if not r.score.met]
    out += ["", f"## Calls that did not meet their expectations ({len(failed)})", ""]
    for r in failed:
        a = r.answer
        out += [
            f"### {r.case_id}, step {r.step} ({r.table}, {r.kind})",
            "",
            f"- prompt: {r.prompt}",
        ]
        if r.sent_current_chart is not None:
            source = "the case's fallback chart" if r.fallback else "the previous step's chart"
            out.append(f"- current chart sent ({source}): `{describe_chart(r.sent_current_chart)}`")
        out += [
            f"- expected: `{json.dumps(r.expect, ensure_ascii=False)}`",
            f"- returned: `{describe_chart(a.chart)}`"
            + (f" (HTTP {a.status})" if a.status != 200 else "")
            + (f", degraded `{a.reason}`" if a.degraded else ""),
        ]
        if a.title:
            out.append(f"- title: {a.title}")
        if a.explanation:
            out.append(f"- explanation: {a.explanation}")
        if a.message:
            out.append(f"- message: {a.message.replace(chr(10), ' ')}")
        if a.error:
            out.append(f"- error: {a.error}")
        if a.warnings:
            out.append(f"- warnings: {' '.join(a.warnings)}")
        repairs = "unknown" if a.repairs is None else list(a.repairs)
        out.append(f"- attempts: {_attempts(a.attempts)}; repairs: {repairs}")
        for check in r.score.checks:
            if not check.held:
                out.append(f"- not met: {check.expectation} — {check.why}")
        if a.chart is not None:
            out += ["", "```json", json.dumps(a.chart, indent=2, ensure_ascii=False), "```"]
        out.append("")
    return "\n".join(out) + "\n"


def write_reports(out_dir: Path, report: dict[str, Any], results: Sequence[StepResult]) -> None:
    """Write ``report`` (from ``build_report`` over ``results``) as both files in ``out_dir``."""
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), "utf-8")
    (out_dir / "report.md").write_text(render_markdown(report, results), "utf-8")
