"""Scoring: a case step's partial expectations against one answer of ``POST /ai/generate_chart``.

Pure functions over plain dicts, no I/O. An expectation states only what a correct answer
must contain, never one exact chart. The vocabulary (every key optional but ``outcome``):

- ``outcome``: ``"chart"`` or ``"decline"``, or a list of both when either is right.
  A decline is no chart with reason ``no_chart`` (the model declined) or ``invalid_chart``
  (core refused what the model built, after the correction round).
- ``marks``: the acceptable marks.
- ``fields``: references that must be on a channel. Each is a *ref pattern*:
  ``field`` (exact column name, ``"*"`` for the row count; absent = any field),
  ``channel`` (allowed channels), ``aggregate`` and ``timeUnit`` (a value, a list of values,
  ``null`` for none, ``true`` for any), ``bin`` (``true``/``false`` or the count), or
  ``any_of`` (a list of patterns, one of which must match).
- ``absent``: ref patterns that no reference may match.
- ``absent_channels``: channels that must be empty.
- ``mentions``: columns that must appear on a channel or in a filter.
- ``filters``: ``{field, kind?, include?, exclude?}``: a filter on the field must exist
  (of that kind), and every ``include`` value must pass all filters on the field while
  every ``exclude`` value must fail one, so ``oneOf [false]`` and ``notIn [true]`` both hold.
  Date values are ``"YYYY-MM-DD"``.
- ``sort``, ``limit``: the value, or ``null`` for absent. ``stack``: a value or a list
  (``null`` in the list for absent). ``raw_rows``: ``true`` when ``"aggregate": false``.
"""

from __future__ import annotations

import json
import math
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any

EXPECT_KEYS: tuple[str, ...] = (
    "outcome",
    "marks",
    "fields",
    "absent",
    "absent_channels",
    "mentions",
    "filters",
    "sort",
    "limit",
    "stack",
    "raw_rows",
)
REF_KEYS: tuple[str, ...] = ("field", "channel", "aggregate", "timeUnit", "bin", "any_of")
FILTER_KEYS: tuple[str, ...] = ("field", "kind", "include", "exclude")
FILTER_KINDS: tuple[str, ...] = ("oneOf", "notIn", "range", "dateRange")
CHANNELS: tuple[str, ...] = ("x", "y", "color", "size", "theta")
DECLINE_REASONS: tuple[str, ...] = ("no_chart", "invalid_chart")


@dataclass(frozen=True)
class Answer:
    """One call's result. ``status`` is ``None`` when no HTTP answer came; ``attempts`` ``None`` when not sent."""

    status: int | None
    latency_ms: int | None = None
    error: str | None = None
    chart: dict[str, Any] | None = None
    title: str | None = None
    explanation: str | None = None
    warnings: tuple[str, ...] = ()
    degraded: bool = False
    reason: str | None = None
    message: str | None = None
    attempts: int | None = None
    repairs: tuple[str, ...] | None = None

    @property
    def outcome(self) -> str:
        """``chart``, ``decline``, ``failed`` (degraded otherwise) or ``error`` (no 200)."""
        if self.status != 200:
            return "error"
        if self.chart is not None:
            return "chart"
        if self.reason in DECLINE_REASONS:
            return "decline"
        return "failed"


def answer_from_response(status: int | None, body: Any, latency_ms: int | None, error: str | None = None) -> Answer:
    """An ``Answer`` from the route's JSON (or the error of a call that got none)."""
    if status != 200 or not isinstance(body, dict):
        detail = error
        if detail is None and isinstance(body, dict):
            detail = json.dumps(body.get("detail", body), default=str)[:500]
        return Answer(status=status, latency_ms=latency_ms, error=detail or "no JSON answer")
    attempts = body.get("attempts")
    repairs = body.get("repairs")
    chart = body.get("chart")
    return Answer(
        status=status,
        latency_ms=latency_ms,
        chart=chart if isinstance(chart, dict) else None,
        title=body.get("title"),
        explanation=body.get("explanation"),
        warnings=tuple(body.get("warnings") or ()),
        degraded=bool(body.get("degraded")),
        reason=body.get("reason"),
        message=body.get("message"),
        attempts=attempts if isinstance(attempts, int) and not isinstance(attempts, bool) else None,
        repairs=tuple(repairs) if isinstance(repairs, list) else None,
    )


@dataclass(frozen=True)
class Check:
    expectation: str
    held: bool
    why: str | None = None


@dataclass
class StepScore:
    outcome: str
    checks: list[Check] = field(default_factory=list)

    @property
    def met(self) -> bool:
        return all(c.held for c in self.checks)


def chart_refs(chart: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Every ``(channel, reference)`` of a chart request, x and y lists flattened."""
    out: list[tuple[str, dict[str, Any]]] = []
    for channel in CHANNELS:
        value = chart.get(channel)
        for ref in value if isinstance(value, list) else [value]:
            if isinstance(ref, dict):
                out.append((channel, ref))
    return out


def describe_ref(ref: dict[str, Any]) -> str:
    name = ref.get("field", "?")
    if ref.get("aggregate"):
        name = f"{ref['aggregate']}({name})"
    if ref.get("timeUnit"):
        name = f"{ref['timeUnit']}({name})"
    if ref.get("bin") is not None:
        name = f"bin{ref['bin']}({name})"
    return name


def describe_chart(chart: dict[str, Any] | None) -> str:
    """A one-line reading of a chart request, for reasons and the report."""
    if chart is None:
        return "no chart"
    parts = [str(chart.get("mark"))]
    for channel in CHANNELS:
        refs = [describe_ref(r) for c, r in chart_refs(chart) if c == channel]
        if refs:
            parts.append(f"{channel}={', '.join(refs)}")
    for flt in chart.get("filters") or []:
        kind = next((k for k in FILTER_KINDS if k in flt), "?")
        parts.append(f"filter {flt.get('field')} {kind} {json.dumps(flt.get(kind), default=str)}")
    for key in ("aggregate", "stack", "sort", "limit"):
        if chart.get(key) is not None:
            parts.append(f"{key}={json.dumps(chart[key])}")
    return " · ".join(parts)


def _describe_pattern(pattern: dict[str, Any]) -> str:
    if "any_of" in pattern:
        return " or ".join(_describe_pattern(p) for p in pattern["any_of"])
    return json.dumps({k: v for k, v in pattern.items()}, default=str)


def _value_ok(want: Any, got: Any) -> bool:
    if want is True:
        return got is not None
    if isinstance(want, list):
        return got in want
    return got == want


def ref_matches(pattern: dict[str, Any], channel: str, ref: dict[str, Any]) -> bool:
    """Whether one ``(channel, reference)`` satisfies a ref pattern."""
    if "any_of" in pattern:
        return any(ref_matches(p, channel, ref) for p in pattern["any_of"])
    if "field" in pattern and ref.get("field") != pattern["field"]:
        return False
    if "channel" in pattern and channel not in pattern["channel"]:
        return False
    for key in ("aggregate", "timeUnit"):
        if key in pattern and not _value_ok(pattern[key], ref.get(key)):
            return False
    if "bin" in pattern:
        want, got = pattern["bin"], ref.get("bin")
        if want is True and got is None:
            return False
        if want is False and got is not None:
            return False
        if not isinstance(want, bool) and got != want:
            return False
    return True


def _passes(flt: dict[str, Any], value: Any) -> bool:
    if "oneOf" in flt:
        return value in (flt["oneOf"] or [])
    if "notIn" in flt:
        return value not in (flt["notIn"] or [])
    bounds = flt.get("range") if "range" in flt else flt.get("dateRange")
    if not isinstance(bounds, list | tuple) or len(bounds) != 2:
        return False
    low, high = bounds
    try:
        return (low is None or low <= value) and (high is None or value <= high)
    except TypeError:
        return False


def _check_filter(want: dict[str, Any], filters: Sequence[dict[str, Any]]) -> Check:
    name = want["field"]
    label = f"filter {_describe_pattern(want)}"
    on_field = [f for f in filters if f.get("field") == name]
    if "kind" in want:
        kinds = want["kind"] if isinstance(want["kind"], list) else [want["kind"]]
        on_field = [f for f in on_field if any(k in f for k in kinds)]
    if not on_field:
        return Check(label, False, f'no filter on "{name}"' + (f" of kind {want['kind']}" if "kind" in want else ""))
    for value in want.get("include", []):
        if not all(_passes(f, value) for f in on_field):
            return Check(label, False, f"{json.dumps(value)} is filtered out")
    for value in want.get("exclude", []):
        if all(_passes(f, value) for f in on_field):
            return Check(label, False, f"{json.dumps(value)} is kept")
    return Check(label, True)


def _expected_outcomes(expect: dict[str, Any]) -> list[str]:
    outcome = expect.get("outcome", "chart")
    return outcome if isinstance(outcome, list) else [outcome]


def score_step(expect: dict[str, Any], answer: Answer) -> StepScore:
    """Score one answer. Without the expected kind of answer only the outcome is scored."""
    outcomes = _expected_outcomes(expect)
    got = answer.outcome
    score = StepScore(outcome=got)
    wanted = " or ".join(outcomes)
    if got not in outcomes:
        why = {
            "error": f"HTTP {answer.status}: {answer.error}" if answer.status else f"no answer: {answer.error}",
            "failed": f"degraded: {answer.reason}",
            "decline": f"declined ({answer.reason}): {answer.explanation or answer.message or ''}".strip(),
            "chart": f"returned a chart: {describe_chart(answer.chart)}",
        }[got]
        score.checks.append(Check(f"outcome {wanted}", False, why))
        return score
    score.checks.append(Check(f"outcome {wanted}", True))
    if got != "chart":
        return score
    chart = answer.chart or {}
    refs = chart_refs(chart)
    shown = describe_chart(chart)

    if "marks" in expect:
        held = chart.get("mark") in expect["marks"]
        score.checks.append(
            Check(f"mark in {expect['marks']}", held, None if held else f'mark is "{chart.get("mark")}"')
        )
    for pattern in expect.get("fields", []):
        held = any(ref_matches(pattern, c, r) for c, r in refs)
        score.checks.append(Check(f"field {_describe_pattern(pattern)}", held, None if held else f"chart: {shown}"))
    for pattern in expect.get("absent", []):
        hits = [describe_ref(r) for c, r in refs if ref_matches(pattern, c, r)]
        score.checks.append(
            Check(f"absent {_describe_pattern(pattern)}", not hits, f"found {', '.join(hits)}" if hits else None)
        )
    for channel in expect.get("absent_channels", []):
        held = not any(c == channel for c, _ in refs)
        score.checks.append(Check(f"no {channel}", held, None if held else f"chart: {shown}"))
    filters = [f for f in chart.get("filters") or [] if isinstance(f, dict)]
    for name in expect.get("mentions", []):
        held = any(r.get("field") == name for _, r in refs) or any(f.get("field") == name for f in filters)
        score.checks.append(Check(f'mentions "{name}"', held, None if held else f"chart: {shown}"))
    for want in expect.get("filters", []):
        score.checks.append(_check_filter(want, filters))
    for key in ("sort", "limit"):
        if key in expect:
            held = chart.get(key) == expect[key]
            label = f"{key} {json.dumps(expect[key])}" if expect[key] is not None else f"no {key}"
            score.checks.append(Check(label, held, None if held else f"{key} is {json.dumps(chart.get(key))}"))
    if "stack" in expect:
        allowed = expect["stack"] if isinstance(expect["stack"], list) else [expect["stack"]]
        held = chart.get("stack") in allowed
        score.checks.append(
            Check(f"stack in {allowed}", held, None if held else f"stack is {json.dumps(chart.get('stack'))}")
        )
    if "raw_rows" in expect:
        raw = chart.get("aggregate") is False
        held = raw == expect["raw_rows"]
        label = "raw rows" if expect["raw_rows"] else "aggregated"
        score.checks.append(Check(label, held, None if held else f"aggregate is {json.dumps(chart.get('aggregate'))}"))
    return score


def validate_expectation(expect: dict[str, Any], columns: Iterable[str]) -> list[str]:
    """Problems in an expectation: unknown keys, and column names the table does not have."""
    names = set(columns) | {"*"}
    problems = [f"unknown key {k!r}" for k in expect if k not in EXPECT_KEYS]
    for outcome in _expected_outcomes(expect):
        if outcome not in ("chart", "decline"):
            problems.append(f"unknown outcome {outcome!r}")

    def check_pattern(pattern: dict[str, Any]) -> None:
        problems.extend(f"unknown ref key {k!r}" for k in pattern if k not in REF_KEYS)
        for sub in pattern.get("any_of", []):
            check_pattern(sub)
        if "field" in pattern and pattern["field"] not in names:
            problems.append(f"no column {pattern['field']!r}")
        for channel in pattern.get("channel", []):
            if channel not in CHANNELS:
                problems.append(f"unknown channel {channel!r}")

    for pattern in [*expect.get("fields", []), *expect.get("absent", [])]:
        check_pattern(pattern)
    for want in expect.get("filters", []):
        problems.extend(f"unknown filter key {k!r}" for k in want if k not in FILTER_KEYS)
        if want.get("field") not in names:
            problems.append(f"no column {want.get('field')!r}")
        kinds = want.get("kind", [])
        for kind in kinds if isinstance(kinds, list) else [kinds]:
            if kind not in FILTER_KINDS:
                problems.append(f"unknown filter kind {kind!r}")
    for name in expect.get("mentions", []):
        if name not in names:
            problems.append(f"no column {name!r}")
    for channel in expect.get("absent_channels", []):
        if channel not in CHANNELS:
            problems.append(f"unknown channel {channel!r}")
    return problems


# The summary.


@dataclass
class StepResult:
    """One call of a case, scored. ``fallback`` is set when the chain's own chart was missing."""

    case_id: str
    table: str
    kind: str
    step: int
    prompt: str
    expect: dict[str, Any]
    answer: Answer
    score: StepScore
    sent_current_chart: dict[str, Any] | None = None
    fallback: bool = False

    def as_dict(self) -> dict[str, Any]:
        answer = asdict(self.answer)
        answer["outcome"] = self.answer.outcome
        return {
            "case_id": self.case_id,
            "table": self.table,
            "kind": self.kind,
            "step": self.step,
            "prompt": self.prompt,
            "expect": self.expect,
            "sent_current_chart": self.sent_current_chart,
            "fallback_current_chart_used": self.fallback,
            "answer": answer,
            "met": self.score.met,
            "checks": [asdict(c) for c in self.score.checks],
        }


def percentile(values: Sequence[float], q: float) -> float | None:
    """Nearest-rank percentile (``q`` in 0..100); ``None`` for no values."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(q / 100 * len(ordered)))
    return ordered[rank - 1]


def median(values: Sequence[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    return ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2


def summarize(results: Sequence[StepResult]) -> dict[str, Any]:
    """The counts of the report over ``results`` (one entry per call)."""
    answers = [r.answer for r in results]
    charts = [a for a in answers if a.outcome == "chart"]
    expects_decline = [r for r in results if "decline" in _expected_outcomes(r.expect)]
    expects_chart_only = [r for r in results if _expected_outcomes(r.expect) == ["chart"]]
    checks = [c for r in results for c in r.score.checks]
    latencies = [a.latency_ms for a in answers if a.latency_ms is not None and a.status is not None]
    cases: dict[str, bool] = {}
    for r in results:
        cases[r.case_id] = cases.get(r.case_id, True) and r.score.met
    repairs = Counter(code for a in charts for code in (a.repairs or ()))
    return {
        "cases": len(cases),
        "cases_met": sum(cases.values()),
        "calls": len(results),
        "calls_met": sum(r.score.met for r in results),
        "http_errors": sum(a.outcome == "error" for a in answers),
        "status_counts": dict(sorted(Counter(str(a.status) for a in answers).items())),
        "valid_charts": len(charts),
        "first_call": sum(a.attempts == 1 for a in charts),
        "after_correction": sum(a.attempts is not None and a.attempts >= 2 for a in charts),
        "attempts_unknown": sum(a.attempts is None for a in charts),
        "with_repairs": sum(bool(a.repairs) for a in charts),
        "repairs_unknown": sum(a.repairs is None for a in charts),
        "repairs_by_code": dict(repairs.most_common()),
        "degraded_by_reason": dict(Counter(a.reason or "unknown" for a in answers if a.status == 200 and a.degraded)),
        "declines": sum(a.outcome == "decline" for a in answers),
        "declines_right": sum(r.answer.outcome == "decline" for r in expects_decline),
        "declines_wrong": sum(r.answer.outcome == "decline" for r in expects_chart_only),
        "declines_missed": sum(
            r.answer.outcome == "chart" for r in expects_decline if "chart" not in _expected_outcomes(r.expect)
        ),
        "expectations": len(checks),
        "expectations_met": sum(c.held for c in checks),
        "latency_ms_median": median(latencies),
        "latency_ms_p90": percentile(latencies, 90),
    }


def summarize_by(results: Sequence[StepResult], key: str) -> dict[str, dict[str, Any]]:
    """``summarize`` per value of ``key`` (``table`` or ``kind``)."""
    groups: dict[str, list[StepResult]] = {}
    for r in results:
        groups.setdefault(getattr(r, key), []).append(r)
    return {name: summarize(group) for name, group in sorted(groups.items())}
