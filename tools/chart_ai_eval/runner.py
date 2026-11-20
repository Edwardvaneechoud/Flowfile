"""Seed the tables, run the cases against a live core, and remove the tables again.

Talks only to the ``--api`` base URL. The token is sent as a header and never printed or
written; the reports hold prompts, charts and the core's answers only.
"""

from __future__ import annotations

import json
import tempfile
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from tools.chart_ai_eval import tables
from tools.chart_ai_eval.scoring import Answer, StepResult, answer_from_response, score_step

TABLE_PREFIX = "ai_eval_"
CASES_FILE = Path(__file__).with_name("cases.json")
ADOPT_WAIT = 10.0
ADOPT_PAUSE = 0.5


def load_cases(path: Path = CASES_FILE) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))["cases"]


def select_cases(cases: Sequence[dict[str, Any]], tables_: Sequence[str] | None, limit: int | None) -> list[dict]:
    """The cases on ``tables_`` (all when ``None``), the first ``limit`` of them."""
    chosen = [c for c in cases if tables_ is None or c["table"] in tables_]
    return chosen[:limit] if limit is not None else chosen


def deletable(name: str | None) -> bool:
    """Only a table this tool made may be deleted: its name starts with ``ai_eval_``."""
    return isinstance(name, str) and name.startswith(TABLE_PREFIX) and len(name) > len(TABLE_PREFIX)


class CoreError(RuntimeError):
    """A setup call the run cannot go on without failed; the message holds no credential."""


@dataclass
class SeededTable:
    key: str
    id: int
    name: str


class Core:
    """The few routes the evaluation uses, on one base URL with one bearer token.

    ``unanswered`` holds the names of registrations that were sent and got no answer (an
    interrupt, a timeout): core finishes such a call by itself, so those tables may exist.
    """

    def __init__(self, client: httpx.Client, token: str) -> None:
        self.client = client
        self._headers = {"Authorization": f"Bearer {token}"}
        self.unanswered: set[str] = set()

    @classmethod
    def login(cls, client: httpx.Client, token: str | None = None) -> Core:
        if token:
            return cls(client, token)
        response = client.post("/auth/token")
        if response.status_code != 200:
            raise CoreError(f"POST /auth/token answered {response.status_code}; pass --token")
        return cls(client, response.json()["access_token"])

    def _call(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        return self.client.request(method, path, headers=self._headers, **kwargs)

    def default_namespace_id(self) -> int | None:
        response = self._call("GET", "/catalog/default-namespace-id")
        return response.json() if response.status_code == 200 else None

    def register_table(self, name: str, file_path: str, namespace_id: int | None) -> dict[str, Any]:
        body = {"name": name, "file_path": file_path, "namespace_id": namespace_id}
        self.unanswered.add(name)
        response = self._call("POST", "/catalog/tables", json=body)
        self.unanswered.discard(name)
        if response.status_code not in (200, 201):
            hint = "; if core may not read the seed folder, pass --seed-dir" if response.status_code == 403 else ""
            raise CoreError(
                f"POST /catalog/tables ({name}) answered {response.status_code}: {response.text[:300]}{hint}"
            )
        return response.json()

    def list_tables(self) -> list[dict[str, Any]]:
        response = self._call("GET", "/catalog/tables")
        if response.status_code != 200:
            raise CoreError(f"GET /catalog/tables answered {response.status_code}")
        return response.json()

    def table_name(self, table_id: int) -> str | None:
        response = self._call("GET", f"/catalog/tables/{table_id}")
        return response.json().get("name") if response.status_code == 200 else None

    def delete_table(self, table_id: int) -> int:
        return self._call("DELETE", f"/catalog/tables/{table_id}", params={"delete_file": "true"}).status_code

    def generate_chart(self, body: dict[str, Any]) -> Answer:
        started = time.monotonic()
        try:
            response = self._call("POST", "/ai/generate_chart", json=body)
        except httpx.HTTPError as exc:
            return answer_from_response(None, None, None, error=type(exc).__name__)
        latency = int((time.monotonic() - started) * 1000)
        try:
            payload = response.json()
        except ValueError:
            payload = None
        return answer_from_response(response.status_code, payload, latency)


def seed_tables(core: Core, keys: Sequence[str], folder: Path, tag: str, log: Callable[[str], None]) -> list:
    """Write each table as parquet in ``folder`` and register it; returns what was registered."""
    namespace_id = core.default_namespace_id()
    seeded: list[SeededTable] = []
    for key in keys:
        frame = tables.build(key)
        path = folder / f"{key}.parquet"
        frame.write_parquet(path)
        name = f"{TABLE_PREFIX}{key}_{tag}"
        table = core.register_table(name, str(path), namespace_id)
        seeded.append(SeededTable(key=key, id=int(table["id"]), name=str(table.get("name", name))))
        log(f"seeded {name} ({frame.height} rows, {frame.width} columns) as table {table['id']}")
    return seeded


def remove_tables(core: Core, seeded: Sequence[SeededTable], log: Callable[[str], None]) -> list[str]:
    """Delete the seeded tables, refusing any whose name (as sent or as core holds it) is not ours.

    Returns the names still in the catalog; a call that fails leaves its table and goes on to the next.
    """
    left: list[str] = []
    for table in seeded:
        try:
            current = core.table_name(table.id)
            if not deletable(table.name) or (current is not None and not deletable(current)):
                log(f"refused to delete table {table.id}: its name does not start with {TABLE_PREFIX}")
                left.append(table.name)
                continue
            status = core.delete_table(table.id)
        except httpx.HTTPError as exc:
            log(f"could not delete {table.name} ({type(exc).__name__})")
            left.append(table.name)
            continue
        if status in (200, 204, 404):
            log(f"deleted {table.name}")
        else:
            log(f"could not delete {table.name} (HTTP {status})")
            left.append(table.name)
    return left


def adopt_unanswered(core: Core, log: Callable[[str], None]) -> tuple[list[SeededTable], list[str]]:
    """The tables of ``core.unanswered``, found by name, and the names that did not turn up.

    The table of a registration whose caller went away appears once core is done with it, so
    the catalog is listed until every name is there or ``ADOPT_WAIT`` seconds have passed.
    """
    missing = sorted(core.unanswered)
    found: list[SeededTable] = []
    deadline = time.monotonic() + ADOPT_WAIT
    while missing:
        try:
            listed = {t.get("name"): t for t in core.list_tables()}
        except (CoreError, httpx.HTTPError) as exc:
            log(f"could not look for {', '.join(missing)} in the catalog ({type(exc).__name__})")
            break
        for name in [n for n in missing if n in listed]:
            key = name.removeprefix(TABLE_PREFIX).rsplit("_", 1)[0]
            found.append(SeededTable(key=key, id=int(listed[name]["id"]), name=name))
            missing.remove(name)
            log(f"found {name}, registered by a call that got no answer")
        if not missing or time.monotonic() >= deadline:
            break
        time.sleep(ADOPT_PAUSE)
    for name in missing:
        log(f"{name} is not in the catalog now; its registration got no answer, so it may still appear")
    return found, missing


def run_case(
    core: Core,
    case: dict[str, Any],
    table_id: int,
    *,
    provider: str | None,
    model: str | None,
    on_step: Callable[[StepResult], None] = lambda _r: None,
) -> list[StepResult]:
    """Every step of a case in order; a follow-up sends the chart the step before returned."""
    results: list[StepResult] = []
    previous: dict[str, Any] | None = None
    for index, step in enumerate(case["steps"]):
        current, fallback = previous, False
        if index > 0 and current is None:
            current, fallback = step.get("fallback_current"), True
        body: dict[str, Any] = {"table_id": table_id, "prompt": step["prompt"]}
        if current is not None:
            body["current_chart"] = current
        if provider:
            body["provider"] = provider
        if model:
            body["model"] = model
        answer = core.generate_chart(body)
        result = StepResult(
            case_id=case["id"],
            table=case["table"],
            kind=case["kind"],
            step=index + 1,
            prompt=step["prompt"],
            expect=step["expect"],
            answer=answer,
            score=score_step(step["expect"], answer),
            sent_current_chart=current,
            fallback=fallback,
            case_steps=len(case["steps"]),
        )
        results.append(result)
        on_step(result)
        previous = answer.chart
    return results


@contextmanager
def seeded_tables(
    core: Core,
    keys: Sequence[str],
    *,
    keep: bool,
    log: Callable[[str], None],
    seed_dir: Path | None = None,
    left: list[str] | None = None,
) -> Iterator[dict[str, SeededTable]]:
    """The tables for ``keys``, removed on exit (also on an error or an interrupt) unless ``keep``.

    The parquet files go into a folder of their own under ``seed_dir`` (the system temp folder
    when ``None``), which core must be able to read at that path. On exit ``left`` holds the
    names that may still be in the catalog; with ``keep`` it stays empty.
    """
    tag = uuid.uuid4().hex[:6]
    seeded: list[SeededTable] = []
    left = [] if left is None else left
    with tempfile.TemporaryDirectory(prefix="flowfile-chart-ai-eval-", dir=seed_dir) as folder:
        try:
            for key in keys:
                seeded.extend(seed_tables(core, [key], Path(folder), tag, log))
            yield {t.key: t for t in seeded}
        finally:
            if keep:
                for table in seeded:
                    log(f"kept {table.name} (table {table.id})")
                for name in sorted(core.unanswered):
                    log(f"{name} may be in the catalog too: its registration got no answer")
            else:
                left[:] = [*(t.name for t in seeded), *sorted(core.unanswered)]
                found, missing = adopt_unanswered(core, log)
                left[:] = [*remove_tables(core, [*seeded, *found], log), *missing]
