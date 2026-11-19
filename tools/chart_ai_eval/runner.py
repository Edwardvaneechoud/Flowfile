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
    """The few routes the evaluation uses, on one base URL with one bearer token."""

    def __init__(self, client: httpx.Client, token: str) -> None:
        self.client = client
        self._headers = {"Authorization": f"Bearer {token}"}

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
        response = self._call("POST", "/catalog/tables", json=body)
        if response.status_code not in (200, 201):
            raise CoreError(f"POST /catalog/tables ({name}) answered {response.status_code}: {response.text[:300]}")
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
    """Delete the seeded tables, refusing any whose name (as sent or as core holds it) is not ours."""
    left: list[str] = []
    for table in seeded:
        current = core.table_name(table.id)
        if not deletable(table.name) or (current is not None and not deletable(current)):
            log(f"refused to delete table {table.id}: its name does not start with {TABLE_PREFIX}")
            left.append(table.name)
            continue
        status = core.delete_table(table.id)
        if status in (200, 204, 404):
            log(f"deleted {table.name}")
        else:
            log(f"could not delete {table.name} (HTTP {status})")
            left.append(table.name)
    return left


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
        )
        results.append(result)
        on_step(result)
        previous = answer.chart
    return results


@contextmanager
def seeded_tables(
    core: Core, keys: Sequence[str], *, keep: bool, log: Callable[[str], None]
) -> Iterator[dict[str, SeededTable]]:
    """The tables for ``keys``, removed on exit (also on an error or an interrupt) unless ``keep``."""
    tag = uuid.uuid4().hex[:6]
    seeded: list[SeededTable] = []
    with tempfile.TemporaryDirectory(prefix="flowfile-chart-ai-eval-") as folder:
        try:
            for key in keys:
                seeded.extend(seed_tables(core, [key], Path(folder), tag, log))
            yield {t.key: t for t in seeded}
        finally:
            if keep:
                for table in seeded:
                    log(f"kept {table.name} (table {table.id})")
            elif seeded:
                left = remove_tables(core, seeded, log)
                if left:
                    log("left behind: " + ", ".join(left))
