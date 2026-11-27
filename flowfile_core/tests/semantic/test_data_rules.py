"""Data rules of the semantic model: a proposed rule that cannot work is refused, accepted rules run on the compute
backend against real Delta tables, and the newest results are read back for the rules accepted now.

Runs in electron mode (one user, solo review, so a proposed rule is accepted at once); who may run and read rules
under team review is in ``tests/sharing/test_sharing_semantic.py``.
"""

from __future__ import annotations

import json

import polars as pl
import pytest

from flowfile_core.catalog.semantic import data_rules, service
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogTable
from flowfile_core.flowfile.execution.errors import ComputeUnavailable
from flowfile_core.flowfile.flow_data_engine import bounded
from tests.semantic.world import World, add_table, say


def _base(ns_id: int) -> str:
    return f"/catalog/namespaces/{ns_id}/semantic-model"


def _rules(world: World, column: str, *rules: dict) -> dict:
    return {"kind": "field", "table_id": world.orders_id, "column": column, "attribute": "checks", "value": list(rules)}


def _propose(client, ns_id: int, *proposals: dict):
    return client.post(f"{_base(ns_id)}/proposals", json={"proposals": list(proposals)})


def _by_rule(tables: list[dict], table_id: int) -> dict[tuple[str, str], dict]:
    [table] = [t for t in tables if t["table_id"] == table_id]
    return {(r["column"], r["rule"]["check"]): r for r in table["results"]}


@pytest.mark.parametrize(
    ("column", "rule", "says"),
    [
        ("amount", {"check": "nope"}, "is not a data rule"),
        ("amount", {"check": "sql", "expression": "{column} >= 0"}, "is not a data rule"),
        ("region", {"check": "matches", "pattern": "("}, "matches"),
        ("amount", {"check": "between"}, "between needs a min or a max"),
        ("amount", {"check": "between", "min": "a"}, "cannot run on column 'amount'"),
        ("region", {"check": "length", "max": -1}, "length bounds"),
        ("region", {"check": "one_of", "values": []}, "one_of needs at least one value"),
        ("amount", {"check": "formula", "expression": "[nope] > 1"}, "cannot run on column 'amount'"),
        ("amount", {"check": "formula", "expression": "[amount] + 1"}, "must answer true or false"),
        ("amount", {"check": "formula", "expression": " "}, "formula needs an expression"),
        ("amount", {"check": "between", "min": 9, "max": 1}, "min to be at most max"),
        ("region", {"check": "one_of", "values": "West"}, "one_of"),
        ("amount", 1, "each with its 'check'"),
        ("amount", "not_null", "each with its 'check'"),
    ],
)
def test_a_rule_that_cannot_work_is_refused_and_nothing_is_stored(world: World, client, column, rule, says) -> None:
    answer = _propose(client, world.sales_id, _rules(world, column, rule))
    assert answer.status_code == 422, answer.text
    assert says in answer.json()["detail"]
    model = client.get(_base(world.sales_id)).json()
    [orders] = [t for t in model["tables"] if t["table_id"] == world.orders_id]
    assert all("checks" not in f["facts"] for f in orders["fields"])


def test_rules_run_on_the_backend_and_report_what_fails(world: World, client) -> None:
    stored = _propose(
        client,
        world.sales_id,
        _rules(world, "order_id", {"check": "not_null"}, {"check": "unique"}),
        _rules(world, "customer_id", {"check": "unique"}),
        _rules(world, "amount", {"check": "between", "min": 0, "max": 8}),
        _rules(world, "region", {"check": "one_of", "values": ["West", "East"]}, {"check": "length", "max": 4}),
        _rules(world, "card_number", {"check": "matches", "pattern": r"^CARDVAL-\d{4}$"}),
    )
    assert stored.status_code == 200, stored.text
    assert {f["status"] for f in stored.json()} == {"accepted"}

    ran = client.post(f"{_base(world.sales_id)}/data-rules/run")
    assert ran.status_code == 200, ran.text
    body = ran.json()
    assert body["run_id"] and [t["skipped"] for t in body["tables"]] == [None]
    found = _by_rule(body["tables"], world.orders_id)
    failed = {key: (r["passed"], r["failed_rows"], r["total_rows"], r["error"]) for key, r in found.items()}
    assert failed == {
        ("order_id", "not_null"): (True, 0, 6, None),
        ("order_id", "unique"): (True, 0, 6, None),
        ("customer_id", "unique"): (False, 5, 6, None),
        ("amount", "between"): (False, 1, 6, None),
        ("region", "one_of"): (False, 1, 6, None),
        ("region", "length"): (False, 1, 6, None),
        ("card_number", "matches"): (True, 0, 6, None),
    }

    read = client.get(f"{_base(world.sales_id)}/data-rules/results")
    assert read.status_code == 200, read.text
    again = _by_rule(read.json(), world.orders_id)
    assert {k: (r["passed"], r["failed_rows"]) for k, r in again.items()} == {
        k: (r["passed"], r["failed_rows"]) for k, r in found.items()
    }
    assert all(r["ran_at"] for r in again.values())


def test_a_formula_rule_is_one_line_that_is_true_for_a_passing_row(world: World, client) -> None:
    formula = {"check": "formula", "expression": "[amount] >= 4 and [region] != 'North'"}
    assert _propose(client, world.sales_id, _rules(world, "amount", formula)).status_code == 200
    ran = client.post(f"{_base(world.sales_id)}/data-rules/run", json={"table_ids": [world.orders_id]})
    assert ran.status_code == 200, ran.text
    [result] = ran.json()["tables"][0]["results"]
    assert (result["rule"], result["passed"], result["failed_rows"], result["total_rows"]) == (formula, False, 1, 6)


def test_one_of_takes_whole_numbers_on_a_float_column(world: World, client) -> None:
    rule = {"check": "one_of", "values": [5, 3, 8]}
    stored = _propose(client, world.sales_id, _rules(world, "amount", rule))
    assert stored.status_code == 200 and stored.json()[0]["rationale"] is None, stored.text
    ran = client.post(f"{_base(world.sales_id)}/data-rules/run")
    [result] = ran.json()["tables"][0]["results"]
    assert (result["error"], result["failed_rows"], result["total_rows"]) == (None, 3, 6)


def test_a_rules_error_quotes_no_value_of_the_data_and_no_path(world: World, client) -> None:
    """What an engine says about a rule it cannot evaluate is logged and shown, so it must not carry rows."""
    formula = {"check": "formula", "expression": "to_integer([region]) > 0"}
    assert _propose(client, world.sales_id, _rules(world, "region", formula)).status_code == 200
    ran = client.post(f"{_base(world.sales_id)}/data-rules/run")
    [result] = ran.json()["tables"][0]["results"]
    assert result["passed"] is False and "conversion" in result["error"], result
    assert not any(value in result["error"] for value in ("West", "East", "North", "["))
    [logged] = client.get(f"{_base(world.sales_id)}/data-rules/results").json()[0]["results"]
    assert logged["error"] == result["error"]
    assert data_rules._safe_reason("No such file (os error 2): /srv/store/catalog/t1/part-0.parquet") == (
        "No such file (os error 2): part-0.parquet"
    )


def test_results_are_kept_only_for_the_rules_accepted_now(world: World, client) -> None:
    between, not_null = {"check": "between", "min": 0, "max": 8}, {"check": "not_null"}
    assert _propose(client, world.sales_id, _rules(world, "amount", between, not_null)).status_code == 200
    assert client.post(f"{_base(world.sales_id)}/data-rules/run").status_code == 200
    assert set(_by_rule(client.get(f"{_base(world.sales_id)}/data-rules/results").json(), world.orders_id)) == {
        ("amount", "between"),
        ("amount", "not_null"),
    }

    assert _propose(client, world.sales_id, _rules(world, "amount", not_null)).status_code == 200
    kept = _by_rule(client.get(f"{_base(world.sales_id)}/data-rules/results").json(), world.orders_id)
    assert set(kept) == {("amount", "not_null")}

    assert _propose(client, world.sales_id, _rules(world, "amount")).status_code == 200
    assert client.get(f"{_base(world.sales_id)}/data-rules/results").json() == []
    assert client.post(f"{_base(world.sales_id)}/data-rules/run").json() == {"run_id": None, "tables": []}


def test_a_table_outside_the_model_is_refused(world: World, client) -> None:
    answer = client.post(f"{_base(world.sales_id)}/data-rules/run", json={"table_ids": [world.customers_id]})
    assert answer.status_code == 422, answer.text


def test_a_virtual_table_is_skipped_not_resolved(world: World, client) -> None:
    with get_db_context() as db:
        row = CatalogTable(
            name="orders_view",
            namespace_id=world.sales_id,
            owner_id=1,
            table_type="virtual",
            sql_query=f'SELECT * FROM "{world.key("orders")}"',
            schema_json=json.dumps([{"name": "amount", "dtype": "Float64"}]),
        )
        db.add(row)
        db.commit()
        view_id = row.id
    key = f"{world.catalog}.sales.orders_view"
    world.extra.append((view_id, world.paths[0].with_name("no_such_table"), key))
    service.maybe_sync(force=True)
    say(f"field:{key}/amount", "checks", [{"check": "not_null"}])

    ran = client.post(f"{_base(world.sales_id)}/data-rules/run")
    assert ran.status_code == 200, ran.text
    [table] = ran.json()["tables"]
    assert table["table_id"] == view_id and table["results"] == [] and "virtual" in table["skipped"]
    assert ran.json()["run_id"] is None
    assert client.get(f"{_base(world.sales_id)}/data-rules/results").json() == []


def test_a_table_on_local_disk_is_skipped_for_a_backend_that_does_not_read_it(
    world: World, client, monkeypatch
) -> None:
    """A worker on another host reads catalog tables from object storage only, so no plan of a local path is sent."""
    from flowfile_core.catalog.services import virtual_tables

    assert _propose(client, world.sales_id, _rules(world, "amount", {"check": "not_null"})).status_code == 200
    sent: list[object] = []
    monkeypatch.setattr(virtual_tables, "reads_core_disk", lambda backend: False)
    monkeypatch.setattr(bounded, "single_row", lambda *args, **kwargs: sent.append(args))
    ran = client.post(f"{_base(world.sales_id)}/data-rules/run")
    assert ran.status_code == 200, ran.text
    [table] = ran.json()["tables"]
    assert table["results"] == [] and "is on local disk, not object storage" in table["skipped"]
    assert ran.json()["run_id"] is None and sent == []


def test_a_stored_rule_flowfile_does_not_run_is_an_error_and_is_never_evaluated(world: World, client) -> None:
    say(
        world.field("orders", "amount"),
        "checks",
        [{"check": "sql", "expression": "{column} >= 0"}, {"check": "not_null"}],
    )
    ran = client.post(f"{_base(world.sales_id)}/data-rules/run")
    assert ran.status_code == 200, ran.text
    found = _by_rule(ran.json()["tables"], world.orders_id)
    assert found[("amount", "not_null")]["passed"] is True
    assert (found[("amount", "sql")]["passed"], found[("amount", "sql")]["total_rows"]) == (False, 6)
    assert "is not a data rule" in found[("amount", "sql")]["error"]


def test_compute_that_cannot_be_reached_is_a_503_and_logs_nothing(world: World, client, monkeypatch) -> None:
    assert _propose(client, world.sales_id, _rules(world, "amount", {"check": "not_null"})).status_code == 200

    def unreachable(*_args, **_kwargs):
        raise ComputeUnavailable("the worker did not answer")

    monkeypatch.setattr(bounded, "single_row", unreachable)
    answer = client.post(f"{_base(world.sales_id)}/data-rules/run")
    assert answer.status_code == 503, answer.text
    assert client.get(f"{_base(world.sales_id)}/data-rules/results").json() == []


def test_one_rule_the_data_refuses_does_not_hide_the_others(world: World, client, monkeypatch) -> None:
    """A rule can stop working after it was accepted; it reports its own error and the rest still count."""
    assert _propose(client, world.sales_id, _rules(world, "amount", {"check": "not_null"})).status_code == 200
    say(world.field("orders", "region"), "checks", [{"check": "between", "min": 1}])
    ran = client.post(f"{_base(world.sales_id)}/data-rules/run")
    assert ran.status_code == 200, ran.text
    found = _by_rule(ran.json()["tables"], world.orders_id)
    assert (found[("amount", "not_null")]["passed"], found[("amount", "not_null")]["total_rows"]) == (True, 6)
    assert found[("region", "between")]["passed"] is False and found[("region", "between")]["error"]


def test_a_formula_rule_is_parsed_and_never_run_as_python(world: World, client, tmp_path) -> None:
    """The attempts of ``flowfile_wasm``'s ``test_filter_advanced_expr_does_not_evaluate_python``, as a rule: core
    builds a formula rule's expression itself, when it is proposed and again when it runs."""
    canary = tmp_path / "canary"
    hostile = [
        f'__import__("os").system("touch {canary}")',
        f'open("{canary}", "w")',
        f'[amount] > 1 and open("{canary}", "w")',
        f'[region] = "x\\" + open(\\"{canary}\\", \\"w\\") + \\"y"',
        f'[region] = \'x" + open("{canary}", "w") + "y\'',
    ]
    for expression in hostile:
        rule = {"check": "formula", "expression": expression}
        answer = _propose(client, world.sales_id, _rules(world, "amount", rule))
        assert answer.status_code in (200, 422), answer.text
        say(world.field("orders", "amount"), "checks", [rule])
        assert client.post(f"{_base(world.sales_id)}/data-rules/run").status_code == 200
        assert not canary.exists(), expression


def test_a_value_error_that_is_not_about_the_rules_does_not_refuse_them(world: World, client, monkeypatch) -> None:
    def broken(_table):
        raise ValueError("the stored schema could not be read")

    monkeypatch.setattr(data_rules, "table_schema", broken)
    stored = _propose(client, world.sales_id, _rules(world, "amount", {"check": "not_null"}))
    assert stored.status_code == 200, stored.text
    assert stored.json()[0]["rationale"].startswith("Not checked")


def test_a_rule_is_stored_when_the_check_itself_cannot_be_made(world: World, client, monkeypatch) -> None:
    """Only what the check proves wrong is refused: a check that breaks stores the rule with a note, and the rule
    is judged when it runs."""

    def broken(*_args, **_kwargs):
        raise RuntimeError("the checker broke")

    monkeypatch.setattr(data_rules, "_check", broken)
    stored = _propose(client, world.sales_id, _rules(world, "amount", {"check": "not_null"}))
    assert stored.status_code == 200, stored.text
    [fact] = stored.json()
    assert fact["status"] == "accepted" and fact["rationale"].startswith("Not checked")
    assert "the checker broke" not in fact["rationale"]

    monkeypatch.undo()
    ran = client.post(f"{_base(world.sales_id)}/data-rules/run")
    assert ran.status_code == 200, ran.text
    assert _by_rule(ran.json()["tables"], world.orders_id)[("amount", "not_null")]["passed"] is True


def _retyped(world: World, table_id: int, dtype: str) -> None:
    with get_db_context() as db:
        db.query(CatalogTable).filter(CatalogTable.id == table_id).update(
            {"schema_json": json.dumps([{"name": "n", "dtype": dtype}])}, synchronize_session=False
        )
        db.commit()
    service.maybe_sync(force=True)


def test_a_rule_on_a_column_of_a_type_flowfile_does_not_know_is_stored_unchecked(world: World, client) -> None:
    table_id = add_table(world, world.sales_id, "odd", pl.DataFrame({"n": [1, 2, -3]}), 1)
    _retyped(world, table_id, "long")
    rule = {"kind": "field", "table_id": table_id, "column": "n", "attribute": "checks"}
    stored = _propose(client, world.sales_id, {**rule, "value": [{"check": "formula", "expression": "[n] >= 0"}]})
    assert stored.status_code == 200, stored.text
    assert stored.json()[0]["rationale"] == "Not checked: the catalog stores no type Flowfile knows for n."
    ran = client.post(f"{_base(world.sales_id)}/data-rules/run", json={"table_ids": [table_id]})
    assert [(r["failed_rows"], r["total_rows"]) for r in ran.json()["tables"][0]["results"]] == [(1, 3)]
