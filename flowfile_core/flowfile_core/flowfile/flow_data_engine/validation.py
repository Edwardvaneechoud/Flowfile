"""Validation engine for data contracts and the data_validation node.

Each rule provides its own Flowfile expression via ``to_expression()``.
This module collects those expressions, converts them to Polars expressions
via ``to_expr()`` (``polars_expr_transformer.simple_function_to_expr``), and
evaluates them against a ``LazyFrame``.

Shared between:
- The ``data_validation`` flow node (appends ``_is_valid`` / ``_violations`` columns)
- The catalog contract validation endpoint (returns a ``ValidationResult``)
"""

from __future__ import annotations

import logging

import polars as pl
from polars_expr_transformer import simple_function_to_expr as to_expr

from flowfile_core.schemas.contract_schema import (
    DataContractDefinition,
    DtypeRule,
    RuleResult,
    UniqueRule,
    ValidationResult,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Expression collection
# ---------------------------------------------------------------------------


def _build_column_check(col_name: str, rule) -> tuple[str, pl.Expr | None]:
    """Return ``(rule_name, pl.Expr | None)`` for a single column rule.

    Most rules produce a Flowfile expression string that is parsed via
    ``to_expr()``.  Special cases (``DtypeRule``, ``UniqueRule``) are
    handled with native Polars operations.
    """
    rule_name = f"{col_name}:{rule.rule_type}"

    # DtypeRule is a schema-level check — cannot be a row-level expression
    if isinstance(rule, DtypeRule):
        return (rule_name, None)

    # UniqueRule needs native Polars (is_unique is not in polars_expr_transformer)
    if isinstance(rule, UniqueRule):
        return (rule_name, pl.col(col_name).is_unique())

    # All other rules produce a Flowfile expression string
    ff_expr = rule.to_expression(col_name)
    try:
        return (rule_name, to_expr(ff_expr))
    except Exception:
        logger.warning("Failed to parse Flowfile expression: %s", ff_expr)
        return (rule_name, None)


def collect_checks(definition: DataContractDefinition) -> list[tuple[str, pl.Expr]]:
    """Collect ``(rule_name, pl.Expr)`` pairs from the contract definition.

    Skips rules that cannot be represented as row-level expressions (e.g.
    ``DtypeRule``).
    """
    checks: list[tuple[str, pl.Expr]] = []
    for col in definition.columns:
        for rule in col.rules:
            name, expr = _build_column_check(col.name, rule)
            if expr is not None:
                checks.append((name, expr))

    for rule in definition.general_rules:
        try:
            checks.append((rule.name, to_expr(rule.to_expression())))
        except Exception:
            logger.warning("Failed to parse general rule expression: %s", rule.expression)

    return checks


# ---------------------------------------------------------------------------
# Node-level: append validation columns to a LazyFrame
# ---------------------------------------------------------------------------


def apply_validation(
    lf: pl.LazyFrame,
    definition: DataContractDefinition,
    add_validity: bool = True,
    add_details: bool = False,
) -> pl.LazyFrame:
    """Append ``_is_valid`` and optionally ``_violations`` columns to *lf*."""
    checks = collect_checks(definition)

    if not checks:
        if add_validity:
            lf = lf.with_columns(pl.lit(True).alias("_is_valid"))
        if add_details:
            lf = lf.with_columns(pl.lit("").alias("_violations"))
        return lf

    if add_validity:
        validity_expr = pl.lit(True)
        for _, expr in checks:
            validity_expr = validity_expr & expr
        lf = lf.with_columns(validity_expr.alias("_is_valid"))

    if add_details:
        violation_parts = [pl.when(~expr).then(pl.lit(name)).otherwise(pl.lit("")) for name, expr in checks]
        if violation_parts:
            lf = lf.with_columns(pl.concat_str(violation_parts, separator=",").alias("_violations"))
        else:
            lf = lf.with_columns(pl.lit("").alias("_violations"))

    return lf


# ---------------------------------------------------------------------------
# Contract-level: full validation returning a ValidationResult
# ---------------------------------------------------------------------------


def _validate_schema(lf: pl.LazyFrame, definition: DataContractDefinition) -> list[RuleResult]:
    """Check column existence, dtype, and extra-column rules against the schema."""
    results: list[RuleResult] = []
    actual_schema = dict(lf.collect_schema())

    for col in definition.columns:
        if col.name not in actual_schema:
            results.append(
                RuleResult(
                    rule_name=f"{col.name}:exists",
                    column=col.name,
                    passed=False,
                    message=f"Column '{col.name}' missing",
                )
            )
            continue

        # Check DtypeRule(s) on this column
        for rule in col.rules:
            if isinstance(rule, DtypeRule):
                actual_dtype = str(actual_schema[col.name])
                passed = actual_dtype == rule.expected_dtype
                results.append(
                    RuleResult(
                        rule_name=f"{col.name}:dtype",
                        column=col.name,
                        passed=passed,
                        message=f"Expected {rule.expected_dtype}, got {actual_dtype}" if not passed else "",
                    )
                )

    if not definition.allow_extra_columns:
        expected_names = {c.name for c in definition.columns}
        for name in actual_schema:
            if name not in expected_names:
                results.append(
                    RuleResult(
                        rule_name=f"{name}:unexpected",
                        column=name,
                        passed=False,
                        message=f"Unexpected column '{name}'",
                    )
                )

    return results


def validate_contract(lf: pl.LazyFrame, definition: DataContractDefinition) -> ValidationResult:
    """Run full contract validation: schema checks + row-level rule evaluation."""
    all_results: list[RuleResult] = []

    # 1. Schema-level checks
    all_results.extend(_validate_schema(lf, definition))

    # 2. Row-level checks
    checks = collect_checks(definition)
    if checks:
        # Build a frame that evaluates each check and counts violations
        check_exprs = [expr.alias(name) for name, expr in checks]
        try:
            evaluated = lf.select([pl.len().alias("_total")] + check_exprs).collect()
            total_rows = evaluated["_total"][0]

            for name, _ in checks:
                col_data = evaluated[name]
                violation_count = int(col_data.not_().sum())
                passed = violation_count == 0
                col_name = name.split(":")[0] if ":" in name else None
                all_results.append(
                    RuleResult(
                        rule_name=name,
                        column=col_name,
                        passed=passed,
                        violation_count=violation_count,
                        message=f"{violation_count} violation(s)" if not passed else "",
                    )
                )
        except Exception as e:
            logger.warning("Error evaluating row-level checks: %s", e)
            total_rows = 0
    else:
        try:
            total_rows = lf.select(pl.len()).collect().item()
        except Exception:
            total_rows = 0

    # 3. Row count checks
    if definition.row_count:
        rc = definition.row_count
        if rc.min_rows is not None:
            passed = total_rows >= rc.min_rows
            all_results.append(
                RuleResult(
                    rule_name="table:min_row_count",
                    passed=passed,
                    violation_count=0 if passed else 1,
                    message="" if passed else f"Expected >= {rc.min_rows} rows, got {total_rows}",
                )
            )
        if rc.max_rows is not None:
            passed = total_rows <= rc.max_rows
            all_results.append(
                RuleResult(
                    rule_name="table:max_row_count",
                    passed=passed,
                    violation_count=0 if passed else 1,
                    message="" if passed else f"Expected <= {rc.max_rows} rows, got {total_rows}",
                )
            )

    overall_passed = all(r.passed for r in all_results)
    valid_rows = total_rows - max((r.violation_count for r in all_results), default=0)

    return ValidationResult(
        passed=overall_passed,
        rule_results=all_results,
        total_rows=total_rows,
        valid_rows=max(valid_rows, 0),
    )
