"""Pydantic schemas for the Data Contract system.

Defines typed rule models for column-level and table-level validation,
the contract definition, and API request/response schemas.

Each column rule provides a `to_expression(column)` method that returns
a Flowfile expression string. The validation engine collects these
expressions and evaluates them via `to_expr()` (polars_expr_transformer).
"""

from __future__ import annotations

from abc import abstractmethod
from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field

from flowfile_core.types import DataTypeStr

# ==================== Column-Level Rule Types ====================


class BaseColumnRule(BaseModel):
    """Base for all column-level rules.

    Each subclass must implement ``to_expression(column)`` which returns a
    Flowfile expression string for the given column name.
    """

    @abstractmethod
    def to_expression(self, column: str) -> str:
        """Return a Flowfile expression string for this rule applied to *column*.

        Examples: ``'is_not_empty([age])'``, ``'([age]>=0) & ([age]<=150)'``
        """
        ...


class NotNullRule(BaseColumnRule):
    rule_type: Literal["not_null"] = "not_null"

    def to_expression(self, column: str) -> str:
        return f"is_not_empty([{column}])"


class UniqueRule(BaseColumnRule):
    rule_type: Literal["unique"] = "unique"

    def to_expression(self, column: str) -> str:
        # is_unique is not a standard Flowfile expression function;
        # the validation engine handles this rule via Polars directly.
        return f"is_unique([{column}])"


class DtypeRule(BaseColumnRule):
    rule_type: Literal["dtype"] = "dtype"
    expected_dtype: DataTypeStr

    def to_expression(self, column: str) -> str:
        # dtype checks are schema-level, not row-level.
        # The validation engine handles this separately.
        return f'dtype([{column}]) = "{self.expected_dtype}"'


class ValueRangeRule(BaseColumnRule):
    rule_type: Literal["value_range"] = "value_range"
    min_value: float | None = None
    max_value: float | None = None

    def to_expression(self, column: str) -> str:
        parts: list[str] = []
        if self.min_value is not None:
            parts.append(f"([{column}]>={self.min_value})")
        if self.max_value is not None:
            parts.append(f"([{column}]<={self.max_value})")
        return " & ".join(parts) if parts else "true"


class AllowedValuesRule(BaseColumnRule):
    rule_type: Literal["allowed_values"] = "allowed_values"
    values: list[str]

    def to_expression(self, column: str) -> str:
        if len(self.values) == 1:
            return f'[{column}]="{self.values[0]}"'
        conditions = [f'([{column}]="{v}")' for v in self.values]
        return " | ".join(conditions)


class RegexRule(BaseColumnRule):
    rule_type: Literal["regex"] = "regex"
    pattern: str

    def to_expression(self, column: str) -> str:
        return f'contains([{column}], "{self.pattern}")'


class CustomExpressionRule(BaseColumnRule):
    """Free-form Flowfile expression.

    The user writes the expression directly using Flowfile syntax.
    """

    rule_type: Literal["custom_expression"] = "custom_expression"
    expression: str

    def to_expression(self, column: str) -> str:
        return self.expression


# Discriminated union — Pydantic picks the right model by ``rule_type``
ColumnRule = Annotated[
    NotNullRule | UniqueRule | DtypeRule | ValueRangeRule | AllowedValuesRule | RegexRule | CustomExpressionRule,
    Field(discriminator="rule_type"),
]


# ==================== Column Contract ====================


class ColumnContract(BaseModel):
    """A column in the contract: its identity plus a list of typed rules."""

    name: str
    rules: list[ColumnRule] = Field(default_factory=list)

    def get_expressions(self) -> list[tuple[str, str]]:
        """Return ``(rule_name, flowfile_expression)`` for each rule on this column."""
        return [(f"{self.name}:{rule.rule_type}", rule.to_expression(self.name)) for rule in self.rules]


# ==================== General (Table-Level) Rules ====================


class GeneralRule(BaseModel):
    """A table-level / cross-column rule written as a Flowfile expression.

    Examples::

        GeneralRule(name="start_before_end", expression="[start_date]<=[end_date]")
        GeneralRule(name="has_contact", expression="is_not_empty([email]) | is_not_empty([phone])")
    """

    name: str
    expression: str
    description: str | None = None

    def to_expression(self) -> str:
        return self.expression


class RowCountRule(BaseModel):
    """Convenience typed rule for row count bounds."""

    min_rows: int | None = None
    max_rows: int | None = None


# ==================== Contract Definition ====================


class DataContractDefinition(BaseModel):
    """The full contract definition: columns with rules + general rules."""

    columns: list[ColumnContract] = Field(default_factory=list)
    allow_extra_columns: bool = False
    general_rules: list[GeneralRule] = Field(default_factory=list)
    row_count: RowCountRule | None = None


# ==================== Validation Result Models ====================


class RuleResult(BaseModel):
    """Result of evaluating a single validation rule."""

    rule_name: str
    column: str | None = None
    passed: bool
    violation_count: int = 0
    message: str = ""


class ValidationResult(BaseModel):
    """Aggregated result of all contract rules."""

    passed: bool
    rule_results: list[RuleResult] = Field(default_factory=list)
    total_rows: int = 0
    valid_rows: int = 0


# ==================== API Request / Response Schemas ====================


class DataContractCreate(BaseModel):
    table_id: int
    name: str
    description: str | None = None
    definition: DataContractDefinition = Field(default_factory=DataContractDefinition)
    status: Literal["draft", "active"] = "draft"


class DataContractUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    definition: DataContractDefinition | None = None
    status: Literal["draft", "active", "archived"] | None = None


class DataContractOut(BaseModel):
    id: int
    table_id: int
    name: str
    description: str | None
    definition: DataContractDefinition
    last_validated_version: int | None
    last_validated_at: datetime | None
    last_validation_passed: bool | None
    status: str
    version: int
    owner_id: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ContractSummary(BaseModel):
    """Lightweight contract info attached to CatalogTableOut."""

    status: Literal["validated", "stale", "failed", "draft", "none"]
    last_validated_version: int | None = None
    current_version: int | None = None
    rule_count: int = 0
