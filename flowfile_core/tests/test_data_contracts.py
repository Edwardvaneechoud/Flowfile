"""Tests for data contract models and validation engine."""

import polars as pl
import pytest

from flowfile_core.schemas.contract_schema import (
    AllowedValuesRule,
    ColumnContract,
    CustomExpressionRule,
    DataContractDefinition,
    DtypeRule,
    GeneralRule,
    NotNullRule,
    RegexRule,
    RowCountRule,
    UniqueRule,
    ValidationResult,
    ValueRangeRule,
)


# ---------------------------------------------------------------------------
# Rule to_expression tests
# ---------------------------------------------------------------------------


class TestRuleExpressions:
    def test_not_null_expression(self):
        rule = NotNullRule()
        assert rule.to_expression("age") == "is_not_empty([age])"

    def test_unique_expression(self):
        rule = UniqueRule()
        assert rule.to_expression("id") == "is_unique([id])"

    def test_dtype_expression(self):
        rule = DtypeRule(expected_dtype="Int64")
        expr = rule.to_expression("age")
        assert "Int64" in expr
        assert "[age]" in expr

    def test_value_range_both_bounds(self):
        rule = ValueRangeRule(min_value=0, max_value=150)
        expr = rule.to_expression("age")
        assert "([age]>=0" in expr
        assert "([age]<=150" in expr
        assert "&" in expr

    def test_value_range_min_only(self):
        rule = ValueRangeRule(min_value=0)
        expr = rule.to_expression("age")
        assert "([age]>=0" in expr
        assert "<=" not in expr

    def test_value_range_max_only(self):
        rule = ValueRangeRule(max_value=100)
        expr = rule.to_expression("count")
        assert "([count]<=100" in expr

    def test_value_range_no_bounds(self):
        rule = ValueRangeRule()
        assert rule.to_expression("x") == "true"

    def test_allowed_values_multiple(self):
        rule = AllowedValuesRule(values=["A", "B", "C"])
        expr = rule.to_expression("status")
        assert '([status]="A")' in expr
        assert '([status]="B")' in expr
        assert "|" in expr

    def test_allowed_values_single(self):
        rule = AllowedValuesRule(values=["active"])
        expr = rule.to_expression("status")
        assert expr == '[status]="active"'

    def test_regex_expression(self):
        rule = RegexRule(pattern=r"^\d+$")
        expr = rule.to_expression("code")
        assert "contains([code]" in expr

    def test_custom_expression(self):
        rule = CustomExpressionRule(expression="[age] > 0")
        assert rule.to_expression("age") == "[age] > 0"


# ---------------------------------------------------------------------------
# ColumnContract tests
# ---------------------------------------------------------------------------


class TestColumnContract:
    def test_get_expressions(self):
        col = ColumnContract(
            name="age",
            rules=[NotNullRule(), ValueRangeRule(min_value=0, max_value=150)],
        )
        expressions = col.get_expressions()
        assert len(expressions) == 2
        assert expressions[0][0] == "age:not_null"
        assert expressions[1][0] == "age:value_range"

    def test_empty_rules(self):
        col = ColumnContract(name="x")
        assert col.get_expressions() == []


# ---------------------------------------------------------------------------
# DataContractDefinition serialization tests
# ---------------------------------------------------------------------------


class TestContractSerialization:
    def test_roundtrip_json(self):
        definition = DataContractDefinition(
            columns=[
                ColumnContract(
                    name="age",
                    rules=[DtypeRule(expected_dtype="Int64"), NotNullRule()],
                ),
                ColumnContract(
                    name="name",
                    rules=[NotNullRule(), RegexRule(pattern="^[A-Z]")],
                ),
            ],
            allow_extra_columns=False,
            general_rules=[
                GeneralRule(name="positive_age", expression="[age] > 0"),
            ],
            row_count=RowCountRule(min_rows=1),
        )
        json_str = definition.model_dump_json()
        restored = DataContractDefinition.model_validate_json(json_str)

        assert len(restored.columns) == 2
        assert len(restored.columns[0].rules) == 2
        assert isinstance(restored.columns[0].rules[0], DtypeRule)
        assert isinstance(restored.columns[0].rules[1], NotNullRule)
        assert isinstance(restored.columns[1].rules[1], RegexRule)
        assert len(restored.general_rules) == 1
        assert restored.row_count.min_rows == 1

    def test_discriminated_union(self):
        """Pydantic correctly picks rule type from JSON based on rule_type."""
        data = {
            "columns": [
                {
                    "name": "id",
                    "rules": [
                        {"rule_type": "not_null"},
                        {"rule_type": "unique"},
                        {"rule_type": "value_range", "min_value": 1},
                        {"rule_type": "allowed_values", "values": ["a", "b"]},
                        {"rule_type": "regex", "pattern": "^[a-z]+$"},
                        {"rule_type": "dtype", "expected_dtype": "Int64"},
                        {"rule_type": "custom_expression", "expression": "[id] > 0"},
                    ],
                }
            ]
        }
        definition = DataContractDefinition.model_validate(data)
        rules = definition.columns[0].rules
        assert isinstance(rules[0], NotNullRule)
        assert isinstance(rules[1], UniqueRule)
        assert isinstance(rules[2], ValueRangeRule)
        assert isinstance(rules[3], AllowedValuesRule)
        assert isinstance(rules[4], RegexRule)
        assert isinstance(rules[5], DtypeRule)
        assert isinstance(rules[6], CustomExpressionRule)


# ---------------------------------------------------------------------------
# Validation engine tests
# ---------------------------------------------------------------------------


class TestValidationEngine:
    @pytest.fixture
    def sample_lf(self):
        return pl.LazyFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", None, "Diana", "Eve"],
                "age": [25, 30, -5, 45, 200],
                "status": ["active", "active", "inactive", "active", "unknown"],
            }
        )

    def test_apply_validation_adds_is_valid(self, sample_lf):
        from flowfile_core.flowfile.flow_data_engine.validation import apply_validation

        definition = DataContractDefinition(
            columns=[
                ColumnContract(name="name", rules=[NotNullRule()]),
            ]
        )
        result = apply_validation(sample_lf, definition, add_validity=True)
        df = result.collect()
        assert "_is_valid" in df.columns
        # Row 3 (index 2) has null name
        assert df["_is_valid"].to_list() == [True, True, False, True, True]

    def test_apply_validation_value_range(self, sample_lf):
        from flowfile_core.flowfile.flow_data_engine.validation import apply_validation

        definition = DataContractDefinition(
            columns=[
                ColumnContract(name="age", rules=[ValueRangeRule(min_value=0, max_value=150)]),
            ]
        )
        result = apply_validation(sample_lf, definition, add_validity=True)
        df = result.collect()
        # Row 3 has age=-5, row 5 has age=200
        valid = df["_is_valid"].to_list()
        assert valid[0] is True  # age=25
        assert valid[2] is False  # age=-5
        assert valid[4] is False  # age=200

    def test_apply_validation_empty_definition(self, sample_lf):
        from flowfile_core.flowfile.flow_data_engine.validation import apply_validation

        definition = DataContractDefinition()
        result = apply_validation(sample_lf, definition, add_validity=True)
        df = result.collect()
        assert all(df["_is_valid"].to_list())

    def test_validate_contract_schema_check(self, sample_lf):
        from flowfile_core.flowfile.flow_data_engine.validation import validate_contract

        definition = DataContractDefinition(
            columns=[
                ColumnContract(name="id", rules=[DtypeRule(expected_dtype="Int64")]),
                ColumnContract(name="missing_col", rules=[]),
            ],
            allow_extra_columns=True,
        )
        result = validate_contract(sample_lf, definition)
        assert isinstance(result, ValidationResult)
        # missing_col should fail existence check
        missing_results = [r for r in result.rule_results if r.rule_name == "missing_col:exists"]
        assert len(missing_results) == 1
        assert missing_results[0].passed is False

    def test_validate_contract_row_count(self, sample_lf):
        from flowfile_core.flowfile.flow_data_engine.validation import validate_contract

        definition = DataContractDefinition(
            row_count=RowCountRule(min_rows=10),
            allow_extra_columns=True,
        )
        result = validate_contract(sample_lf, definition)
        assert result.passed is False
        row_check = [r for r in result.rule_results if r.rule_name == "table:min_row_count"]
        assert len(row_check) == 1
        assert row_check[0].passed is False

    def test_validate_contract_unique_rule(self, sample_lf):
        from flowfile_core.flowfile.flow_data_engine.validation import validate_contract

        definition = DataContractDefinition(
            columns=[
                ColumnContract(name="status", rules=[UniqueRule()]),
            ],
            allow_extra_columns=True,
        )
        result = validate_contract(sample_lf, definition)
        # status has duplicates ("active" appears 3 times)
        unique_check = [r for r in result.rule_results if r.rule_name == "status:unique"]
        assert len(unique_check) == 1
        assert unique_check[0].passed is False
        assert unique_check[0].violation_count > 0

    def test_validate_contract_extra_columns_rejected(self, sample_lf):
        from flowfile_core.flowfile.flow_data_engine.validation import validate_contract

        definition = DataContractDefinition(
            columns=[ColumnContract(name="id", rules=[])],
            allow_extra_columns=False,
        )
        result = validate_contract(sample_lf, definition)
        unexpected = [r for r in result.rule_results if "unexpected" in r.rule_name]
        # name, age, status are unexpected
        assert len(unexpected) == 3

    def test_validate_contract_all_pass(self):
        from flowfile_core.flowfile.flow_data_engine.validation import validate_contract

        lf = pl.LazyFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        definition = DataContractDefinition(
            columns=[
                ColumnContract(name="id", rules=[NotNullRule()]),
                ColumnContract(name="name", rules=[NotNullRule()]),
            ],
            allow_extra_columns=False,
        )
        result = validate_contract(lf, definition)
        assert result.passed is True
        assert result.total_rows == 3
