"""Unit tests for kernel-script generation v2 (no Docker).

Asserts generated script content and executes the script against a stub
``flowfile_ctx`` (pure Python; touches no services).
"""

import ast

import polars as pl
import pytest

from flowfile_core.flowfile.user_defined.kernel_codegen import (
    KernelCodegenError,
    generate_kernel_script,
)

SINGLE_OUTPUT_SOURCE = '''
import polars as pl

from flowfile import node_designer as nd


class GreetSettings(nd.NodeSettings):
    main: nd.Section = nd.Section(
        title="Main",
        name_col=nd.ColumnSelector(label="Name"),
    )


class GreetNode(nd.CustomNodeBase):
    node_name: str = "Greet"
    settings_schema: GreetSettings = GreetSettings()

    def _prefix(self) -> str:
        return "Hello, "

    def process(self, *inputs):
        lf = inputs[0]
        col = self.settings_schema.main.name_col.value
        return lf.with_columns((pl.lit(self._prefix()) + pl.col(col)).alias("greeting"))
'''

MULTI_OUTPUT_SOURCE = '''
import polars as pl

from flowfile import node_designer as nd


class SplitNode(nd.CustomNodeBase):
    node_name: str = "Split"
    number_of_outputs: int = 2
    output_names: list = ["hi", "lo"]

    def process(self, *inputs):
        lf = inputs[0]
        return {"hi": lf.filter(pl.col("v") > 1), "lo": lf.filter(pl.col("v") <= 1)}
'''

SECRET_SOURCE = '''
from flowfile import node_designer as nd


class SecretNode(nd.CustomNodeBase):
    node_name: str = "Secret"

    def process(self, *inputs):
        return self.settings_schema.main.api_key.secret_value
'''

DATAFRAME_RETURN_SOURCE = '''
import polars as pl

from flowfile import node_designer as nd


class EagerNode(nd.CustomNodeBase):
    node_name: str = "Eager"

    def process(self, *inputs):
        return inputs[0].collect()
'''

COLUMN_ACTION_SOURCE = '''
import polars as pl

from flowfile import node_designer as nd


class AggSettings(nd.NodeSettings):
    agg: nd.Section = nd.Section(
        title="Agg",
        column_actions=nd.ColumnActionInput(label="Aggregations", actions=["sum", "mean"]),
    )


class AggNode(nd.CustomNodeBase):
    node_name: str = "Agg"
    settings_schema: AggSettings = AggSettings()

    def process(self, *inputs):
        lf = inputs[0]
        cfg: nd.ColumnActionValue = self.settings_schema.agg.column_actions.value
        return lf.group_by(cfg.group_by_columns).agg([
            getattr(pl.col(r.column), r.action)().alias(r.output_name)
            for r in cfg.rows
        ])
'''

COLUMN_ACTION_SETTINGS = {
    "agg": {
        "column_actions": {
            "rows": [{"column": "v", "action": "sum", "output_name": "v_sum"}],
            "group_by_columns": ["g"],
            "order_by_column": None,
        }
    }
}


class StubCtx:
    """Minimal stand-in for the kernel runtime's flowfile_ctx."""

    def __init__(self, inputs: dict[str, list[pl.LazyFrame]]):
        self._inputs = inputs
        self.published: dict[str, object] = {}

    def read_inputs(self) -> dict[str, list[pl.LazyFrame]]:
        return self._inputs

    def publish_output(self, df, name: str = "main") -> None:
        self.published[name] = df


def _run(script: str, inputs: dict[str, list[pl.LazyFrame]]) -> StubCtx:
    ctx = StubCtx(inputs)
    exec(compile(script, "<kernel-test>", "exec"), {"flowfile_ctx": ctx})
    return ctx


def _collect(value) -> pl.DataFrame:
    return value.collect() if isinstance(value, pl.LazyFrame) else value


class TestGeneratedContent:
    def test_script_is_valid_python(self):
        script = generate_kernel_script(
            node_source=SINGLE_OUTPUT_SOURCE,
            class_name="GreetNode",
            settings_values={"main": {"name_col": "name"}},
            output_names=["main"],
            number_of_inputs=1,
        )
        ast.parse(script)  # must not raise

    def test_settings_baked_as_json_not_repr(self):
        script = generate_kernel_script(
            node_source=SINGLE_OUTPUT_SOURCE,
            class_name="GreetNode",
            settings_values={"main": {"name_col": "name"}},
            output_names=["main"],
            number_of_inputs=1,
        )
        assert "_json.loads(" in script
        assert '"name_col": "name"' in script

    def test_no_return_rewrite_artifacts(self):
        script = generate_kernel_script(
            node_source=SINGLE_OUTPUT_SOURCE,
            class_name="GreetNode",
            settings_values={"main": {"name_col": "name"}},
            output_names=["main"],
            number_of_inputs=1,
        )
        # The real process def (with its return) survives verbatim; the deprecated
        # generator's "result = <expr>" rewrite does not appear.
        assert "return lf.with_columns" in script
        assert "result = lf.with_columns" not in script

    def test_helper_method_carried_over(self):
        script = generate_kernel_script(
            node_source=SINGLE_OUTPUT_SOURCE,
            class_name="GreetNode",
            settings_values={"main": {"name_col": "name"}},
            output_names=["main"],
            number_of_inputs=1,
        )
        assert "def _prefix(self)" in script

    def test_node_own_import_lifted_sdk_import_dropped(self):
        script = generate_kernel_script(
            node_source=SINGLE_OUTPUT_SOURCE,
            class_name="GreetNode",
            settings_values={"main": {"name_col": "name"}},
            output_names=["main"],
            number_of_inputs=1,
        )
        assert "import polars as pl" in script
        assert "node_designer" not in script


class TestExecution:
    def test_single_output_with_helper_and_settings(self):
        script = generate_kernel_script(
            node_source=SINGLE_OUTPUT_SOURCE,
            class_name="GreetNode",
            settings_values={"main": {"name_col": "name"}},
            output_names=["main"],
            number_of_inputs=1,
        )
        ctx = _run(script, {"main": [pl.LazyFrame({"name": ["Alice", "Bob"]})]})
        out = _collect(ctx.published["main"])
        assert out["greeting"].to_list() == ["Hello, Alice", "Hello, Bob"]

    def test_multi_output_publishes_each_declared_name(self):
        script = generate_kernel_script(
            node_source=MULTI_OUTPUT_SOURCE,
            class_name="SplitNode",
            settings_values={},
            output_names=["hi", "lo"],
            number_of_inputs=1,
        )
        ctx = _run(script, {"main": [pl.LazyFrame({"v": [0, 1, 2, 3]})]})
        assert set(ctx.published) == {"hi", "lo"}
        assert _collect(ctx.published["hi"])["v"].to_list() == [2, 3]
        assert _collect(ctx.published["lo"])["v"].to_list() == [0, 1]

    def test_dataframe_return_publishes_unwrapped(self):
        script = generate_kernel_script(
            node_source=DATAFRAME_RETURN_SOURCE,
            class_name="EagerNode",
            settings_values={},
            output_names=["main"],
            number_of_inputs=1,
        )
        ctx = _run(script, {"main": [pl.LazyFrame({"x": [1, 2]})]})
        assert _collect(ctx.published["main"])["x"].to_list() == [1, 2]

    def test_multiple_input_ports_passed_positionally(self):
        source = '''
import polars as pl

from flowfile import node_designer as nd


class JoinNode(nd.CustomNodeBase):
    node_name: str = "Join"
    number_of_inputs: int = 2

    def process(self, *inputs):
        a, b = inputs
        return a.join(b, how="cross")
'''
        script = generate_kernel_script(
            node_source=source,
            class_name="JoinNode",
            settings_values={},
            output_names=["main"],
            number_of_inputs=2,
        )
        ctx = _run(
            script,
            {"df_1": [pl.LazyFrame({"a": [1]})], "df_2": [pl.LazyFrame({"b": [2]})]},
        )
        out = _collect(ctx.published["main"])
        assert out.columns == ["a", "b"]
        assert out.to_dicts() == [{"a": 1, "b": 2}]


class TestColumnActionAttributeAccess:
    """A ColumnActionInput value is a plain dict inside the kernel; the baked wrapper
    must still allow the typed attribute access (cfg.rows, r.column) the SDK provides."""

    def test_wrap_helper_baked_in(self):
        script = generate_kernel_script(
            node_source=COLUMN_ACTION_SOURCE,
            class_name="AggNode",
            settings_values=COLUMN_ACTION_SETTINGS,
            output_names=["main"],
            number_of_inputs=1,
        )
        assert "class _AttrDict(dict):" in script
        assert "def _wrap(value):" in script

    def test_attribute_access_executes(self):
        script = generate_kernel_script(
            node_source=COLUMN_ACTION_SOURCE,
            class_name="AggNode",
            settings_values=COLUMN_ACTION_SETTINGS,
            output_names=["main"],
            number_of_inputs=1,
        )
        ctx = _run(script, {"main": [pl.LazyFrame({"g": ["a", "a", "b"], "v": [1, 2, 3]})]})
        out = _collect(ctx.published["main"]).sort("g")
        assert out.to_dicts() == [{"g": "a", "v_sum": 3}, {"g": "b", "v_sum": 3}]


class TestErrorPaths:
    def test_missing_class_raises_at_generation(self):
        with pytest.raises(KernelCodegenError, match="not found"):
            generate_kernel_script(
                node_source=SINGLE_OUTPUT_SOURCE,
                class_name="NoSuchNode",
                settings_values={},
                output_names=["main"],
                number_of_inputs=1,
            )

    def test_class_without_process_raises(self):
        source = '''
from flowfile import node_designer as nd


class NoProcessNode(nd.CustomNodeBase):
    node_name: str = "NoProcess"
'''
        with pytest.raises(KernelCodegenError, match="process"):
            generate_kernel_script(
                node_source=source,
                class_name="NoProcessNode",
                settings_values={},
                output_names=["main"],
                number_of_inputs=1,
            )

    def test_syntax_error_source_raises(self):
        with pytest.raises(KernelCodegenError, match="valid Python"):
            generate_kernel_script(
                node_source="class Broken(:\n    pass",
                class_name="Broken",
                settings_values={},
                output_names=["main"],
                number_of_inputs=1,
            )

    def test_non_json_setting_raises_naming_path(self):
        with pytest.raises(KernelCodegenError, match=r"main\.weird"):
            generate_kernel_script(
                node_source=SINGLE_OUTPUT_SOURCE,
                class_name="GreetNode",
                settings_values={"main": {"weird": object()}},
                output_names=["main"],
                number_of_inputs=1,
            )

    def test_secret_value_raises_at_execution(self):
        script = generate_kernel_script(
            node_source=SECRET_SOURCE,
            class_name="SecretNode",
            settings_values={"main": {"api_key": "ref"}},
            output_names=["main"],
            number_of_inputs=1,
        )
        with pytest.raises(RuntimeError, match="Secrets are not available"):
            _run(script, {"main": [pl.LazyFrame({"x": [1]})]})

    def test_multi_output_missing_declared_name_raises(self):
        source = '''
import polars as pl

from flowfile import node_designer as nd


class PartialNode(nd.CustomNodeBase):
    node_name: str = "Partial"
    number_of_outputs: int = 2
    output_names: list = ["hi", "lo"]

    def process(self, *inputs):
        return {"hi": inputs[0]}
'''
        script = generate_kernel_script(
            node_source=source,
            class_name="PartialNode",
            settings_values={},
            output_names=["hi", "lo"],
            number_of_inputs=1,
        )
        with pytest.raises(RuntimeError, match="lo"):
            _run(script, {"main": [pl.LazyFrame({"x": [1]})]})

    def test_single_output_none_return_raises(self):
        source = '''
from flowfile import node_designer as nd


class NoneNode(nd.CustomNodeBase):
    node_name: str = "None"

    def process(self, *inputs):
        return None
'''
        script = generate_kernel_script(
            node_source=source,
            class_name="NoneNode",
            settings_values={},
            output_names=["main"],
            number_of_inputs=1,
        )
        with pytest.raises(RuntimeError, match="None"):
            _run(script, {"main": [pl.LazyFrame({"x": [1]})]})


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
