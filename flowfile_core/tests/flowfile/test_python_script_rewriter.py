"""
Unit tests for python_script_rewriter.py — the AST rewriting engine that
transforms flowfile.* API calls into plain Python equivalents.
"""

import ast

import pytest

from flowfile_core.flowfile.code_generator.python_script_rewriter import (
    FlowfileUsageAnalysis,
    analyze_flowfile_usage,
    build_function_code,
    extract_imports,
    get_import_names,
    get_required_packages,
    rewrite_flowfile_calls,
)


# ---------------------------------------------------------------------------
# Tests for analyze_flowfile_usage
# ---------------------------------------------------------------------------


class TestAnalyzeFlowfileUsage:
    def test_single_input(self):
        code = "df = flowfile.read_input()"
        analysis = analyze_flowfile_usage(code)
        assert analysis.input_mode == "single"

    def test_multi_input(self):
        code = "inputs = flowfile.read_inputs()"
        analysis = analyze_flowfile_usage(code)
        assert analysis.input_mode == "multi"

    def test_no_input(self):
        code = "x = 1 + 2"
        analysis = analyze_flowfile_usage(code)
        assert analysis.input_mode == "none"

    def test_publish_output(self):
        code = "flowfile.publish_output(df)"
        analysis = analyze_flowfile_usage(code)
        assert analysis.has_output is True
        assert len(analysis.output_exprs) == 1

    def test_no_output(self):
        code = "df = flowfile.read_input()"
        analysis = analyze_flowfile_usage(code)
        assert analysis.has_output is False

    def test_passthrough_output(self):
        code = "flowfile.publish_output(flowfile.read_input())"
        analysis = analyze_flowfile_usage(code)
        assert analysis.passthrough_output is True

    def test_non_passthrough_output(self):
        code = "flowfile.publish_output(result)"
        analysis = analyze_flowfile_usage(code)
        assert analysis.passthrough_output is False

    def test_artifact_publish(self):
        code = 'flowfile.publish_artifact("model", clf)'
        analysis = analyze_flowfile_usage(code)
        assert len(analysis.artifacts_published) == 1
        assert analysis.artifacts_published[0][0] == "model"

    def test_artifact_consume(self):
        code = 'model = flowfile.read_artifact("model")'
        analysis = analyze_flowfile_usage(code)
        assert analysis.artifacts_consumed == ["model"]

    def test_artifact_delete(self):
        code = 'flowfile.delete_artifact("model")'
        analysis = analyze_flowfile_usage(code)
        assert analysis.artifacts_deleted == ["model"]

    def test_dynamic_artifact_name_detected(self):
        code = "flowfile.read_artifact(name_var)"
        analysis = analyze_flowfile_usage(code)
        assert len(analysis.dynamic_artifact_names) == 1

    def test_dynamic_publish_artifact_name_detected(self):
        code = "flowfile.publish_artifact(name_var, obj)"
        analysis = analyze_flowfile_usage(code)
        assert len(analysis.dynamic_artifact_names) == 1

    def test_dynamic_delete_artifact_name_detected(self):
        code = "flowfile.delete_artifact(name_var)"
        analysis = analyze_flowfile_usage(code)
        assert len(analysis.dynamic_artifact_names) == 1

    def test_logging(self):
        code = 'flowfile.log("hello")'
        analysis = analyze_flowfile_usage(code)
        assert analysis.has_logging is True

    def test_log_with_level(self):
        code = 'flowfile.log("hello", "ERROR")'
        analysis = analyze_flowfile_usage(code)
        assert analysis.has_logging is True

    def test_log_info(self):
        code = 'flowfile.log_info("hello")'
        analysis = analyze_flowfile_usage(code)
        assert analysis.has_logging is True

    def test_list_artifacts(self):
        code = "arts = flowfile.list_artifacts()"
        analysis = analyze_flowfile_usage(code)
        assert analysis.has_list_artifacts is True

    def test_unsupported_display_call(self):
        code = "flowfile.display(fig)"
        analysis = analyze_flowfile_usage(code)
        assert len(analysis.unsupported_calls) == 1
        assert analysis.unsupported_calls[0][0] == "display"

    def test_multiple_artifacts(self):
        code = (
            'flowfile.publish_artifact("model", clf)\n'
            'flowfile.publish_artifact("scaler", sc)\n'
            'x = flowfile.read_artifact("model")\n'
        )
        analysis = analyze_flowfile_usage(code)
        assert len(analysis.artifacts_published) == 2
        assert analysis.artifacts_consumed == ["model"]

    def test_syntax_error_raises(self):
        code = "def foo(:"
        with pytest.raises(SyntaxError):
            analyze_flowfile_usage(code)

    def test_complete_script(self):
        code = (
            "import polars as pl\n"
            "from sklearn.ensemble import RandomForestClassifier\n"
            "\n"
            "df = flowfile.read_input().collect()\n"
            "X = df.select(['f1', 'f2']).to_numpy()\n"
            "y = df.get_column('target').to_numpy()\n"
            "\n"
            "model = RandomForestClassifier()\n"
            "model.fit(X, y)\n"
            "\n"
            'flowfile.publish_artifact("rf_model", model)\n'
            "flowfile.publish_output(flowfile.read_input())\n"
        )
        analysis = analyze_flowfile_usage(code)
        assert analysis.input_mode == "single"
        assert analysis.has_output is True
        assert analysis.passthrough_output is True
        assert len(analysis.artifacts_published) == 1
        assert analysis.artifacts_published[0][0] == "rf_model"


# ---------------------------------------------------------------------------
# Tests for rewrite_flowfile_calls
# ---------------------------------------------------------------------------


class TestRewriteFlowfileCalls:
    def test_read_input_replaced(self):
        code = "df = flowfile.read_input()"
        analysis = analyze_flowfile_usage(code)
        result, _ = rewrite_flowfile_calls(code, analysis)
        assert "flowfile" not in result
        assert "input_df" in result

    def test_read_input_with_collect(self):
        code = "df = flowfile.read_input().collect()"
        analysis = analyze_flowfile_usage(code)
        result, _ = rewrite_flowfile_calls(code, analysis)
        assert "flowfile" not in result
        assert "input_df.collect()" in result

    def test_read_inputs_replaced(self):
        code = 'dfs = flowfile.read_inputs()\ndf = dfs["main"]'
        analysis = analyze_flowfile_usage(code)
        result, _ = rewrite_flowfile_calls(code, analysis)
        assert "flowfile" not in result
        assert "inputs" in result

    def test_publish_output_removed(self):
        code = "x = 1\nflowfile.publish_output(df)\ny = 2"
        analysis = analyze_flowfile_usage(code)
        result, _ = rewrite_flowfile_calls(code, analysis)
        assert "publish_output" not in result
        assert "x = 1" in result
        assert "y = 2" in result

    def test_publish_artifact_becomes_assignment(self):
        code = 'flowfile.publish_artifact("model", clf)'
        analysis = analyze_flowfile_usage(code)
        result, _ = rewrite_flowfile_calls(code, analysis, kernel_id="k1")
        assert "flowfile" not in result
        assert "_artifacts" in result
        assert "k1" in result
        assert "model" in result

    def test_read_artifact_becomes_subscript(self):
        code = 'model = flowfile.read_artifact("model")'
        analysis = analyze_flowfile_usage(code)
        result, _ = rewrite_flowfile_calls(code, analysis, kernel_id="k1")
        assert "flowfile" not in result
        assert "_artifacts" in result
        assert "k1" in result
        assert "model" in result

    def test_delete_artifact_becomes_del(self):
        code = 'flowfile.delete_artifact("model")'
        analysis = analyze_flowfile_usage(code)
        result, _ = rewrite_flowfile_calls(code, analysis, kernel_id="k1")
        assert "flowfile" not in result
        assert "del _artifacts" in result
        assert "k1" in result

    def test_list_artifacts_becomes_kernel_dict(self):
        code = "arts = flowfile.list_artifacts()"
        analysis = analyze_flowfile_usage(code)
        result, _ = rewrite_flowfile_calls(code, analysis, kernel_id="k1")
        assert "flowfile" not in result
        assert "_artifacts" in result
        assert "k1" in result
        assert "dict(" in result

    def test_default_kernel_id_when_none(self):
        code = 'flowfile.publish_artifact("model", clf)'
        analysis = analyze_flowfile_usage(code)
        result, _ = rewrite_flowfile_calls(code, analysis, kernel_id=None)
        assert "_default" in result

    def test_artifacts_scoped_to_kernel(self):
        """Verify artifact access includes the kernel_id key."""
        code = 'model = flowfile.read_artifact("model")'
        analysis = analyze_flowfile_usage(code)
        result, _ = rewrite_flowfile_calls(code, analysis, kernel_id="my_kernel")
        assert "my_kernel" in result
        assert "model" in result

    def test_log_becomes_print(self):
        code = 'flowfile.log("hello")'
        analysis = analyze_flowfile_usage(code)
        result, _ = rewrite_flowfile_calls(code, analysis)
        assert "print" in result
        assert "flowfile" not in result

    def test_log_with_level_becomes_print(self):
        code = 'flowfile.log("hello", "ERROR")'
        analysis = analyze_flowfile_usage(code)
        result, _ = rewrite_flowfile_calls(code, analysis)
        assert "print" in result

    def test_log_info_becomes_print(self):
        code = 'flowfile.log_info("processing")'
        analysis = analyze_flowfile_usage(code)
        result, _ = rewrite_flowfile_calls(code, analysis)
        assert "print" in result
        assert "INFO" in result

    def test_non_flowfile_code_unchanged(self):
        code = "x = 1 + 2\ny = x * 3"
        analysis = analyze_flowfile_usage(code)
        result, _ = rewrite_flowfile_calls(code, analysis)
        assert "x = 1 + 2" in result
        assert "y = x * 3" in result

    def test_chained_collect(self):
        code = "df = flowfile.read_input().collect()\nresult = df.select(['a'])"
        analysis = analyze_flowfile_usage(code)
        result, _ = rewrite_flowfile_calls(code, analysis)
        assert "input_df.collect()" in result
        assert "result = df.select" in result

    def test_unsupported_call_returns_marker(self):
        """Unsupported calls should produce markers for comment replacement."""
        code = "flowfile.publish_global('model', obj)\nx = 1"
        analysis = analyze_flowfile_usage(code)
        result, markers = rewrite_flowfile_calls(code, analysis)
        assert len(markers) == 1
        marker = list(markers.keys())[0]
        assert marker in result
        source = list(markers.values())[0]
        assert "publish_global" in source


# ---------------------------------------------------------------------------
# Tests for extract_imports
# ---------------------------------------------------------------------------


class TestExtractImports:
    def test_standard_import(self):
        code = "import numpy as np\nx = 1"
        result = extract_imports(code)
        assert "import numpy as np" in result

    def test_from_import(self):
        code = "from sklearn.ensemble import RandomForestClassifier"
        result = extract_imports(code)
        assert len(result) == 1
        assert "RandomForestClassifier" in result[0]

    def test_flowfile_import_excluded(self):
        code = "import flowfile\nimport numpy as np"
        result = extract_imports(code)
        assert len(result) == 1
        assert "numpy" in result[0]

    def test_polars_import_included(self):
        code = "import polars as pl"
        result = extract_imports(code)
        assert "import polars as pl" in result

    def test_no_imports(self):
        code = "x = 1 + 2"
        result = extract_imports(code)
        assert result == []

    def test_multiple_imports(self):
        code = "import numpy\nimport pandas\nfrom os import path"
        result = extract_imports(code)
        assert len(result) == 3


# ---------------------------------------------------------------------------
# Tests for build_function_code
# ---------------------------------------------------------------------------


class TestBuildFunctionCode:
    def test_simple_single_input(self):
        code = "df = input_df.collect()\nresult = df.select(['a'])"
        analysis = FlowfileUsageAnalysis(input_mode="single", has_output=True)
        # Add a mock output expr
        analysis.output_exprs = [ast.parse("result", mode="eval").body]
        func_def, call_code = build_function_code(
            node_id=5,
            rewritten_code=code,
            analysis=analysis,
            input_vars={"main": "df_3"},
        )
        assert "def _node_5(input_df: pl.LazyFrame)" in func_def
        assert "return" in func_def
        assert "df_5 = _node_5(df_3)" == call_code

    def test_no_input(self):
        code = "x = 42"
        analysis = FlowfileUsageAnalysis(input_mode="none")
        func_def, call_code = build_function_code(
            node_id=1,
            rewritten_code=code,
            analysis=analysis,
            input_vars={},
        )
        assert "def _node_1()" in func_def
        assert "df_1 = _node_1()" == call_code

    def test_multi_input(self):
        code = 'df = inputs["main"]'
        analysis = FlowfileUsageAnalysis(input_mode="multi")
        func_def, call_code = build_function_code(
            node_id=2,
            rewritten_code=code,
            analysis=analysis,
            input_vars={"main": "df_1", "right": "df_0"},
        )
        # Runtime uses dict[str, list[pl.LazyFrame]]
        assert "inputs: dict[str, list[pl.LazyFrame]]" in func_def
        assert "df_2 = _node_2(" in call_code

    def test_multi_input_grouped(self):
        """Multiple main inputs (main_0, main_1) should be grouped into a list."""
        code = 'dfs = inputs["main"]'
        analysis = FlowfileUsageAnalysis(input_mode="multi")
        func_def, call_code = build_function_code(
            node_id=2,
            rewritten_code=code,
            analysis=analysis,
            input_vars={"main_0": "df_1", "main_1": "df_3"},
        )
        assert "inputs: dict[str, list[pl.LazyFrame]]" in func_def
        # The call should group main_0 and main_1 under "main"
        assert '"main": [df_1, df_3]' in call_code

    def test_passthrough_return(self):
        code = "x = 1"
        analysis = FlowfileUsageAnalysis(input_mode="single", has_output=True, passthrough_output=True)
        analysis.output_exprs = [ast.parse("flowfile.read_input()", mode="eval").body]
        func_def, _ = build_function_code(
            node_id=3,
            rewritten_code=code,
            analysis=analysis,
            input_vars={"main": "df_1"},
        )
        assert "return input_df" in func_def

    def test_implicit_passthrough_no_output(self):
        """If no publish_output is called with single input, pass through."""
        code = "_artifacts['model'] = clf"
        analysis = FlowfileUsageAnalysis(input_mode="single")
        func_def, _ = build_function_code(
            node_id=4,
            rewritten_code=code,
            analysis=analysis,
            input_vars={"main": "df_2"},
        )
        assert "return input_df" in func_def


# ---------------------------------------------------------------------------
# Tests for get_import_names / get_required_packages
# ---------------------------------------------------------------------------


class TestPackageMapping:
    def test_scikit_learn(self):
        assert get_import_names("scikit-learn") == ["sklearn"]

    def test_pillow(self):
        assert get_import_names("pillow") == ["PIL"]

    def test_standard_package(self):
        assert get_import_names("numpy") == ["numpy"]

    def test_dash_to_underscore(self):
        assert get_import_names("my-package") == ["my_package"]


class TestGetRequiredPackages:
    def test_basic_match(self):
        user_imports = ["from sklearn.ensemble import RandomForestClassifier"]
        kernel_packages = ["scikit-learn", "numpy", "polars"]
        result = get_required_packages(user_imports, kernel_packages)
        assert result == ["scikit-learn"]

    def test_multiple_matches(self):
        user_imports = [
            "import numpy as np",
            "from sklearn.ensemble import RandomForestClassifier",
        ]
        kernel_packages = ["scikit-learn", "numpy", "polars"]
        result = get_required_packages(user_imports, kernel_packages)
        assert result == ["numpy", "scikit-learn"]

    def test_no_match(self):
        user_imports = ["import polars as pl"]
        kernel_packages = ["scikit-learn"]
        result = get_required_packages(user_imports, kernel_packages)
        assert result == []

    def test_empty_inputs(self):
        assert get_required_packages([], []) == []

    def test_pillow_mapping(self):
        user_imports = ["from PIL import Image"]
        kernel_packages = ["pillow"]
        result = get_required_packages(user_imports, kernel_packages)
        assert result == ["pillow"]
