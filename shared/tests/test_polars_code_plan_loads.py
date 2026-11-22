"""On a multi-user server a Polars Code step cannot load a plan that Polars would unpickle (GHSA-j7qm-5jvr-886f)."""

import base64
from io import BytesIO

import polars as pl
import pytest

from shared.polars_code_parser import PLAN_LOAD_REFUSED_MESSAGE, expression_plan_is_inert, polars_code_parser

FIRED: list[str] = []


def _fire():
    FIRED.append("fired")


class _Payload:
    def __reduce__(self):
        return (_fire, ())

    def __call__(self, frame):
        return frame


@pytest.fixture(autouse=True)
def _server(monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    FIRED.clear()
    yield
    FIRED.clear()


def _udf_blob() -> str:
    return base64.b64encode(pl.LazyFrame({"a": [1]}).map_batches(_Payload()).serialize()).decode()


def _json_literal(expression: pl.Expr) -> str:
    return repr(expression.meta.serialize(format="json").encode())


def _refused(code: str, num_inputs: int = 1) -> None:
    with pytest.raises(ValueError, match="serialized Polars plan") as caught:
        polars_code_parser.get_executable(code, num_inputs)
    assert str(caught.value) == PLAN_LOAD_REFUSED_MESSAGE
    assert FIRED == []


def test_the_payload_runs_when_loaded_unchecked():
    pl.LazyFrame.deserialize(BytesIO(base64.b64decode(_udf_blob())))

    assert FIRED == ["fired"]


def test_the_reported_line_is_refused():
    _refused(f'pl.LazyFrame.deserialize(BytesIO(base64.b64decode("{_udf_blob()}")))', 0)


@pytest.mark.parametrize(
    "template",
    [
        "output_df = pl.DataFrame.deserialize(BytesIO(base64.b64decode('{blob}'))).lazy()",
        "load = pl.LazyFrame.deserialize\noutput_df = load(BytesIO(base64.b64decode('{blob}')))",
        "output_df = input_df.select(pl.Expr.deserialize(BytesIO(base64.b64decode('{blob}'))))",
        "output_df = input_df.select(pl.Expr.deserialize(BytesIO(base64.b64decode('{blob}')), format='json'))",
        "output_df = input_df.select(pl.Expr.deserialize(BytesIO(base64.b64decode('{blob}')), format='binary'))",
        "def transform(input_df):\n    return pl.LazyFrame.deserialize(BytesIO(base64.b64decode('{blob}')))",
    ],
    ids=["dataframe", "alias", "expr binary", "expr json decoded", "expr explicit binary", "function form"],
)
def test_every_other_deserialize_is_refused(template):
    _refused(template.format(blob=_udf_blob()))


@pytest.mark.parametrize(
    "expression",
    [
        pl.col("a").map_elements(lambda x: x, return_dtype=pl.Int64),
        pl.col("a").map_batches(lambda s: s, return_dtype=pl.Int64),
        pl.fold(pl.lit(0), lambda acc, x: acc + x, [pl.col("a")]),
        pl.col("a").name.map(lambda name: name + "_x"),
    ],
    ids=["map_elements", "map_batches", "fold", "name.map"],
)
def test_an_expression_plan_holding_a_function_is_refused(expression):
    _refused(f"output_df = input_df.select(pl.Expr.deserialize(BytesIO({_json_literal(expression)}), format='json'))")


@pytest.mark.parametrize(
    "plan",
    [
        b'{"\\u0041nonymousFunction": {"input": [], "function": [128, 5, 46]}}',
        b'{ "AnonymousFunction" : {"input": [], "function": [128, 5, 46]}}',
        b'{"Column": "a", "Column": "b"}',
        b'{"Alias": [{"Column": "a"}, "b"], "extra": [99, 111, 115, 10]}',
        b'{"PythonUdf": {"payload": "x"}}',
        b"not json",
    ],
    ids=["escaped key", "spaced key", "duplicate key", "bytes outside a literal", "python key", "not json"],
)
def test_a_plan_that_hides_or_carries_code_is_not_inert(plan):
    assert not expression_plan_is_inert(plan)
    _refused(f"output_df = input_df.select(pl.Expr.deserialize(BytesIO({plan!r}), format='json'))")


@pytest.mark.parametrize(
    "prefix",
    [
        "BytesIO = lambda data: data\n",
        "pl = pl\n",
        "def helper(pl):\n    return pl\n",
    ],
    ids=["BytesIO rebound", "pl rebound", "pl as a parameter"],
)
def test_an_expression_load_with_its_names_rebound_is_refused(prefix):
    literal = _json_literal(pl.col("a") + 1)
    _refused(f"{prefix}output_df = input_df.select(pl.Expr.deserialize(BytesIO({literal}), format='json'))")


@pytest.mark.parametrize(
    "expression, expected",
    [
        ((pl.col("a") + 1).alias("b"), [2, 3]),
        (pl.col("a").is_in([1, 5, 9]).alias("b"), [True, False]),
        (pl.col("a").replace({1: 10}).alias("b"), [10, 2]),
        (pl.lit(b"raw").alias("b"), [b"raw", b"raw"]),
    ],
    ids=["arithmetic", "is_in", "replace", "binary literal"],
)
def test_an_inert_expression_plan_still_runs(expression, expected):
    code = (
        f"output_df = input_df.with_columns(pl.Expr.deserialize(BytesIO({_json_literal(expression)}), format='json'))"
    )

    result = polars_code_parser.get_executable(code, 1)(pl.LazyFrame({"a": [1, 2]})).collect()

    assert result["b"].to_list() == expected


@pytest.mark.parametrize("mode", ["electron", "package"])
def test_a_single_user_install_still_loads_a_plan(monkeypatch, mode):
    monkeypatch.setenv("FLOWFILE_MODE", mode)
    code = "output_df = pl.LazyFrame.deserialize(BytesIO(base64.b64decode('{blob}')))".format(
        blob=base64.b64encode(pl.LazyFrame({"a": [1, 2]}).serialize()).decode()
    )

    result = polars_code_parser.get_executable(code, 1)(pl.LazyFrame({"a": [0]})).collect()

    assert result["a"].to_list() == [1, 2]


def test_server_mode_refuses_too(monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "server")
    _refused(f'pl.LazyFrame.deserialize(BytesIO(base64.b64decode("{_udf_blob()}")))', 0)
