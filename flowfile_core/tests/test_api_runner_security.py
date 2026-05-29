"""Security & correctness tests for the HTTP-data-API runner (``flowfile.api_runner``).

Covers the P1 review findings:

* #1  Untrusted query params must not be able to inject Polars into an ``exec()``'d
      ``polars_code`` node: string-typed values containing string-literal/escape/
      call characters are rejected at the API seam, before substitution & exec.
* #4  Concurrent runs of the *same* published flow are serialized per ``flow_id``.
* #5  The ``columns`` orientation pushes the row limit into ``collect()`` instead
      of materializing everything and slicing.
* #10 Endpoint ``default`` values go through the same coercion/validation as
      request values.
"""

import threading
import time
from types import SimpleNamespace

import polars as pl
import pytest

from flowfile_core.flowfile import api_runner
from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.flow_api_schema import ApiParamSpec
from flowfile_core.schemas.schemas import FlowParameter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_and_save_flow(path, flow_id: int = 1, orientation: str = "records") -> None:
    """manual_input -> polars filter on ``'${region}'`` -> api_response, saved to *path*.

    The filter interpolates the ``region`` parameter *inside a string literal* in
    ``polars_code`` — exactly the sink finding #1 is about, so a value that can
    break out of that literal could bypass the filter or call ``pl.read_csv``.
    """
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="api_flow",
            path=str(path),
            execution_mode="Development",
            execution_location="local",
        )
    )
    graph = handler.get_flow(flow_id)
    graph.flow_settings.parameters = [FlowParameter(name="region", default_value="EU")]

    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist(
                [{"region": "EU", "v": 1}, {"region": "US", "v": 2}]
            ),
        )
    )

    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=2, node_type="polars_code"))
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="output_df = input_df.filter(pl.col('region') == '${region}')"
            ),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=3, node_type="api_response"))
    graph.add_api_response(
        input_schema.NodeApiResponse(flow_id=flow_id, node_id=3, depending_on_id=2, orientation=orientation)
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))

    graph.save_flow(str(path))


# ---------------------------------------------------------------------------
# #1 — injection rejection at the API seam (unit level)
# ---------------------------------------------------------------------------


# Each value can break out of a Polars ``'${region}'`` string literal and/or
# splice in a call/statement once exec()'d. The runner must reject them.
INJECTION_PAYLOADS = [
    "') | True | ('",  # OR the filter predicate to True -> returns every row
    "pl.read_csv('/etc/passwd')",  # arbitrary file read
    "scan_csv('secret.csv')",  # lazy file read
    'x" or "1"=="1',  # double-quote break-out
    "a); evil(",  # parentheses to alter/append a call
    "a; b",  # semicolon statement separator
    "first\nsecond",  # newline statement separator
    "c:\\windows",  # backslash escape
    "${other_param}",  # smuggle a second substitution token
]


@pytest.mark.parametrize("payload", INJECTION_PAYLOADS)
def test_string_injection_values_are_rejected(payload):
    specs = [ApiParamSpec(name="region", type="string")]
    with pytest.raises(api_runner.ApiParamError):
        api_runner.resolve_params(specs, {"region": payload})


@pytest.mark.parametrize(
    "value",
    [
        "US",
        "EU-WEST-1",
        "north_america",
        "2024-01-01",
        "a.b.c",
        "hello world",
        "category|subcategory",  # pipe/comma/equals are harmless inside a literal
        "a,b,c",
        "k=v&n=2",
        "42",
        "100%",
        "path/to/thing",
    ],
)
def test_safe_string_values_are_accepted(value):
    specs = [ApiParamSpec(name="region", type="string")]
    assert api_runner.resolve_params(specs, {"region": value}) == {"region": value}


def test_enum_values_are_not_subject_to_the_string_gate():
    """Enum values come from an owner-defined allow-list, so the char gate doesn't apply."""
    spec = ApiParamSpec(name="r", type="enum", enum_values=["a", "b"])
    assert api_runner._coerce(spec, "a") == "a"
    with pytest.raises(api_runner.ApiParamError):
        api_runner._coerce(spec, "c")


# ---------------------------------------------------------------------------
# #1 — injection rejection end-to-end (through run_flow_as_api)
# ---------------------------------------------------------------------------


def test_filter_bypass_injection_rejected_end_to_end(tmp_path):
    """A value engineered to OR the row filter to True is rejected before exec()."""
    flow_path = tmp_path / "flow.yaml"
    _build_and_save_flow(flow_path)
    specs = [ApiParamSpec(name="region", type="string")]

    # Positive control: a benign value filters normally to a single row.
    ok = api_runner.run_flow_as_api(str(flow_path), owner_id=1, param_specs=specs, query={"region": "US"})
    assert ok["data"] == [{"region": "US", "v": 2}]

    # The injection never reaches run_graph()/exec(), so the filter can't be bypassed.
    with pytest.raises(api_runner.ApiParamError):
        api_runner.run_flow_as_api(
            str(flow_path), owner_id=1, param_specs=specs, query={"region": "') | True | ('"}
        )


def test_read_csv_injection_cannot_exfiltrate(tmp_path):
    """An injected ``pl.read_csv`` aimed at a real secret file is rejected at the seam."""
    secret = tmp_path / "secret.csv"
    secret.write_text("col\nTOPSECRET\n")

    flow_path = tmp_path / "flow.yaml"
    _build_and_save_flow(flow_path)
    specs = [ApiParamSpec(name="region", type="string")]

    # Break out of the string literal and try to read the secret into the predicate.
    malicious = f"x') | (pl.col('region') == pl.read_csv('{secret}')['col'][0]) | ('"
    with pytest.raises(api_runner.ApiParamError):
        api_runner.run_flow_as_api(str(flow_path), owner_id=1, param_specs=specs, query={"region": malicious})

    # And the legitimate path still works (and never surfaces the secret content).
    ok = api_runner.run_flow_as_api(str(flow_path), owner_id=1, param_specs=specs, query={"region": "EU"})
    assert ok["data"] == [{"region": "EU", "v": 1}]
    assert "TOPSECRET" not in repr(ok)


# ---------------------------------------------------------------------------
# #10 — endpoint defaults go through _coerce
# ---------------------------------------------------------------------------


def test_invalid_enum_default_is_rejected():
    specs = [ApiParamSpec(name="r", type="enum", enum_values=["a", "b"], default="not-in-enum")]
    with pytest.raises(api_runner.ApiParamError):
        api_runner.resolve_params(specs, {})


def test_invalid_integer_default_is_rejected():
    specs = [ApiParamSpec(name="n", type="integer", default="not-an-int")]
    with pytest.raises(api_runner.ApiParamError):
        api_runner.resolve_params(specs, {})


def test_unsafe_string_default_is_rejected():
    specs = [ApiParamSpec(name="s", type="string", default="x'); evil(")]
    with pytest.raises(api_runner.ApiParamError):
        api_runner.resolve_params(specs, {})


def test_valid_defaults_pass_coercion():
    specs = [
        ApiParamSpec(name="n", type="integer", default="5"),
        ApiParamSpec(name="r", type="enum", enum_values=["a", "b"], default="a"),
        ApiParamSpec(name="s", type="string", default="ok-value"),
    ]
    assert api_runner.resolve_params(specs, {}) == {"n": "5", "r": "a", "s": "ok-value"}


# ---------------------------------------------------------------------------
# #5 — columns orientation pushes the limit into collect()
# ---------------------------------------------------------------------------


class _RecordingData:
    """Stand-in for FlowDataEngine that records the ``n_records`` collect() saw."""

    def __init__(self, df: pl.DataFrame, capture: dict):
        self._df = df
        self._capture = capture

    def collect(self, n_records: int = None) -> pl.DataFrame:
        self._capture["n_records"] = n_records
        return self._df if n_records is None else self._df.head(n_records)


def test_columns_orientation_pushes_limit_into_collect():
    df = pl.DataFrame({"a": [1, 2, 3, 4, 5], "b": ["v", "w", "x", "y", "z"]})
    capture: dict = {}
    settings = SimpleNamespace(orientation="columns", max_rows=2)

    out = api_runner._serialize(_RecordingData(df, capture), settings)

    # The limit was pushed into collect(), not applied by slicing afterwards.
    assert capture["n_records"] == 2
    assert out == {"data": {"a": [1, 2], "b": ["v", "w"]}, "row_count": 2, "orientation": "columns"}


def test_columns_orientation_without_limit_collects_all():
    df = pl.DataFrame({"a": [1, 2, 3]})
    capture: dict = {}
    settings = SimpleNamespace(orientation="columns", max_rows=None)

    out = api_runner._serialize(_RecordingData(df, capture), settings)

    assert capture["n_records"] is None
    assert out == {"data": {"a": [1, 2, 3]}, "row_count": 3, "orientation": "columns"}


# ---------------------------------------------------------------------------
# #4 — per-flow_id serialization
# ---------------------------------------------------------------------------


def test_flow_run_lock_is_per_flow_id_singleton():
    a1 = api_runner._flow_run_lock(101)
    a2 = api_runner._flow_run_lock(101)
    b = api_runner._flow_run_lock(202)
    assert a1 is a2  # same flow -> same lock
    assert a1 is not b  # different flow -> independent lock


def test_flow_run_lock_serializes_same_flow_id():
    """Threads contending the same flow_id lock never overlap in the critical section."""
    in_section = 0
    max_concurrent = 0
    guard = threading.Lock()

    def worker():
        nonlocal in_section, max_concurrent
        with api_runner._flow_run_lock(987654):
            with guard:
                in_section += 1
                max_concurrent = max(max_concurrent, in_section)
            time.sleep(0.02)  # widen the window so a broken lock would overlap
            with guard:
                in_section -= 1

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert max_concurrent == 1


def test_run_flow_as_api_serializes_on_flow_id(tmp_path, monkeypatch):
    """run_flow_as_api acquires the per-flow lock keyed by the flow's own flow_id."""
    flow_path = tmp_path / "flow.yaml"
    _build_and_save_flow(flow_path, flow_id=7)

    acquired_for = []
    real = api_runner._flow_run_lock

    def spy(flow_id):
        acquired_for.append(flow_id)
        return real(flow_id)

    monkeypatch.setattr(api_runner, "_flow_run_lock", spy)

    out = api_runner.run_flow_as_api(
        str(flow_path), owner_id=1, param_specs=[ApiParamSpec(name="region")], query={"region": "EU"}
    )
    assert out["data"] == [{"region": "EU", "v": 1}]
    assert acquired_for == [7]  # serialized on the flow's saved flow_id, exactly once
