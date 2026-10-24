"""What every compute backend guarantees, checked on each backend in ``conftest.BACKENDS``.

- Core receives at most ``n`` rows of a result: ``sample`` and the sample ``submit`` returns are the only rows,
  ``count`` answers a number and ``plan_for`` a plan.
- Only the backend that minted a result reads it: every other backend refuses its reference with ``ForeignRef``, in
  every method that takes one, and leaves the result where it is.
- Core learns a result's schema from its reference: every result carries the columns of its rows, core predicts the
  columns downstream from them, and a stored result whose file core cannot open is still known by its reference.
- Materialising is the backend's choice except where core marks it required: a required result is computed once and
  answers the same rows on every read, a plan that need not be materialised stays deferred, and a wide plan follows
  the backend's documented policy.
- A backend runs the operations its capabilities name and refuses every other kind with ``OperationUnsupported``.
- Failures are typed: a compute out of reach is ``ComputeUnavailable``, never a missing result; a released result is
  missing; a compute on other versions is refused with ``VersionMismatch`` before it runs anything.
"""

import io
import uuid
from collections.abc import Callable, Iterator
from datetime import date, datetime

import polars as pl
import pyarrow as pa
import pytest

from flowfile_core.configs import settings
from flowfile_core.flowfile.execution.errors import (
    ComputeUnavailable,
    ForeignRef,
    OperationUnsupported,
    ResultMissing,
    VersionMismatch,
)
from flowfile_core.flowfile.execution.protocol import (
    ComputeBackend,
    OperationKind,
    ResultRef,
    SubmitHints,
    SubmitResult,
)
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn

PEOPLE = pl.DataFrame({"id": list(range(10)), "name": [f"n{i}" for i in range(10)]})

MIXED = pl.DataFrame(
    {
        "id": [0, 1, 2],
        "name": ["a", None, "c"],
        "score": [0.5, 1.5, 2.5],
        "active": [True, False, True],
        "day": [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)],
        "seen": [datetime(2026, 1, 1, 12), datetime(2026, 1, 2, 12), datetime(2026, 1, 3, 12)],
        "tags": [[1, 2], [], [3]],
        "point": [{"x": 1, "label": "a"}, {"x": 2, "label": "b"}, {"x": 3, "label": None}],
    },
    schema_overrides={"seen": pl.Datetime("us", "UTC")},
)

DOWNSTREAM = [
    (pl.col("score") * pl.col("id")).alias("weighted"),
    pl.col("day").dt.offset_by("1mo").alias("next_month"),
    pl.col("seen").dt.offset_by("1d").alias("next_day"),
    pl.col("tags").list.first().alias("first_tag"),
    pl.col("point").struct.field("label"),
]
"""Each column's dtype follows from an input column's, so a wrong dtype in the reference shows downstream."""

STATES = pytest.mark.parametrize("must_materialise", [False, True], ids=["deferred", "materialised"])

POLICIES: dict[str, Callable[[SubmitHints], bool]] = {
    "local": lambda given: given.must_materialise and bool(settings.FLOWFILE_COMPUTE_PROTOCOL),
    "worker": lambda given: given.must_materialise or (given.wide and given.preview_rows is not None),
}
"""Whether each backend in ``conftest.BACKENDS`` stores a submit with these hints, as its class docstring says."""

SHUFFLED = pl.LazyFrame({"id": list(range(1000))}).select(pl.col("id").shuffle())
"""A plan whose rows come in another order each time it runs."""

CALLS_WITH_A_REF: dict[str, Callable[[ComputeBackend, ResultRef], object]] = {
    "plan_for": lambda backend, ref: backend.plan_for(ref),
    "sample": lambda backend, ref: backend.sample(ref, 1),
    "count": lambda backend, ref: backend.count(ref),
    "exists": lambda backend, ref: backend.exists(ref),
    "release": lambda backend, ref: backend.release(ref),
}


def hints(*, must_materialise: bool = False, wide: bool = False, preview_rows: int | None = None) -> SubmitHints:
    return SubmitHints(must_materialise=must_materialise, wide=wide, fanout=1, preview_rows=preview_rows)


def submit_people(
    backend: ComputeBackend,
    submit_hints: SubmitHints,
    frame: pl.DataFrame = PEOPLE,
    plan: pl.LazyFrame | None = None,
) -> SubmitResult:
    if plan is None:
        plan = frame.lazy().filter(pl.col("id") >= 0)
    return backend.submit(plan, [], submit_hints, f"conformance-{uuid.uuid4().hex}", flow_id=-1, node_id=-1)


def rows(stream: bytes) -> int:
    return pa.ipc.open_stream(io.BytesIO(stream)).read_all().num_rows


def ids(stream: bytes) -> list[int]:
    return pl.read_ipc_stream(stream)["id"].to_list()


def polars_columns(schema: pl.Schema) -> list[tuple[str, str]]:
    return [(name, str(dtype)) for name, dtype in schema.items()]


def flowfile_columns(schema: list[FlowfileColumn]) -> list[tuple[str, str]]:
    return [(column.column_name, column.data_type) for column in schema]


@pytest.fixture
def submitted(backend: ComputeBackend) -> Iterator[Callable[..., SubmitResult]]:
    """Submits ``PEOPLE``, ``frame`` or ``plan`` to ``backend`` with the given hints; each is released at the end."""
    refs = []

    def submit(frame: pl.DataFrame = PEOPLE, plan: pl.LazyFrame | None = None, **hint_values) -> SubmitResult:
        result = submit_people(backend, hints(**hint_values), frame, plan)
        refs.append(result.ref)
        return result

    yield submit
    for ref in refs:
        backend.release(ref)


@STATES
def test_sample_returns_at_most_n_rows(backend, submitted, must_materialise):
    ref = submitted(must_materialise=must_materialise).ref

    for n in (0, 1, 4, PEOPLE.height + 5):
        assert rows(backend.sample(ref, n)) == min(n, PEOPLE.height), f"sample(ref, {n})"


@STATES
@pytest.mark.parametrize("preview_rows", [None, 1, 4])
def test_the_inline_sample_holds_at_most_preview_rows(submitted, must_materialise, preview_rows):
    sample = submitted(must_materialise=must_materialise, preview_rows=preview_rows).sample

    if preview_rows is None:
        assert sample is None
    elif sample is not None:
        assert rows(sample) <= preview_rows


@STATES
def test_count_answers_a_number_and_plan_for_a_plan(backend, submitted, must_materialise):
    ref = submitted(must_materialise=must_materialise).ref

    assert backend.count(ref) == PEOPLE.height
    assert isinstance(backend.plan_for(ref), pl.LazyFrame)


@pytest.mark.parametrize("call", list(CALLS_WITH_A_REF))
def test_another_backend_refuses_a_result_it_did_not_mint(backend, minted_elsewhere, call):
    for minter, ref in minted_elsewhere:
        with pytest.raises(ForeignRef):
            CALLS_WITH_A_REF[call](backend, ref)
        assert minter.exists(ref), f"the {minter.id} backend lost its {ref.state} result"


@STATES
def test_every_result_carries_the_schema_of_its_rows(backend, submitted, must_materialise):
    ref = submitted(frame=MIXED, must_materialise=must_materialise).ref
    expected = polars_columns(MIXED.schema)

    assert flowfile_columns(ref.schema) == expected
    assert polars_columns(pl.read_ipc_stream(backend.sample(ref, 0)).schema) == expected


@STATES
def test_core_predicts_downstream_columns_from_the_reference(submitted, must_materialise):
    ref = submitted(frame=MIXED, must_materialise=must_materialise).ref

    predicted = FlowDataEngine.create_from_schema(ref.schema).data_frame.select(DOWNSTREAM).collect_schema()

    assert predicted == MIXED.lazy().select(DOWNSTREAM).collect_schema()


@STATES
def test_a_stored_result_is_known_by_its_reference_where_core_cannot_open_it(unreadable, submitted, must_materialise):
    ref = submitted(frame=MIXED, must_materialise=must_materialise).ref
    leaf = unreadable.plan_for(ref)
    if ref.state == "materialised":
        with pytest.raises(FileNotFoundError):
            leaf.collect_schema()

    engine = FlowDataEngine(leaf, schema=ref.schema, number_of_records=ref.row_count)

    assert flowfile_columns(engine.schema) == polars_columns(MIXED.schema)
    assert unreadable.count(ref) == MIXED.height
    assert rows(unreadable.sample(ref, 2)) == 2


def test_a_required_result_is_computed_once_and_read_the_same_twice(backend, backend_name, submitted):
    if not POLICIES[backend_name](hints(must_materialise=True)):
        pytest.skip(f"the {backend_name} backend holds every plan deferred with the protocol flag off")
    deferred = submitted(plan=SHUFFLED).ref
    assert ids(backend.sample(deferred, 1000)) != ids(backend.sample(deferred, 1000)), "the plan reads one order"

    ref = submitted(plan=SHUFFLED, must_materialise=True).ref

    assert ref.state == "materialised"
    first, second = ids(backend.sample(ref, 1000)), ids(backend.sample(ref, 1000))
    assert first == second
    assert sorted(first) == list(range(1000))
    assert backend.count(ref) == 1000


def test_a_result_that_need_not_be_materialised_stays_deferred(backend, submitted):
    ref = submitted().ref

    assert ref.state == "deferred"
    assert ref.row_count is None
    assert backend.count(ref) == PEOPLE.height


@pytest.mark.parametrize("preview_rows", [None, 4])
def test_a_wide_plan_follows_the_backend_policy(backend_name, submitted, preview_rows):
    ref = submitted(wide=True, preview_rows=preview_rows).ref

    stored = POLICIES[backend_name](hints(wide=True, preview_rows=preview_rows))
    assert ref.state == ("materialised" if stored else "deferred")


def test_capabilities_name_only_operation_kinds(backend):
    assert backend.capabilities().operations <= {kind.value for kind in OperationKind}


@pytest.mark.parametrize("kind", list(OperationKind), ids=lambda kind: kind.value)
def test_run_op_refuses_exactly_the_operations_capabilities_leave_out(backend, kind):
    if kind.value in backend.capabilities().operations:
        # a declared kind reaches its handler, which refuses the empty payload
        with pytest.raises((TypeError, ValueError)):
            backend.run_op(kind, {})
    else:
        with pytest.raises(OperationUnsupported):
            backend.run_op(kind, {})


def test_a_compute_out_of_reach_is_unavailable_never_missing(backend, unreachable, submitted):
    ref = submitted(must_materialise=True).ref

    with pytest.raises(ComputeUnavailable):
        unreachable.exists(ref)
    with pytest.raises(ComputeUnavailable):
        unreachable.capabilities()
    with pytest.raises(ComputeUnavailable):
        submit_people(unreachable, hints(must_materialise=True))


@STATES
def test_a_released_result_is_missing(backend, submitted, must_materialise):
    ref = submitted(must_materialise=must_materialise).ref

    backend.release(ref)

    assert backend.exists(ref) is False
    with pytest.raises(ResultMissing):
        backend.sample(ref, 1)


def test_a_compute_on_other_versions_runs_nothing(on_other_versions):
    with pytest.raises(VersionMismatch, match="polars_version"):
        submit_people(on_other_versions, hints(must_materialise=True))
