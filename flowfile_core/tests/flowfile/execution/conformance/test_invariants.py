"""What every compute backend guarantees, checked on each backend in ``conftest.BACKENDS``.

- Core receives at most ``n`` rows of a result: ``sample`` and the sample ``submit`` returns are the only rows,
  ``count`` answers a number and ``plan_for`` a plan.
- Only the backend that minted a result reads it: every other backend refuses its reference with ``ForeignRef``, in
  every method that takes one, and leaves the result where it is.
- A backend runs the operations its capabilities name and refuses every other kind with ``OperationUnsupported``.
- Failures are typed: a compute out of reach is ``ComputeUnavailable``, never a missing result; a released result is
  missing; a compute on other versions is refused with ``VersionMismatch`` before it runs anything.
"""

import io
import uuid
from collections.abc import Callable, Iterator

import polars as pl
import pyarrow as pa
import pytest

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

PEOPLE = pl.DataFrame({"id": list(range(10)), "name": [f"n{i}" for i in range(10)]})

STATES = pytest.mark.parametrize("must_materialise", [False, True], ids=["deferred", "materialised"])

CALLS_WITH_A_REF: dict[str, Callable[[ComputeBackend, ResultRef], object]] = {
    "plan_for": lambda backend, ref: backend.plan_for(ref),
    "sample": lambda backend, ref: backend.sample(ref, 1),
    "count": lambda backend, ref: backend.count(ref),
    "exists": lambda backend, ref: backend.exists(ref),
    "release": lambda backend, ref: backend.release(ref),
}


def hints(*, must_materialise: bool = False, preview_rows: int | None = None) -> SubmitHints:
    return SubmitHints(must_materialise=must_materialise, wide=False, fanout=1, preview_rows=preview_rows)


def submit_people(backend: ComputeBackend, submit_hints: SubmitHints) -> SubmitResult:
    plan = PEOPLE.lazy().filter(pl.col("id") >= 0)
    return backend.submit(plan, [], submit_hints, f"conformance-{uuid.uuid4().hex}", flow_id=-1, node_id=-1)


def rows(stream: bytes) -> int:
    return pa.ipc.open_stream(io.BytesIO(stream)).read_all().num_rows


@pytest.fixture
def submitted(backend: ComputeBackend) -> Iterator[Callable[..., SubmitResult]]:
    """Submits ``PEOPLE`` to ``backend`` with the given hints; every result is released when the test ends."""
    refs = []

    def submit(**hint_values) -> SubmitResult:
        result = submit_people(backend, hints(**hint_values))
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
