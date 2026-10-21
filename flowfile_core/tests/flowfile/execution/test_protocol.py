"""The compute interface's value types are frozen, its operation set is closed, and backends are structural."""

import dataclasses

import pytest

from flowfile_core.flowfile.execution.protocol import (
    ComputeBackend,
    CredentialRef,
    JobHandle,
    JobStatus,
    OperationKind,
    OpResult,
    ResultRef,
    SubmitHints,
    SubmitResult,
)

BACKEND_METHODS = (
    "capabilities",
    "submit",
    "run_op",
    "sample",
    "count",
    "exists",
    "release",
    "plan_for",
    "export",
    "import_uri",
    "status",
    "cancel",
)


def test_the_operation_set_is_exactly_the_eighteen_kinds():
    assert [kind.name for kind in OperationKind] == [
        "database_read",
        "kafka_read",
        "google_analytics_read",
        "rest_api_read",
        "database_write",
        "cloud_write",
        "file_write",
        "file_read",
        "fuzzy_join",
        "custom_node",
        "random_split",
        "ml_train",
        "ml_apply",
        "polars_code",
        "delta_write",
        "delta_merge",
        "delta_scd2",
        "viz_session",
    ]
    assert all(kind.value == kind.name for kind in OperationKind)


def test_value_types_are_frozen():
    handle = JobHandle(backend_id="local", job_id="j1")
    ref = ResultRef(backend_id="local", result_id="h1", schema=[], row_count=None, state="deferred", location=b"opaque")
    values = [
        handle,
        ref,
        SubmitHints(must_materialise=False, wide=True, fanout=2, preview_rows=100),
        SubmitResult(ref=ref, sample=None, handle=handle),
        JobStatus(state="running", progress=None, error=None),
        CredentialRef(connection_id=3, user_id=1),
        OpResult(refs={"result": ref}, metadata={"version": 1}),
    ]
    for value in values:
        field = dataclasses.fields(value)[0].name
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(value, field, None)


def _backend_class(methods: tuple[str, ...]) -> type:
    namespace = {name: (lambda self, *args: None) for name in methods}
    namespace.update(id="probe", protocol_version=1)
    return type("Backend", (), namespace)


def test_a_class_with_every_member_is_a_compute_backend():
    assert isinstance(_backend_class(BACKEND_METHODS)(), ComputeBackend)


@pytest.mark.parametrize("missing", BACKEND_METHODS)
def test_a_class_missing_a_method_is_not_a_compute_backend(missing):
    methods = tuple(name for name in BACKEND_METHODS if name != missing)

    assert not isinstance(_backend_class(methods)(), ComputeBackend)


def test_a_reference_is_a_table_unless_it_says_blob():
    table = ResultRef("local", "h1", [], None, "deferred", b"h1")
    blob = ResultRef("local", "h2", [], None, "materialised", b"h2", kind="blob")

    assert (table.kind, blob.kind) == ("table", "blob")
