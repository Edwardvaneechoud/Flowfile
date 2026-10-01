"""The worker's failure descriptions must lead with the exception class name.

Core never sees a worker child's exception object — only the string the child wrote
into the shared error Array — so the class name has to travel inside that string.
``flowfile_core/flowfile/flow_node/models.py::recover_error_class`` splits the
description on its first colon and keeps the head when it is a plain identifier;
``_recovered_class`` below is a copy of that rung, so these tests fail if the worker's
prefix format ever drifts away from what core parses.
"""

from multiprocessing import Queue
from pathlib import Path

import polars as pl
import pytest

from flowfile_worker import mp_context
from flowfile_worker.funcs import (
    add_catalog_key_column_task,
    calculate_number_of_records,
    generic_task,
    store,
)
from flowfile_worker.task_errors import (
    WorkerEnvironmentError,
    describe_exception,
    record_task_failure,
    record_task_failure_text,
    split_failure,
)

pytestmark = [pytest.mark.worker, pytest.mark.timeout(120)]


def _recovered_class(description: str | None) -> str | None:
    """Copy of core's description rung in ``recover_error_class`` (models.py)."""
    head, separator, _ = (description or "").partition(":")
    return head if separator and head.isidentifier() else None


def _shared():
    return mp_context.Value("i", 0), mp_context.Array("c", 1024), Queue(maxsize=1)


def _text(error_message) -> str:
    return error_message.value.decode(errors="replace").rstrip("\x00")


def _write(path: Path, content: bytes) -> Path:
    path.write_bytes(content)
    return path


class TestDescribeException:
    def test_a_polars_error_keeps_its_class(self):
        try:
            pl.LazyFrame({"a": [1]}).select(pl.col("missing")).collect()
        except Exception as e:
            description = describe_exception(e)

        assert description.startswith("ColumnNotFoundError: ")
        assert 'unable to find column "missing"' in description
        assert _recovered_class(description) == "ColumnNotFoundError"

    def test_a_custom_exception_keeps_its_class(self):
        class NodeConfigurationError(Exception):
            pass

        description = describe_exception(NodeConfigurationError("pick a column first"))

        assert description == "NodeConfigurationError: pick a column first"
        assert _recovered_class(description) == "NodeConfigurationError"

    def test_the_class_name_is_never_a_module_path(self):
        # A dotted head fails str.isidentifier, so core would recover nothing from it.
        description = describe_exception(pl.exceptions.ComputeError("boom"))

        assert description == "ComputeError: boom"
        assert "polars." not in description

    def test_a_message_that_already_names_its_class_is_not_prefixed_twice(self):
        description = describe_exception(pl.exceptions.ComputeError("ComputeError: already said so"))

        assert description == "ComputeError: already said so"
        assert _recovered_class(description) == "ComputeError"

    def test_a_message_merely_starting_with_another_class_name_is_still_prefixed(self):
        description = describe_exception(ValueError("ComputeError: came from somewhere else"))

        assert description == "ValueError: ComputeError: came from somewhere else"
        assert _recovered_class(description) == "ValueError"

    def test_an_empty_message_still_carries_a_parseable_prefix(self):
        description = describe_exception(KeyboardInterrupt())

        assert description == "KeyboardInterrupt:"
        assert _recovered_class(description) == "KeyboardInterrupt"

    def test_a_base_exception_is_described_like_any_other(self):
        class FakePanic(BaseException):
            pass

        assert describe_exception(FakePanic("the engine gave up")) == "FakePanic: the engine gave up"


class TestRecording:
    def test_the_description_is_written_and_the_task_is_marked_failed(self):
        progress, error_message, _ = _shared()

        record_task_failure(error_message, progress, ValueError("bad literal"))

        assert _text(error_message) == "ValueError: bad literal"
        assert progress.value == -1

    def test_the_description_is_truncated_to_the_buffer_limit(self):
        progress, error_message, _ = _shared()

        record_task_failure(error_message, progress, ValueError("x" * 5000), limit=256)

        assert len(_text(error_message)) == 256
        assert _recovered_class(_text(error_message)) == "ValueError"

    def test_an_environment_failure_is_marked_and_described_by_its_original(self):
        progress, error_message, _ = _shared()

        record_task_failure(error_message, progress, WorkerEnvironmentError(ModuleNotFoundError("No module named 'x'")))

        assert progress.value == -1
        assert split_failure(_text(error_message)) == ("environment", "ModuleNotFoundError: No module named 'x'")

    def test_an_ordinary_failure_is_a_task_failure(self):
        progress, error_message, _ = _shared()

        record_task_failure(error_message, progress, ValueError("bad value"))

        assert split_failure(_text(error_message)) == ("task", "ValueError: bad value")

    def test_pre_formatted_text_is_recorded_verbatim(self):
        progress, error_message, _ = _shared()

        record_task_failure_text(error_message, progress, "STALE_WRITE:3:5")

        assert _text(error_message) == "STALE_WRITE:3:5"
        assert progress.value == -1


class TestFuncsTargets:
    def test_a_polars_failure_ships_its_class(self, tmp_path):
        progress, error_message, queue = _shared()
        plan = pl.LazyFrame({"a": [1]}).select(pl.col("missing")).serialize()

        store(
            polars_serializable_object=plan,
            progress=progress,
            error_message=error_message,
            queue=queue,
            file_path=str(tmp_path / "out.arrow"),
            flowfile_flow_id=1,
            flowfile_node_id=1,
        )

        assert progress.value == -1
        assert split_failure(_text(error_message))[0] == "task", "a missing column is the plan's own mistake"
        assert _recovered_class(_text(error_message)) == "ColumnNotFoundError"

    @pytest.mark.parametrize(
        ("make_source", "error_class"),
        [
            (lambda tmp_path: tmp_path / "missing.parquet", "FileNotFoundError"),
            (lambda tmp_path: _write(tmp_path / "corrupt.parquet", b"not a parquet file"), "ComputeError"),
        ],
        ids=["missing", "corrupt"],
    )
    def test_a_bad_source_file_is_the_plans_own_failure(self, tmp_path, make_source, error_class):
        """Both fail while the schema resolves; neither is the worker's fault, so the node must fail, not degrade."""
        progress, error_message, queue = _shared()
        plan = pl.scan_parquet(make_source(tmp_path)).serialize()

        store(
            polars_serializable_object=plan,
            progress=progress,
            error_message=error_message,
            queue=queue,
            file_path=str(tmp_path / "out.arrow"),
            flowfile_flow_id=1,
            flowfile_node_id=1,
        )

        kind, description = split_failure(_text(error_message))
        assert kind == "task"
        assert _recovered_class(description) == error_class

    def test_a_plugin_library_the_worker_cannot_load_is_an_environment_failure(self, tmp_path):
        from polars_grouper import _internal, hierarchy_totals

        progress, error_message, queue = _shared()
        plan = pl.LazyFrame({"p": ["a"], "c": ["b"]}).select(hierarchy_totals("p", "c")).serialize()
        library = Path(_internal.__file__).name.encode()
        assert library in plan
        # Same length: the plan stores the library path as a length-prefixed string.
        unloadable = plan.replace(library, library.replace(b"_internal", b"_notfound"))

        store(
            polars_serializable_object=unloadable,
            progress=progress,
            error_message=error_message,
            queue=queue,
            file_path=str(tmp_path / "out.arrow"),
            flowfile_flow_id=1,
            flowfile_node_id=1,
        )

        kind, description = split_failure(_text(error_message))
        assert kind == "environment"
        assert description.startswith("ComputeError: error loading dynamic library"), description

    def test_a_corrupt_plan_ships_its_class(self):
        progress, error_message, queue = _shared()

        calculate_number_of_records(
            polars_serializable_object=b"not-a-serialized-plan",
            progress=progress,
            error_message=error_message,
            queue=queue,
            flowfile_flow_id=1,
        )

        assert progress.value == -1
        kind, description = split_failure(_text(error_message))
        assert kind == "environment", "a plan this worker can't deserialize is not the data's fault"
        assert _recovered_class(description) == "ComputeError"

    def test_a_connector_failure_ships_its_class(self, tmp_path):
        progress, error_message, queue = _shared()

        def boom():
            raise ConnectionRefusedError("could not reach the database")

        generic_task(boom, progress, error_message, queue, str(tmp_path / "missing.arrow"), 1, 1)

        assert progress.value == -1
        assert _text(error_message) == "ConnectionRefusedError: could not reach the database"
        assert _recovered_class(_text(error_message)) == "ConnectionRefusedError"

    def test_a_base_exception_is_recorded_and_still_kills_the_child(self, tmp_path):
        """A pyo3 PanicException is a BaseException: describe it, then keep unwinding."""
        progress, error_message, queue = _shared()

        class FakePanic(BaseException):
            pass

        def panic():
            raise FakePanic("the engine gave up")

        with pytest.raises(FakePanic):
            generic_task(panic, progress, error_message, queue, str(tmp_path / "missing.arrow"), 1, 1)

        assert progress.value == -1
        assert _text(error_message) == "FakePanic: the engine gave up"
        assert _recovered_class(_text(error_message)) == "FakePanic"

    def test_the_catalog_edit_sentinel_is_never_prefixed(self, tmp_path):
        """The parent route maps these by prefix, so a class name in front would break it."""
        progress, error_message, queue = _shared()

        add_catalog_key_column_task(
            table_path=str(tmp_path / "no_such_table"),
            column_name="row_id",
            expected_version=None,
            progress=progress,
            error_message=error_message,
            queue=queue,
        )

        assert progress.value == -1
        assert _text(error_message).startswith("EDIT_INVALID:")
