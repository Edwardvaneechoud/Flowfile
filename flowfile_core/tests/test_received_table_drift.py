"""Drift guard for the two hand-duplicated ReceivedTable models.

``flowfile_core.schemas.input_schema.ReceivedTable`` and
``flowfile_worker.create.models.ReceivedTable`` are separate classes describing the same wire
payload, and neither forbids extra fields — so a field added on one side is silently dropped by
the other and a directory-mode read quietly degrades into a single-file read on the worker.
"""

import json

import pytest

from flowfile_core.schemas import input_schema

# Importing flowfile_worker sets the multiprocessing start method to "spawn" process-wide.
# That is inert here: flowfile_core uses no multiprocessing.
from flowfile_worker.create import models as worker_models

CORE_QUALIFIER = "flowfile_core.schemas.input_schema."
WORKER_QUALIFIER = "flowfile_worker.create.models."

# table_settings is a package-local discriminated union of package-local classes; comparing it
# would only compare module paths.
SKIPPED_FIELDS = {"table_settings"}


def _annotations(model, qualifier: str) -> dict[str, str]:
    return {
        name: str(field.annotation).replace(qualifier, "")
        for name, field in model.model_fields.items()
        if name not in SKIPPED_FIELDS
    }


def test_field_names_match():
    """A field on one side only is invisible to the other: pydantic ignores unknown keys."""
    assert set(input_schema.ReceivedTable.model_fields) == set(worker_models.ReceivedTable.model_fields)


def test_field_annotations_match():
    """Same names are not enough — the types have to agree or the payload changes meaning."""
    assert _annotations(input_schema.ReceivedTable, CORE_QUALIFIER) == _annotations(
        worker_models.ReceivedTable, WORKER_QUALIFIER
    )


@pytest.mark.parametrize("field_name", ["scan_mode", "include_file_paths"])
def test_directory_fields_exist_on_both_models(field_name):
    """The two fields this feature adds must cross the core -> worker boundary intact."""
    core_field = input_schema.ReceivedTable.model_fields[field_name]
    worker_field = worker_models.ReceivedTable.model_fields[field_name]

    assert str(core_field.annotation).replace(CORE_QUALIFIER, "") == str(worker_field.annotation).replace(
        WORKER_QUALIFIER, ""
    )
    assert core_field.default == worker_field.default


def test_directory_settings_survive_the_wire():
    """A core-built directory read must arrive at the worker as a directory read over the same
    pattern — the path crosses as one glob string, never as an expanded list."""
    core_table = input_schema.ReceivedTable(
        name="folder",
        path="/data/**/*.csv",
        file_type="csv",
        scan_mode="directory",
        include_file_paths="src",
    )

    worker_table = worker_models.ReceivedTable.model_validate_json(core_table.model_dump_json())

    assert worker_table.scan_mode == "directory"
    assert worker_table.include_file_paths == "src"
    assert worker_table.path == "/data/**/*.csv"
    assert isinstance(worker_table.abs_file_path, str)
    assert worker_table.abs_file_path == core_table.abs_file_path


def test_blank_include_file_paths_normalizes_to_none_on_both_models():
    """An empty UI box arrives as whitespace; both sides must read that as 'no extra column'."""
    core_table = input_schema.ReceivedTable(
        name="folder", path="/data", file_type="csv", scan_mode="directory", include_file_paths="  "
    )
    assert core_table.include_file_paths is None

    # Splice the blank value back into the payload: the core validator already normalised it away,
    # and the worker's validator only runs when the key is present.
    payload = json.loads(core_table.model_dump_json())
    payload["include_file_paths"] = "  "
    worker_table = worker_models.ReceivedTable.model_validate_json(json.dumps(payload))

    assert worker_table.include_file_paths is None


def test_worker_set_absolute_filepath_skips_name_append_in_directory_mode():
    """Mirror of the core pin: in directory mode ``name`` is a node label, so appending it would
    turn a directory into a file path that does not exist."""
    # Unlike the core model, the worker model has no before-validator filling table_settings in,
    # so it is passed explicitly as None for its own field validator to default by file_type.
    directory_table = worker_models.ReceivedTable(
        name="sales.csv", path="/data/*.csv", file_type="csv", scan_mode="directory", table_settings=None
    )
    assert not directory_table.abs_file_path.endswith("sales.csv")
    assert directory_table.abs_file_path.endswith("*.csv")

    single_file_table = worker_models.ReceivedTable(
        name="sales.csv", path="/data", file_type="csv", table_settings=None
    )
    assert single_file_table.abs_file_path.endswith("sales.csv")
