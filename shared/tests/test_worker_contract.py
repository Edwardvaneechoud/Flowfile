"""The worker contract: what core and a worker exchange, pinned so a change to it cannot ship without a new number.

``PROTOCOL_VERSION`` (``shared/compute_protocol.py``) names the contract, and ``worker_contract.json`` beside this
module pins it: the number and one sha256 per entry. An entry is one of:

- ``route <METHOD> <path>``: a worker route's OpenAPI operation, its parameters, request body and answers, with every
  ``$ref`` it reaches inlined, so a change to a nested model changes each route that reaches it. ``POST /v1/submit``
  adds ``SubmitMeta``, its ``meta`` form field, and ``SubmitResult``.
- ``socket <path>``: a submit socket. ``/v1/ws/submit`` carries ``SubmitMeta`` in its text frame and answers a
  ``SubmitResult``; the legacy ``/ws/submit`` is pinned by its path.
- ``operation <kind>``: a ``POST /v1/ops/<kind>`` route, and ``operation task:<type>`` the keyword parameters of the
  task function a legacy submit dispatches by its operation type, minus those the worker fills in itself, with the
  shape of each model a parameter is annotated with and of each report the task answers (``TASK_RESULTS``).
- ``catalog cloud_storage_connections``: the columns ``shared.database.cloud_connection_record`` reads from the catalog.

Descriptions, summaries, titles and operation ids are not shape and are left out. A changed or removed entry, or a new
entry that is no operation, fails until ``PROTOCOL_VERSION`` is bumped and the pin rewritten
(``python shared/tests/test_worker_contract.py``); a new operation passes without a bump, as a worker that does not
declare it is refused only that operation's nodes. Not pinned, as no model describes them: the sockets' frame envelopes
(``type``, ``progress``, the error frame), the legacy submit's ``X-*`` header names and its socket's metadata keys.

Every model a worker route's request reaches refuses an unknown field, so a field core sends that a worker does not know
fails the request instead of being dropped; ``FuzzyMapping``, a third-party dataclass inside ``FuzzyJoinInput``, is the
one exception, its version compared by the handshake instead.
"""

import hashlib
import inspect
import json
import typing
from collections.abc import Iterable, Sequence
from pathlib import Path

import pytest
from pydantic import ValidationError

PIN = Path(__file__).with_name("worker_contract.json")

INJECTED = frozenset(
    {
        "polars_serializable_object",
        "progress",
        "error_message",
        "queue",
        "file_path",
        "flowfile_flow_id",
        "flowfile_node_id",
    }
)
"""The keyword arguments the worker passes a task function itself, never read from core's request."""

TASK_RESULTS = {
    "write_delta": ("DeltaWriteReport", "DeltaSkipReport"),
    "merge_delta": ("DeltaWriteReport", "DeltaSkipReport"),
    "scd2_delta": ("Scd2WriteReport", "DeltaSkipReport"),
}
"""The ``flowfile_worker.models`` reports a task operation puts on its queue, which core reads from ``/status``."""

NOT_SHAPE = frozenset({"summary", "description", "operationId", "title", "tags"})
UNCHECKED_MODELS = frozenset({"FuzzyMapping"})
BUMP = (
    "the worker contract changed: bump PROTOCOL_VERSION in shared/compute_protocol.py and rewrite the pin with "
    "python shared/tests/test_worker_contract.py"
)


def worker_routes() -> list[Sequence]:
    """The worker's routes, one sequence per router, and the app's own ``/shutdown``."""
    from fastapi.routing import APIRoute

    from flowfile_worker import main, protocol_routes, routes, streaming

    return [
        routes.router.routes,
        protocol_routes.router.routes,
        streaming.streaming_router.routes,
        streaming.v1_streaming_router.routes,
        [route for route in main.app.routes if isinstance(route, APIRoute)],
    ]


def _shape(node, schemas: dict, seen: tuple[str, ...] = ()):
    """``node`` with each ``$ref`` inlined from ``schemas`` and the keys that are not shape left out."""
    if isinstance(node, list):
        return [_shape(item, schemas, seen) for item in node]
    if not isinstance(node, dict):
        return node
    if "$ref" in node:
        name = node["$ref"].rsplit("/", 1)[1]
        return {"$ref": name} if name in seen else _shape(schemas[name], schemas, (*seen, name))
    shaped = {}
    for key, value in node.items():
        if key == "properties":
            shaped[key] = {field: _shape(schema, schemas, seen) for field, schema in value.items()}
        elif key not in NOT_SHAPE:
            shaped[key] = _shape(value, schemas, seen)
    return shaped


def _model_shape(model) -> dict:
    schema = model.model_json_schema()
    return _shape(schema, schema.get("$defs", {}))


def _models_in(annotation) -> list[type]:
    """The pydantic models ``annotation`` names, itself or inside a union or a container."""
    from pydantic import BaseModel

    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return [annotation]
    return [model for argument in typing.get_args(annotation) for model in _models_in(argument)]


def _task_parameters(function) -> dict[str, list[type]]:
    """The models each of ``function``'s parameters the worker does not fill in is annotated with, by name."""
    from deltalake import DeltaTable
    from pl_fuzzy_frame_match import FuzzyMapping

    from flowfile_worker import models
    from flowfile_worker.external_sources.s3_source.models import CloudStorageWriteSettings
    from flowfile_worker.external_sources.sql_source.models import DatabaseWriteSettings

    type_checking = {
        "DeltaTable": DeltaTable,
        "FuzzyMapping": FuzzyMapping,
        "models": models,
        "CloudStorageWriteSettings": CloudStorageWriteSettings,
        "DatabaseWriteSettings": DatabaseWriteSettings,
    }
    hints = typing.get_type_hints(function, localns=type_checking)
    parameters = inspect.signature(function).parameters
    return {name: _models_in(hints.get(name)) for name in parameters if name not in INJECTED}


def _task_operations() -> dict[str, dict]:
    from flowfile_worker import funcs, models

    operations = {}
    for operation_type in typing.get_args(models.OperationType):
        function = getattr(funcs, operation_type, None)
        if function is not None:
            parameters = inspect.signature(function).parameters.values()
            operations[f"operation task:{operation_type}"] = {
                "parameters": [str(p) for p in parameters if p.name not in INJECTED],
                "models": {
                    name: [_model_shape(model) for model in found]
                    for name, found in _task_parameters(function).items()
                    if found
                },
                "results": [_model_shape(getattr(models, name)) for name in TASK_RESULTS.get(operation_type, ())],
            }
    return operations


def _catalog() -> dict[str, dict]:
    from shared import database

    return {
        "catalog cloud_storage_connections": {
            "cloud_storage_connections": [*database._CLOUD_CONNECTION_FIELDS]
            + [f"{name}_id" for name in database._CLOUD_CONNECTION_SECRETS],
            "secrets": ["id", "encrypted_value"],
        }
    }


def contract(route_groups: Iterable[Sequence]) -> dict[str, object]:
    """Each entry of the contract over ``route_groups``, by key; a later group's route replaces an earlier one's."""
    from fastapi.openapi.utils import get_openapi
    from fastapi.routing import APIRoute, APIWebSocketRoute

    from flowfile_worker.protocol_routes import SubmitMeta
    from shared.compute_protocol import SubmitResult

    submit = {"meta": _model_shape(SubmitMeta), "answer": _model_shape(SubmitResult)}
    entries: dict[str, object] = {}
    for group in route_groups:
        spec = get_openapi(title="worker", version="contract", routes=[r for r in group if isinstance(r, APIRoute)])
        schemas = spec.get("components", {}).get("schemas", {})
        for path, item in spec.get("paths", {}).items():
            for method, operation in item.items():
                if method == "post" and path.startswith("/v1/ops/"):
                    key = f"operation {path.removeprefix('/v1/ops/')}"
                else:
                    key = f"route {method.upper()} {path}"
                shape = _shape(operation, schemas)
                entries[key] = {"route": shape, **submit} if key == "route POST /v1/submit" else shape
        for route in group:
            if isinstance(route, APIWebSocketRoute):
                entries[f"socket {route.path}"] = submit if route.path == "/v1/ws/submit" else {"path": route.path}
    return {**entries, **_task_operations(), **_catalog()}


def fingerprint(route_groups: Iterable[Sequence]) -> dict[str, str]:
    """The sha256 of each entry's canonical JSON, by key."""
    return {
        key: hashlib.sha256(json.dumps(shape, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        for key, shape in contract(route_groups).items()
    }


def contract_changes(entries: dict[str, str], pin: dict, protocol_version: int) -> list[str]:
    """What keeps ``entries`` from matching ``pin`` at ``protocol_version``; empty when they match."""
    if pin["protocol_version"] != protocol_version:
        return [f"PROTOCOL_VERSION is {protocol_version} but the pin is of {pin['protocol_version']}"]
    pinned = pin["entries"]
    return (
        [f"changed: {key}" for key in pinned if key in entries and entries[key] != pinned[key]]
        + [f"removed: {key}" for key in pinned if key not in entries]
        + [f"added: {key}" for key in entries if key not in pinned and not key.startswith("operation ")]
    )


def pin_of(entries: dict[str, str], protocol_version: int) -> dict:
    return {"protocol_version": protocol_version, "entries": dict(sorted(entries.items()))}


def request_models(route_groups: Iterable[Sequence]) -> dict[str, type]:
    """Every pydantic model a worker route's request body, the submit's ``meta`` or a task operation's keyword
    arguments reach, by qualified name."""
    from fastapi.routing import APIRoute
    from pydantic import BaseModel

    from flowfile_worker import funcs, models
    from flowfile_worker.protocol_routes import SubmitMeta

    found: dict[str, type] = {}

    def visit(annotation) -> None:
        for argument in typing.get_args(annotation):
            visit(argument)
        if not (isinstance(annotation, type) and issubclass(annotation, BaseModel)):
            return
        name = f"{annotation.__module__}.{annotation.__qualname__}"
        if annotation.__name__ in UNCHECKED_MODELS or name in found:
            return
        found[name] = annotation
        for field in annotation.model_fields.values():
            visit(field.annotation)

    def visit_dependant(dependant) -> None:
        for parameter in dependant.body_params:
            visit(parameter.field_info.annotation)
        for dependency in dependant.dependencies:
            visit_dependant(dependency)

    for group in route_groups:
        for route in group:
            if isinstance(route, APIRoute):
                visit_dependant(route.dependant)
    visit(SubmitMeta)
    for operation_type in typing.get_args(models.OperationType):
        function = getattr(funcs, operation_type, None)
        if function is not None:
            for found_models in _task_parameters(function).values():
                for model in found_models:
                    visit(model)
    return dict(sorted(found.items()))


def test_the_worker_speaks_the_pinned_contract():
    from shared.compute_protocol import PROTOCOL_VERSION

    changes = contract_changes(fingerprint(worker_routes()), json.loads(PIN.read_text()), PROTOCOL_VERSION)

    assert changes == [], f"{BUMP}: {changes}"


def _scratch_router():
    from fastapi import APIRouter

    from flowfile_worker import protocol_routes

    return APIRouter(prefix="/v1", dependencies=protocol_routes.router.dependencies)


def _pinned_now() -> dict:
    """The worker's fingerprint pinned at the current number, whatever the committed pin says."""
    from shared.compute_protocol import PROTOCOL_VERSION

    return pin_of(fingerprint(worker_routes()), PROTOCOL_VERSION)


def test_a_route_replaced_by_the_same_shape_changes_nothing():
    from flowfile_worker.protocol_routes import ExportBody
    from shared.compute_protocol import PROTOCOL_VERSION

    scratch = _scratch_router()

    @scratch.post("/results/{result_id}/export")
    def export(result_id: str, body: ExportBody) -> dict[str, str]:
        raise NotImplementedError

    assert contract_changes(fingerprint([*worker_routes(), scratch.routes]), _pinned_now(), PROTOCOL_VERSION) == []


def test_a_changed_request_field_without_a_bump_fails_naming_its_route():
    from flowfile_worker.protocol_routes import ExportBody
    from shared.compute_protocol import PROTOCOL_VERSION

    class WiderExportBody(ExportBody):
        overwrite: bool = False

    scratch = _scratch_router()

    @scratch.post("/results/{result_id}/export")
    def export(result_id: str, body: WiderExportBody) -> dict[str, str]:
        raise NotImplementedError

    changes = contract_changes(fingerprint([*worker_routes(), scratch.routes]), _pinned_now(), PROTOCOL_VERSION)

    assert changes == ["changed: route POST /v1/results/{result_id}/export"]


def test_a_changed_nested_model_fails_naming_every_route_that_reaches_it():
    from flowfile_worker.protocol_routes import CredentialRefBody, ExportBody, ImportBody
    from shared.compute_protocol import PROTOCOL_VERSION, ResultRef

    class WiderCredential(CredentialRefBody):
        region: str | None = None

    class ExportWithWiderCredential(ExportBody):
        credential: WiderCredential | None = None

    class ImportWithWiderCredential(ImportBody):
        credential: WiderCredential | None = None

    scratch = _scratch_router()

    @scratch.post("/results/{result_id}/export")
    def export(result_id: str, body: ExportWithWiderCredential) -> dict[str, str]:
        raise NotImplementedError

    @scratch.post("/results/{result_id}/import", response_model=ResultRef)
    def import_uri(result_id: str, body: ImportWithWiderCredential) -> ResultRef:
        raise NotImplementedError

    changes = contract_changes(fingerprint([*worker_routes(), scratch.routes]), _pinned_now(), PROTOCOL_VERSION)

    assert changes == [
        "changed: route POST /v1/results/{result_id}/export",
        "changed: route POST /v1/results/{result_id}/import",
    ]


def test_a_new_operation_kind_needs_no_bump_and_a_new_route_does():
    from flowfile_worker.protocol_routes import OpOutputs, PolarsCodeInput
    from shared.compute_protocol import PROTOCOL_VERSION

    scratch = _scratch_router()

    @scratch.post("/ops/brand_new_kind", response_model=OpOutputs)
    def brand_new_kind(body: PolarsCodeInput) -> OpOutputs:
        raise NotImplementedError

    assert contract_changes(fingerprint([*worker_routes(), scratch.routes]), _pinned_now(), PROTOCOL_VERSION) == []

    @scratch.get("/brand_new_route")
    def brand_new_route() -> dict[str, str]:
        raise NotImplementedError

    changes = contract_changes(fingerprint([*worker_routes(), scratch.routes]), _pinned_now(), PROTOCOL_VERSION)

    assert changes == ["added: route GET /v1/brand_new_route"]


def test_a_changed_delta_storage_payload_without_a_bump_fails_naming_each_delta_operation(monkeypatch):
    from flowfile_worker import models
    from shared.compute_protocol import PROTOCOL_VERSION

    pinned = _pinned_now()

    class WiderStoragePayload(models.DeltaStoragePayload):
        region: str | None = None

    monkeypatch.setattr(models, "DeltaStoragePayload", WiderStoragePayload)

    assert contract_changes(fingerprint(worker_routes()), pinned, PROTOCOL_VERSION) == [
        "changed: operation task:merge_delta",
        "changed: operation task:scd2_delta",
        "changed: operation task:write_delta",
    ]


def test_a_changed_delta_report_without_a_bump_fails_naming_each_operation_that_answers_it(monkeypatch):
    from flowfile_worker import models
    from shared.compute_protocol import PROTOCOL_VERSION

    pinned = _pinned_now()

    class WiderWriteReport(models.DeltaWriteReport):
        files_added: int = 0

    monkeypatch.setattr(models, "DeltaWriteReport", WiderWriteReport)

    assert contract_changes(fingerprint(worker_routes()), pinned, PROTOCOL_VERSION) == [
        "changed: operation task:merge_delta",
        "changed: operation task:write_delta",
    ]


def test_a_delta_operation_with_an_unknown_storage_payload_key_fails_and_writes_nothing(tmp_path):
    from multiprocessing import Array, Queue, Value

    import polars as pl

    from flowfile_worker import funcs

    progress, error_message, queue = Value("i", 0), Array("c", 1024), Queue(maxsize=1)
    target = tmp_path / "table"

    funcs.write_delta(
        pl.LazyFrame({"a": [1]}).serialize(),
        progress,
        error_message,
        queue,
        file_path=str(tmp_path / "unused.arrow"),
        output_path=str(target),
        storage_payload={"base_uri": "s3://bucket/catalog", "an_unknown_field": 1},
    )

    assert progress.value == -1
    assert b"an_unknown_field" in error_message.value
    assert not target.exists()


def test_a_pin_of_another_number_fails_until_it_is_rewritten():
    from shared.compute_protocol import PROTOCOL_VERSION

    entries = fingerprint(worker_routes())
    older = pin_of(entries, PROTOCOL_VERSION - 1)

    assert contract_changes(entries, older, PROTOCOL_VERSION) == [
        f"PROTOCOL_VERSION is {PROTOCOL_VERSION} but the pin is of {PROTOCOL_VERSION - 1}"
    ]
    assert contract_changes(entries, pin_of(entries, PROTOCOL_VERSION), PROTOCOL_VERSION) == []


def test_every_task_operation_and_the_catalog_columns_have_an_entry():
    entries = fingerprint(worker_routes())

    assert {key for key in entries if key.startswith("operation task:")} == {
        f"operation task:{name}"
        for name in (
            "store",
            "calculate_schema",
            "calculate_number_of_records",
            "write_output",
            "store_sample",
            "write_to_database",
            "write_to_cloud_storage",
            "write_parquet",
            "write_delta",
            "merge_delta",
            "scd2_delta",
            "polars_code",
        )
    }
    assert "catalog cloud_storage_connections" in entries
    assert {"socket /ws/submit", "socket /v1/ws/submit", "route POST /shutdown"} <= set(entries)


REQUEST_MODELS = request_models(worker_routes())


@pytest.mark.parametrize("model", REQUEST_MODELS.values(), ids=REQUEST_MODELS.keys())
def test_every_worker_request_model_refuses_an_unknown_field(model):
    with pytest.raises(ValidationError) as refused:
        model.model_validate({"an_unknown_field": 1})

    assert {"type": "extra_forbidden", "loc": ("an_unknown_field",)} in [
        {"type": error["type"], "loc": error["loc"]} for error in refused.value.errors()
    ]


@pytest.fixture
def worker_client(monkeypatch):
    from fastapi.testclient import TestClient

    from flowfile_worker import internal_token, main

    monkeypatch.setattr(internal_token, "_token", "worker-contract-token")
    return TestClient(main.app)


@pytest.mark.parametrize(
    ("path", "body"),
    [
        ("/catalog/table_metadata", {"table_path": "orders"}),
        ("/v1/results/x/export", {"uri": "file:///nowhere/x.parquet"}),
    ],
    ids=["task-route", "v1-route"],
)
def test_the_worker_answers_a_body_with_an_unknown_field_422(worker_client, path, body):
    from shared.compute_protocol import PROTOCOL_VERSION, PROTOCOL_VERSION_HEADER

    headers = {"X-Flowfile-Internal": "worker-contract-token", PROTOCOL_VERSION_HEADER: str(PROTOCOL_VERSION)}

    answer = worker_client.post(path, json={**body, "an_unknown_field": 1}, headers=headers)

    assert answer.status_code == 422
    assert [error["type"] for error in answer.json()["detail"]] == ["extra_forbidden"]


if __name__ == "__main__":
    from shared.compute_protocol import PROTOCOL_VERSION

    PIN.write_text(json.dumps(pin_of(fingerprint(worker_routes()), PROTOCOL_VERSION), indent=2) + "\n")
