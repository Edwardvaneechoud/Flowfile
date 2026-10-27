"""The task routes core does not call with the protocol flag on answer ``Deprecation: true``; the other routes do not."""

import uuid

import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from flowfile_worker import main, models, routes, status_dict, status_dict_lock
from tests.conftest import INTERNAL_AUTH_HEADERS

pytestmark = pytest.mark.worker

client = TestClient(main.app, headers=INTERNAL_AUTH_HEADERS)

DEPRECATED = {
    ("POST", "/store_sample/"),
    ("POST", "/store_database_read_result"),
    ("POST", "/store_kafka_read_result"),
    ("POST", "/store_google_analytics_read_result"),
    ("POST", "/store_rest_api_read_result"),
    ("GET", "/kafka_offsets/{task_id}"),
    ("POST", "/create_table/{file_type}"),
    ("POST", "/train_ml_model"),
    ("POST", "/apply_ml_model"),
    ("POST", "/add_fuzzy_join"),
    ("POST", "/write_results/"),
    ("POST", "/store_database_write_result/"),
    ("POST", "/write_data_to_cloud/"),
    ("GET", "/memory_usage/{task_id}"),
    ("GET", "/ids"),
}


def test_exactly_the_replaced_task_routes_are_marked_deprecated():
    marked = {
        (method, route.path)
        for route in routes.router.routes
        if isinstance(route, APIRoute)
        and any(dependency.dependency is routes.deprecated_route for dependency in route.dependencies)
        for method in route.methods
    }

    assert marked == DEPRECATED


@pytest.fixture
def finished_task(tmp_path):
    """A completed task in ``status_dict`` that wrote no Kafka offsets, removed when the test ends."""
    task_id = f"t-deprecated-{uuid.uuid4().hex}"
    with status_dict_lock:
        status_dict[task_id] = models.Status(
            background_task_id=task_id, status="Completed", file_ref=str(tmp_path / "result.arrow")
        )
    yield task_id
    with status_dict_lock:
        status_dict.pop(task_id, None)


def test_a_marked_route_answers_with_the_deprecation_header(finished_task):
    response = client.get(f"/kafka_offsets/{finished_task}")

    assert response.status_code == 200
    assert response.headers["Deprecation"] == "true"


@pytest.mark.parametrize("path", ["/capabilities", "/pool"])
def test_an_unmarked_route_answers_without_it(path):
    response = client.get(path)

    assert response.status_code == 200
    assert "Deprecation" not in response.headers
