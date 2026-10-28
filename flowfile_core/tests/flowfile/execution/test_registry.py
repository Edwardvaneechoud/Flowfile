"""Compute backends are known by id: registered in process or installed as entry points, made once per process."""

import importlib.metadata
import ssl
import threading
from collections.abc import Callable

import pytest

from flowfile_core.auth.jwt import get_internal_token
from flowfile_core.configs import settings
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import subprocess_operations as subops
from flowfile_core.secret_manager.secret_manager import encrypt_secret
from test_utils.tls import self_signed_certificate

TEAM_URL = "http://team-worker.test:63579"
TLS_TEAM_URL = "https://tls-team-worker.test:63579"


class FakeEntryPoint:
    """Stands in for an installed ``flowfile.compute_backends`` entry point; counts its loads."""

    def __init__(self, name: str, factory):
        self.name = name
        self._factory = factory
        self.loads = 0

    def load(self):
        self.loads += 1
        return self._factory


class Made:
    """What a test factory makes: a marker carrying the id it was made for."""

    def __init__(self, backend_id: str):
        self.id = backend_id


class BuiltIn:
    """The factory a test's built-in table holds; the registry calls it without arguments."""


def counting_factory(backend_id: str) -> tuple[list[Made], Callable[[], Made]]:
    made: list[Made] = []

    def factory() -> Made:
        instance = Made(backend_id)
        made.append(instance)
        return instance

    return made, factory


class Installed:
    """The entry points the registry discovers (none unless a test adds one) and the group of each scan."""

    def __init__(self):
        self.entry_points: list[FakeEntryPoint] = []
        self.scans: list[str] = []

    def __call__(self, *, group: str) -> list[FakeEntryPoint]:
        self.scans.append(group)
        return list(self.entry_points) if group == registry.ENTRY_POINT_GROUP else []


@pytest.fixture
def installed(monkeypatch) -> Installed:
    fake = Installed()
    monkeypatch.setattr(importlib.metadata, "entry_points", fake)
    return fake


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch, installed):
    monkeypatch.delenv(registry.BACKEND_ENV_VAR, raising=False)
    registry.reset()
    yield
    registry.reset()


@pytest.fixture
def configured_backend():
    """Add a ``compute_backends`` row with a token of its own by id, URL and other columns; every row goes after."""
    added: list[str] = []

    def add(backend_id: str = "worker:team", url: str = TEAM_URL, **columns) -> str:
        with get_db_context() as db:
            db.add(ConfiguredBackend(id=backend_id, url=url, token=encrypt_secret("row-token", 1), **columns))
            db.commit()
        added.append(backend_id)
        return backend_id

    yield add
    with get_db_context() as db:
        for backend_id in added:
            row = db.get(ConfiguredBackend, backend_id)
            if row is not None:
                db.delete(row)
        db.commit()


@pytest.fixture
def configured_row(configured_backend) -> str:
    """A ``compute_backends`` row configuring ``worker:team`` with a token of its own."""
    return configured_backend()


@pytest.fixture
def no_override():
    """The test starts with no default override and whatever it sets is undone after it."""
    previous = registry.set_default_override(None)
    yield
    registry.set_default_override(previous)


@pytest.fixture
def serving(monkeypatch):
    """The test runs in a process its entry point marked as one that serves; the mark goes after it."""
    monkeypatch.setattr(registry, "_serving", False)
    registry.mark_serving()


@pytest.fixture
def single_file(monkeypatch, serving):
    """The test runs in the pip server's process: serving, in single-file mode, the worker's routes at its mount."""
    monkeypatch.setattr(settings.SINGLE_FILE_MODE, "value", True)
    monkeypatch.setattr(settings, "WORKER_URL", "http://127.0.0.1:63578/worker")


def test_a_registered_backend_is_made_once_and_shared():
    made, factory = counting_factory("probe")
    registry.register_backend("probe", factory)

    assert made == []
    first = registry.backend("probe")
    second = registry.backend("probe")

    assert first is second
    assert first.id == "probe"
    assert made == [first]


def test_threads_asking_at_once_share_one_instance():
    made, factory = counting_factory("probe")
    registry.register_backend("probe", factory)
    start = threading.Barrier(8, timeout=10)
    seen = []
    seen_lock = threading.Lock()

    def ask():
        start.wait()
        instance = registry.backend("probe")
        with seen_lock:
            seen.append(instance)

    threads = [threading.Thread(target=ask) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert len(seen) == 8
    assert len(made) == 1
    assert all(instance is made[0] for instance in seen)


def test_registering_an_id_again_replaces_its_factory_and_instance():
    old_made, old_factory = counting_factory("probe")
    new_made, new_factory = counting_factory("probe")
    registry.register_backend("probe", old_factory)
    old = registry.backend("probe")

    registry.register_backend("probe", new_factory)
    new = registry.backend("probe")

    assert new is not old
    assert old_made == [old]
    assert new_made == [new]


def test_an_entry_point_backend_is_scanned_once_and_loaded_on_first_use(installed):
    made, factory = counting_factory("plugin")
    entry_point = FakeEntryPoint("plugin", factory)
    installed.entry_points.append(entry_point)
    registry.register_backend("probe", counting_factory("probe")[1])

    registry.backend("probe")
    assert installed.scans == [registry.ENTRY_POINT_GROUP]
    assert entry_point.loads == 0

    first = registry.backend("plugin")
    second = registry.backend("plugin")

    assert first is second
    assert made == [first]
    assert entry_point.loads == 1
    assert installed.scans == ["flowfile.compute_backends"]


def test_an_in_process_registration_wins_over_an_entry_point_of_the_same_name(installed):
    plugin_made, plugin_factory = counting_factory("probe")
    entry_point = FakeEntryPoint("probe", plugin_factory)
    installed.entry_points.append(entry_point)
    made, factory = counting_factory("probe")
    registry.register_backend("probe", factory)

    instance = registry.backend("probe")

    assert made == [instance]
    assert plugin_made == []
    assert entry_point.loads == 0


def test_an_unknown_id_names_every_known_id(installed):
    installed.entry_points.append(FakeEntryPoint("plugin", counting_factory("plugin")[1]))
    registry.register_backend("worker", counting_factory("worker")[1])
    registry.register_backend("local", counting_factory("local")[1])

    with pytest.raises(LookupError) as raised:
        registry.backend("cluster")

    assert str(raised.value) == "Unknown compute backend 'cluster'; known backends: 'local', 'plugin', 'worker'"


def test_an_unknown_id_with_no_backends_says_so(monkeypatch):
    monkeypatch.setattr(registry, "_BUILTIN", {})

    with pytest.raises(LookupError) as raised:
        registry.backend("cluster")

    assert str(raised.value) == "Unknown compute backend 'cluster'; known backends: none"


def test_an_unknown_id_names_the_built_in_ids_too(monkeypatch):
    monkeypatch.setattr(registry, "_BUILTIN", {"builtin": BuiltIn})
    registry.register_backend("probe", counting_factory("probe")[1])

    with pytest.raises(LookupError) as raised:
        registry.backend("cluster")

    assert str(raised.value) == "Unknown compute backend 'cluster'; known backends: 'builtin', 'probe'"


def test_an_entry_point_never_shadows_a_built_in_id(monkeypatch, installed):
    monkeypatch.setattr(registry, "_BUILTIN", {"builtin": BuiltIn})
    plugin_made, plugin_factory = counting_factory("builtin")
    entry_point = FakeEntryPoint("builtin", plugin_factory)
    installed.entry_points.append(entry_point)
    registry.register_backend("probe", counting_factory("probe")[1])
    registry.backend("probe")

    instance = registry.backend("builtin")

    assert isinstance(instance, BuiltIn)
    assert plugin_made == []
    assert entry_point.loads == 0


def test_reset_forgets_registrations_instances_and_the_scan(installed):
    made, factory = counting_factory("probe")
    registry.register_backend("probe", factory)
    registry.backend("probe")

    registry.reset()

    with pytest.raises(LookupError):
        registry.backend("probe")
    assert installed.scans == [registry.ENTRY_POINT_GROUP, registry.ENTRY_POINT_GROUP]
    registry.register_backend("probe", factory)
    assert registry.backend("probe") is made[1]
    assert len(made) == 2


@pytest.mark.parametrize("offload", [True, False])
def test_the_backend_variable_names_the_default_whatever_the_offload_setting(monkeypatch, offload):
    monkeypatch.setattr(settings.OFFLOAD_TO_WORKER, "value", offload)
    monkeypatch.setenv("FLOWFILE_COMPUTE_BACKEND", "cluster")

    assert registry.default_backend_id() == "cluster"


def test_with_no_backend_variable_offload_on_defaults_to_the_worker(monkeypatch):
    monkeypatch.setattr(settings.OFFLOAD_TO_WORKER, "value", True)

    assert registry.default_backend_id() == "worker"


def test_with_no_backend_variable_offload_off_defaults_to_local(monkeypatch):
    monkeypatch.setattr(settings.OFFLOAD_TO_WORKER, "value", False)

    assert registry.default_backend_id() == "local"


@pytest.mark.parametrize("blank", ["", "   "])
def test_a_blank_backend_variable_counts_as_unset(monkeypatch, blank):
    monkeypatch.setattr(settings.OFFLOAD_TO_WORKER, "value", False)
    monkeypatch.setenv("FLOWFILE_COMPUTE_BACKEND", blank)

    assert registry.default_backend_id() == "local"


@pytest.mark.parametrize("offload", [True, False])
def test_a_process_that_serves_defaults_to_the_worker_whatever_the_offload_setting(monkeypatch, serving, offload):
    monkeypatch.setattr(settings.OFFLOAD_TO_WORKER, "value", offload)

    assert registry.serving() is True
    assert registry.default_backend_id() == "worker"


def test_a_process_that_serves_never_defaults_to_local_by_the_variable_or_the_override(
    monkeypatch, serving, no_override
):
    monkeypatch.setenv("FLOWFILE_COMPUTE_BACKEND", "local")
    assert registry.default_backend_id() == "worker"

    registry.set_default_override("local")
    assert registry.default_backend_id() == "worker"


def test_a_process_that_serves_defaults_to_another_backend_its_variable_names(monkeypatch, serving):
    monkeypatch.setenv("FLOWFILE_COMPUTE_BACKEND", "worker:team")

    assert registry.default_backend_id() == "worker:team"


def test_a_process_that_serves_leaves_the_embedded_worker_s_jobs_out_of_its_own_process(monkeypatch, serving):
    from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
    from flowfile_worker import spawner

    monkeypatch.setattr(spawner, "in_process", False)

    assert isinstance(registry.backend(registry.LOCAL_BACKEND_ID), EmbeddedWorkerBackend)
    assert spawner.in_process is False


def test_in_single_file_mode_the_worker_is_the_embedded_worker_its_jobs_left_to_its_pool(monkeypatch, single_file):
    from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
    from flowfile_worker import spawner

    monkeypatch.setattr(spawner, "in_process", False)

    worker = registry.backend(registry.WORKER_BACKEND_ID)

    assert isinstance(worker, EmbeddedWorkerBackend)
    assert worker.id == registry.WORKER_BACKEND_ID
    assert registry.default_backend() is worker
    assert spawner.in_process is False


def test_in_single_file_mode_the_worker_url_is_the_mount_and_no_backend_is_made_for_it(single_file):
    made, factory = counting_factory(registry.WORKER_BACKEND_ID)
    registry.register_backend(registry.WORKER_BACKEND_ID, factory)

    assert registry.worker_url() == "http://127.0.0.1:63578/worker"
    assert made == []


def test_in_single_file_mode_a_configured_worker_stays_a_worker_backend(single_file, configured_row):
    from flowfile_core.flowfile.execution.backends.worker import WorkerBackend

    team = registry.backend(configured_row)

    assert isinstance(team, WorkerBackend)
    assert team.url == TEAM_URL


def test_the_default_backend_is_the_instance_of_the_default_id(monkeypatch):
    made, factory = counting_factory("cluster")
    registry.register_backend("cluster", factory)
    monkeypatch.setenv("FLOWFILE_COMPUTE_BACKEND", "cluster")

    assert registry.default_backend() is registry.backend("cluster")
    assert len(made) == 1


def test_the_built_in_backends_are_the_embedded_worker_and_the_worker_at_the_configured_url():
    from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
    from flowfile_core.flowfile.execution.backends.worker import WorkerBackend

    local, worker = registry.backend(registry.LOCAL_BACKEND_ID), registry.backend(registry.WORKER_BACKEND_ID)

    assert isinstance(local, EmbeddedWorkerBackend)
    assert isinstance(worker, WorkerBackend)
    assert worker.url == settings.WORKER_URL
    assert registry.worker_url() == worker.url


@pytest.mark.parametrize("offload", [True, False])
def test_the_override_names_the_default_ahead_of_the_variable_and_the_flag(monkeypatch, no_override, offload):
    monkeypatch.setattr(settings.OFFLOAD_TO_WORKER, "value", offload)
    monkeypatch.setenv("FLOWFILE_COMPUTE_BACKEND", "cluster")

    assert registry.set_default_override("local") is None

    assert registry.default_override() == "local"
    assert registry.default_backend_id() == "local"


def test_undoing_the_override_gives_back_the_default_it_replaced(monkeypatch, no_override):
    monkeypatch.setattr(settings.OFFLOAD_TO_WORKER, "value", True)
    before = registry.default_backend_id()

    previous = registry.set_default_override("local")
    assert registry.default_backend_id() == "local"
    assert registry.set_default_override(previous) == "local"

    assert registry.default_override() is None
    assert registry.default_backend_id() == before == "worker"


def test_an_override_set_over_another_returns_it_so_undoing_restores_it(no_override):
    registry.set_default_override("cluster")

    previous = registry.set_default_override("local")
    registry.set_default_override(previous)

    assert previous == "cluster"
    assert registry.default_backend_id() == "cluster"


def test_the_override_outlives_a_reset(no_override):
    registry.set_default_override("local")

    registry.reset()

    assert registry.default_backend_id() == "local"


def test_the_default_backend_follows_the_override(no_override):
    made, factory = counting_factory("cluster")
    registry.register_backend("cluster", factory)

    registry.set_default_override("cluster")

    assert registry.default_backend() is registry.backend("cluster")
    assert len(made) == 1


def test_a_child_runs_on_core_s_own_backend_by_both_names():
    assert registry.child_env() == {"FLOWFILE_COMPUTE_BACKEND": "local", "FLOWFILE_OFFLOAD_TO_WORKER": "0"}


def test_the_child_env_makes_core_s_own_backend_the_default_whatever_the_flag(monkeypatch, no_override):
    monkeypatch.setattr(settings.OFFLOAD_TO_WORKER, "value", True)
    for name, value in registry.child_env().items():
        monkeypatch.setenv(name, value)

    assert registry.default_backend_id() == registry.LOCAL_BACKEND_ID


def test_a_configured_worker_resolves_with_its_url_and_its_own_token(configured_row):
    from flowfile_core.flowfile.execution.backends.worker import WorkerBackend

    worker = registry.backend("worker:team")

    assert isinstance(worker, WorkerBackend)
    assert worker.id == "worker:team"
    assert worker.url == TEAM_URL
    assert worker.token == "row-token"
    assert worker.token != get_internal_token()
    assert registry.backend("worker:team") is worker
    assert subops.ssl_context_for(TEAM_URL) is None
    assert worker.shares_core_disk is False


def test_a_row_marked_as_seeing_cores_files_makes_a_backend_that_shares_its_disk(configured_backend):
    configured_backend("worker:nfs-team", shares_core_disk=True)

    assert registry.backend("worker:nfs-team").shares_core_disk is True


def test_a_configured_worker_with_a_ca_certificate_has_the_transport_trust_it_for_its_url(configured_backend):
    pem, _ = self_signed_certificate("tls-team-worker.test")
    configured_backend("worker:tls-team", url=TLS_TEAM_URL, ca_cert=pem)

    worker = registry.backend("worker:tls-team")

    assert worker.url == TLS_TEAM_URL
    assert isinstance(subops.ssl_context_for(TLS_TEAM_URL), ssl.SSLContext)


def test_a_worker_name_no_row_configures_is_unknown():
    with pytest.raises(LookupError) as raised:
        registry.backend("worker:nobody")

    assert str(raised.value) == "Unknown compute backend 'worker:nobody'; no compute_backends row has that id"


def test_forget_drops_the_instance_so_a_changed_row_is_read_again(configured_row):
    first = registry.backend("worker:team")
    with get_db_context() as db:
        db.get(ConfiguredBackend, "worker:team").url = "http://moved.test:63579"
        db.commit()
    assert registry.backend("worker:team") is first

    registry.forget("worker:team")
    again = registry.backend("worker:team")

    assert again is not first
    assert again.url == "http://moved.test:63579"
    assert again.token == "row-token"


def test_a_registration_wins_a_configured_name(configured_row):
    made, factory = counting_factory("worker:team")
    registry.register_backend("worker:team", factory)

    instance = registry.backend("worker:team")

    assert made == [instance]
