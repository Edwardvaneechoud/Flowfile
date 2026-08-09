"""Suite-wide storage isolation for the scheduler tests.

``FlowScheduler._tick`` calls ``cleanup_old_logs()``, which deletes files from
``storage.logs_directory``; several tests drive ``_tick`` directly. Without this
redirect the suite expires logs out of the developer's real ``~/.flowfile/logs``.
"""

from __future__ import annotations

import pytest

from shared.storage_config import storage


@pytest.fixture(scope="session", autouse=True)
def _isolate_storage(tmp_path_factory):
    base = tmp_path_factory.mktemp("scheduler_storage")
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(storage, "_base_dir", base)
        storage.logs_directory.mkdir(parents=True, exist_ok=True)
        yield base
