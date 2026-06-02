# CLAUDE.md - flowfile_scheduler

Lightweight, embeddable engine that polls the shared catalog DB for due flow schedules and fire-and-forgets `flowfile run flow` subprocesses. Package-specific notes; see the root `/CLAUDE.md` for monorepo-wide setup, build, lint, ports, and cross-package contracts.

## Role
`FlowScheduler` (`flowfile_scheduler/engine.py`) runs an async polling loop (`DEFAULT_POLL_INTERVAL = 30`s) that, each tick, acquires a DB advisory lock and launches flows whose schedules are due.

- **Who calls it:** `flowfile_core` re-exports `FlowScheduler` via its own shim `flowfile_core/scheduler/__init__.py` (which also owns the `_scheduler` singleton + `get_scheduler`/`set_scheduler`). Core's `main.py` lifespan instantiates it, calls `await scheduler.start()` + `set_scheduler(...)` **only** when `os.environ["FLOWFILE_SCHEDULER_ENABLED"].lower()` is in `("true", "1", "yes")`, and `await scheduler.stop()` + `set_scheduler(None)` on shutdown. Also runnable standalone via the `flowfile_scheduler` console script.
- **What it imports:** only `croniter`, `sqlalchemy`, stdlib, and `shared.*` (`shared.storage_config.get_database_url`, `shared.subprocess_utils.spawn_flow_subprocess`, and the ORM models via the local `flowfile_scheduler.models` shim, which re-exports from `shared.models`). **No `flowfile_core` imports** — the dependency arrow points core → scheduler, never the reverse. This is a load-bearing invariant: do not add a `flowfile_core` import here.
- **DB:** talks to the shared SQLite catalog (`get_database_url()`, defaults to `<storage>/flowfile_catalog.db`) via SQLAlchemy ORM. On init it runs `Base.metadata.create_all(self._engine, checkfirst=True)` to ensure tables exist.
- **No ports** — it is a background loop / subprocess launcher, not an HTTP service.

## Layout
- `flowfile_scheduler/__init__.py` — exports `FlowScheduler` only.
- `flowfile_scheduler/engine.py` — the entire engine: loop, lock, the four schedule-type processors, launch helpers.
- `flowfile_scheduler/__main__.py` — standalone CLI entry (`main()`); `--once` runs a single tick, otherwise polls until SIGINT/SIGTERM.
- `flowfile_scheduler/models.py` — backward-compat shim re-exporting `Base, CatalogTable, FlowRegistration, FlowRun, FlowSchedule, SchedulerLock, ScheduleTriggerTable` from `shared.models` (the single source of truth).
- `tests/` — package-root test dir (sibling of the inner package); `tests/test_cron_schedules.py` holds cron evaluation tests.

## Key patterns & conventions
- **Single-leader via DB lock:** `_acquire_lock`/`_release_lock` use the `SchedulerLock` row (id=1, `holder_id` = a per-instance `uuid4().hex[:12]`). A tick is skipped unless this instance holds the lock; a held lock is refreshed via `heartbeat_at`. A foreign lock is forcibly taken over after `STALE_THRESHOLD = 90`s without heartbeat.
- **Four schedule types**, dispatched by `_tick` in order: `interval`, `cron`, `table_trigger`, `table_set_trigger` (each `_process_*_schedules` returns a launch count). Filter is always `enabled.is_(True)` plus the matching `schedule_type`.
- **Cron in naive local wall-clock:** `_process_cron_schedules` advances a `last_cron_slot` cursor in the schedule's `cron_timezone` (default UTC) with `croniter`, comparing against `now` localized + naive. DST: the fall-back repeated hour fires once; spring-forward skips are caught on the next tick. On fire, the cursor is advanced to **now** (not the slot) so a downed scheduler catches up with a single fire, never backfilling. `last_triggered_at` still records the real UTC fire time.
- **Double-launch guard:** `_maybe_launch` skips if `_has_active_run` finds a `FlowRun` with `ended_at IS NULL` for the registration. It creates the `FlowRun` row (`run_type="scheduled"`, `schedule_id`, later `pid`) and sets `last_triggered_at` **before** spawning, so the run shows active immediately; spawn failure marks the run `ended_at`/`success=False`.
- **table_trigger is a poll safety-net:** the primary/fast trigger is a push path on the catalog-write side (`CatalogService._fire_table_trigger_schedules`, reached via `overwrite_table_data`). This package's poll path compares `CatalogTable.updated_at` against the schedule's `last_trigger_table_updated_at` as a fallback. `table_set_trigger` requires ≥2 linked tables (via `ScheduleTriggerTable`) all updated since `last_triggered_at`.
- **Blocking DB work is offloaded** off the event loop via `asyncio.to_thread(self._tick)`; never block the loop directly.

## Running / entry points
```bash
poetry run flowfile_scheduler          # continuous polling loop
poetry run flowfile_scheduler --once   # single tick, then exit
```
Embedded mode: started automatically from core when `FLOWFILE_SCHEDULER_ENABLED` is truthy (see core `main.py` lifespan). Launched flows run as detached subprocesses — `python -m flowfile run flow <path> --run-id <id>` (or `<exe> --run-flow <path> --run-id <id>` when `sys.frozen`), logging to `~/.flowfile/logs/scheduled_run_<run_id>.log`.

## Testing
```bash
poetry run pytest flowfile_scheduler/tests
```
Tests live in `flowfile_scheduler/tests/`. They bind a `FlowScheduler` to a throwaway SQLite DB by monkeypatching `engine.get_database_url`, pin `engine._utcnow`, and stub the instance's `_spawn_flow` so no real subprocess launches. Helpers drive `_process_cron_schedules` directly; no Docker or markers needed.

## Gotchas
- Do not import `flowfile_core` (or anything that transitively pulls it in) anywhere in this package — it breaks the "core depends on scheduler" direction and the standalone/lightweight contract.
- `models.py` is a compatibility shim only; add or change ORM columns in `shared/models.py`, not here.
- `run_once` and `stop` both call `_release_lock`; never delete a lock you don't hold (the release is guarded by `holder_id`).
- Datetimes from the DB are stored naive-UTC; the engine `.replace(tzinfo=timezone.utc)` on read. Preserve this when adding comparisons, or you'll get aware/naive arithmetic errors.
- Cron correctness hinges on the `last_cron_slot` cursor being in naive local time — don't "fix" it to UTC or advance it to `next_run` instead of `now`; both regress the DST/catch-up behavior the tests pin.

## Key files
- `flowfile_scheduler/engine.py` — `FlowScheduler`: loop, lock, schedule processors, launch (the whole engine).
- `flowfile_scheduler/__main__.py` — standalone CLI (`--once` / polling).
- `flowfile_scheduler/__init__.py` — public surface (`FlowScheduler`).
- `flowfile_scheduler/models.py` — compat re-export from `shared.models`.
- `tests/test_cron_schedules.py` — cron/DST/catch-up/lock test coverage.
- `../shared/models.py` — actual ORM table definitions (source of truth).
- `../shared/storage_config.py` — `get_database_url()` (catalog SQLite path).
- `../shared/subprocess_utils.py` — `spawn_flow_subprocess()` (the fire-and-forget launcher).
- `../flowfile_core/flowfile_core/scheduler/__init__.py` — core-side re-export + `_scheduler` singleton accessors.
