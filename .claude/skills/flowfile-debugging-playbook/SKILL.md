---
name: flowfile-debugging-playbook
description: Symptom-to-cause triage playbook for Flowfile (core/worker/kernel/frontend/AI) — covers "no such table" DB cascades (two distinct causes), import-time Alembic migration corruption, silent axios/FastAPI 307 redirects in Docker, browser "CORS error" that is really an unhandled 500, stale-vs-rerunning node caching, "flow no longer in memory" 404s, Docker kernel container failures, Tauri desktop sidecar leaks, and AI agent misbehavior via the prompt log. Use when a flow run fails unexpectedly, a pytest run shows failures you didn't cause, the UI silently no-ops, the browser console says "CORS error", a node's result looks stale or keeps re-running for no reason, a scheduled/desktop run's logs seem to have vanished, a kernel (Python Script) node won't connect, quitting the desktop app leaves processes running, or an AI agent loops/hallucinates/misreferences columns — anytime you need to find root cause before writing a fix.
---

# Flowfile Debugging Playbook

A field guide for triaging live breakage in the Flowfile monorepo: symptom in,
probable cause + discriminating check + fix pointer out. This is not an
architecture reference and not a "how to run the test suite" guide — see
"When NOT to use this skill" below.

## When NOT to use this skill

- Starting/operating services under normal conditions (no symptom yet) →
  `flowfile-run-and-operate`.
- Full test-suite-by-suite run commands, markers, CI evidence bar → `flowfile-testing-and-validation`.
- Complete environment-variable reference → `flowfile-config-and-flags`.
- Understanding *why* the DAG engine, node lifecycle, or worker-offload seam
  is built the way it is (not "it's broken, why") → `flowfile-architecture-contract`.
- Deep git/commit archaeology on a past incident, or writing up a postmortem → `flowfile-failure-archaeology`.
- AI agent/prompt architecture beyond "read the prompt log" → `flowfile-ai-subsystem`.
- Frontend code conventions (Pinia store map, node-UI-by-convention, Tauri
  bridge patterns) beyond the sidecar-leak triage below → `flowfile-frontend-conventions`.

---

## Quick orientation: what runs where

```
Frontend (Tauri/Web) --HTTP--> flowfile_core (:63578) --HTTP--> flowfile_worker (:63579)
                                      |                          (spawned subprocesses hold data)
                                      +--Docker SDK--> kernel_runtime containers (host ports 19000-19999)
```

Flows live in **core's process memory** (`flow_file_handler`), not just the DB —
a core restart or reload invalidates every open flow_id in the browser. The
worker never talks to a database; it holds results in spawned child processes
and an on-disk Arrow cache. Keep this triangle in mind: almost every "it's
broken" symptom below is really "which of these three lost state, or which two
disagree."

---

## The triage table

| Symptom | Likely cause | Discriminating check | Fix pointer |
|---|---|---|---|
| `sqlite3.OperationalError: no such table: <x>` appearing **mid-run**, nondeterministically, across unrelated tests | **Cause A**: a concurrent pytest session on the same machine tore down the shared fixed test DB (its teardown runs `DROP ALL` + deletes the file) | `ps aux \| grep pytest` — is there a second session? Check if the failure timing correlates with another terminal finishing | Isolate every session: `FLOWFILE_DB_PATH=/tmp/ff_$$.db poetry run pytest flowfile_core/tests` |
| `sqlite3.OperationalError: no such table: users` immediately, on the **very first** DB access | **Cause B**: you combined `FLOWFILE_SKIP_STARTUP_MIGRATION=1` with a brand-new/nonexistent DB file — migrations never ran, so no tables were ever created | `echo $FLOWFILE_SKIP_STARTUP_MIGRATION`; `sqlite3 <db path> '.tables'` (empty or missing file) | Never combine `FLOWFILE_SKIP_STARTUP_MIGRATION=1` with a fresh `FLOWFILE_DB_PATH`. For diagnostics against a fresh DB, drop the skip flag and let Alembic run; for the pytest suite, **never** set the skip flag at all |
| Live catalog DB's `alembic_version` looks wrong / behind repo head after nothing but reading code | `import flowfile_core` (even transitively, e.g. `python -c "import flowfile_core"`) runs `run_startup_migration()` against **whatever DB resolves** at import time. A stale Poetry env whose local migration scripts only go up to an older revision will re-stamp the *live* DB backward without reverting its schema | `sqlite3 ~/.flowfile/database/flowfile_catalog.db 'select * from alembic_version;'` vs. `ls flowfile_core/flowfile_core/alembic/versions/ \| tail -3` (head is `028` as of 2026-07-03) | Always set `FLOWFILE_SKIP_STARTUP_MIGRATION=1` for **diagnostic** imports against real/live data — never for the test suite. Recover a corrupted stamp by manually re-stamping to the correct revision, then re-running `alembic upgrade head` |
| UI action appears to do nothing in Docker (works fine in local dev) | axios path and the FastAPI route decorator disagree on a **trailing slash** → FastAPI issues an absolute 307 redirect that silently fails; the Vite dev proxy and pytest's `TestClient` both paper over the mismatch, so it only manifests in Docker/nginx | `docker compose logs -f flowfile-core \| grep 307`; diff the axios call's path string against the `@router.post("/...")` decorator it targets, character for character | Fix the **frontend** path to match the decorator's slash exactly (e.g. `POST /update_settings/` needs the trailing `/`) — see `flowfile-frontend-conventions` |
| Browser devtools shows "CORS error" / "blocked by CORS policy" | Almost always **not actually a CORS problem** — an unhandled exception (commonly `AttributeError` on a `None` node/connection) becomes a FastAPI 500, and a 500 response is missing the CORS headers a normal response would carry; the browser mislabels this as CORS | Read the **core log** for a traceback at the same timestamp (`~/.flowfile/logs/flow_<flow_id>.log`, or core's stdout) — do NOT trust the browser console's diagnosis | Fix the underlying 500 (typically a missing `None`/existence guard before touching a possibly-deleted node/connection) |
| A node's preview/result doesn't reflect data that changed **outside** the flow (e.g. new rows landed in a source the node reads) | Worker cache hit: the node's hash (settings + inputs + `parent_uuid`) didn't change, so Development-mode execution logic skips and serves the stale cached result | Force a refresh in the UI (reset_cache) and compare; if scripting directly against a `FlowGraph`/`FlowNode` object, no route exposes this — call it in-process | `node.invalidate_cache()` bumps `_cache_epoch`, busting the hash without touching settings — this is the intended escape hatch for out-of-band data changes (e.g. Kafka offset resets use this pattern) |
| A node **re-executes every single run** even though nothing changed and cache_results-style behavior is expected | `results_exists()` returns `False` unconditionally whenever the worker is unreachable or `OFFLOAD_TO_WORKER` is off — Development-mode "skip" decisions silently degrade to "always run" | Is the worker actually up? `curl -sf http://localhost:63579/docs`; check core's startup log line `Worker configured at <URL>` | Start/restart `poetry run flowfile_worker`; verify `WORKER_HOST`/`FLOWFILE_WORKER_PORT`. Remember `FLOWFILE_OFFLOAD_TO_WORKER` only treats the exact string `"1"` as true |
| Running a flow (or fetching a node) returns `404: Flow <id> is no longer in memory. Reload the flow and try again.` | Flows live only in core's in-process `flow_file_handler`; a core restart, or a "Save As" that mints a new flow_id, orphans the frontend's cached flow_id | Check core's stdout for a recent restart timestamp; compare the frontend's flow_id against the current `flow_registrations` row for that flow name | Not a bug — reload the flow in the UI. Route/message: `flowfile_core/flowfile_core/routes/routes.py` around the flow-run handler |
| A Python Script (kernel) node hangs, errors connecting, or the container seems gone | The Docker kernel container isn't running, its mapped host port (range **19000-19999**) is blocked/reused, or the image tag doesn't match `FLOWFILE_KERNEL_IMAGE` | `docker ps \| grep flowfile-kernel-`; `docker logs flowfile-kernel-<kernel_id>`; check core's startup/kernel-manager log for the assigned host port | Reconnect/retry from the UI, or `docker rm -f flowfile-kernel-<kernel_id>` and retry so a fresh container is created; confirm `FLOWFILE_KERNEL_IMAGE`/`_BASE`/`_ML`/`_LITE` point at a pulled image |
| Quit the Tauri desktop app (either via window close **or** Cmd+Q/dock-quit) but `flowfile_core`/`flowfile_worker` processes are still running | Two known open races: (a) quitting during startup can shut down before sidecar PIDs are recorded, so that spawn is never reaped; (b) a sidecar dying mid-shutdown can leave a stale PID that a later `killpg` targets after the PID was recycled. There is currently **no automated desktop E2E test** guarding this | `ps aux \| grep flowfile_core; ps aux \| grep flowfile_worker` — run this test **both** ways (window close, and Cmd+Q/dock-quit) after quitting | Kill leftover PIDs manually for now. This is a known, tracked gap — see `flowfile-frontend-conventions` for the exact races (`lib.rs`, `sidecar/mod.rs`) before touching sidecar shutdown code |
| An AI agent loops on a tool name, references a nonexistent column, or otherwise does something surprising | Need to see exactly what the model was given (system prompt, tool catalog, message history) — guessing from the output is unreliable | Set `FLOWFILE_AI_LOG_PROMPTS=true`, reproduce the run, then `python -m flowfile_core.ai.prompt_log tail 20` | Also `python -m flowfile_core.ai.prompt_log grep PATTERN [SURFACE]`; see "AI agent misbehavior" below and `flowfile-ai-subsystem` for architecture |
| A scheduled flow's log is empty or missing, even though `FLOWFILE_STORAGE_DIR` is set to a custom path | Scheduled-run subprocess stdout/stderr is **hardcoded** to the real home directory, ignoring `FLOWFILE_STORAGE_DIR` | Check `~/.flowfile/logs/scheduled_run_<run_id>.log` (real home, not the configured storage dir) | Not a bug to fix locally — known gotcha (`shared/subprocess_utils.py`); look there, not in `$FLOWFILE_STORAGE_DIR/logs` |
| A flow's execution log is gone after restarting core, even though the flow definitely ran recently | `clear_all_flow_logs()` deletes every `*.log` under the logs directory on **every core shutdown**, and a 1-hour/7-day sweep runs on startup too | Query the durable record instead: `sqlite3 ~/.flowfile/database/flowfile_catalog.db 'select id,flow_name,started_at,ended_at,success from flow_runs order by id desc limit 10;'` | Flow logs are not history; `flow_runs` is. Don't chase "missing log" as a bug |
| Worker offload fails with `Connection refused` mentioning `/submit_query/` | The worker process isn't running, or core is pointed at the wrong worker URL | Core startup log line `Worker configured at <WORKER_URL>`; `curl -sf <WORKER_URL>/docs` | Start `poetry run flowfile_worker`; or set the flow's `execution_location` to `local` to bypass the worker entirely |
| Worker-offloaded node fails with error code `-1` / "unknown error … process got killed by the server" | The worker's **spawned child process** was OOM-killed mid-computation | Check worker stdout around that timestamp for OS-level kill signals; check available memory on the worker host | Core degrades gracefully (keeps its own lazy frame, shows a warning, no example data) — not silent data loss, but you likely need to reduce data volume or fix a memory leak in the transform |
| "My unrelated change broke N tests" right after a routine edit | See "Discriminating-experiment discipline" below **before** trusting this conclusion | — | — |

---

## Deep dive: "no such table" cascades (the two causes, precisely)

Both causes produce the same error text, so **do not** assume it's whichever one
you saw last time.

**Cause A — concurrent pytest sessions clobbering the shared test DB.**
`TESTING=True` resolves the catalog DB to one fixed path,
`<storage base>/temp/test_flowfile_catalog.db` — every pytest invocation on the
machine that doesn't override `FLOWFILE_DB_PATH` shares that single file. The
core test suite's session-scoped teardown does `Base.metadata.drop_all(engine)`
and then deletes the file. If a second pytest session (yours, or a leftover
background one) finishes and tears down while your session is still mid-run,
your tables vanish out from under you — nondeterministically, often on a table
your test never even touches directly.

Check: `ps aux | grep pytest`. Fix: give every session its own DB —
```bash
FLOWFILE_DB_PATH=/tmp/ff_isolated_$$.db poetry run pytest flowfile_core/tests
```
This also isolates the worker subprocess the core conftest spawns for you,
since it inherits your env.

**Cause B — skip-migration flag combined with a fresh DB.**
`FLOWFILE_SKIP_STARTUP_MIGRATION=1` skips `run_startup_migration()` at
`flowfile_core` import. That's exactly what you want for a **diagnostic**
import against a database that already has its schema. But if you also point
`FLOWFILE_DB_PATH` at a path that doesn't exist yet, skipping migration means
the tables are simply never created — every query fails immediately with
`no such table: users` (the seed table). This is not corruption; it's an empty
file.

```bash
# WRONG — combines both, empty DB, nothing ever gets created:
FLOWFILE_DB_PATH=/tmp/throwaway.db FLOWFILE_SKIP_STARTUP_MIGRATION=1 \
  poetry run python -c "import flowfile_core"

# RIGHT for a throwaway diagnostic DB — let migrations run once:
FLOWFILE_DB_PATH=/tmp/throwaway.db poetry run python -c "import flowfile_core"

# RIGHT for skipping migration — only against a DB that's already migrated:
FLOWFILE_SKIP_STARTUP_MIGRATION=1 FLOWFILE_DB_PATH=/tmp/already_migrated.db \
  poetry run python -c "import flowfile_core"
```

**Never set `FLOWFILE_SKIP_STARTUP_MIGRATION=1` for the pytest suite itself** —
the session-autouse `setup_test_db` fixture relies on the migration to build
the schema; skipping it fails *every* test with `no such table: users`.

---

## Deep dive: import-time migration against the live DB

`import flowfile_core` is not inert. Its package `__init__.py` runs
`validate_setup()` and `init_db()` at **import time**, and `init_db()` runs
Alembic migrations against whatever database URL resolves — by default that's
the real, live catalog DB (`~/.flowfile/database/flowfile_catalog.db`), the
same one the desktop app or your running `poetry run flowfile_core` uses.

Real incident (worth internalizing the shape of): a diagnostic script was run
through a **stale Poetry virtualenv** whose local copy of
`flowfile_core/flowfile_core/alembic/versions/` only went up to an older
migration (e.g. 010), while the live DB was already stamped at a newer
revision (e.g. 013). Alembic saw the DB's stamp as "ahead of what I know
about," and the stale env's migration run rewrote the `alembic_version` row
**backward** to match what it knew — without reverting any of the schema
changes those newer migrations had made. The DB ended up schema-013 but
labeled 010: the next *correct* env's migration attempt then tried to
re-apply already-applied migrations and broke.

Rule: **any ad-hoc import of `flowfile_core` against real data must set
`FLOWFILE_SKIP_STARTUP_MIGRATION=1`.**

```bash
FLOWFILE_SKIP_STARTUP_MIGRATION=1 poetry run python -c "
import flowfile_core  # inspect models, call helpers, etc. — no migration runs
"
```

If you suspect a stamp has already been corrupted this way: compare
`sqlite3 <db> 'select * from alembic_version;'` against the actual head file
under `flowfile_core/flowfile_core/alembic/versions/` (as of 2026-07-03 that's
`028_catalog_namespace_storage.py`, `down_revision = "027"`), and re-stamp
manually before running `alembic upgrade head` for real.

---

## Deep dive: kernel container triage

Kernel containers back Python Script nodes (sandboxed user code). Each is a
Docker container that `EXPOSE`s port 9999 internally; core maps that to a host
port in **19000-19999** and the kernel calls back to core on `:63578`.

```bash
docker ps | grep flowfile-kernel-          # is it even running?
docker logs flowfile-kernel-<kernel_id>    # container-side error?
```

Container naming is `flowfile-kernel-<kernel_id>` (the kernel's DB id/uuid, not
the flow id). If a stale container from a previous test run or crashed session
is holding a port, force-remove it and let core recreate it:

```bash
docker rm -f flowfile-kernel-<kernel_id>
```

Local (non-Docker-in-Docker) mode bind-mounts the cache and catalog-tables
directories into the container — if kernel code can't see catalog tables or
shared artifacts, verify those directories weren't relocated out from under
the kernel-visible volume (`shared_directory`/`artifact_staging_directory`/
`global_artifacts_directory` must stay under `<base>/temp/kernel_shared` or
`$FLOWFILE_SHARED_DIR`).

Core also stops **every** kernel container on its own shutdown — if kernels
vanish right when you stop core in dev, that's expected, not a leak.

---

## Deep dive: desktop (Tauri) sidecar leaks

The desktop shell spawns `flowfile_core` and `flowfile_worker` as sidecar
subprocesses and is supposed to reap them on quit via an HTTP `/shutdown` →
SIGTERM → SIGKILL ladder. Two things make this fragile enough to warrant a
manual check after any change near `src-tauri/src/lib.rs` or
`src-tauri/src/sidecar/`:

1. On macOS, Cmd+Q and dock-quit route through `RunEvent::Exit`, not
   `RunEvent::ExitRequested` — code that only handles the latter leaks
   sidecars on those quit paths specifically (window-close-button quit is
   unaffected). **Always test both quit paths**, not just one.
2. A quit that lands mid-startup, or a sidecar that dies exactly during
   shutdown, can leave a PID uncleared that a later kill targets after the OS
   has recycled it onto an unrelated process.

There is currently no automated E2E test covering desktop sidecar lifecycle —
manual verification is the only guardrail:

```bash
# after quitting via the window's close button:
ps aux | grep flowfile_core; ps aux | grep flowfile_worker
# after quitting via Cmd+Q / dock-quit — check again, separately:
ps aux | grep flowfile_core; ps aux | grep flowfile_worker
```

Any hits after a clean quit mean the shutdown ladder didn't reach that
process; kill it manually and treat the reproduction steps as a bug report
against the sidecar lifecycle code, not a one-off fluke.

---

## Deep dive: AI agent misbehavior

When an agent run does something surprising (tool-name loops, planner
self-loops, references to columns that don't exist, refuses to stop), the
fastest path to ground truth is the prompt log — it captures the exact system
prompt, full message history, tool catalog, and model response for every LLM
call, regardless of provider (all vendors route through the same
`LiteLLMProvider` seam).

```bash
export FLOWFILE_AI_LOG_PROMPTS=true     # accepts true/1/yes/on
# reproduce the failing flow/chat/agent session, then:
python -m flowfile_core.ai.prompt_log tail 20
python -m flowfile_core.ai.prompt_log grep '<pattern>' [SURFACE]
```

The file lives at `<storage base>/ai_prompts/YYYY-MM-DD.jsonl` (UTC-dated —
`~/.flowfile/ai_prompts/...` by default), one JSON line per LLM call,
`jq`-parseable. Lines over 256 KiB (runaway loops) keep the system prompt and
most-recent turns verbatim and stub older message bodies with
`[...truncated, len=N chars]` plus `"truncated": true`, so it stays parseable
even for pathological sessions. Set `FLOWFILE_AI_LOG_PROMPTS_SCRUB=true` before
sharing a transcript externally — it masks PII in user/tool messages while
leaving system+assistant content verbatim (that's the part you're debugging).
Both flags default off; don't leave prompt logging on in production.

For the agent architecture itself (assist/copilot/planner surfaces, provider
registry, rate-limit scheduler caveats) see `flowfile-ai-subsystem`.

---

## Where every log file lives

| Log | Location | Notes |
|---|---|---|
| Per-flow execution log | `<storage base>/logs/flow_<flow_id>.log` | Wiped on **every** core shutdown (`clear_all_flow_logs()`) and by a 168h sweep — not durable history |
| Scheduled-run subprocess output | `~/.flowfile/logs/scheduled_run_<run_id>.log` | **Hardcoded to the real home dir**, ignores `FLOWFILE_STORAGE_DIR`; truncated fresh each run |
| Core service log | stdout only, no file handler | Electron/Tauri pipes it through the shell's own log plugin |
| Worker service log | stdout only, no file handler | Worker subprocesses ship flow-scoped log lines back to core over `POST /raw_logs`, landing in the same `flow_<id>.log` |
| AI prompt log | `<storage base>/ai_prompts/YYYY-MM-DD.jsonl` | UTC-dated; only written when `FLOWFILE_AI_LOG_PROMPTS` is truthy |
| Docker container logs | `docker compose logs -f [service]` | Core/worker/kernel containers all log to stdout |
| Durable run record (survives log wipes) | `flow_runs` table in the catalog DB | `sqlite3 ~/.flowfile/database/flowfile_catalog.db 'select id,flow_name,started_at,ended_at,success,pid from flow_runs order by id desc limit 10;'` |

`<storage base>` = `FLOWFILE_STORAGE_DIR` env if set, else `~/.flowfile`
locally, else `/app/internal_storage` in Docker mode.

---

## Discriminating-experiment discipline

Before you conclude "my change broke N tests" (or "my change fixed the bug"),
run the elimination checklist — false positives from shared, stateful
infrastructure are the norm here, not the exception:

1. **Check for concurrent sessions first.** `ps aux | grep pytest` (another
   pytest run can drop your shared test DB's tables mid-run — see the "no such
   table" deep dive above). Also check for a second `flowfile_core`/
   `flowfile_worker` already listening on the default ports (`lsof -i :63578`,
   `lsof -i :63579`) — your "new" server might be talking through a stale one.
2. **Re-run the exact failing subset in isolation**, with a scratch DB and
   without prior state:
   ```bash
   FLOWFILE_DB_PATH=/tmp/ff_recheck_$$.db poetry run pytest \
     path/to/test_file.py::TestClass::test_name -q -p no:cacheprovider -rX
   ```
   If it passes alone but failed in the full run, you likely hit shared-state
   bleed (catalog namespace rows wiped by another module's cleanup, a stale
   worker/postgres/mysql session-singleton left dirty by a previous run,
   process-wide env mutation not restored) — not a regression from your change.
3. **Compare skip counts, not just pass/fail counts.** A large slice of the
   integration surface only runs when Docker/MinIO/GCS/Azurite/Redpanda are up;
   a run with more skips than your baseline is weaker evidence, not stronger,
   even if it's "green."
4. **XPASS on an `xfail` marker means the bug was already fixed — delete the
   marker, don't add work to preserve it.** (Two markers in this repo are
   already known-stale as of 2026-07-03; verify with `-rX` and look for
   `xpassed` in the summary line before trusting an xfail's reason text.)
5. Only after 1-4 clear the bar should you attribute a result to your change.

---

## Provenance and maintenance

Volatile facts below are dated **2026-07-03 (v0.12.7)** — re-verify with the
commands shown before relying on the numbers in a new session.

- App version: `grep '^version' pyproject.toml` → `0.12.7`.
- Alembic head: `ls flowfile_core/flowfile_core/alembic/versions/ | sort | tail -3` → `028_catalog_namespace_storage.py`. Live DB check: `sqlite3 ~/.flowfile/database/flowfile_catalog.db 'select * from alembic_version;'`.
- `results_exists` worker-down behavior: `grep -n "def results_exists" -A 15 flowfile_core/flowfile_core/flowfile/flow_data_engine/subprocess_operations/subprocess_operations.py`.
- `invalidate_cache` escape hatch: `grep -n "def invalidate_cache" -A 10 flowfile_core/flowfile_core/flowfile/flow_node/flow_node.py`.
- "Flow no longer in memory" 404 message/route: `grep -n "no longer in memory" flowfile_core/flowfile_core/routes/routes.py`.
- CORS-masked-500 guard comment: `grep -n "CORS" flowfile_core/flowfile_core/flowfile/flow_graph.py`.
- Kernel host port range: `grep -n "_BASE_PORT\|_PORT_RANGE" flowfile_core/flowfile_core/kernel/manager.py`.
- Kernel container naming: `grep -n 'f"flowfile-kernel-' flowfile_core/flowfile_core/kernel/manager.py`.
- AI prompt log CLI: `python -m flowfile_core.ai.prompt_log tail 5` (run against a scratch `FLOWFILE_DB_PATH`/`FLOWFILE_STORAGE_DIR` first if you don't want to touch real data).
- Scheduled-run log path hardcoding: `grep -n "scheduled_run_" shared/subprocess_utils.py`.
- Core shutdown log wipe: `grep -n "clear_all_flow_logs" flowfile_core/flowfile_core/main.py`.
- Test DB isolation model / shared-fixed-path behavior: `grep -n "get_database_url" -A 20 shared/storage_config.py`.
- `FLOWFILE_SKIP_STARTUP_MIGRATION` gate: `grep -n "FLOWFILE_SKIP_STARTUP_MIGRATION" flowfile_core/flowfile_core/database/init_db.py`.
- Tauri Cmd+Q / `RunEvent::Exit` handling and the two open TODO races: `grep -n "RunEvent::Exit\|TODO(D)\|TODO(C)" flowfile_frontend/src-tauri/src/lib.rs flowfile_frontend/src-tauri/src/sidecar/mod.rs`.
- Sidecar process names for `ps aux` triage: `grep -n 'BINARIES = ' tools/rename_sidecar.py` (binaries are named `flowfile_core`/`flowfile_worker`).
- Trailing-slash 307 pattern: `grep -n "proxy_redirect" flowfile_frontend/nginx.conf`; `grep -n "configure:" flowfile_frontend/vite.config.mjs`.
- xfail staleness (currently 2 stale XPASS markers): re-run per the command in the "Discriminating-experiment discipline" section against `flowfile_core/tests/flowfile/test_code_generator_edge_cases.py::TestBasicFilterOperators::test_in_operator_numeric` and `flowfile_core/tests/flowfile/node_designer/test_node_designer.py::TestNumericStringAliasBug` — if still `xpassed`, the markers are still stale.
