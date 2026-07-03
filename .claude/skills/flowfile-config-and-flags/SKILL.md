---
name: flowfile-config-and-flags
description: Complete catalog of every Flowfile environment variable and runtime flag (FLOWFILE_MODE semantics, MutableBool live-flip flags, storage/database/catalog/AI/kernel/artifact/OAuth vars), which file:line reads each one, its default and truthy-parsing rule, and the documented-vs-actual drift between CLAUDE.md/.env.example/compose/docs — use when adding, changing, debugging, or auditing any FLOWFILE_*/env-var-driven behavior, when a config value "isn't taking effect", when writing a .env or docker-compose entry, or when asked "what env vars does Flowfile read" / "how do I flip a feature flag without restarting".
---

# Flowfile configuration and feature flags

Flowfile has **no central config schema** — ~90 environment variables are read
ad hoc across 6 packages via `os.environ`/`os.getenv`, plus a handful of
runtime-mutable flags and 4 Starlette-`Config` keys. This skill is the single
source of truth for "what does this var do and where is it read." Read it
before touching any `FLOWFILE_*` variable, `.env.example`, or
`docker-compose.yml` env block.

## When NOT to use this skill

- **How to build/install/launch each service** (Poetry, npm, Makefile targets, Docker Compose usage) → `flowfile-build-and-env` / `flowfile-run-and-operate`.
- **Why core never `.collect()`s, or the core↔worker↔kernel data-flow contract** → `flowfile-architecture-contract`.
- **AI provider registry, BYOK, agents, prompt-log format** beyond the `FEATURE_FLAG_AI` gate → `flowfile-ai-subsystem`.
- **Test markers, fixtures, Docker test infra beyond the env-var names** → `flowfile-testing-and-validation`.
- **Debugging a live incident step-by-step** (this skill tells you *what a var does*; the playbook tells you *how to investigate*) → `flowfile-debugging-playbook`.
- **Adding a new node type's config schema** (Pydantic node settings, not env vars) → `flowfile-node-development`.

---

## 1. The three config layers

1. **Plain env vars** — `os.environ`/`os.getenv`, read directly. The vast majority of config (see §3).
2. **Starlette `Config(".env")`** — `flowfile_core/flowfile_core/configs/settings.py:129`, `config = Config(".env")`. Reads the **process's current working directory's** `.env` file, with real env vars taking precedence over the file (Starlette semantics). Only 4 keys go through it: `DEBUG`, `FILE_LOCATION`, `AVAILABLE_RAM`, `FLOWFILE_WORKER_URL` (settings.py:130-133). **Gotcha:** which `.env` gets read depends entirely on the directory you launch `flowfile_core` from — not the repo root, not the package dir.
3. **Runtime-mutable flags** (`MutableBool`, `flowfile_core/flowfile_core/configs/utils.py`) — an env var seeds the boot value, but an admin HTTP endpoint (or any Python call site) can flip `.set(bool)` live, process-wide, with zero restart. See §4.

---

## 2. `FLOWFILE_MODE` — semantics per value

Declared `flowfile_core/flowfile_core/configs/settings.py:138`:
`FLOWFILE_MODE = os.getenv("FLOWFILE_MODE", "electron")`, with helpers
`is_docker_mode()` / `is_electron_mode()` / `is_package_mode()` (settings.py:141-153).

**Where the default gets stamped into `os.environ` (import-time side effect — four places):**
- `flowfile_core/flowfile_core/__init__.py:12-13`
- `flowfile_core/flowfile_core/configs/__init__.py:8-9`
- `flowfile_core/flowfile_core/main.py:53-54`
- `flowfile/flowfile/web/__init__.py:146-147` (inside `start_server`, i.e. `flowfile run ui`)
- Tauri shell hard-sets it too: `flowfile_frontend/src-tauri/src/env.rs:52` → `FLOWFILE_MODE=electron`
- Docker bakes it: `flowfile_core/Dockerfile:70`, `flowfile_worker/Dockerfile:62`, `docker-compose.yml:24,69` → `FLOWFILE_MODE=docker`

**Caching split (load-bearing gotcha):** `settings.FLOWFILE_MODE` is captured **once, at import**. Several other call sites re-read `os.environ` **per call** instead: `auth/sharing.py:76` (`sharing_enabled()`), `shared/storage_config.py:26-28`, `kernel/manager.py:325`, `routes/file_manager.py:36`, `routes/secrets.py:97,139,180`, `auth/jwt.py:44,61,97,142,238`, `auth/secrets.py:19-25,180,210`, `flowfile_worker/secrets.py:131`, `database/init_db.py:55`, `flowfile/api.py:375`. **Changing the env var mid-process changes only the per-call group** — `settings.FLOWFILE_MODE`-derived constants stay frozen at the import-time value.

| Value | Who sets it | What it gates |
|---|---|---|
| `electron` (default) | unset → stamped at import; Tauri `env.rs` | Single-user desktop. `local_user` (id=1) auto-auth (`auth/jwt.py:97-102`); JWT secret auto-generated + persisted via `SecureStorage` (`auth/jwt.py:61-66`); master key auto-generated + stored (`auth/secrets.py:210-224`); `FLOWFILE_INTERNAL_TOKEN` auto-generated if absent (`auth/jwt.py:44-46`); secrets routes force `user_id=1` (`routes/secrets.py:97`); sharing routers 404 (`sharing_enabled()` false); file-manager routes 403 (`routes/file_manager.py:36`); storage under `~/.flowfile`; user data = `$HOME`. |
| `docker` | Dockerfiles / compose | Multi-user. **Startup fails without `JWT_SECRET_KEY`** (`auth/jwt.py:68-70`); master key required via `FLOWFILE_MASTER_KEY` env or `/run/secrets/flowfile_master_key` (`auth/secrets.py:140-169,210`); `FLOWFILE_INTERNAL_TOKEN` required (`auth/jwt.py:47-51`); admin seeded from `FLOWFILE_ADMIN_USER`/`_PASSWORD` (`database/init_db.py:55-65`); storage `/app/internal_storage` + `/data/user` (compose overrides user data to `/app/user_data`); file manager + sharing enabled; `/project` router 404s unless `FLOWFILE_ENABLE_PROJECTS` truthy. |
| `package` | **Nothing in the repo sets this** — an operator sets it manually (verified: only compose/Dockerfiles set `docker`; only import-time defaults set `electron`) | `is_package_mode()` true; because it's ≠ `electron`, sharing is **enabled**, and JWT/master-key follow the non-electron path (`JWT_SECRET_KEY` required; `SecureStorage` path falls to `SECURE_STORAGE_PATH` default `/tmp/.flowfile`). Projects always on (`routes/public.py:49`). |
| `tauri` (fallback string, never actually set) | `routes/public.py:45`: `mode = os.environ.get("FLOWFILE_MODE", "tauri")` — a **different default string** than everywhere else | Unreachable through normal startup because importing `flowfile_core` always stamps `electron` first. The frontend treats `electron`\|`tauri`\|`desktop` as synonyms (public.py:42-44 comment, env.rs:47-52). Don't "unify" this default — the comment explains the intent. |

---

## 3. Master env-var table

Every variable read anywhere in package source, grouped as in the codebase. "Read at" is `file:line` in the current tree; re-verify with the greps in "Provenance" before trusting a line number across a refactor.

### 3.1 Mode / auth / secrets

| Var | Read at | Default | Effect |
|---|---|---|---|
| `FLOWFILE_MODE` | settings.py:138 (+ ~20 per-call sites, §2) | `electron` (stamped at import) | Runtime mode; gates auth/secrets/sharing/storage/file-manager (see §2 table). |
| `JWT_SECRET_KEY` | `auth/jwt.py:68` | none — raises in non-electron mode | JWT signing secret. |
| `FLOWFILE_MASTER_KEY` | `auth/secrets.py:150`; `flowfile_worker/flowfile_worker/secrets.py:90` | none; falls back to Docker secret file `/run/secrets/flowfile_master_key` (`auth/secrets.py:159`); env wins; validated as a Fernet key, quotes stripped | Fernet master key encrypting all user secrets. Worker must be given the **same** key. |
| `FLOWFILE_INTERNAL_TOKEN` | `auth/jwt.py:42`; `kernel/manager.py:1227`; `kernel_runtime/flowfile_client.py:243` | auto-generated in electron (`jwt.py:44-46`, written back to `os.environ`); raises otherwise | `X-Internal-Token` header for kernel→core service auth. |
| `FLOWFILE_INTERNAL_SERVICE_USER_ID` | `auth/jwt.py:318` | `1` | User id attributed to the `_internal_service` principal when a kernel's owner can't be resolved. |
| `FLOWFILE_ADMIN_USER` / `FLOWFILE_ADMIN_PASSWORD` | `database/init_db.py:58-59` | none → warning, no admin created | Seeds the initial admin account. Docker-only guard at `init_db.py:55`. |
| `SECURE_STORAGE_PATH` | `auth/secrets.py:25` | `/tmp/.flowfile` | `SecureStorage` dir for **non-electron** modes (Fernet-encrypted JSON + `.secret_key`). |
| `APPDATA` | `auth/secrets.py:22`; `flowfile_worker/secrets.py:31` | `~/.config` fallback | Windows app-data root for electron `SecureStorage` (`<APPDATA>/flowfile`). OS var, not Flowfile-specific. |

### 3.2 Ports & service discovery

| Var | Read at | Default | Effect |
|---|---|---|---|
| `FLOWFILE_WORKER_PORT` | settings.py:105,125 | `63579` (`DEFAULT_WORKER_PORT`, settings.py:16); CLI `--worker-port` wins | Port core dials the worker on. |
| `WORKER_HOST` | settings.py:102,127 | `0.0.0.0` (non-Windows) / `127.0.0.1` (Windows) | Host core dials the worker on. Compose sets `flowfile-worker` (compose:35); Tauri sets `127.0.0.1` (env.rs:72). |
| `FLOWFILE_WORKER_URL` | settings.py:133 (Starlette Config: env or CWD `.env`) | `get_default_worker_url(WORKER_PORT)` — `http://{WORKER_HOST}:{port}` + `/worker` suffix iff `SINGLE_FILE_MODE` | Full worker URL override; consumed as `WORKER_URL` throughout `flow_data_engine/subprocess_operations/subprocess_operations.py`. |
| `CORE_HOST` / `CORE_PORT` | `flowfile_worker/flowfile_worker/configs.py:16-17` | `0.0.0.0`/`127.0.0.1`(Win), `63578` | Where the worker (and its spawned children) call core back. Compose sets `CORE_HOST=flowfile-core` (compose:70); Tauri sets both (env.rs:68-69). |
| `FLOWFILE_HOST` / `FLOWFILE_PORT` | `flowfile/flowfile/api.py:22-23` | `127.0.0.1` / `63578` | Where the `flowfile` CLI/api client probes and spawns core. |
| `FLOWFILE_MODULE_NAME` | `flowfile/flowfile/api.py:25` | `flowfile` | Module the CLI launches as server. |
| `FORCE_POETRY` / `POETRY_PATH` / `POETRY_ACTIVE` / `VIRTUAL_ENV` | `flowfile/flowfile/api.py:26-27,103-106` | empty / `poetry` / empty / empty | How `flowfile.api` decides to spawn the server via Poetry vs a venv Python (dev only). |
| CLI args | settings.py:71-83 `parse_args` (`--host --port --worker-port`); `flowfile_worker/configs.py:23-45` (`--host --port --core-host --core-port`) | server `0.0.0.0:63578`; worker `63579` | Args win over env vars. |

Non-env port facts: Tauri scans a free `(core, worker)` port pair starting at 63578 (`src-tauri/src/sidecar/mod.rs:66-77,118-136`) and injects `window.__FLOWFILE_PORTS__` for the renderer. Kernel containers get host ports 19000-19999 (`kernel/manager.py`, `_BASE_PORT=19000`, `_PORT_RANGE=1000`); container-internal port is always 9999 (`kernel_runtime/Dockerfile` `EXPOSE 9999`).

### 3.3 Runtime feature flags (`MutableBool` — see §4)

| Var | Read at | Default | Truthy parse | Effect |
|---|---|---|---|---|
| `FLOWFILE_SINGLE_FILE_MODE` | settings.py:19; `flowfile/web/__init__.py:72-73` | `"0"` | **exact `"1"` only** | Worker routes co-hosted on core under `/worker`; worker URL gets a `/worker` suffix. |
| `FLOWFILE_OFFLOAD_TO_WORKER` | settings.py:22 | `"1"` | **exact `"1"` only** | Heavy compute routed to the worker; off = in-core execution (violates the "core never collects" contract if you rely on it for large data — see `flowfile-architecture-contract`). |
| `FEATURE_FLAG_AI` | settings.py:25-27 | on | `true`/`1`/`yes`/`on` (case/space-insensitive) | Master gate for the entire `/ai/*` router (503 when off). |
| `FLOWFILE_LSP_ENABLED` | settings.py:32-34 | on | `true`/`1`/`yes`/`on` | Notebook Jedi/LSP bridge; off ⇒ `/lsp/*` degrades to empty 200 (never 503) so editors silently fall back to client-side completion. |
| `FLOWFILE_AI_LOG_PROMPTS` | settings.py:37-39 | off | `true`/`1`/`yes`/`on` | Appends a JSONL line per LLM call to `<base>/ai_prompts/YYYY-MM-DD.jsonl`. |
| `FLOWFILE_AI_LOG_PROMPTS_SCRUB` | settings.py:42-44 | off | `true`/`1`/`yes`/`on` | PII-scrubs user/tool messages in the prompt log (system + assistant stay verbatim). |
| `FLOWFILE_ENABLE_PROJECTS` | settings.py:49-51 | off (env default); compose defaults **true** (compose:34) | `true`/`1`/`yes`/`on` | Enables the `/project/*` git-tracking router in docker mode (404 when off); always on outside docker (`public.py:49`). |
| `FLOWFILE_SCHEDULER_ENABLED` | `flowfile_core/flowfile_core/main.py:73` | off; compose sets `true` (compose:32) | **`true`/`1`/`yes` — NO `on`** (differs from all the others above!) | Auto-starts the embedded `FlowScheduler` in core's lifespan. |

### 3.4 Storage & database

| Var | Read at | Default | Effect |
|---|---|---|---|
| `FLOWFILE_STORAGE_DIR` | `shared/storage_config.py:44` (docker), `:46` (local) | docker `/app/internal_storage`; local `~/.flowfile` | Internal root (`base_directory`): cache, temp, logs, DB, template_data. Tauri `env.rs:44` sets it to `~/.flowfile` explicitly. |
| `FLOWFILE_USER_DATA_DIR` | `shared/storage_config.py:59` | docker `/data/user`; local `$HOME` | User-data root: flows, uploads, outputs, catalog_tables, notebooks. **Setting this locally does nothing** — local path is always `Path.home()`, env only honored in docker mode. Compose overrides to `/app/user_data`. |
| `FLOWFILE_SHARED_DIR` | `shared/storage_config.py:157,171,243` | `<base>/temp/kernel_shared` | Core↔worker↔kernel exchange dir + `global_artifacts`/`artifact_staging` subpaths. Must stay Docker-visible — don't relocate under `base_directory` for Docker deployments. |
| `FLOWFILE_DB_PATH` | `shared/storage_config.py:410,427` | none | Explicit SQLite path override; wins over `TESTING`; also disables legacy-DB migration lookup. |
| `TESTING` | `shared/storage_config.py:414,430` | none; `flowfile_core/tests/conftest.py:23` sets `'True'` | `== "True"` ⇒ DB becomes `<base>/temp/test_flowfile_catalog.db` — **one shared file per machine**; concurrent pytest sessions clobber each other's teardown. Isolate with `FLOWFILE_DB_PATH` per session. |
| `FLOWFILE_SKIP_STARTUP_MIGRATION` | `flowfile_core/flowfile_core/database/init_db.py:26` | unset | Any value ⇒ skip the Alembic startup migration on import. Needed for diagnostics — importing `flowfile_core` otherwise migrates the live catalog DB. |
| `FLOWFILE_DB_READ_HEDGE_DELAY` | `shared/db_reader.py:25` | `8` (seconds, float) | Delay before a hedged SQLAlchemy read races `connectorx`. |
| `TEMP_DIR` (env var, distinct from the `TEMP_DIR` module constant) | settings.py:88 (`get_temp_dir()`) | `tempfile.gettempdir()` | **DEAD.** `get_temp_dir()` has zero callers repo-wide. The unrelated `TEMP_DIR` module constant (settings.py:134) is `storage.temp_directory`. |

### 3.5 Catalog object storage

| Var | Read at | Default | Effect |
|---|---|---|---|
| `FLOWFILE_CATALOG_STORAGE_URI` | settings.py:54-60 (**read per call**, not cached) | none | Creation-time default storage root (e.g. `s3://bucket/catalog`) for **new** catalog tables. Not a live override — the seeded "General" catalog stays local even if you set this later. |
| `FLOWFILE_CATALOG_STORAGE_CONNECTION` | settings.py:63-68 (per call) | none | Name of an existing `CloudStorageConnection` supplying credentials; required when the URI above is set. |

### 3.6 AI subsystem

| Var | Read at | Default | Effect |
|---|---|---|---|
| `FLOWFILE_AI_<PROVIDER>_RPM` / `_RPD` | `ai/scheduler.py:296-297` templates + `.upper()` (:324); parsed by `_read_int_env` (:244-263 — non-int or ≤0 logged + ignored) | unset = no enforcement | Soft per-provider request budget, **per worker process** (in-memory deque). Providers: `ANTHROPIC OPENAI GOOGLE GROQ OPENROUTER`; `ollama` is unlimited (`_UNLIMITED_PROVIDERS`, scheduler.py:268). |
| `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GEMINI_API_KEY`/`GOOGLE_API_KEY`, `GROQ_API_KEY`, `OPENROUTER_API_KEY` | `ai/byok.py:52-63` map, `:85` `detect_env_fallback`; litellm also reads them itself | unset | Provider fallback when no BYOK credential row is saved for that user. |
| `LITELLM_LOCAL_MODEL_COST_MAP` | `ai/__init__.py:31` (`os.environ.setdefault(...)`, a **write**, before any lazy litellm import) | `"True"` | Keeps litellm from fetching its cost map over the network at import time. |
| `FLOWFILE_LOCAL_MODEL_CTX` | `ai/local_model/manager.py:110` | `16384` | **First-run seed only** for the local llama.cpp context window; a UI-persisted sidecar file wins after the first run. Clamped 2048-32768 (`:113-114`). |

### 3.7 Kernel orchestration (core side)

| Var | Read at | Default | Effect |
|---|---|---|---|
| `FLOWFILE_KERNEL_IMAGE` | `kernel/manager.py:64-68` via `_envvar_or_default` (`:48-55` — **empty string counts as unset**, deliberately, because compose writes `${VAR:-}` as `""`) | `edwardvaneechoud/flowfile-kernel-base:0.4.0` (manager.py:38) | Legacy/base-flavour image override; read at lookup time, not import. Docs (`docs/users/deployment/docker.md`) still say `0.3.0` — stale, code is `0.4.0`. |
| `FLOWFILE_KERNEL_IMAGE_BASE` / `_ML` / `_LITE` | manager.py:65-76 | base/ml `...:0.4.0` (manager.py:38-39); lite `edwardvaneechoud/flowfile-kernel-lite:0.4.0` | Per-flavour pins; `_BASE` wins over the legacy `FLOWFILE_KERNEL_IMAGE`. |
| `FLOWFILE_DOCKER_NETWORK` | manager.py:393 | auto-detected (`_detect_docker_network`) | Docker-in-Docker network that kernel containers join. |
| `FLOWFILE_CORE_URL` | manager.py:1216 (core writes it into the kernel's env); `kernel_runtime/flowfile_client.py:219` (kernel reads it) | DinD: `http://flowfile-core:63578`; local: `http://host.docker.internal:63578` | How a kernel container dials core back. |

### 3.8 Kernel-container contract vars (set by core in `_build_kernel_env`, `manager.py:1200-1258`; read inside `kernel_runtime`)

These are **container-internal contract vars** — an operator normally never sets them by hand; core sets them per-kernel at launch.

| Var | Set at (core) | Read at (kernel) | Kernel-side default | Effect |
|---|---|---|---|---|
| `KERNEL_PACKAGES` | manager.py:1208 (always `""` — packages are pre-baked into the derived image) | `kernel_runtime/entrypoint.sh` | `""` | pip-install loop at container boot (constraints-pinned). |
| `KERNEL_CONSTRAINTS_FILE` | Dockerfile `ENV`, `kernel_runtime/Dockerfile:84` | `entrypoint.sh` | `/opt/constraints.txt` | pip constraint file for entrypoint installs. |
| `FLOWFILE_CORE_URL` | manager.py:1210-1217 | `flowfile_client.py:219` | `http://host.docker.internal:63578` | Core API endpoint. |
| `FLOWFILE_INTERNAL_TOKEN` | manager.py:1219-1230 (prefers `get_internal_token()`) | `flowfile_client.py:243` (fallback; per-request ctx token preferred) | none | `X-Internal-Token` header. |
| `FLOWFILE_KERNEL_ID` | manager.py:1232 | `flowfile_client.py:248,339` | none | `X-Kernel-Id` lineage/ownership header. |
| `FLOWFILE_HOST_SHARED_DIR` | manager.py:1238-1239 (only when bind-mounted, i.e. NOT named-volume DinD) | `flowfile_client.py:58` | unset | Host→container path translation for the shared dir. |
| `FLOWFILE_KERNEL_SHARED_DIR` | manager.py:1243 | `flowfile_client.py:598` | `/shared` | Shared-dir path as seen inside the container. |
| `FLOWFILE_HOST_CATALOG_TABLES_DIR` | manager.py:1250-1251 (local mode only) | `flowfile_client.py:52,999` | unset | Host catalog-tables path for translation. |
| `FLOWFILE_KERNEL_CATALOG_TABLES_DIR` | manager.py:1252 | `flowfile_client.py:989` | `/catalog_tables` | In-container catalog-tables mount. |
| `KERNEL_ID` | manager.py:1253 | `kernel_runtime/main.py:200` | `default` | Persistence sub-path key. |
| `PERSISTENCE_ENABLED` | manager.py:1254 | `main.py:198` | `true` (parses `1/true/yes`) | Artifact persistence on/off. |
| `PERSISTENCE_PATH` | manager.py:1255 | `main.py:199` | `/shared/artifacts` | Artifact persistence root. |
| `RECOVERY_MODE` | manager.py:1256 (`kernel.recovery_mode.value`) | `main.py:201` | `lazy` (enum `lazy\|eager\|clear`; `clear` is destructive) | Artifact recovery behavior at boot. |
| `PERSISTENCE_CLEANUP_HOURS` | not set by core (kernel default only) | `main.py:203` | `24` (float; `0` disables) | Startup GC of old artifacts. |
| `MAX_NAMESPACES` | not set by core | `main.py:76` | `20` | LRU cap on per-flow notebook namespaces. |
| `MAX_DISPLAY_OUTPUTS` | not set by core | `main.py:82` | `200` | LRU cap on stored display outputs. |
| `FLOWFILE_SHARED_PATH` | **nobody sets it** | `flowfile_client.py:221` (`_SHARED_PATH`) | `/shared` | **DEAD** — assigned once into a module var, never read again. |

### 3.9 Public flow-API tunables

| Var | Read at | Default | Effect |
|---|---|---|---|
| `FLOWFILE_API_RUN_TIMEOUT_SECONDS` | `flowfile_core/flowfile_core/routes/flow_api.py:51` | `120` (float) | Timeout for one public-API-triggered flow run. |
| `FLOWFILE_API_MAX_CONCURRENT_RUNS` | `routes/flow_api.py:57` | `4` (int → `asyncio.Semaphore`) | Global cap on concurrent public-API runs; beyond cap ⇒ fast 503. |

### 3.10 Global-artifact storage backend

| Var | Read at | Default | Effect |
|---|---|---|---|
| `FLOWFILE_ARTIFACT_STORAGE` | `flowfile_core/flowfile_core/artifacts/__init__.py:64` | `filesystem` | `s3` switches to a presigned-URL `S3Storage` (`shared/artifact_storage.py`). |
| `FLOWFILE_S3_BUCKET` | `artifacts/__init__.py:69` | none — **raises `ValueError`** when backend is `s3` and this is unset | S3 bucket name. |
| `FLOWFILE_S3_PREFIX` | `artifacts/__init__.py:75` | `global_artifacts/` | Key prefix. |
| `FLOWFILE_S3_REGION` | `artifacts/__init__.py:76` | `us-east-1` | Region. |
| `FLOWFILE_S3_ENDPOINT_URL` | `artifacts/__init__.py:77` | none | Custom endpoint (MinIO etc.). |

### 3.11 Project git-tracking secret placeholders

| Var | Read at | Effect |
|---|---|---|
| `FLOWFILE_SECRET_<NAME>` | `flowfile_core/flowfile_core/project/secrets_resolver.py:24-26` (`env_key`: `FLOWFILE_SECRET_` + name uppercased, non-alnum runs → `_`), `:44-48` (`resolve`: **env var wins over the project's `.env`**) | Supplies values for `${secret:NAME}` placeholders when importing a git project. Project-root `.env` (untracked) is the fallback. |

### 3.12 Google OAuth (GA connections)

| Var | Read at | Default | Effect |
|---|---|---|---|
| `GOOGLE_OAUTH_CLIENT_ID` / `GOOGLE_OAUTH_CLIENT_SECRET` | settings.py:163-164 | `""` | Fallback OAuth app credentials when no per-user DB secret row exists (`configs/app_settings.py:69-70`: `get_user_secret(...) or GOOGLE_OAUTH_CLIENT_ID`). |
| `GOOGLE_OAUTH_REDIRECT_URI` | settings.py:165-168 | `http://localhost:{SERVER_PORT}/ga_connections/oauth/callback` | OAuth redirect fallback. |

### 3.13 Sidecar / desktop shell (Rust — vars *written* for the Python children)

`build_child_env(core_port, worker_port)` in `flowfile_frontend/src-tauri/src/env.rs:10-96` starts from the shell's environment and **overrides**: `HOME`, `TMPDIR`, `DOCKER_CONFIG` (=`~/.docker`), `FLOWFILE_STORAGE_DIR` (=`~/.flowfile`, pre-creating cache/temp/logs/system_logs/flows/database subdirs), `FLOWFILE_MODE=electron` (env.rs:52), `FLOWFILE_SUPERVISOR_PID` (=shell PID, env.rs:58-61), `FLOWFILE_WORKER_PORT`, `CORE_PORT`, `CORE_HOST=127.0.0.1`, `WORKER_HOST=127.0.0.1` (env.rs:67-72), `DOCKER_HOST` (`npipe:////./pipe/docker_engine` on Windows else `unix:///var/run/docker.sock`, env.rs:74-84), and prepends `/usr/local/bin:...` to `PATH` on Unix.

| Var | Read at | Effect |
|---|---|---|
| `FLOWFILE_SUPERVISOR_PID` | `shared/parent_watcher.py:32` | Presence-gated: enables a parent-death watcher thread in sidecars (polls `os.getppid()` every 1s; exits when reparented). Never set for CLI/Docker runs, so the watcher never fires there. |
| `FLOWFILE_TARGET_TRIPLE` | **compile-time** `env!()` in `src-tauri/src/sidecar/mod.rs:162`, emitted by `build.rs:8` `cargo:rustc-env` | Picks the `binaries/<name>-<triple>` sidecar filename. |

### 3.14 Frontend / WASM build & test-time (not backend config)

| Var | Read at | Effect |
|---|---|---|
| `VITE_FLOWFILE_UPDATER_ENABLED` | `src/renderer/app/composables/useDesktopUpdater.ts:18` (+ `import.meta.env.DEV` guard `:17`) | **Build-time** opt-in for the Tauri auto-updater; unset ⇒ updater dormant. |
| `NODE_ENV` | `src/renderer/config/environment.ts:12-18`; `.eslintrc.js:29-30` | Derives `ENV.isDevelopment/enableDevTools/...`; compose frontend sets `NODE_ENV=production` (compose:11). |
| `import.meta.env.MODE` / `BASE_URL` / `DEV` | `DocumentationView.vue:15`; `router/index.ts:149`; `useDesktopUpdater.ts:17` | Dev-mode doc links, hash-router base. |
| `CI` | `playwright.config.ts:9-10` | Retries/`forbidOnly` in E2E. |
| `TEST_URL` / `API_URL` | `tests/web-flow.spec.ts:23-24`; `tests/canvas-overlays.spec.ts:20-21` | Playwright targets; `Makefile:203` sets `TEST_URL=http://localhost:4173` for `make test_e2e`. |
| `BUILD_MODE` | `flowfile_wasm/vite.config.ts:5` (`'lib'`) | WASM lib-vs-app build; set by `package.json` `build:lib`. |
| `npm_package_version` | `flowfile_wasm/vite.config.ts:6` | Injected app version. |

### 3.15 Starlette-Config keys (settings.py:129-133 — env var OR `.env` in the process CWD)

| Key | Default | Status |
|---|---|---|
| `DEBUG` | `False` | **DEAD** — no consumer outside settings.py. |
| `FILE_LOCATION` | `".\\files\\"` | **DEAD** — no consumer. |
| `AVAILABLE_RAM` | `8` (GB, int) | Live — `flow_data_engine/utils.py:40` uses it to decide if an estimated frame fits in memory. |
| `FLOWFILE_WORKER_URL` | computed worker URL | Live — see §3.2. |

### 3.16 Test-infrastructure env (not runtime config, but easy to confuse with the above)

- `TEST_MODE` — `flowfile_worker/configs.py:18`: **presence-based** (`"TEST_MODE" in os.environ`) — `TEST_MODE=0` still enables it! Effect: `flowfile_worker/secrets.py:128` returns a static test master key instead of real key resolution. CI sets it in `.github/workflows/test-kafka-integration.yml:115` and `test-kernel-integration.yml:72`.
- `TESTING='True'` — set by `flowfile_core/tests/conftest.py:23`; see §3.4 for the shared-DB hazard.
- `FLOWFILE_WORKER_HOST` (conftest.py:53, default `0.0.0.0`), `FLOWFILE_STARTUP_TIMEOUT` (`:56`, 30s), `FLOWFILE_SHUTDOWN_TIMEOUT` (`:58`, 15s), `SKIP_WORKER_TESTS` (`:226`, `== "1"`) — test-only. **`FLOWFILE_WORKER_HOST` is NOT read by package source** (which uses `WORKER_HOST`) — near-identical name, different variable, easy to set the wrong one.
- `test_utils/` Docker fixtures: `TEST_POSTGRES_{HOST,PORT,USER,PASSWORD,DB,SCHEMA,IMAGE,CONTAINER,STARTUP_TIMEOUT,SHUTDOWN_TIMEOUT}`, `TEST_MYSQL_*` (same shape + `ROOT_PASSWORD`), `TEST_MINIO_{HOST,PORT,CONSOLE_PORT,ACCESS_KEY,SECRET_KEY,CONTAINER}`, `TEST_GCS_{HOST,PORT,CONTAINER}`, `TEST_AZURITE_{HOST,BLOB_PORT,CONTAINER}`, `TEST_REDPANDA_{HOST,PORT,IMAGE,CONTAINER}`, `KEEP_{MINIO,GCS,AZURITE,REDPANDA}_RUNNING`, `CI`, `GITHUB_ACTIONS`.
- Tests also export `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_ENDPOINT_URL`/`AWS_REGION`/`AWS_ALLOW_HTTP`/`AWS_SESSION_TOKEN` for object-store SDKs. **Package source never reads `AWS_*` env vars** — runtime cloud creds always come from `CloudStorageConnection` DB rows, never ambient AWS env.

### 3.17 CI-only runtime env (across all 12 workflows — only 3 hits)

`TEST_MODE: "1"` (kafka + kernel integration workflows); `FLOWFILE_INTERNAL_TOKEN: ${{ github.run_id }}-test-token` (`test-kernel-integration.yml:74`); `FLOWFILE_KERNEL_IMAGE: flowfile-kernel-base:test` (`test-kernel-integration.yml:76`).

### 3.18 Interpreter hygiene (Dockerfiles, not app config)

`PYTHONPATH=/app`, `PYTHONDONTWRITEBYTECODE=1`, `PYTHONUNBUFFERED=1` in `flowfile_core/Dockerfile:69-72`, `flowfile_worker/Dockerfile:61-64`, `kernel_runtime/Dockerfile:80-84` (+ `docker-compose.yml:46-47,75-76`).

---

## 4. `MutableBool` — flipping a feature flag without restart

`flowfile_core/flowfile_core/configs/utils.py` defines `MutableBool`: a dataclass wrapping `value: bool` with `__bool__`, `&`/`|` combinators, int/float coercion, and `.set(value)`. `settings.py` exports six instances: `SINGLE_FILE_MODE`, `OFFLOAD_TO_WORKER`, `FEATURE_FLAG_AI`, `FLOWFILE_LSP_ENABLED`, `FLOWFILE_AI_LOG_PROMPTS`, `FLOWFILE_AI_LOG_PROMPTS_SCRUB`. Every call site does `bool(_settings.FEATURE_FLAG_AI)` **at call time**, so `.set()` takes effect immediately, process-wide, with no reload — but only in **that** process (see caveat below).

**`FEATURE_FLAG_AI` live flip:**
- Gate: `flowfile_core/flowfile_core/ai/feature_flag.py` — `is_ai_enabled()` reads the `MutableBool` per call; `require_ai_enabled()` is a router-level `Depends` raising 503 with `"AI features are disabled. Set FEATURE_FLAG_AI=true to enable."`.
- Flip: `POST /system/feature_flags/ai` (`ai/admin_routes.py:58-78`, admin-only, mounted under `/system` — deliberately **outside** the gated `/ai` router so it stays callable while the gate is off). Sets **both** `settings.FEATURE_FLAG_AI.set(enabled)` **and** `os.environ["FEATURE_FLAG_AI"]` (admin_routes.py:76-77). Response always has `persisted: false` — surviving a restart is the operator's `.env`'s job, not this endpoint's. `GET /system/feature_flags/ai` reads current state (also admin-only).

**`FLOWFILE_LSP_ENABLED` live flip:** identical pattern in `flowfile_core/flowfile_core/lsp/admin_routes.py` — `POST`/`GET /system/feature_flags/lsp` (admin_routes.py:49,60). Difference: `/lsp/*` never 503s when off — it degrades to empty-200 so the editor falls back to client-side completion. Caveat in the module docstring: open editor sessions cache `/lsp/capabilities` per session, so a live flip only reaches them on reload.

**Other live-mutable state (not admin-endpoint-backed):**
- `OFFLOAD_TO_WORKER.set(False)` is forced by `python -m flowfile_core.main --run-flow` (per `flowfile_core/CLAUDE.md`), and `flowfile/web/__init__.py:157` sets `OFFLOAD_TO_WORKER.value = True` in unified (pip) mode.
- **`import flowfile` mutates `os.environ` unconditionally**: `flowfile/flowfile/__init__.py:17-18` sets `FLOWFILE_WORKER_PORT=63578` and `FLOWFILE_SINGLE_FILE_MODE=1` at import time, before any of your code runs. This is how the pip-installed unified mode co-hosts the worker on the core port. Consequence: any process that does `import flowfile` (even just to reach `flowfile_frame`) silently flips core into single-file mode if `flowfile_core.configs.settings` is imported afterward. Import order matters.

**Multi-process caveat:** in a multi-worker deployment (e.g. `gunicorn -w N`), a flip via HTTP hits **one** worker process only (inferred from the per-process nature of `MutableBool`; `.env.example` documents the identical caveat for AI rate limits — §3.6, per-process in-memory deques).

---

## 5. Parsing quirks — truthy rules are NOT uniform

This is the single most common source of "I set the var and nothing changed" bugs. There are **five different truthy rules** in play:

| Rule | Applies to |
|---|---|
| `true`/`1`/`yes`/`on` (case/whitespace-insensitive) | `FEATURE_FLAG_AI`, `FLOWFILE_LSP_ENABLED`, `FLOWFILE_AI_LOG_PROMPTS`, `FLOWFILE_AI_LOG_PROMPTS_SCRUB`, `FLOWFILE_ENABLE_PROJECTS` |
| `true`/`1`/`yes` — **no `on`** | `FLOWFILE_SCHEDULER_ENABLED` — `FLOWFILE_SCHEDULER_ENABLED=on` silently does nothing. |
| exact string `"1"` only | `FLOWFILE_SINGLE_FILE_MODE`, `FLOWFILE_OFFLOAD_TO_WORKER` — `=true` or `=yes` silently does nothing. |
| presence-only (value ignored) | `TEST_MODE` (`TEST_MODE=0` still enables it!), `FLOWFILE_SKIP_STARTUP_MIGRATION`, `FLOWFILE_SUPERVISOR_PID`. |
| exact string `"True"` (capitalized) | `TESTING` — `TESTING=true` (lowercase) does **not** match `== "True"`. |

Corollary gotchas:
- `_envvar_or_default` (kernel image vars, `manager.py:48-55`) treats an **empty string as unset**, deliberately, because Compose's `${VAR:-}` interpolation writes `""` for an unset host var. Don't "fix" this to treat `""` as a real override.
- `AVAILABLE_RAM`, `DEBUG` go through Starlette's `cast=bool`/`cast=int`, not the hand-rolled truthy checks above — different failure mode (raises on unparseable input rather than silently falling to default).

---

## 6. Drift report (both directions)

### Documented but missing or stale
1. **`docker-remote/` does not exist.** Root `CLAUDE.md`'s Repository Structure table lists a `docker-remote/` directory ("Compose stack using published Docker Hub images"). Verified empty on both `git log --all -- docker-remote` and `git ls-files | grep -i docker-remote` — it never existed in tracked history. The only compose file in the repo is root `docker-compose.yml`.
2. **`shared/crypto` listed in root `CLAUDE.md`'s Repository Structure** but `git ls-files -- shared/crypto` is empty; on disk it holds only `__pycache__`. `shared/CLAUDE.md` itself already flags this.
3. **`docs/users/deployment/docker.md` pins kernel image `0.3.0`** while code defaults are `0.4.0` (`kernel/manager.py:38-39` and the lite default).
4. `.env.example:57` calls the `/project` router "admin-only" in docker mode — unverified nuance (the router-level permission dependency itself is out of this skill's scope); treat as a flag, not a confirmed fact.

### Read in code but dead
5. **`TEMP_DIR` env var** — `settings.get_temp_dir()` (settings.py:86-92) has zero callers repo-wide.
6. **`FLOWFILE_SHARED_PATH`** — `kernel_runtime/flowfile_client.py:221` assigns `_SHARED_PATH`; never referenced again.
7. **`DEBUG`** and **`FILE_LOCATION`** Starlette-Config keys — read into module constants with zero consumers.

### Read in code but absent from BOTH `.env.example` and root `CLAUDE.md`
`FLOWFILE_LSP_ENABLED` (+ its admin flip endpoint) · `FLOWFILE_API_RUN_TIMEOUT_SECONDS` · `FLOWFILE_API_MAX_CONCURRENT_RUNS` · `FLOWFILE_ARTIFACT_STORAGE` / `FLOWFILE_S3_BUCKET` / `_PREFIX` / `_REGION` / `_ENDPOINT_URL` · `FLOWFILE_LOCAL_MODEL_CTX` · `FLOWFILE_DB_READ_HEDGE_DELAY` · `FLOWFILE_DOCKER_NETWORK` · `FLOWFILE_DB_PATH` / `TESTING` / `FLOWFILE_SKIP_STARTUP_MIGRATION` · `FLOWFILE_HOST` / `FLOWFILE_PORT` / `FLOWFILE_MODULE_NAME` / `FORCE_POETRY` / `POETRY_PATH` · `SECURE_STORAGE_PATH` · `GOOGLE_OAUTH_CLIENT_ID` / `_CLIENT_SECRET` / `_REDIRECT_URI` · `FLOWFILE_SECRET_*` placeholder prefix · `AVAILABLE_RAM` (Starlette key, live) · `FLOWFILE_INTERNAL_SERVICE_USER_ID` (documented only in `docs/for-developers/kernel-architecture.md`) · most of the kernel-container contract vars in §3.8 (`PERSISTENCE_*`, `RECOVERY_MODE`, `KERNEL_ID`, `MAX_NAMESPACES`, `MAX_DISPLAY_OUTPUTS`, `KERNEL_PACKAGES`, `KERNEL_CONSTRAINTS_FILE`, the `FLOWFILE_HOST_*`/`FLOWFILE_KERNEL_*` path-translation quartet — some appear in `docs/for-developers/kernel-architecture.md`, most don't anywhere).

### `.env.example` ↔ `docker-compose.yml` divergences (trap-shaped, not bugs)
- `.env.example` has **no** `FLOWFILE_SCHEDULER_ENABLED` line at all; compose hard-codes `true`; `docs/users/deployment/docker.md` says default is `false`. Net effect: local/pip runs never start the scheduler unless you set it yourself; a fresh `.env` copied from `.env.example` for a **non-compose** docker run also won't start it.
- `FLOWFILE_ENABLE_PROJECTS`: env-level code default is off (settings.py:50), but compose defaults it **on** (compose:34) and `.env.example:62` ships `true`. Read the compose file, not just the code default, before assuming projects are off.
- Compose passes empty strings for unset kernel-image vars (`${VAR:-}`). This is safe only because `_envvar_or_default` treats empty as unset (§5) — copying that `${VAR:-}` pattern for a var that does NOT special-case empty (e.g. `FLOWFILE_MASTER_KEY`, compose:31) yields an empty-string env that the consuming code must handle explicitly (it does, via a falsy check in `secrets.py`, but this is not free — verify before reusing the pattern for a new var).

---

## 7. Add-a-config-axis checklist

When you add a new environment-variable-driven behavior:

1. **Read it in exactly one place if possible.** If it must be read per-call (like `sharing_enabled()` or the catalog-storage getters), say so in a comment — the import-time-vs-per-call split is the #1 source of "I set it but nothing changed" bugs in this codebase (§2, §6).
2. **Pick a truthy rule deliberately and be consistent with a sibling flag**, not with "whatever felt natural." Prefer the `true/1/yes/on` rule used by the `MutableBool` flags (§3.3) unless you have a specific reason (e.g. `FLOWFILE_SINGLE_FILE_MODE`'s exact-`"1"` rule exists to avoid ambiguity in a mode-switch, not by accident). Document the rule inline — this table exists because five different rules already crept in un-intentionally (§5).
3. **Give it a real default in code**, and decide whether an empty string counts as "set" (compose's `${VAR:-}` idiom means it usually shouldn't — mirror `_envvar_or_default`, manager.py:48-55, if the var can be Compose-templated).
4. **Add a line to `.env.example`** (repo root, the only one in the repo) with a one-line comment; if the semantics are non-obvious (like `FLOWFILE_CATALOG_STORAGE_URI`'s creation-time-only caveat), write the caveat there, not just in code.
5. **Add it to root `CLAUDE.md`'s Environment Variables table** if it's operator-facing; if it's a container-internal contract var like the ones in §3.8, a one-line mention in `docs/for-developers/kernel-architecture.md` is enough — don't bloat the root table with vars nobody sets by hand.
6. **Wire it into `docker-compose.yml`** if docker deployments need a non-default value, and make sure the compose default doesn't silently diverge from the code default (§6 has three live examples of exactly this drift — don't add a fourth).
7. **Add or extend a test** that asserts the default behavior AND at least one non-default value takes effect. If the var is read in the worker or a kernel container, a subprocess/container-level test (not just a unit test of the getter) is the only way to catch the "set at core, but the string mismatches at the read site" class of bug.
8. **If it's a live-flippable flag**, follow the `MutableBool` pattern (§4): mount the admin endpoint under `/system`, not the feature's own gated router, set both the `MutableBool` and `os.environ`, and return `persisted: false` honestly — do not silently write back to a `.env` file.
9. **Decide whether the frontend needs to know** (maintainer guidance, 2026-07-03: many config axes must be frontend-accessible — plan the exposure path when you add the var, not after). The renderer cannot read backend env: in Docker it is static files behind nginx, and `import.meta.env` is baked at **build** time — fine for build concerns, wrong for anything an operator sets at deploy time. If the var changes UI-visible behavior, expose it through a backend response and make the UI *react*. The two existing patterns to copy: the `mode` field the backend returns from `FLOWFILE_MODE` (consumed in `flowfile_frontend/src/renderer/app/services/auth.service.ts`), and AI availability signaled by the `/ai/*` 503 detail string (`AI_DISABLED_DETAIL` check in `app/api/ai.api.ts`). Never mint a `VITE_*` var for deployment config.

---

## 8. Quick reference: minimal env per deployment

- **Desktop (Tauri)**: nothing — `env.rs` supplies everything (mode, ports, storage, supervisor PID).
- **Local dev** (`poetry run flowfile_core` + `poetry run flowfile_worker`): nothing required; defaults to electron mode, `~/.flowfile`, ports 63578/63579. Optional: `FLOWFILE_SCHEDULER_ENABLED=1` (note: the `=on` spelling silently does nothing, §5).
- **pip unified** (`flowfile run ui`): nothing; `import flowfile`'s side effect sets single-file mode; serves the UI at `:63578/ui` (host/port hard-checked to `127.0.0.1:63578`, `flowfile/web/__init__.py:154-157`).
- **Docker**: required `JWT_SECRET_KEY`, `FLOWFILE_INTERNAL_TOKEN`, `FLOWFILE_MASTER_KEY` (or the Docker secret file); recommended `FLOWFILE_ADMIN_USER`/`_PASSWORD`; compose wires `WORKER_HOST`, `CORE_HOST`, storage dirs, scheduler, projects, and kernel-image overrides for you — check compose defaults against §6 before assuming `.env.example` alone tells the whole story.

**Classification rule (maintainer, 2026-07-03):** split every var into *installation-specific* (paths, hosts, ports — defaulted per machine or wired by `env.rs`/compose) vs *deployment config* (secrets, tokens, mode, admin bootstrap, feature gates). Everything in the second group is required-or-relevant **for hosting**: a server deployment must consciously set them even though local/desktop runs never touch them. `.env.example` is the hosting operator's checklist — which is why the undocumented-var drift in §6 is a hosting hazard, not a docs nit.

---

## Provenance and maintenance

All facts above were spot-verified by reading the cited files at their cited lines against the repo as of **2026-07-03 (v0.12.7)**, on a clean checkout. Re-run these before trusting any file:line in this document after a refactor:

```bash
# Re-derive the full env-var read list (definitive checklist; compare unique names against §3)
grep -rhoE 'environ(\.get)?\[?\(? ?"[A-Z_0-9]+"|getenv\("[A-Z_0-9]+"' . --include="*.py" -r \
  | grep -oE '"[A-Z_0-9]+"' | tr -d '"' | sort -u

# Re-check every occurrence with file:line (use this to re-verify a specific var's line numbers)
grep -rn --include="*.py" -E "os\.environ|os\.getenv|environ\.get|getenv\(" . \
  --exclude-dir=node_modules --exclude-dir=target --exclude-dir=dist --exclude-dir=.venv --exclude-dir=.git

# TS/JS/Vue env reads (frontend + wasm)
grep -rn --include="*.ts" --include="*.js" --include="*.vue" --include="*.mjs" \
  -E "import\.meta\.env|process\.env" flowfile_frontend flowfile_wasm | grep -v node_modules

# Rust env reads (Tauri sidecar)
grep -rn -E "env::|std::env|option_env!|env!\(" flowfile_frontend/src-tauri/src

# FLOWFILE_MODE default-stamping sites (should be exactly 4 Python + 1 Rust + Dockerfiles/compose)
grep -rn 'FLOWFILE_MODE.*=.*"electron"\|FLOWFILE_MODE.*not in os.environ' \
  flowfile_core flowfile flowfile_frontend/src-tauri/src --include="*.py" --include="*.rs"

# MutableBool truthy-parse sites (confirms §3.3 / §5 truthy rules haven't drifted)
grep -n "MutableBool\|strip().lower() in" flowfile_core/flowfile_core/configs/settings.py

# Admin flip endpoints still mounted under /system, not under the gated router
grep -n "feature_flags" flowfile_core/flowfile_core/ai/admin_routes.py flowfile_core/flowfile_core/lsp/admin_routes.py

# Kernel image defaults + empty-string-is-unset guard
grep -n "_KERNEL_IMAGE.*DEFAULT\|_envvar_or_default" flowfile_core/flowfile_core/kernel/manager.py

# Compose vs .env.example divergence re-check (scheduler / projects)
grep -n "FLOWFILE_SCHEDULER_ENABLED\|FLOWFILE_ENABLE_PROJECTS" docker-compose.yml .env.example

# Drift: docker-remote / shared/crypto never tracked
git log --all --oneline -- docker-remote; git ls-files | grep -i docker-remote
git ls-files -- shared/crypto

# Kernel image version doc-drift re-check
grep -n "flowfile-kernel-base:0\." docs/users/deployment/docker.md flowfile_core/flowfile_core/kernel/manager.py
```

If a grep above turns up a line-number shift but the same variable name and semantics, just fix the file:line in this document — don't re-litigate the fact. If a variable disappears entirely, move its row to a "removed" note rather than silently deleting history from this table (this is the config record for the whole monorepo; treat entries as append-mostly).
