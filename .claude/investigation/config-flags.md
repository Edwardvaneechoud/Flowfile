# Discovery dossier — KEY=config-flags

> **FROZEN EVIDENCE** — snapshot at commit `f6963c77` (2026-07-03, v0.12.7); deliberately unmaintained and expected to drift.
> Authority order: **live repo → `.claude/skills/` → this file (leads only — re-verify before citing).** See [`README.md`](./README.md).

## Exhaustive configuration catalog for the Flowfile monorepo

Everything below was verified 2026-07-03 against the working tree at
`Flowfile` (branch `improvement/improve-naming-unnamed-flows`,
clean) by reading files and running read-only greps. Items I could not fully verify are marked **inferred**.

Verification commands actually run (all read-only):

```bash
# Full Python env-read sweep (265 raw hits incl. tests)
grep -rn --include="*.py" -E "os\.environ|os\.getenv|environ\.get|getenv\(" . \
  --exclude-dir=node_modules --exclude-dir=target --exclude-dir=dist --exclude-dir=.venv --exclude-dir=.git

# Unique env-var names read anywhere in Python (definitive checklist)
grep -rhoE 'environ(\.get)?\[?\(? ?"[A-Z_0-9]+"|getenv\("[A-Z_0-9]+"' . --include="*.py" -r \
  | grep -oE '"[A-Z_0-9]+"' | tr -d '"' | sort -u

# TS/JS env reads
grep -rn --include="*.ts" --include="*.js" --include="*.vue" --include="*.mjs" \
  -E "import\.meta\.env|process\.env" flowfile_frontend flowfile_wasm | grep -v node_modules

# Rust env
grep -rn -E "env::|std::env|option_env!|env!\(" flowfile_frontend/src-tauri/src

# Drift check: docker-remote/ never existed
git log --all --oneline -- docker-remote   # (empty)
git ls-files | grep -i docker-remote        # (empty)

# shared/crypto is untracked pycache only
git ls-files -- shared/crypto               # (empty); ls shows only __pycache__
```

---

## 1. The three config layers

1. **Env vars** read via `os.environ` / `os.getenv` (the vast majority).
2. **Starlette `Config(".env")`** — `flowfile_core/flowfile_core/configs/settings.py:129`
   `config = Config(".env")`. Reads the **process CWD's** `.env` file, with real env vars
   taking precedence (Starlette semantics). Only 4 keys go through it: `DEBUG`,
   `FILE_LOCATION`, `AVAILABLE_RAM`, `FLOWFILE_WORKER_URL` (settings.py:130–133).
   Gotcha: which `.env` is read depends on where you launch core from.
3. **Runtime-mutable flags** (`MutableBool`, `flowfile_core/flowfile_core/configs/utils.py`) —
   env var seeds the boot value; admin endpoints and any Python call site can flip
   `.set(bool)` live without restart. See §4.

---

## 2. FLOWFILE_MODE — semantics per value

Declared: `flowfile_core/flowfile_core/configs/settings.py:136-138`
`FLOWFILE_MODE = os.getenv("FLOWFILE_MODE", "electron")` with helpers
`is_docker_mode()` / `is_electron_mode()` / `is_package_mode()` (settings.py:141-153).

**Where the default is stamped into `os.environ` (import-time side effect, three places + CLI):**
- `flowfile_core/flowfile_core/__init__.py:12-13` — `if "FLOWFILE_MODE" not in os.environ: os.environ["FLOWFILE_MODE"] = "electron"`
- `flowfile_core/flowfile_core/configs/__init__.py:8-9` — same
- `flowfile_core/flowfile_core/main.py:53-54` — same
- `flowfile/flowfile/web/__init__.py:146-147` — same (in `start_server`, i.e. `flowfile run ui`)
- Tauri shell: `flowfile_frontend/src-tauri/src/env.rs:52` — hard-sets `FLOWFILE_MODE=electron` for sidecars
- Docker: `flowfile_core/Dockerfile:70` and `flowfile_worker/Dockerfile:62` bake `FLOWFILE_MODE=docker`; `docker-compose.yml:24,69` set it too.

**Caching split (load-bearing gotcha):** `settings.FLOWFILE_MODE` is captured **at import**
(settings.py:138), but several call sites re-read `os.environ` **per call**:
`auth/sharing.py:76` (`sharing_enabled()` — root CLAUDE.md documents this deliberately),
`shared/storage_config.py:26-28`, `kernel/manager.py:325`, `routes/file_manager.py:36`,
`routes/secrets.py:97,139,180`, `auth/jwt.py:44,61,97,142,238`, `auth/secrets.py:19-25,180,210`,
`flowfile_worker/secrets.py:131`, `database/init_db.py:55`, `flowfile/api.py:375`.
Changing the env var after import changes *some* behavior but not `settings.FLOWFILE_MODE`-derived constants.

| Value | Who sets it | Behavior |
|---|---|---|
| `electron` (default) | unset → stamped at import; Tauri env.rs | Single-user desktop. `local_user` (id=1) auto-auth in JWT (`auth/jwt.py:97-102`), JWT secret auto-generated + persisted via `SecureStorage` (`auth/jwt.py:61-66`), master key auto-generated + stored (`auth/secrets.py:210-224` `get_master_key`), `FLOWFILE_INTERNAL_TOKEN` auto-generated if absent (`auth/jwt.py:44-46`), secrets routes force `user_id=1` (`routes/secrets.py:97`), sharing routers 404 (`sharing_enabled()` false), file-manager routes 403 (`routes/file_manager.py:36`), storage under `~/.flowfile`, user data = `$HOME`. |
| `docker` | Dockerfiles / compose | Multi-user. **Startup fails without `JWT_SECRET_KEY`** (`auth/jwt.py:68-70`), master key required via `FLOWFILE_MASTER_KEY` env or `/run/secrets/flowfile_master_key` (`auth/secrets.py:140-169,210`), `FLOWFILE_INTERNAL_TOKEN` required (`auth/jwt.py:47-51`), admin seeded from `FLOWFILE_ADMIN_USER`/`_PASSWORD` (`database/init_db.py:55-65`), storage `/app/internal_storage` + `/data/user` (compose overrides user data to `/app/user_data`), file manager enabled, sharing enabled, `/project` router 404 unless `FLOWFILE_ENABLE_PROJECTS` truthy. |
| `package` | **nothing in the repo sets it** — user/operator sets manually (verified: only compose/Dockerfile set `docker`; only import-time defaults set `electron`) | `is_package_mode()` true; because it's ≠ electron, sharing is **enabled** (`sharing.py:76`) and JWT/master-key follow the non-electron path (i.e. `JWT_SECRET_KEY` required, SecureStorage path falls to `SECURE_STORAGE_PATH` default `/tmp/.flowfile` — `auth/secrets.py:25`). Projects always on (`routes/public.py:49` `projects_enabled=(not is_docker_mode()) or flag`). |
| `tauri` (fallback string only) | never set | `routes/public.py:45` `mode = os.environ.get("FLOWFILE_MODE", "tauri")` — a **different default** than everywhere else. In practice unreachable through normal startup because importing `flowfile_core` stamps `electron` first; the frontend treats `electron`\|`tauri`\|`desktop` as synonyms (comment at public.py:42-44, env.rs:47-52). |

---

## 3. MASTER TABLE — every env var read in package source

Legend for "Documented": `CLAUDE.md` = root CLAUDE.md; `.env.example` = repo root (the only
`.env.example` in the repo — verified with `find`); `docs` = mkdocs site (`docs/…`); `pkg CLAUDE` =
a package-level CLAUDE.md; `UNDOC` = none of those.

### 3.1 Mode / auth / secrets

| Variable | Read at (file:line) | Default | Effect | Mode | Documented |
|---|---|---|---|---|---|
| `FLOWFILE_MODE` | settings.py:138; ~20 per-call sites (§2) | `electron` (stamped at import); `tauri` fallback in routes/public.py:45 only | runtime mode; gates auth/secrets/sharing/storage/file-manager | all | CLAUDE.md, docs/users/deployment/docker.md:90 |
| `JWT_SECRET_KEY` | flowfile_core/flowfile_core/auth/jwt.py:68 | none — raises in non-electron mode | JWT signing secret | docker/package | CLAUDE.md, .env.example:28, docs docker.md:93 |
| `FLOWFILE_MASTER_KEY` | flowfile_core/flowfile_core/auth/secrets.py:150; flowfile_worker/flowfile_worker/secrets.py:90 | none; fallback file `/run/secrets/flowfile_master_key` (auth/secrets.py:159); env wins; validated as Fernet key, quotes stripped | Fernet master key for all user secrets; worker must share it with core | docker (electron auto-generates via SecureStorage) | CLAUDE.md, .env.example:14, docs docker.md:94 |
| `FLOWFILE_INTERNAL_TOKEN` | auth/jwt.py:42; kernel/manager.py:1227; kernel_runtime/flowfile_client.py:243 | auto-generated in electron (jwt.py:44-46, written back to os.environ); required error otherwise (jwt.py:47-51) | kernel→core service auth (`X-Internal-Token`) | docker required; electron auto | CLAUDE.md, .env.example:36, docs |
| `FLOWFILE_INTERNAL_SERVICE_USER_ID` | auth/jwt.py:318 | `1` | user id attributed to `_internal_service` principal when kernel owner can't be resolved | docker | docs (kernel-architecture.md); NOT in CLAUDE.md/.env.example |
| `FLOWFILE_ADMIN_USER` / `FLOWFILE_ADMIN_PASSWORD` | database/init_db.py:58-59 | none → warning, no admin created | seed initial admin account | docker only (init_db.py:55 guard) | CLAUDE.md, .env.example:20-21, docs |
| `SECURE_STORAGE_PATH` | auth/secrets.py:25 | `/tmp/.flowfile` | SecureStorage dir in **non-electron** modes (Fernet-encrypted json files + `.secret_key`) | docker/package | **UNDOC** |
| `APPDATA` | auth/secrets.py:22; flowfile_worker/secrets.py:31 | `~/.config` fallback | Windows app-data root for electron SecureStorage (`<APPDATA>/flowfile`) | electron | UNDOC (OS var) |

### 3.2 Ports & service discovery

| Variable | Read at | Default | Effect | Mode | Documented |
|---|---|---|---|---|---|
| `FLOWFILE_WORKER_PORT` | settings.py:105,125 | `63579` (`DEFAULT_WORKER_PORT` settings.py:16); CLI `--worker-port` arg wins | port core dials the worker on | all | CLAUDE.md ("Local-dev tunables") |
| `WORKER_HOST` | settings.py:102,127 | `0.0.0.0` non-Windows / `127.0.0.1` Windows | host core dials the worker on | all; compose=`flowfile-worker` (docker-compose.yml:35); Tauri=`127.0.0.1` (env.rs:72) | CLAUDE.md |
| `FLOWFILE_WORKER_URL` | settings.py:133 (starlette Config: env or CWD `.env`) | `get_default_worker_url(WORKER_PORT)` — `http://{WORKER_HOST}:{port}` + `/worker` suffix iff SINGLE_FILE_MODE (settings.py:95-117) | full worker URL override; consumed as `WORKER_URL` throughout `flow_data_engine/subprocess_operations/subprocess_operations.py` | all | CLAUDE.md |
| `CORE_HOST` / `CORE_PORT` | flowfile_worker/flowfile_worker/configs.py:16-17 | `0.0.0.0`/`127.0.0.1`(win), `63578` | where the worker (and its spawned children) call core back | all; compose sets `CORE_HOST=flowfile-core` (compose:70); Tauri sets both (env.rs:68-69) | CLAUDE.md (as `CORE_HOST`) |
| `FLOWFILE_HOST` / `FLOWFILE_PORT` | flowfile/flowfile/api.py:22-23 | `127.0.0.1` / `63578` | where the `flowfile` CLI/api client probes & spawns core | package/CLI | **UNDOC** |
| `FLOWFILE_MODULE_NAME` | flowfile/flowfile/api.py:25 | `flowfile` | module the CLI launches as server | CLI | **UNDOC** |
| `FORCE_POETRY` / `POETRY_PATH` / `POETRY_ACTIVE` / `VIRTUAL_ENV` | flowfile/flowfile/api.py:26-27,103-106 | ``/`poetry`/``/`` | how `flowfile.api` decides to spawn the server via poetry vs venv python | CLI dev | **UNDOC** |
| CLI args | settings.py:71-83 `parse_args` (`--host --port --worker-port`, `parse_known_args`); flowfile_worker/configs.py:23-45 (`--host --port --core-host --core-port`) | server `0.0.0.0:63578`; worker `63579` | args override env | all | pkg CLAUDE (core) |

Port facts (non-env): Tauri scans a free `(core, worker)` pair starting 63578
(`src-tauri/src/sidecar/mod.rs:66-77,118-136` passes `--port`/worker port args) and injects
`window.__FLOWFILE_PORTS__` (renderer: `src/renderer/config/constants.ts`, `lib/desktop.ts`).
Kernels: host ports 19000–19999 (`kernel/manager.py` `_BASE_PORT = 19000`, `_PORT_RANGE = 1000`),
container-internal 9999 (`kernel_runtime/Dockerfile` `EXPOSE 9999`).

### 3.3 Runtime feature flags (MutableBool — see §4)

| Variable | Read at | Default | Truthy parse | Effect | Documented |
|---|---|---|---|---|---|
| `FLOWFILE_SINGLE_FILE_MODE` | settings.py:19; flowfile/web/__init__.py:72-73 | `0` | **exact `"1"` only** | worker routes co-hosted on core under `/worker`; worker URL gets `/worker` suffix | CLAUDE.md |
| `FLOWFILE_OFFLOAD_TO_WORKER` | settings.py:22 | `1` | **exact `"1"` only** | heavy compute → worker; off = in-core execution | CLAUDE.md |
| `FEATURE_FLAG_AI` | settings.py:25-27 | on | `true/1/yes/on` (ci) | master gate for `/ai/*` (503 when off, `ai/feature_flag.py`) | CLAUDE.md, .env.example:50 |
| `FLOWFILE_LSP_ENABLED` | settings.py:32-34 | on | `true/1/yes/on` | notebook Jedi/LSP bridge; off ⇒ `/lsp/*` degrade to empty 200 (never 503) | **UNDOC** (in-code docstrings only) |
| `FLOWFILE_AI_LOG_PROMPTS` | settings.py:37-39 | off | `true/1/yes/on` | JSONL prompt log at `<base>/ai_prompts/YYYY-MM-DD.jsonl` | CLAUDE.md, .env.example:123 |
| `FLOWFILE_AI_LOG_PROMPTS_SCRUB` | settings.py:42-44 | off | `true/1/yes/on` | PII-scrub user/tool messages in the prompt log | CLAUDE.md, .env.example:129 |
| `FLOWFILE_ENABLE_PROJECTS` | settings.py:49-51 | off (env); compose defaults **true** (compose:34) | `true/1/yes/on` | `/project/*` git-tracking router in docker mode (404 when off); always on outside docker (public.py:49) | CLAUDE.md, .env.example:62, docs docker.md:96 |
| `FLOWFILE_SCHEDULER_ENABLED` | flowfile_core/flowfile_core/main.py:73 | off; compose sets `true` (compose:32) | **`true/1/yes` — NO `on`** (differs from the others!) | auto-start embedded `FlowScheduler` in core's lifespan | CLAUDE.md, docs docker.md:95 |

### 3.4 Storage & database

| Variable | Read at | Default | Effect | Documented |
|---|---|---|---|---|
| `FLOWFILE_STORAGE_DIR` | shared/storage_config.py:44 (docker), :46 (local) | docker `/app/internal_storage`; local `~/.flowfile` | internal root (`base_directory`): cache, temp, logs, DB, template_data… Tauri env.rs:44 sets it to `~/.flowfile` explicitly | CLAUDE.md |
| `FLOWFILE_USER_DATA_DIR` | shared/storage_config.py:59 | docker `/data/user`; local `$HOME` (env ignored locally!) | user-data root: flows, uploads, outputs, catalog_tables, notebooks in docker mode | CLAUDE.md (compose sets `/app/user_data`, compose:37) |
| `FLOWFILE_SHARED_DIR` | shared/storage_config.py:157,171,243 | `<base>/temp/kernel_shared` | core↔worker↔kernel exchange dir + `global_artifacts` + `artifact_staging` subpaths; must stay Docker-visible | shared/CLAUDE.md |
| `FLOWFILE_DB_PATH` | shared/storage_config.py:410,427 | none | explicit SQLite path override (wins over TESTING); also disables legacy-DB migration lookup | user MEMORY.md only — **not** in CLAUDE.md/.env.example |
| `TESTING` | shared/storage_config.py:414,430 | none; conftest sets `'True'` (flowfile_core/tests/conftest.py:23) | `== "True"` ⇒ DB becomes `<base>/temp/test_flowfile_catalog.db` (one shared file per machine — concurrent pytest sessions clobber each other; see MEMORY.md incident) | MEMORY.md only |
| `FLOWFILE_SKIP_STARTUP_MIGRATION` | flowfile_core/flowfile_core/database/init_db.py:26 | unset | any value ⇒ skip Alembic startup migration on import (needed for diagnostics/alembic CLI; importing flowfile_core otherwise migrates the live DB) | MEMORY.md only |
| `FLOWFILE_DB_READ_HEDGE_DELAY` | shared/db_reader.py:25 | `8` (seconds, float) | delay before hedged SQLAlchemy read races connectorx | **UNDOC** |
| `TEMP_DIR` | settings.py:88 (`get_temp_dir`) | `tempfile.gettempdir()` | **DEAD** — `get_temp_dir()` has zero callers repo-wide (verified). The `TEMP_DIR` module constant (settings.py:134) is `storage.temp_directory`, unrelated to the env var | UNDOC + dead |

### 3.5 Catalog object storage

| Variable | Read at | Default | Effect | Documented |
|---|---|---|---|---|
| `FLOWFILE_CATALOG_STORAGE_URI` | settings.py:54-60 (**read per call**, not cached) | none | creation-time default storage root (e.g. `s3://bucket/catalog`) for new catalogs | CLAUDE.md, .env.example:83 (long semantics comment: NOT a live override; seeded "General" catalog stays local) |
| `FLOWFILE_CATALOG_STORAGE_CONNECTION` | settings.py:63-68 (per call) | none | name of `CloudStorageConnection` for credentials; required when URI set | CLAUDE.md, .env.example:84 |

### 3.6 AI subsystem

| Variable | Read at | Default | Effect | Documented |
|---|---|---|---|---|
| `FLOWFILE_AI_<PROVIDER>_RPM` / `_RPD` | ai/scheduler.py:296-297 templates + `provider.upper()` (:324); parsed by `_read_int_env` (:244-263 — non-int / ≤0 logged + ignored) | unset = no enforcement | soft per-provider request budgets, per worker process (in-memory deques). Providers: `ANTHROPIC OPENAI GOOGLE GROQ OPENROUTER`; `ollama` is in `_UNLIMITED_PROVIDERS` (scheduler.py:268) — never limited | CLAUDE.md, .env.example:101-110 |
| `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GEMINI_API_KEY`/`GOOGLE_API_KEY`, `GROQ_API_KEY`, `OPENROUTER_API_KEY` | ai/byok.py:52-63 map, :85 `detect_env_fallback`; litellm reads them itself when no BYOK row | unset | provider fallback when no BYOK credential row saved | .env.example:48 (mentioned), CLAUDE.md |
| `LITELLM_LOCAL_MODEL_COST_MAP` | ai/__init__.py:31 (**setdefault "True"** — a write, before any lazy litellm import) | `True` | keeps litellm from fetching its cost map over the network | core CLAUDE.md |
| `FLOWFILE_LOCAL_MODEL_CTX` | ai/local_model/manager.py:110 | `16384` | **first-run seed only** for local llama.cpp ctx window (UI-persisted sidecar wins after); clamped 2048–32768 (:113-114) | **UNDOC** |

Feature flag plumbing: see §4.

### 3.7 Kernel orchestration (core side)

| Variable | Read at | Default | Effect | Documented |
|---|---|---|---|---|
| `FLOWFILE_KERNEL_IMAGE` | kernel/manager.py:64-68 (via `_envvar_or_default` :48-55 — **empty string == unset**, deliberately, because compose `${VAR:-}` writes empty strings) | `edwardvaneechoud/flowfile-kernel-base:0.4.0` (manager.py:38) | legacy/base-flavour image override; read at lookup time, not import | CLAUDE.md, compose:42, docs docker.md:99 (**docs say 0.3.0 — stale**) |
| `FLOWFILE_KERNEL_IMAGE_BASE` / `_ML` / `_LITE` | manager.py:66,71-75 | base/ml `…:0.4.0` (manager.py:38-39), lite `edwardvaneechoud/flowfile-kernel-lite:0.4.0` (manager.py:~310) | per-flavour pins; `_BASE` wins over legacy `FLOWFILE_KERNEL_IMAGE` | CLAUDE.md, compose:43-45 |
| `FLOWFILE_DOCKER_NETWORK` | manager.py:393 | auto-detected (`_detect_docker_network`) | Docker-in-Docker network kernels join | **UNDOC** |
| `FLOWFILE_CORE_URL` | manager.py:1216 (core building kernel env); kernel_runtime/flowfile_client.py:219 (kernel reading it) | DinD: `http://flowfile-core:63578`; local: `http://host.docker.internal:63578` | how kernels dial core back | docs (kernel-architecture.md); not in root CLAUDE.md |

### 3.8 Kernel-container env (set by core in `_build_kernel_env`, manager.py:1200-1258; read inside `kernel_runtime`)

All of these are **container-internal contract vars** — an operator normally never sets them; core does.

| Variable | Set at (core) | Read at (kernel) | Default in kernel | Effect |
|---|---|---|---|---|
| `KERNEL_PACKAGES` | manager.py:1208 (always `""` — packages pre-baked into derived image) | kernel_runtime/entrypoint.sh | `""` | pip-install loop at container boot (constraints-pinned) |
| `KERNEL_CONSTRAINTS_FILE` | (Dockerfile ENV) kernel_runtime/Dockerfile:84 | entrypoint.sh | `/opt/constraints.txt` | pip constraint file for entrypoint installs |
| `FLOWFILE_CORE_URL` | manager.py:1210-1217 | flowfile_client.py:219 | `http://host.docker.internal:63578` | core API endpoint |
| `FLOWFILE_INTERNAL_TOKEN` | manager.py:1219-1230 (prefers `get_internal_token()`) | flowfile_client.py:243 (fallback; per-request ctx token preferred) | none | `X-Internal-Token` header |
| `FLOWFILE_KERNEL_ID` | manager.py:1232 | flowfile_client.py:248,339 | none | `X-Kernel-Id` lineage/ownership header |
| `FLOWFILE_HOST_SHARED_DIR` | manager.py:1238-1239 (only when bind-mount, i.e. NOT named-volume DinD) | flowfile_client.py:58 | unset | host→container path translation for shared dir |
| `FLOWFILE_KERNEL_SHARED_DIR` | manager.py:1243 | flowfile_client.py:598 | `/shared` | shared dir path as seen in-container |
| `FLOWFILE_HOST_CATALOG_TABLES_DIR` | manager.py:1250-1251 (local mode only) | flowfile_client.py:52,999 | unset | host catalog-tables path for translation |
| `FLOWFILE_KERNEL_CATALOG_TABLES_DIR` | manager.py:1252 | flowfile_client.py:989 | `/catalog_tables` | in-container catalog tables mount |
| `KERNEL_ID` | manager.py:1253 | kernel_runtime/main.py:200 | `default` | persistence sub-path key |
| `PERSISTENCE_ENABLED` | manager.py:1254 | main.py:198 | `true` (parse `1/true/yes`) | artifact persistence on/off |
| `PERSISTENCE_PATH` | manager.py:1255 | main.py:199 | `/shared/artifacts` | artifact persistence root |
| `RECOVERY_MODE` | manager.py:1256 (`kernel.recovery_mode.value`) | main.py:201 | `lazy` (enum `lazy|eager|clear`, kernel/models.py:15-18; `clear` is destructive) | artifact recovery behavior at boot |
| `PERSISTENCE_CLEANUP_HOURS` | not set by core (kernel default) | main.py:203 | `24` (float; `0` disables) | startup GC of old artifacts |
| `MAX_NAMESPACES` | not set by core | main.py:76 | `20` | LRU cap on per-flow notebook namespaces |
| `MAX_DISPLAY_OUTPUTS` | not set by core | main.py:82 | `200` | LRU cap on stored display outputs |
| `FLOWFILE_SHARED_PATH` | **nobody** | flowfile_client.py:221 (`_SHARED_PATH`) | `/shared` | **DEAD — read once into `_SHARED_PATH`, never used anywhere** (verified single reference) |

### 3.9 Public flow-API tunables

| Variable | Read at | Default | Effect | Documented |
|---|---|---|---|---|
| `FLOWFILE_API_RUN_TIMEOUT_SECONDS` | flowfile_core/flowfile_core/routes/flow_api.py:51 | `120` (float) | timeout for one public API-triggered flow run | **UNDOC** |
| `FLOWFILE_API_MAX_CONCURRENT_RUNS` | routes/flow_api.py:57 | `4` (int → `asyncio.Semaphore`) | global cap on concurrent public API runs; beyond cap = fast 503 | **UNDOC** |

### 3.10 Global-artifact storage backend

| Variable | Read at | Default | Effect | Documented |
|---|---|---|---|---|
| `FLOWFILE_ARTIFACT_STORAGE` | flowfile_core/flowfile_core/artifacts/__init__.py:64 | `filesystem` | `s3` switches to presigned-URL `S3Storage` (shared/artifact_storage.py) | **UNDOC** |
| `FLOWFILE_S3_BUCKET` | artifacts/__init__.py:69 | none — **raises ValueError** when backend is s3 and unset | S3 bucket | **UNDOC** |
| `FLOWFILE_S3_PREFIX` | artifacts/__init__.py:75 | `global_artifacts/` | key prefix | **UNDOC** |
| `FLOWFILE_S3_REGION` | artifacts/__init__.py:76 | `us-east-1` | region | **UNDOC** |
| `FLOWFILE_S3_ENDPOINT_URL` | artifacts/__init__.py:77 | none | custom endpoint (MinIO etc.) | **UNDOC** |

### 3.11 Project git-tracking secret placeholders

| Variable | Read at | Effect |
|---|---|---|
| `FLOWFILE_SECRET_<NAME>` | flowfile_core/flowfile_core/project/secrets_resolver.py:24-26 (`env_key`: `FLOWFILE_SECRET_` + name uppercased, non-alnum runs → `_`), :44-48 (`resolve`: **env var wins over project `.env`**) | supplies values for `${secret:NAME}` placeholders when importing a project; project-root `.env` (untracked) is the fallback (:29-41) |

### 3.12 Google OAuth (GA connections)

| Variable | Read at | Default | Effect | Documented |
|---|---|---|---|---|
| `GOOGLE_OAUTH_CLIENT_ID` / `GOOGLE_OAUTH_CLIENT_SECRET` | settings.py:163-164 | `""` | fallback OAuth app creds when no per-user DB secret rows exist (`configs/app_settings.py:69-70`: `get_user_secret(...) or GOOGLE_OAUTH_CLIENT_ID`) | **UNDOC** |
| `GOOGLE_OAUTH_REDIRECT_URI` | settings.py:165-168 | `http://localhost:{SERVER_PORT}/ga_connections/oauth/callback` | OAuth redirect fallback (app_settings.py:71) | **UNDOC** |

### 3.13 Sidecar / desktop shell (Rust, `src-tauri/src/env.rs` — vars **written** for the Python children)

`build_child_env(core_port, worker_port)` (env.rs:10-96) starts from the shell's env and overrides:
`HOME`, `TMPDIR`, `DOCKER_CONFIG` (=`~/.docker`), `FLOWFILE_STORAGE_DIR` (=`~/.flowfile`, pre-creating
cache/temp/logs/system_logs/flows/database subdirs), `FLOWFILE_MODE=electron` (env.rs:52, with the
comment explaining the backend hard-codes electron checks), `FLOWFILE_SUPERVISOR_PID` (=shell PID,
env.rs:58-61), `FLOWFILE_WORKER_PORT`, `CORE_PORT`, `CORE_HOST=127.0.0.1`, `WORKER_HOST=127.0.0.1`
(env.rs:67-72), `DOCKER_HOST` (`npipe:////./pipe/docker_engine` on Windows else
`unix:///var/run/docker.sock`, env.rs:74-84), and prepends `/usr/local/bin:...` to `PATH` on Unix.

| Variable | Read at | Effect |
|---|---|---|
| `FLOWFILE_SUPERVISOR_PID` | shared/parent_watcher.py:32 | presence-gated: enables the parent-death watcher thread in sidecars (poll `os.getppid()` each 1s; exit when reparented). Never set for CLI/Docker runs so the watcher never fires there | frontend CLAUDE.md gotchas |
| `FLOWFILE_TARGET_TRIPLE` | **compile-time** `env!()` in src-tauri/src/sidecar/mod.rs:162; emitted by build.rs:8 `cargo:rustc-env` | picks the `binaries/<name>-<triple>` sidecar filename |

### 3.14 Frontend / WASM build & test-time (not backend config)

| Variable | Read at | Effect |
|---|---|---|
| `VITE_FLOWFILE_UPDATER_ENABLED` | flowfile_frontend/src/renderer/app/composables/useDesktopUpdater.ts:18 (+ `import.meta.env.DEV` guard :17) | **build-time** opt-in for the Tauri auto-updater; unset ⇒ updater dormant |
| `NODE_ENV` | src/renderer/config/environment.ts:12-18; .eslintrc.js:29-30 | derives `ENV.isDevelopment/enableDevTools/...`; compose frontend sets `NODE_ENV=production` (compose:11) |
| `import.meta.env.MODE` / `BASE_URL` / `DEV` | DocumentationView.vue:15; router/index.ts:149; useDesktopUpdater.ts:17 | dev-mode doc links, hash-router base |
| `CI` | playwright.config.ts:9-10 | retries/forbidOnly in E2E |
| `TEST_URL` / `API_URL` | tests/web-flow.spec.ts:23-24; tests/canvas-overlays.spec.ts:20-21 | Playwright targets; `Makefile:203` sets `TEST_URL=http://localhost:4173` for `make test_e2e` |
| `BUILD_MODE` | flowfile_wasm/vite.config.ts:5 (`'lib'`); set by `package.json` `build:lib` | WASM lib-vs-app build |
| `npm_package_version` | flowfile_wasm/vite.config.ts:6 | injected app version |

### 3.15 Starlette-Config keys (settings.py:129-133; env var OR `.env` in process CWD)

| Key | Default | Status |
|---|---|---|
| `DEBUG` | `False` | **DEAD** — no consumer outside settings.py (verified grep) |
| `FILE_LOCATION` | `".\\files\\"` | **DEAD** — no consumer (verified) |
| `AVAILABLE_RAM` | `8` (GB, int) | used in flow_data_engine/utils.py:40 to decide if an estimated frame fits memory |
| `FLOWFILE_WORKER_URL` | computed worker URL | live (see §3.2) |

### 3.16 Test infrastructure env (read in tests/test_utils only — not runtime config, but skill-relevant)

- `TEST_MODE` — flowfile_worker/configs.py:18: **presence-based** (`"TEST_MODE" in os.environ`) — `TEST_MODE=0` still enables it! Effect: `flowfile_worker/secrets.py:128` returns a static test master key instead of real key resolution. Set by CI: `.github/workflows/test-kafka-integration.yml:115`, `test-kernel-integration.yml:72`.
- `TESTING='True'` — set by `flowfile_core/tests/conftest.py:23`; consumed by shared/storage_config.py:414 (shared per-machine test DB → the phantom "no such table" cascade from MEMORY.md; use `FLOWFILE_DB_PATH` per session to isolate).
- `FLOWFILE_WORKER_HOST` (conftest.py:53, default `0.0.0.0`), `FLOWFILE_STARTUP_TIMEOUT` (:56, 30s), `FLOWFILE_SHUTDOWN_TIMEOUT` (:58, 15s), `SKIP_WORKER_TESTS` (:226, `== "1"`) — **test-only**; note `FLOWFILE_WORKER_HOST` is NOT read by package source (source uses `WORKER_HOST`) — easy to confuse.
- `test_utils/` Docker fixtures: `TEST_POSTGRES_{HOST,PORT,USER,PASSWORD,DB,SCHEMA,IMAGE,CONTAINER,STARTUP_TIMEOUT,SHUTDOWN_TIMEOUT}`, `TEST_MYSQL_*` (same shape + `ROOT_PASSWORD`), `TEST_MINIO_{HOST,PORT,CONSOLE_PORT,ACCESS_KEY,SECRET_KEY,CONTAINER}`, `TEST_GCS_{HOST,PORT,CONTAINER}`, `TEST_AZURITE_{HOST,BLOB_PORT,CONTAINER}`, `TEST_REDPANDA_{HOST,PORT,IMAGE,CONTAINER}`, `KEEP_{MINIO,GCS,AZURITE,REDPANDA}_RUNNING`, `CI`, `GITHUB_ACTIONS`.
- Tests also export `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_ENDPOINT_URL`/`AWS_REGION`/`AWS_ALLOW_HTTP`/`AWS_SESSION_TOKEN` for object-store SDKs (e.g. flowfile_core/tests/flowfile/flowfile_table/test_flow_data_engine_cloud.py:39-48, test_utils/s3/data_generator.py:239-242). **Package source never reads AWS_* env vars** — runtime cloud creds come from CloudStorageConnections.

### 3.17 CI workflows (only 3 runtime-relevant env vars appear across all 12 workflows — verified grep)

- `TEST_MODE: "1"` (kafka + kernel integration workflows)
- `FLOWFILE_INTERNAL_TOKEN: ${{ github.run_id }}-test-token` (test-kernel-integration.yml:74)
- `FLOWFILE_KERNEL_IMAGE: flowfile-kernel-base:test` (test-kernel-integration.yml:76)

### 3.18 Interpreter hygiene vars (Dockerfiles)

`PYTHONPATH=/app`, `PYTHONDONTWRITEBYTECODE=1`, `PYTHONUNBUFFERED=1` in
flowfile_core/Dockerfile:69-72, flowfile_worker/Dockerfile:61-64, kernel_runtime/Dockerfile:80-84
(+ compose:46-47,75-76). Not app config.

---

## 4. Runtime feature flags that are NOT plain env vars

### `MutableBool` (flowfile_core/flowfile_core/configs/utils.py)
A dataclass wrapping `value: bool` with `__bool__`, `&`/`|` combinators, int/float coercion and
`.set(value)`. Settings export six instances (§3.3). Because call sites do `bool(_settings.FEATURE_FLAG_AI)`
**at call time**, `.set()` takes effect immediately, process-wide, no reload.

### FEATURE_FLAG_AI live flip
- Gate: `flowfile_core/flowfile_core/ai/feature_flag.py` — `is_ai_enabled()` reads the MutableBool per
  call; `require_ai_enabled()` is a router-level `Depends` raising 503 with
  `"AI features are disabled. Set FEATURE_FLAG_AI=true to enable."`.
- Flip: `POST /system/feature_flags/ai` (`ai/admin_routes.py:58-78`, admin-only,
  mounted under `/system` — deliberately OUTSIDE the gated `/ai` router so it stays callable when off).
  It sets BOTH `settings.FEATURE_FLAG_AI.set(enabled)` AND `os.environ["FEATURE_FLAG_AI"]`
  (admin_routes.py:76-77). Response always `persisted: false` — restart persistence is the user's
  `.env`'s job. `GET /system/feature_flags/ai` reads it (also admin-only).

### FLOWFILE_LSP_ENABLED live flip
Identical pattern: `flowfile_core/flowfile_core/lsp/admin_routes.py` — `POST/GET
/system/feature_flags/lsp`, sets MutableBool + os.environ. Difference: `/lsp/*` never 503s when off —
degrades to empty-200 so editors fall back to client-side completion. Caveat in module docstring: open
editors cache `/lsp/capabilities` per session; a live flip reaches them only on reload.

### Other live-mutable state
- `OFFLOAD_TO_WORKER.set(False)` is forced by `python -m flowfile_core.main --run-flow` (per core
  CLAUDE.md) and `flowfile/web/__init__.py:161` sets `OFFLOAD_TO_WORKER.value = True` in unified mode.
- `importing flowfile` (the CLI package) **mutates os.environ**: `flowfile/flowfile/__init__.py:17-18`
  sets `FLOWFILE_WORKER_PORT=63578` and `FLOWFILE_SINGLE_FILE_MODE=1` unconditionally at import time.
  This is how the pip-installed unified mode co-hosts the worker on the core port. Consequence: any
  process that `import flowfile` (even just for `flowfile_frame`-style usage via the `flowfile`
  namespace) silently flips core into single-file mode if core settings are imported afterwards.

---

## 5. Drift report (both directions)

### Documented but missing / stale
1. **`docker-remote/` does not exist.** Root CLAUDE.md's Repository Structure lists
   `docker-remote/ # Compose stack using published Docker Hub images`. Verified:
   `git log --all -- docker-remote` and `git ls-files | grep docker-remote` are both empty — the
   directory never existed in tracked history. The only compose file is root `docker-compose.yml`.
2. **`shared/crypto` listed in root CLAUDE.md structure** (`shared/ # …storage_config, crypto, …`) but
   `git ls-files -- shared/crypto` is empty; on disk it holds only `__pycache__`. shared/CLAUDE.md
   itself already warns about this.
3. **docs/users/deployment/docker.md:99,163,172 pin kernel image `0.3.0`** while code defaults are
   `0.4.0` (kernel/manager.py:38-39 and the lite default near :310).
4. `.env.example:57` says the `/project` router is "**admin-only**" in docker mode — **inferred**
   nuance: I did not verify the admin-only dependency on the router itself (out of scope); flag for
   the routes/permissions dimension.

### Read in code but dead
5. **`TEMP_DIR`** — `settings.get_temp_dir()` (settings.py:86-92) has no callers anywhere.
6. **`FLOWFILE_SHARED_PATH`** — kernel_runtime/flowfile_client.py:221 assigns `_SHARED_PATH`; that
   name is never referenced again (single grep hit).
7. **`DEBUG`** and **`FILE_LOCATION`** starlette-Config keys — read into module constants with zero
   consumers.

### Read in code but absent from BOTH `.env.example` and root CLAUDE.md (UNDOCUMENTED runtime config)
- `FLOWFILE_LSP_ENABLED` (+ its `/system/feature_flags/lsp` flip endpoint)
- `FLOWFILE_API_RUN_TIMEOUT_SECONDS`, `FLOWFILE_API_MAX_CONCURRENT_RUNS`
- `FLOWFILE_ARTIFACT_STORAGE`, `FLOWFILE_S3_BUCKET`, `FLOWFILE_S3_PREFIX`, `FLOWFILE_S3_REGION`, `FLOWFILE_S3_ENDPOINT_URL`
- `FLOWFILE_LOCAL_MODEL_CTX`
- `FLOWFILE_DB_READ_HEDGE_DELAY`
- `FLOWFILE_DOCKER_NETWORK`
- `FLOWFILE_DB_PATH`, `TESTING`, `FLOWFILE_SKIP_STARTUP_MIGRATION` (only in the user's private MEMORY.md)
- `FLOWFILE_HOST`, `FLOWFILE_PORT`, `FLOWFILE_MODULE_NAME`, `FORCE_POETRY`, `POETRY_PATH`
- `SECURE_STORAGE_PATH`
- `GOOGLE_OAUTH_CLIENT_ID` / `GOOGLE_OAUTH_CLIENT_SECRET` / `GOOGLE_OAUTH_REDIRECT_URI`
- `FLOWFILE_SECRET_*` placeholder prefix
- `AVAILABLE_RAM` (starlette key, live)
- `FLOWFILE_INTERNAL_SERVICE_USER_ID` (documented only in docs/for-developers/kernel-architecture.md)
- Kernel-container contract vars (`PERSISTENCE_*`, `RECOVERY_MODE`, `KERNEL_ID`, `MAX_NAMESPACES`,
  `MAX_DISPLAY_OUTPUTS`, `KERNEL_PACKAGES`, `KERNEL_CONSTRAINTS_FILE`, the `FLOWFILE_HOST_/KERNEL_`
  path-translation quartet) — partially covered in docs/for-developers/kernel-architecture.md
  (FLOWFILE_CORE_URL, FLOWFILE_KERNEL_ID, FLOWFILE_HOST_SHARED_DIR, FLOWFILE_KERNEL_SHARED_DIR appear
  there per grep), rest undocumented.

### Documented-in-.env.example ↔ compose divergences (not bugs, but trap-shaped)
- `.env.example` has **no** `FLOWFILE_SCHEDULER_ENABLED` line; compose hard-codes `true` (compose:32)
  while docs docker.md:95 lists default `false`. Local/pip runs: scheduler never starts unless you set it.
- `FLOWFILE_ENABLE_PROJECTS`: env default off (settings.py:50) but compose defaults **on** (compose:34)
  and `.env.example:62` ships `true`.
- compose passes empty strings for unset kernel-image vars; only safe because `_envvar_or_default`
  treats empty as unset — copying that `${VAR:-}` pattern for OTHER vars (e.g. `FLOWFILE_MASTER_KEY`,
  compose:31) yields empty-string envs that Python code must (and in secrets.py does, via falsy check) handle.

---

## 6. Gotchas a skill must carry (with why)

1. **Truthy parsing is inconsistent by design-accident.** `true/1/yes/on` for
   FEATURE_FLAG_AI/LSP/AI_LOG/ENABLE_PROJECTS (settings.py:26,33,38,43,50);
   `true/1/yes` (no `on`) for FLOWFILE_SCHEDULER_ENABLED (main.py:73);
   exact `"1"` for FLOWFILE_SINGLE_FILE_MODE and FLOWFILE_OFFLOAD_TO_WORKER (settings.py:19,22);
   presence-only for TEST_MODE (worker configs.py:18) and FLOWFILE_SKIP_STARTUP_MIGRATION
   (init_db.py:26) and FLOWFILE_SUPERVISOR_PID (parent_watcher.py:32).
   `FLOWFILE_SCHEDULER_ENABLED=on` silently does nothing; `TEST_MODE=0` silently enables test mode.
2. **`import flowfile` rewrites your env** (flowfile/__init__.py:17-18): FLOWFILE_WORKER_PORT=63578 +
   FLOWFILE_SINGLE_FILE_MODE=1. Import order between `flowfile` and `flowfile_core.configs.settings`
   decides whether core boots in single-file mode.
3. **Importing `flowfile_core` has heavy side effects**: stamps FLOWFILE_MODE=electron, runs
   `validate_setup()`, auto-runs Alembic against the live catalog DB, seeds users
   (flowfile_core/__init__.py:1-16). For diagnostics set `FLOWFILE_SKIP_STARTUP_MIGRATION=1`
   (init_db.py:26) — already a MEMORY.md rule.
4. **Importing `shared.storage_config` mkdirs ~20 directories eagerly** (storage_config.py:248-277,
   singleton at :343). Env vars like FLOWFILE_STORAGE_DIR must be set BEFORE first import.
5. **settings.py caches, others re-read**: `FLOWFILE_MODE`, `WORKER_HOST`, `WORKER_PORT`,
   OAuth vars are import-time constants; `sharing_enabled()`, catalog-storage getters, kernel-image
   getters are per-call. Flipping env mid-process affects only the per-call group.
6. **Kernel image override treats empty as unset** (`_envvar_or_default`, manager.py:48-55) because
   compose `${VAR:-}` writes `""`. Don't "fix" that.
7. **The `.env` read by settings is CWD-relative** (`Config(".env")`, settings.py:129) — launching
   core from another directory silently drops FLOWFILE_WORKER_URL/AVAILABLE_RAM overrides.
8. **`FEATURE_FLAG_AI` / `FLOWFILE_LSP_ENABLED` flips are process-memory only** — admin endpoints
   return `persisted: false`; restart persistence requires the env var. In multi-worker (gunicorn -w N)
   deployments a flip via HTTP hits ONE worker process (**inferred** from per-process MutableBool;
   same caveat .env.example:95-99 documents for AI rate limits).
9. **AI rate limits are per-process** (in-memory deques, ai/scheduler.py) — aggregate = N × configured
   with N workers (.env.example:95-99 says this explicitly).
10. **`FLOWFILE_USER_DATA_DIR` is docker-only** — locally `user_data_directory` is always `Path.home()`
    (storage_config.py:58-63); setting the var outside docker does nothing.
11. **Master key precedence**: env `FLOWFILE_MASTER_KEY` (stripped of quotes, Fernet-validated) →
    `/run/secrets/flowfile_master_key` → error in docker (auth/secrets.py:140-169,210). In electron it's
    auto-generated into SecureStorage. Worker re-derives per-user keys independently — must share the
    same master key (flowfile_worker/secrets.py:90).
12. **`FLOWFILE_WORKER_HOST` (tests) ≠ `WORKER_HOST` (runtime)** — near-identical names, different readers.
13. **TESTING=True routes all sessions to ONE shared test DB** (`<base>/temp/test_flowfile_catalog.db`,
    storage_config.py:414) — concurrent pytest sessions clobber each other; isolate with FLOWFILE_DB_PATH.
14. **Windows changes defaults**: `WORKER_HOST`/`CORE_HOST`/worker bind default to 127.0.0.1 on Windows,
    0.0.0.0 elsewhere (settings.py:127, worker configs.py:13,16).
15. **`routes/public.py:45` defaults mode to `"tauri"`** unlike everywhere else (`electron`) —
    only reachable if FLOWFILE_MODE was somehow never stamped; frontend treats electron/tauri/desktop
    as synonyms. Don't "unify" it blindly; the comment explains the intent.
16. **`FLOWFILE_LOCAL_MODEL_CTX` only seeds the first-run default** — after the user touches the UI
    setting, a sidecar file wins (local_model/manager.py:106-110).

---

## 7. Historical incidents encoded in config code/docs

- **Compose empty-string kernel vars** → `_envvar_or_default` (manager.py:41-55): the docstring records
  that compose `${VAR:-}` used to inject `""` and core then tried `docker run ""`. Fix: empty==unset.
  Status: fixed, guarded by helper.
- **Shared test DB teardown cascade** (MEMORY.md): a concurrent pytest session's teardown dropped the
  single `TESTING` DB → phantom "no such table". Fix: per-session `FLOWFILE_DB_PATH`. Status: process
  rule, code unchanged (storage_config.py still routes TESTING to the shared temp file).
- **Alembic-on-import bit a diagnostic session** (MEMORY.md): importing flowfile_core migrated the live
  catalog DB. Fix: `FLOWFILE_SKIP_STARTUP_MIGRATION=1` escape hatch exists at init_db.py:26. Status: live.
- **Sidecars orphaned after hard-killed shell** → `FLOWFILE_SUPERVISOR_PID` + parent watcher
  (env.rs:54-61, shared/parent_watcher.py:32): presence of the var is the enable switch so CLI/Docker
  runs never self-reap. Status: shipped.
- **Worker URL built with 0.0.0.0 as connect target** (env.rs:70-72 comment): Tauri sets
  `WORKER_HOST=127.0.0.1` because core's default `http://0.0.0.0:<port>` is fragile to connect to.
  Status: worked around in desktop; the fragile default remains for bare `poetry run` (settings.py:112-115).
- **Legacy `codeql.yaml` removed** (root CLAUDE.md): failed weekly on a missing config file; replaced
  by GitHub default setup. Status: resolved (not a workflow file anymore).
- **docs pinned kernel `0.3.0` vs code `0.4.0`** — live doc drift, see §5.3.

---

## 8. Quick-reference: minimal env per deployment

- **Desktop (Tauri)**: nothing — env.rs supplies everything (mode, ports, storage, supervisor PID).
- **Local dev (`poetry run flowfile_core` + `poetry run flowfile_worker`)**: nothing required;
  defaults to electron mode, `~/.flowfile`, ports 63578/63579. Optional: `FLOWFILE_SCHEDULER_ENABLED=1`.
- **pip unified (`flowfile run ui`)**: nothing; import side effects set single-file mode; serves UI at
  `:63578/ui` (host/port hard-checked to 127.0.0.1:63578, web/__init__.py:154-157).
- **Docker**: required `JWT_SECRET_KEY`, `FLOWFILE_INTERNAL_TOKEN`, `FLOWFILE_MASTER_KEY` (or Docker
  secret), recommended `FLOWFILE_ADMIN_USER`/`_PASSWORD`; compose wires `WORKER_HOST`, `CORE_HOST`,
  storage dirs, scheduler, projects, kernel image overrides.
