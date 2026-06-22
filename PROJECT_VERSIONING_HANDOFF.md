# Git-Enabled Flowfile Project — Implementation Status & Frontend Handoff

**Status:** Phase 1 backend + CLI complete and tested. **No frontend exists yet.**
This doc is the context for the next development cycle (frontend + Phase 2 backend).

---

## 1. What this feature is (the product model)

A **Flowfile project** is a git-friendly folder that automatically mirrors this install's
**flows + connections + schedules** as deterministic, **secret-free** YAML. It lets users
version their pipeline environment, review changes via diff/PR, and rebuild on a fresh machine.

The load-bearing UX principle — **do not expose database mechanics**:

- **Projection (DB → files) is automatic and invisible.** Whenever a flow is saved, or a
  connection/schedule/secret is created/edited/deleted, the corresponding secret-free file is
  rewritten as a side-effect. The user never presses "export".
- **Import (files → DB) only happens at real boundaries**, surfaced as user-meaningful actions —
  "Open project" (fresh clone / first setup), later "Restore version" and "Files changed — reload?".
  Never an "apply to database" button.
- **Git is framed as "versions".** Files auto-sync continuously; a git **commit** is a deliberate
  **"Save version"** action with a message. Commits are NOT automatic.
- **Secrets are never committed.** Files carry `${secret:NAME}` placeholders. On a fresh machine,
  values are refilled from `FLOWFILE_SECRET_*` env / `.env`; anything missing becomes an empty
  **placeholder** so setup always completes — the user fills it in later.

---

## 2. What's built (backend + CLI) — DONE

### Python package `flowfile_core/flowfile_core/project/`
| File | Role |
|---|---|
| `service.py` | `project_sync` singleton: invisible-projection hooks (no-op when no project active; failures swallowed) + lifecycle (`init_project`, `open_project`, `save_version`, `close_project`, `has_external_changes`). |
| `projection.py` | DB→files writers (flows, db/cloud connections, schedules, secret manifest). |
| `importer.py` | Single idempotent files→DB path; placeholder-secret refill. |
| `normalize.py` | Canonical YAML dumper + `strip_volatile` + atomic write. |
| `manifest.py`, `secrets_resolver.py`, `git_ops.py` (GitPython), `repository.py`, `models.py` | manifest/layout, `${secret:}` env/.env resolution, git wrapper, DB-row access, dataclasses. |

### Hooks wired into the central save/store functions
`_save_flow_impl` (routes.py); db/cloud connection store/update/delete (db_connections.py); schedule
service create/update/delete (catalog/services/schedules.py); standalone-secret create/delete
(routes/secrets.py). Each lazy-imports `project_sync` and never breaks the primary operation.

### DB + REST + CLI
- `WorkspaceProject` model + Alembic migration **022** (auto-applied at startup).
- **`/project` REST router** (JWT-gated) — see §4.
- **CLI:** `flowfile project {init,open,save}`.
- `gitpython` added to `pyproject.toml` + `poetry.lock`.

### Tests — all green
`flowfile_core/tests/project/` (16 tests): determinism gate (project→import→project byte-identical),
secret-free output, env rebuild, placeholder-on-missing-secret, hook failure-isolation, no-op without
project, git "Save version" lifecycle. No regressions in existing connection/secret/schedule/handler tests.

### Project folder layout (for reference)
```
my-project/
├── project.yaml                 # { project_format, name, project_id, created_with_version }
├── .gitignore                   # generated; ignores .env, *.secret, *.db, data/, catalog_tables/
├── flows/<name>.flow.yaml       # normalized, secret-free (keyed by flow_uuid embedded inside)
├── connections/database/<name>.yaml   # ${secret:...} placeholders
├── connections/cloud/<name>.yaml
├── schedules/<flow_name>.yaml   # interval/cron only; runtime cursors stripped
└── secrets.yaml                 # { required_secrets: [...] } — NAMES of standalone secrets only
```

---

## 3. What is NOT built

- **Any frontend** (no Vue components, no API service, no store).
- **Phase 2 backend endpoints** the richer UI will need (history list, restore, reload, close,
  and a standalone-secret upsert) — see §6.
- GA/Kafka connection projection (hook pattern is identical; deferred).
- `table_trigger` / `table_set_trigger` schedules are intentionally **not** projected (they reference
  catalog table ids; only `interval`/`cron` are projected in Phase 1).
- Full re-projection does not prune files for resources deleted out-of-band (live delete hooks do).

---

## 4. REST API contract (what the frontend wires to TODAY)

Base prefix `/project`, all endpoints **JWT-gated**. Owner = `current_user.id`. The frontend's
configured axios instance (`src/renderer/app/services/axios.config`) already handles auth + base URL.

### `POST /project/init`
Create a new project at a folder (writes manifest + `.gitignore`, `git init`, projects current DB
state, makes the first commit) and activates it.
```jsonc
// request
{ "folder_path": "/abs/path/to/folder", "name": "Sales Analytics" }   // name optional → folder name
// 200
{ "project": { "id": 1, "name": "Sales Analytics", "folder_path": "/abs/path/to/folder" } }
```

### `POST /project/open`
Register + activate an existing project folder and **rebuild** the environment from its files
(synchronous; can take a moment for many flows). Returns counts + which secrets need values.
```jsonc
// request
{ "folder_path": "/abs/path/to/folder" }
// 200
{
  "project": { "id": 1, "name": "Sales Analytics", "folder_path": "/abs/path/to/folder" },
  "imported": { "flows": 4, "connections": 2, "schedules": 1 },
  "placeholder_secrets": ["prod_postgres", "marketing_ga_oauth_refresh_token"]
}
// 404 if the folder has no project.yaml  → { "detail": "No Flowfile project at ... (missing project.yaml)" }
```

### `GET /project/active`
Current active project + whether git changed the files out-of-band (the basis for a "reload?" banner).
```jsonc
// 200 (none active)
{ "project": null }
// 200 (active)
{ "project": { "id": 1, "name": "...", "folder_path": "..." }, "has_external_changes": false }
```

### `POST /project/versions`  — "Save version"
Re-projects current state, then commits. `sha` is `null` when nothing changed.
```jsonc
// request
{ "message": "Add monthly revenue schedule" }
// 200
{ "sha": "a1b2c3d4..." }            // or { "sha": null }
// 409 if no active project → { "detail": "No active project" }
```

---

## 5. Filling placeholder secrets — IMPORTANT nuance for the UI

After `open`, `placeholder_secrets` lists secret NAMES whose values weren't found in env/`.env`.
How the user fixes them depends on the secret's kind:

- **Connection passwords/keys** (most placeholders): the secret is owned by a connection. Fix by
  **editing that connection in the existing Connections UI and re-entering the credential** — this
  already works today via the existing `PUT` connection endpoints (no new backend needed). The
  placeholder name usually equals the connection name (db) or `<conn>_<field>` (cloud).
- **Standalone secrets** (created directly, used by REST/parameter nodes): there is currently **no
  upsert** endpoint. `POST /secrets/secrets` returns **400** if the name already exists, and the
  placeholder row already exists. So filling a standalone placeholder needs the Phase 2 endpoint in
  §6 (or a delete-then-create dance, which is not ideal). **Flag this; don't build a hacky path.**

A good Phase 1 UI: after `open`, show a non-blocking banner — "N secrets need values" — that deep-links
to the Connections screen (for connection placeholders) and notes standalone ones as pending.

---

## 6. Phase 2 backend endpoints to add BEFORE the richer UI

These are **not implemented**. The frontend history/restore/reload/close features depend on them.
The service-layer methods mostly exist already; these are thin route wrappers in
`flowfile_core/flowfile_core/routes/project.py`.

| Endpoint | Maps to | Notes |
|---|---|---|
| `GET /project/versions` | `git_ops.log(root, limit)` | returns `[{sha, message, committed_at}]` for the history list. |
| `POST /project/restore` `{commit_sha}` | `git_ops.restore` + `importer.import_project` | "Restore this version": checkout files → re-import → update head sha. Service helper not yet written. |
| `POST /project/reload` | `importer.import_project` | accept external git changes (after the `has_external_changes` banner). |
| `POST /project/close` | `project_sync.close_project(owner_id)` | method exists; just needs a route. |
| `POST /project/secrets` `{name, value}[]` | `store_secret` (upsert) | set/overwrite placeholder secret values (the §5 gap). |

---

## 7. Frontend work

### Phase 1 (minimal slice — pairs with the existing backend in §4)
1. **API service** `src/renderer/app/api/project.api.ts` — mirror `secrets.api.ts` / `catalog.api.ts`
   (static-method class, `import axios from "../services/axios.config"`). Methods: `init`, `open`,
   `getActive`, `saveVersion`. Base path `/project`.
2. **Types** in `src/renderer/app/types` — `ProjectInfo { id; name; folder_path }`,
   `OpenProjectResult { project; imported; placeholder_secrets }`, `ActiveProjectResult`.
3. **Pinia store** `src/renderer/app/stores/project-store.ts` (mirror `catalog-store.ts`) — holds
   active project, `hasExternalChanges`, last `placeholder_secrets`; actions wrap the API.
4. **UI surface** — a Project panel/section (folder picker → init/open; show active project +
   `has_external_changes`; a "Save version" button with a message box; a banner listing
   `placeholder_secrets` that links to Connections). Folder picking uses the existing file/folder
   picker pattern (see `fileManager.api.ts` / `fileBrowserStore.ts`).

### Phase 2 (richer — needs §6 endpoints first)
- **Version history** list + **"Restore this version"** dialog with an impact preview. Reuse the
  version-history table pattern from `views/CatalogView/TableDetailPanel.vue` /
  `RunHistoryTable.vue` (a parallel idea: git **definition** versions next to Delta **data** versions).
- **External-change banner** → "Reload" flow (`POST /project/reload`).
- **Secret-resolution panel** driven by `placeholder_secrets` (needs `POST /project/secrets`).
- **Close/switch project** action (`POST /project/close`).

---

## 8. Patterns & files to mirror (grounded references)

- API service shape: `flowfile_frontend/src/renderer/app/api/secrets.api.ts`,
  `.../api/catalog.api.ts` (axios from `../services/axios.config`; static-method classes; types from `../types`).
- Stores: `.../stores/catalog-store.ts`, `.../stores/sharing-store.ts` (Pinia, Composition).
- Version-history / detail-panel UI: `.../views/CatalogView/TableDetailPanel.vue`,
  `RunHistoryTable.vue`, `SchedulesPanel.vue`, `ScheduleDetailPanel.vue`.
- Existing connection/secret editors to deep-link for §5: the Connections + Secrets screens
  (`secrets.api.ts`, `cloud_connections`/db connection UIs).

---

## 9. Gotchas & edge cases

- **One active project per owner.** `init`/`open` deactivate the owner's other projects.
- **`open` is synchronous and rebuilds** — show a loading state; surface `imported` counts + placeholders.
- **`has_external_changes`** = live git HEAD ≠ last synced sha. Phase 1 can show a banner; the actual
  reload action is Phase 2.
- **Electron owner id:** routes use `current_user.id` (local user, typically id 1).
- **No project active ⇒ all projection is a no-op** — the feature is fully opt-in; nothing changes
  for users who never create a project.
- **GA/Kafka connections and table-trigger schedules are not yet projected** (see §3) — don't promise
  them in the UI yet.

---

## 10. How to try the backend now (no UI)

```bash
# CLI (uses the local user + ~/.flowfile DB)
flowfile project init  /path/to/my-project       # create + first commit
flowfile project save  "Add nightly schedule"    # commit a version
flowfile project open  /path/to/cloned-project   # rebuild on a fresh machine; prints placeholder secrets

# REST (JWT required)
curl -X POST :63578/project/init  -H 'Authorization: Bearer <jwt>' -d '{"folder_path":"/p","name":"Demo"}'
curl     -X GET  :63578/project/active -H 'Authorization: Bearer <jwt>'
curl -X POST :63578/project/versions -H 'Authorization: Bearer <jwt>' -d '{"message":"snapshot"}'
```

Approved design plan (more rationale): `~/.claude/plans/i-have-this-plan-refactored-whisper.md`.
