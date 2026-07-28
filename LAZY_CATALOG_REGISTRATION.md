# Lazy catalog registration — design guidance

**Status:** proposal / discussion doc. Nothing in here is implemented. The maintainer's
current lean is toward the lazy model (Option A below), with one open nuance —
draft auto-saves — that needs a deliberate decision before implementation starts.

**Audience:** whoever picks this up next (human or agent). All file references were
verified against the tree this document was committed with; line numbers drift, the
symbol names won't.

---

## 1. What happens today (eager auto-registration)

Every flow that comes into existence gets a permanent `FlowRegistration` row in the
catalog DB, immediately, whether or not the user asked for one:

- `POST /editor/create_flow` with no `namespace_id` → `catalog_helpers.auto_register_flow`
  → `CatalogService.auto_register_flow` → a row under `General > Unnamed Flows` (or
  `Local Flows`), keyed by `flow_path`.
- `GET /import_flow/` (`routes/routes.py`) registers **unconditionally** — merely opening
  an existing `.yaml` file creates a catalog row for it.
- `POST /editor/create_from_template` registers too — see the defect list below.

The registration is the naming authority (`display_name`), and four tables hang off it
with `nullable=False` foreign keys: `FlowSchedule`, `FlowApiEndpoint`, `GlobalArtifact`,
`CatalogTableReadLink`.

### Evidence that this is accreted, not designed

- **There is no working opt-out.** The backend accepts `register_in_catalog=False`, but
  since the removal of `openBlankFlow` (PR #611) no production caller passes it. The
  "Also register in catalog" checkbox in `CreateDialog.vue` / `SaveDialog.vue` only nulls
  `namespace_id` — and a null `namespace_id` is **exactly the input that routes into
  `auto_register_flow`**. Unchecking the box does not prevent registration; the label
  is wrong.
- **Silent failure mode.** `CatalogService.auto_register_flow` returns `None` and logs at
  INFO when the `General` root namespace is missing. No error, no self-heal. Result:
  a flow that exists on disk with no registration, and downstream code that assumes
  one. (This is what made the test suite flaky — see `flowfile_core/tests/conftest.py::
  restore_seeded_catalog_namespaces`.)
- **Deletion is not durable.** Deleting a registration has no tombstone
  (`routes/routes.py`, noted inline as a "separate follow-up"), so re-opening the same
  file quietly re-registers it. The user cannot make "no, I don't want this in my
  catalog" stick.
- **No garbage collection.** `shared/storage_config.py::cleanup_directories` sweeps only
  temp/cache/logs. `unnamed_flows/` and `python_editor_flows/` are created eagerly and
  never cleaned; scratch files and their registrations accumulate forever.
- **The frontend spends six mechanisms hiding what the backend created unasked:**
  `AUTO_COLLAPSE_NAMESPACES`, `sectionsDefaultExpanded`, `SYSTEM_NAMESPACE_NAMES`,
  `filterSelectableNamespaces`, the `hide-system-namespaces` toggle, and
  `writableSchemaNamespaces`. That much UI machinery dedicated to suppressing rows is
  the strongest signal the rows shouldn't exist yet.
- **Provenance:** the policy arrived as a mid-PR addendum to #285 and grew through five
  unrelated feature PRs. No design doc, no explicit decision.

## 2. The proposed direction: register lazily

**Rule: a `FlowRegistration` row is created at the moment of deliberate intent, never as
a side effect of existence.**

Deliberate intent means any of:

1. **Explicit save into the catalog** (`/save_flow_to_catalog`, the Save dialog with a
   namespace chosen).
2. **Publishing an API endpoint or creating a schedule** — these need the FK, and both
   already have lazy ensure-registration paths; keep those, they become the *only*
   registration paths besides save.
3. **A catalog-writer node or artifact publish** that needs `CatalogTableReadLink` /
   `GlobalArtifact` — same: ensure-on-demand.

What stops happening:

- `create_flow` without a namespace creates a scratch **file** only.
- `import_flow` (opening a `.yaml`) creates **nothing**. Browsing is read-only.
- Templates create a scratch file only.

Why this simplifies rather than complicates:

- The "Also register in catalog" checkbox becomes truthful for free.
- The six frontend hiding mechanisms shrink toward zero — nothing to hide.
- Deletion becomes durable without tombstones: a deleted registration stays deleted
  because nothing re-creates it behind the user's back.
- The recents-prune contract gets simpler. Today the designer prunes a stale recents
  entry by checking that the old registration survives with `file_exists=False`
  (`HeaderButtons.vue::recordRelocatedFlowAsRecent`). Under lazy registration a
  scratch flow has no registration to consult — but the frontend already *knows* the
  old and new paths at save time and can rewrite the recents entry directly. One
  cross-layer contract deleted.
- The silent-skip bug becomes unreachable from creation paths; the remaining explicit
  paths should **raise** (or self-heal by re-seeding `General`) instead of returning
  `None` — that fix is wanted regardless of direction.

## 3. The nuance: auto-saved drafts are genuinely useful

This is the part that needs discussion before anyone implements.

Two different values are currently fused into one mechanism:

1. **Durability of unsaved work.** Quick-create writes a real file to `unnamed_flows/`
   immediately. Close the app, crash, whatever — the work is on disk. This is good UX
   and worth keeping. Users hate losing an hour of canvas work to a crash.
2. **Membership in the catalog.** The same moment also creates a permanent catalog row.
   This is the part users never asked for and the part that causes the mess above.

The lazy proposal keeps (1) and drops (2): scratch files keep being written, they just
don't get catalog rows. But that raises the question the maintainer flagged directly:

> "Sometimes the auto saves are nice though. But they should also disappear after a
> while."

If drafts are no longer catalog rows, two things need an answer:

- **Where does a user find a draft again?** Today the answer is "in the catalog, under
  Unnamed Flows" (buried, hidden by default, but findable) plus the recents list.
  Under lazy registration the recents list becomes the *only* discovery surface unless
  we add one. A small "Drafts" section (recents-adjacent, sourced by listing
  `unnamed_flows/` with modification times) is probably enough — it reads the
  filesystem, no DB rows involved.
- **When do drafts die?** They should expire; unbounded accumulation is the current
  behavior and it's wrong under either model. Options, roughly in order of preference:
  - **Age-based TTL** on last-modified (e.g. 30 days), swept at core startup alongside
    `cleanup_directories`. Simple, predictable, matches "should disappear after a
    while" literally.
  - **Count cap** (keep the most recent N drafts). Predictable disk usage, but can
    delete a two-day-old draft just because the user quick-created a lot.
  - **Prompt on close** ("Keep draft / Discard / Save to catalog…"). Most explicit,
    most annoying; probably the wrong default for a quick-create flow whose whole
    point is zero ceremony.
  A TTL with a generous default (configurable via one env var, e.g.
  `FLOWFILE_DRAFT_TTL_DAYS`, `0` = never) plus the Drafts UI showing age ("expires in
  12 days") is the least-surprise combination: nothing is deleted that the UI didn't
  warn about, and opening or editing a draft resets its clock for free because the
  sweep keys on mtime.

### The honest counter-position (Option B)

Keep eager registration, but make the rows honest: add an `is_draft` flag (or a
dedicated draft state) to `FlowRegistration`, GC draft rows+files on TTL, and promote
draft → real on explicit save. This preserves "everything is visible in one catalog
tree" and keeps the `file_exists=False` prune contract intact.

Trade-off: it keeps *all* of today's machinery (hiding mechanisms, re-registration on
open, tombstone problem) and **adds** a state machine on top. Lazy registration deletes
a mechanism; Option B decorates it. Option B is only preferable if "the catalog is the
single inventory of every flow that exists" is a product goal in itself — that's the
question to settle in discussion, and it's a product question, not a technical one.

## 4. Constraints any implementation must respect

- **The four `nullable=False` FKs** (`FlowSchedule`, `FlowApiEndpoint`, `GlobalArtifact`,
  `CatalogTableReadLink`). All fire *after* creation, all have (or can share) an
  ensure-registered-then-proceed path. Verify each one explicitly; do not weaken the
  constraints to nullable.
- **`resolve_display_names` / display-name resolution** (`catalog_helpers.py`): recents
  and the header pill prefer the registration's name when one exists. Under lazy
  registration, unregistered flows simply fall back to `flow_settings.name` — that
  fallback already exists; keep it working.
- **Sharing:** every path that deletes a registration must keep calling
  `sharing.delete_grants_for_resource` (SQLite reuses rowids).
- **Project git-tracking** (`flowfile_core/project/projection.py`) projects
  registrations to files; fewer rows is fine, but the projection hooks must keep
  never-raising.
- **Migration of existing rows:** installs in the wild have years of auto-registered
  scratch rows. A one-time migration should delete registrations whose `flow_path`
  lives under `unnamed_flows/` / `python_editor_flows/` **and** that have no dependent
  FK rows; anything with a schedule/endpoint/artifact/read-link stays. Alembic,
  numbered, with the usual pre-migration DB snapshot.
- **Tests:** `flowfile_core/tests/` currently contains tests that *assert* eager
  registration happens (they were written against today's behavior, not as product
  intent). Expect to rewrite those, not contort the implementation to keep them green.

## 5. Known defects to fix regardless of direction

Each of these is a bug under the *current* policy too, verified in the tree:

1. **Silent skip:** `CatalogService.auto_register_flow` returns `None` + INFO log when
   `General` is missing. Raise or self-heal; never silently diverge.
2. **The checkbox lie:** unchecking "Also register in catalog" nulls `namespace_id`,
   which routes into auto-registration anyway (`CreateDialog.vue`, `SaveDialog.vue`).
3. **Template path mismatch:** `create_from_template` (`routes/routes.py`) registers
   `flows_dir/{stem}.yaml` while the flow was imported from a temp file — the
   registered path may not exist at registration time.
4. **Unreachable branch:** the `General > default` fallback inside
   `auto_register_flow` cannot be reached from any current caller.
5. **No deletion tombstone:** re-opening a deleted flow re-registers it (moot under
   lazy registration; needs the tombstone under Option B / status quo).

## 6. Suggested sequencing (if the lazy direction is confirmed)

1. Fix the silent skip (raise/self-heal) — independent, small, ships alone.
2. Stop registering in `import_flow` and `create_from_template`; make `create_flow`
   register only when `namespace_id` is provided. Make the checkbox wire
   `register_in_catalog` for real.
3. Frontend: recents rewrite-on-save (replace the `file_exists=False` prune), Drafts
   section listing `unnamed_flows/` with age.
4. Draft TTL sweep + `FLOWFILE_DRAFT_TTL_DAYS`.
5. Migration cleaning historic scratch registrations.
6. Delete the now-dead hiding mechanisms in the frontend, one PR, mechanically.

Steps 1–2 are the substance; 3–6 are each small and independently shippable.

## 7. Key code anchors

| Concern | Where |
|---|---|
| Auto-registration entry (wrapper) | `flowfile_core/flowfile_core/flowfile/catalog_helpers.py::auto_register_flow` |
| Auto-registration logic + silent skip | `flowfile_core/flowfile_core/catalog/service.py::auto_register_flow` (~L715) |
| Call sites | `routes/routes.py`: flow save (~L406), `import_flow` (~L1773), `create_from_template` (~L2526) |
| Scratch dirs (no GC) | `shared/storage_config.py`: `unnamed_flows_directory`, `python_editor_flows_directory`, `cleanup_directories` |
| Scratch unlink on Save-As | `routes/routes.py::_discard_relocated_scratch_file` (+ unit test in `tests/test_endpoints.py`) |
| Recents prune contract | `flowfile_frontend/.../HeaderButtons.vue::recordRelocatedFlowAsRecent`, `composables/useRecentFlows.ts` |
| The misleading checkbox | `features/designer/components/CreateDialog.vue`, `SaveDialog.vue` (`registerInCatalog`) |
| Dependent FKs | `flowfile_core/database/models.py`: `FlowSchedule`, `FlowApiEndpoint`, `GlobalArtifact`, `CatalogTableReadLink` |
| Test-suite seed guard | `flowfile_core/tests/conftest.py::restore_seeded_catalog_namespaces` |
