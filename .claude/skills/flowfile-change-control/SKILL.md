---
name: flowfile-change-control
description: How changes to the Flowfile monorepo are classified, gated, versioned, and released — version-lockstep bump/check machinery, the stub and formula-docs drift gates, Alembic migration discipline, deliberate dependency pins (fastapi, polars), the v*/wasm-v* release-tag mechanics, real branch-protection state (ghost required checks, admin bypass), and the standing no-commit/no-stash agent working agreement. Use when bumping the app version, adding an Alembic migration, touching flowfile_frame's public API or the formula docs generator, editing a pinned dependency (fastapi, polars, litellm), preparing or reviewing a release/tag, wondering why a PR won't go green, or deciding whether an agent may run `git commit`/`git stash`.
---

# Flowfile change control

## When NOT to use this skill

- Writing or debugging tests → `flowfile-testing-and-validation`.
- Local dev-server / build / Docker setup → `flowfile-build-and-env`.
- Env vars and feature flags → `flowfile-config-and-flags`.
- Cross-package contracts (core/worker/kernel/shared) → `flowfile-architecture-contract`.
- Digging into a specific past bug for its own sake → `flowfile-failure-archaeology`.
- Writing/updating docs or CLAUDE.md files → `flowfile-docs-and-writing`.

This skill is the gate-keeper: what is and isn't allowed to change, how CI proves it, and how a release actually ships.

---

## 1. How change is classified and gated here

Every change lands in one (or more) of these lanes. Know your lane before you touch files:

| You're touching | Gate that fires | Where it's defined |
|---|---|---|
| Any of the 5 version manifests | `version-sync` job (`test.yaml`), tag `--expect` gates on release | `tools/check_version_sync.py` |
| `flowfile_frame` public API (`FlowFrame`, `Expr`, submodules) | `check-stubs` job | `make check_stubs` |
| `docs/users/formulas/functions.md` or the polars-expr-transformer pin | `check-formula-docs` job | `make check_formula_docs` |
| `flowfile_core/flowfile_core/database/models.py` | none automated — you must add a migration yourself | Alembic, §4 |
| `pyproject.toml` deps (fastapi, polars, litellm) | none automated — these are load-bearing pins, see §5 | — |
| Anything else backend/frontend | `backend-tests` matrix / `test-web` / relevant path-filtered workflow | `.github/workflows/*.yml(.yaml)` |

CI is 15 workflow files under `.github/workflows/` as of 2026-07-03 (v0.12.7) — root `CLAUDE.md`'s "12 workflows" table is stale; it's also missing `claude.yml` (interactive `@claude` agent) and `claude-pr-review.yml` (automatic Claude review posted on every non-draft PR — contributors are not told this happens anywhere in the docs). The root CLAUDE.md also claims the legacy `codeql.yaml` "was removed" — it is still tracked on `origin/main` and fails every Monday (references `.github/codeql/codeql-config.yml`, which does not exist; `.github/codeql/` is an empty dir). This is harmless noise, not a blocker: GitHub Advanced Security **default setup** is separately configured and live (`gh api repos/<org>/Flowfile/code-scanning/default-setup` → `"state":"configured"`, weekly, covers python/js-ts/actions/rust) and is what actually reports CodeQL results.

`test.yaml` (the primary CI gate, ~620 lines) is the only workflow with a `concurrency` group: PR runs cancel superseded runs of themselves, but **main-branch runs are never cancelled** — the file's own comment explains why: "docker-publish / release pipelines key off completed main builds."

---

## 2. Non-negotiables — with rationale and the incident behind each

### 2a. Never force-push `main`

`CONTRIBUTING.md`: "**Don't force-push to `main`.** Releases build from it." `docker-publish.yml` publishes kernel Docker images from `main` (kernel-path-filtered, only when the kernel version is unpublished; app images publish from `v*` tags); `test.yaml` runs full CI from `main`. Live branch protection additionally sets `allow_force_pushes: false`, and repo ruleset id `2660650` ("Only admin commits") adds `non_fast_forward` + `required_linear_history` blocks (verified live via `gh api repos/.../branches/main/protection` and `gh api repos/.../rulesets`). A force-push to `main` would desync in-flight release/Docker pipelines that key off a specific commit.

### 2b. Version bumps move in lockstep — never hand-edit a manifest

Five files carry the app version and **must always agree**:

1. `pyproject.toml` → `[tool.poetry] version` (line 3)
2. `shared/_version.py` → `__version__`
3. `flowfile_frontend/src-tauri/Cargo.toml` → `[package] version`
4. `flowfile_frontend/package.json` → `"version"`
5. `flowfile_frontend/src-tauri/tauri.conf.json` → `"version"`

This exists because of a real production incident (commit `b21f518c`, PR #547, "Centralize version management across all manifests"): the version used to be read from 8 places with drifted hardcoded fallbacks (`0.5.0` / `0.12.0` / `0.12.3` / `"unknown"`). In frozen PyInstaller sidecars, `importlib.metadata.version("Flowfile")` doesn't resolve, so a fallback could write a **non-version string into the `NOT NULL` `db_info.app_version` column — breaking desktop startup**. The fix built the tooling below; use it, don't reinvent it.

```bash
# Bump — rewrites all 5 files, nothing else
python tools/bump_version.py X.Y.Z
make bump-version VERSION=X.Y.Z        # same, via Make

# Check — prints all 5, exits 1 on drift
python3 tools/check_version_sync.py
make check-version                     # same, via Make

# Release-gate form: also assert the canonical version equals a value
python3 tools/check_version_sync.py --expect X.Y.Z
```

Gotchas:
- The bump script's own last line of output is **"Done. Refresh Cargo.lock (cargo update -p flowfile) and commit."** — it does NOT refresh `Cargo.lock` for you; run `cd flowfile_frontend/src-tauri && cargo update -p flowfile` as a manual follow-up before opening the PR.
- **`kernel_runtime/pyproject.toml`** (currently `0.4.0`) and **`flowfile_wasm/package.json`** (currently `0.1.0`, the `flowfile-editor` npm package) are **deliberately NOT synced** to the app version — they have their own release cadence (kernel image version, `wasm-v*` npm tags). Don't "fix" them to match.
- The `version-sync` CI job runs unconditionally on every push/PR but is **not** in `test-summary`'s `needs` list and is **not** a required branch-protection check — a version-sync failure fails that job but won't by itself block a merge the way you'd expect. Don't rely on it as your only signal; run `make check-version` yourself before opening a version-touching PR.

### 2c. Stub drift gate (`make check_stubs`) — fails CI silently for newcomers

`flowfile_frame` ships committed `.pyi` type stubs for its public surface (`FlowFrame`, `Expr`, every submodule, `py.typed`). If you add/change/remove a public method, class, or top-level symbol in `flowfile_frame` and don't regenerate stubs, `check-stubs` (CI job, gated on the `backend_frame` path filter) fails with no obvious link back to what you changed — it just reports a `.pyi` git diff.

```bash
make stubs          # regenerates all .pyi files, then prunes unused imports (ruff --select F401 --fix)
make check_stubs    # stubs + `git diff --exit-code` on the .pyi files; CI runs exactly this
```

Three generators run in sequence: `expr_stub_generator.py` → `expr.pyi`, `flow_frame_stub_generator.py` → `flow_frame.pyi`, `submodule_stub_generator.py` → every other `.pyi` including `__init__.pyi`. **Always run `make stubs` after any public API change and stage the diff** (agents: hand off the commit per §6) — this is the single most common newcomer-trips-CI moment for anyone working in `flowfile_frame`. Full stub-authoring guidance lives in `flowfile-frame-and-codegen`; this skill only covers the gate.

### 2d. Formula-docs gate (`make check_formula_docs`) — same failure shape, different surface

`docs/users/formulas/functions.md` is **auto-generated** from `polars-expr-transformer` docstrings (header comment in the file itself says so — never hand-edit it). Bump the `polars-expr-transformer` pin, or otherwise change what generates that page, without regenerating it, and `check-formula-docs` (CI job, gated on a `formula_docs` path filter covering the generator script, the doc file, `pyproject.toml`, and the lockfile) fails on an unrelated-looking doc diff.

```bash
make formula_docs         # regenerate docs/users/formulas/functions.md
make check_formula_docs   # formula_docs + `git diff --exit-code` on that file; CI runs exactly this
```

Neither `CONTRIBUTING.md` nor root `CLAUDE.md` mentions this gate anywhere — a contributor bumping `polars-expr-transformer` for an unrelated reason will hit a red `documentation.yml` build with zero signposting outside the Makefile comment. If you see `check-formula-docs` fail, this is why; run `make formula_docs` and stage the regenerated page (agents: hand off the commit per §6).

### 2e. Alembic migrations: numeric prefix discipline

`flowfile_core/flowfile_core/alembic/versions/` currently holds **28 migrations** (`001_initial_schema.py` … `028_catalog_namespace_storage.py`, as of 2026-07-03 / v0.12.7 — root `CLAUDE.md` still says "001–021"; that's stale by 7 revisions, don't trust the count in prose, `ls` the directory).

Rules, with the incident behind each:
- **Add a new migration for any change to `flowfile_core/flowfile_core/database/models.py`**; never hand-edit a migration that has already merged to `main`. Alembic itself was retrofitted in response to a real bug (commit `0ded1ebf`, PR #403) after a run-type mismatch between local and Docker databases needed an undocumented downgrade path (`006_normalize_run_type.py`) — the whole migration system exists because "align local db and worker db" had to be attempted twice by hand before Alembic was added.
- **Numeric-prefix collisions bite long-running branches.** Two branches cut from the same base each add, say, `029_*.py` independently; whichever merges second collides or silently shadows revision ordering. This has already happened in-tree (`b484a117 fix migrations` on a long-lived branch was a direct fix for exactly this). Before adding a migration, check `origin/main`'s current highest number, not your branch's — someone else may have already claimed `NNN`.
- **Importing `flowfile_core` has a side effect you need to know about**: `run_startup_migration()` fires automatically at import time (`flowfile_core/flowfile_core/database/init_db.py`) unless `FLOWFILE_SKIP_STARTUP_MIGRATION` is set. This runs Alembic against whatever DB `get_database_url()` resolves to — including your live local catalog DB if you're not careful. Set `FLOWFILE_SKIP_STARTUP_MIGRATION=1` for any diagnostic import of `flowfile_core` that isn't meant to touch the DB.

### 2f. Deliberate dependency pins — do not "fix" these without a mandate

| Pin | Value | Why it's pinned | Evidence |
|---|---|---|---|
| `fastapi` | `~0.115.2` (`pyproject.toml`) | An upgrade was attempted and reverted; the reason wasn't recorded in the commit message. Treat the pin as **deliberate** — don't bump it casually. | Commit `eff7287b` "Reverting upgrade Fastapi" (2026-05-11) on branch `feature/LLM-security-patches`; the pin has otherwise been unchanged since the file's initial add. |
| `polars` | `>=1.8.2, <1.43` (`pyproject.toml`; 1.43.0 deadlocks `SQLContext.execute` over `scan_delta` frames — catalog SQL readers/views hang) | Must move **together** with `kernel_runtime`'s own Polars pin, `flowfile_frame`, and the version-coupled `polars-*` plugin packages (e.g. `pl-fuzzy-frame-match`). Kernel containers read their own `poetry.lock` at startup to surface/detect drift. Bumping the root pin alone breaks the kernel/frame contract. | Root `CLAUDE.md` "Things to Avoid"; `CONTRIBUTING.md` additionally still claims a Windows-only `<=1.25.2` ceiling that was **removed** (single cross-platform pin now) — CONTRIBUTING is stale on this point, follow the `pyproject.toml` value, not the prose. |
| API-key hash | SHA-256, `flowfile_core/flowfile_core/auth/api_key.py::hash_api_key` | Intentional for 256-bit random tokens (no password-guessing surface to slow down with a KDF). The CodeQL "weak hash" alert on this line is a **known false positive** — do not "fix" it with bcrypt/argon2/PBKDF2. | Root `CLAUDE.md`; verified in-file (`hashlib.sha256(...).hexdigest()`, one-way, "never recoverable" per the module docstring). |

If an agent (or CodeQL, or a linter) flags any of these three, the correct action is to leave it alone and, if truly necessary, open a Discussion/issue to get the maintainer's sign-off first — not to "fix" it inline.

### 2g. Deliberate non-features — don't treat `NotImplementedError` as a TODO

The codebase has a set of intentional, by-design refusals — places where a feature is **scoped out**, not half-built. Examples: standalone Polars codegen refuses to emit code for external-source / cloud-storage / Kafka / catalog nodes ("Use FlowFrame export" instead — `connector_handlers.py`, `code_generator.py`); exported projects raise `NotImplementedError` for server-backed `flowfile_ctx` calls (global artifacts, catalog) with warnings surfaced in the export manifest; Kernel (Python Script) nodes are skipped by Export-to-Python; `flowfile_frame` refuses lambdas in expressions and `join_asof`/`join_where`; the bundled `flowfile` CLI web UI only accepts `localhost:63578`. If you find one of these while working a task, it is a documented product boundary, not a bug to close — check the surrounding code/docs for the refusal message before "completing" it. Full inventory of these boundaries belongs to `flowfile-node-development` / `flowfile-frame-and-codegen`; this skill flags the pattern so you don't file (or fix) a phantom bug.

---

## 3. Release mechanics

### 3a. The checklist

1. `make bump-version VERSION=X.Y.Z`
2. `cd flowfile_frontend/src-tauri && cargo update -p flowfile` (manual — the bump script tells you to but doesn't do it)
3. `make check-version` — must print "All versions in sync"
4. Open a PR (branch protection blocks direct pushes to `main`), get it merged
5. Tag the merge commit **lowercase** `vX.Y.Z` and push the tag
6. This fires `pypi-release.yml`, `release.yaml`, **and** `docker-publish.yml` (app Docker images) simultaneously (§3b)
7. **First release on the tag-triggered path:** confirm `docker-publish.yml` actually fired on the tag (tag pushes ignore the `paths:` filter — documented GH behavior, but unexercised here). If it didn't, `workflow_dispatch` with `publish_app: true` publishes the app images at the manifest version — the intended escape hatch, safe because the new version's tags don't exist on Docker Hub yet.
8. **Automatic, but confirm it:** the `release` job generates `latest.json` with `tools/make_latest_json.py` and attaches it to the release (§3d) — check the asset landed (`gh release view vX.Y.Z --json assets | grep latest.json`)

### 3b. One `v*` tag push → three workflows

| Workflow | What it does | Hard gate |
|---|---|---|
| `pypi-release.yml` | Builds web frontend into `flowfile/flowfile/web/static/`, `poetry build`, publishes to PyPI via **Trusted Publishing (OIDC)** — no API token | `python3 tools/check_version_sync.py --expect "${GITHUB_REF#refs/tags/v}"` — dies instantly if tag ≠ manifest version |
| `release.yaml` | Builds Tauri desktop installers on a 4-platform matrix (macOS arm64/x86_64, Windows, Linux), signs/notarizes macOS, publishes the GitHub Release | Same `check_version_sync.py --expect` gate, run per-platform before the Rust/PyInstaller build starts |
| `docker-publish.yml` | Publishes app Docker images (`flowfile-core`/`-worker`/`-frontend`) as `:<version>` + `:latest` (no `latest` for `-`-suffixed prerelease tags); self-heals any unpublished kernel image version in the same run | Same `check_version_sync.py --expect` gate, plus `tools/check_kernel_version_sync.py` (manager.py kernel pins must match `kernel_runtime/pyproject.toml`) |

The shared gate means: **tag before bumping = all release pipelines fail fast**, which is the intended failure mode (better than shipping a mismatched artifact).

`wasm-v*` tags separately fire `npm-publish-wasm.yml` (publishes `flowfile-editor` to npm with provenance). As of 2026-07-03 no `wasm-v*` tag has ever been pushed to this repo — all historical runs of that workflow were manual `workflow_dispatch`, and most failed. Treat the npm publish channel as stalled/experimental, not a proven path.

### 3c. `docker-publish.yml` fires once per release, plus kernel-only runs from `main`

App images publish once per release, from the `v*` tag (the old `release: published` re-fire and per-main-push publishing were removed — the `GH_TOKEN` PAT on `release.yaml`'s release step no longer serves that re-fire purpose). Kernel images (`flowfile-kernel-base/ml/lite`) publish from `main` pushes touching kernel paths, and **only when `flowfile-kernel-*:<kernel_version>` is absent from Docker Hub** (`tools/docker_publish_matrix.py` checks; `workflow_dispatch` `force_kernel` overrides, `publish_app` republishes app images at the manifest version). Published version tags are therefore immutable in practice. Versions come from the manifests (root and `kernel_runtime/` `pyproject.toml`), with the `--expect` gate tying app tags to the git tag per §2b.

### 3d. The auto-updater manifest

`tauri.conf.json`'s updater endpoint expects a `latest.json` asset on each GitHub Release. The `release` job now generates one: `tools/make_latest_json.py --version "${GITHUB_REF_NAME#v}" --artifacts-dir artifacts --out latest.json`, listed in the release `files:`. It maps the four platform keys onto the release assets (`Flowfile_aarch64.app.tar.gz`, `Flowfile_x64.app.tar.gz`, `Flowfile_<version>_x64-setup.exe`, `Flowfile_<version>_amd64.deb`) and pairs each with its `.sig` by an independent `rglob` — bundles and signatures are uploaded as separate artifacts, so they never sit in the same directory — failing the release when either is missing, ambiguous or empty. `tools/tests/test_make_latest_json.py` covers it.

Two things that are still true: the check 404s until the newest non-prerelease release carries a `latest.json` (the endpoint always resolves against `/releases/latest`, never against the installed version); and prerelease tags get a manifest too, which is inert because `/releases/latest` skips prereleases (use an `-rc` tag to validate manifest generation without offering the update to anyone).

### 3e. Tag hygiene — read before tagging

- GitHub's `v*` trigger filter is **case-sensitive**. Capital-`V` tags (`V0.10.1`, `V0.12.3` exist in this repo's history) fire **nothing**. Always tag lowercase `vX.Y.Z`.
- A tag literally named `main` exists in this repo's tag namespace, which makes `git <cmd> main` print `warning: refname 'main' is ambiguous` and can resolve to the *tag* instead of the branch (tags win over branches in ref resolution). **Always use `origin/main` or `refs/heads/main`** for comparisons, never bare `main`.
- A suffixed tag (e.g. `v0.10.1-rc.1`) is auto-flagged as a GitHub **prerelease** (`contains(github.ref_name, '-')` in `release.yaml`) so test builds never become the public "Latest" release — use a `-suffix` for any tag you don't want promoted.

---

## 4. Branch protection & required checks — the real, live state

Read this before telling anyone (human or agent) to "wait for CI to go green" or "wait for required checks":

```bash
gh api repos/Edwardvaneechoud/Flowfile/branches/main/protection
```

As of 2026-07-03 this returns 8 required contexts: `electron-tests-macos`, `electron-tests-windows`, `test-web`, `backend-tests-windows`, `backend-tests (macos-latest, 3.11)`, `backend-tests (ubuntu-latest, 3.10/3.11/3.12)`.

**`electron-tests-macos` and `electron-tests-windows` no longer exist** — they were removed in the Electron→Tauri migration (commit `3777c661`, #462). A required context that no workflow ever reports means **branch protection can mathematically never be satisfied** for a non-admin PR — the checks tab will show those two as perpetually pending, forever.

- `backend-tests (ubuntu-latest, 3.13)`, `coverage`, `kernel-tests`, `check-stubs`, `check-formula-docs`, `docs-test`, `test-summary`, `version-sync`, all E2E workflows, and the Claude review are **not** required checks — they can be red and a PR is still technically mergeable by protection rules (modulo the ghost-check problem above).
- Reviews require `required_approving_review_count: 1` and `require_code_owner_reviews: true`, but **there is no `CODEOWNERS` file in the repo** (verified: `git ls-files | grep -i codeowners` → empty) — the code-owner requirement is a no-op.
- A separate repo ruleset, id `2660650` "Only admin commits" (active, targets `refs/heads/main`), duplicates the same stale required-checks list and layers on `required_linear_history` + deletion/non-fast-forward/creation blocks. Its bypass actors are `OrganizationAdmin` (always) and a repository-role id (always) — `current_user_can_bypass: "always"` for the maintainer.

**Practical consequence:** the maintainer merges PRs via admin/ruleset bypass, not by waiting for protection to auto-clear. The real aggregate CI signal to look at is the `test-summary` job in `test.yaml` (`if: always()`, fails if any non-skipped job in its `needs` list failed) — but note `test-summary`'s `needs` list itself excludes `version-sync`, so a version-drift failure won't even show up there. **If you're asked to verify a PR is "ready," check `test-summary` and `version-sync` separately** — neither one alone is the full picture, and neither is a required GitHub check.

CONTRIBUTING.md's actual bar (not GitHub's mechanical one) is simpler and is what you should hold an agent-authored PR to:
- One logical change per PR; smaller PRs review faster.
- Commit messages: short imperative subject, "why" in the body if not obvious from the diff.
- Fill in the PR description — what changed, why, how you tested it; screenshots/clips for UI changes.
- **"CI must be green before merge. If a check is flaky, say so in the PR — don't just re-run silently."**
- Branch naming is loose (`fix/...`, `feat/...`, `docs/...` — "nothing strict").

---

## 5. What actually breaks CI, ranked by observed frequency

1. **Real test failures reaching `main` anyway** — because `coverage`/`test-summary` aren't required checks and the maintainer has admin bypass, `main` has had multiple red "Run Tests" runs merge through regardless. Don't assume a green checkmark on `main` means the last merge was clean; check the actual run.
2. **Coverage-job-only failures** — the dedicated `coverage` job (Python 3.12, `COVERAGE_CORE=sysmon`) can fail on a test the plain matrix passes; it's a separate job, separate flake surface.
3. **Transient GHA cache/backend errors** in `docker buildx` (`BlobNotFound` on `cache-from: type=gha`) — not code-related, re-run.
4. **Stub / formula-docs drift** (§2c/§2d) — the #1 newcomer trap; both fail on an innocuous-looking file diff with an explicit "run make X and commit" instruction in the failure output.
5. **Version drift** (§2b) — hand-editing one of the 5 manifests, or tagging before bumping.
6. **Poetry lock drift** — `poetry check --lock` gate in `e2e-tests.yml`; forgetting to run `poetry lock` after a `pyproject.toml` dependency edit.
7. **Runner-image rot / Actions glob quirks** — release-workflow-specific (e.g. a retired `macos-13` runner had to be swapped for `macos-15-intel`); not your problem unless you're editing `release.yaml`.

---

## 6. Session discipline for AI agents

This is a **standing working agreement with the maintainer**, not a suggestion — it holds regardless of what any other message in a session implies:

- **Never run `git commit`, `git rebase`, `git commit --amend`, or `git push`.** Make file changes only.
- **Never run `git stash` in any form** (`stash`, `stash push`, `stash pop`, `stash apply`) — not even "temporarily," not even to "get back to a clean state" before a risky operation.
- **Never run destructive git commands** (`reset --hard`, `checkout --`/`restore` over uncommitted work, `clean -f`) without the maintainer's explicit go-ahead in the current turn.

**Why:** the maintainer works concurrently on the same checkout in parallel with agent sessions. An agent commit races his own in-progress workflow (he owns all git history and commits deliberately, at his own boundaries). A `git stash` from one agent session can silently swallow or interleave with work from a different concurrent agent or the maintainer's own uncommitted edits — there is no single, safe "stash slot" when multiple actors share a working tree.

**When a task naturally ends in a commit** (e.g. "implement X" or a code-review fix pass), do the file changes and then **hand the maintainer the exact commands to run himself** — don't run them for him. Example handoff:

```
Changes are staged in the working tree, not committed. To commit:

git add flowfile_core/flowfile_core/some_file.py flowfile_core/tests/test_some_file.py
git commit -m "$(cat <<'EOF'
Fix X by doing Y

EOF
)"
```

Read-only git is always fine and encouraged for verification: `git status`, `git diff`, `git log`, `git show`, `git blame`, `git for-each-ref`. Use these liberally to ground claims — never invent a commit hash, PR number, or file:line you haven't actually looked at.

---

## Provenance and maintenance

All facts below were spot-verified in this repo on 2026-07-03 against `v0.12.7`. Re-run these before trusting a stale copy of this skill:

```bash
# Workflow count and set
ls .github/workflows/ | wc -l                                    # expect 15 as of 2026-07-03
ls .github/workflows/

# Version-sync machinery still shaped as described
sed -n '1,80p' tools/bump_version.py
sed -n '1,80p' tools/check_version_sync.py
python3 tools/check_version_sync.py                               # should print "All versions in sync: <X.Y.Z>"
grep -n "bump-version\|check-version\|^stubs:\|^check_stubs:\|^formula_docs:\|^check_formula_docs:" Makefile

# Deliberate pins
grep -n "^fastapi\|^polars " pyproject.toml                       # expect fastapi ~0.115.2, polars >=1.8.2,<1.43
sed -n '1,30p' flowfile_core/flowfile_core/auth/api_key.py        # expect hashlib.sha256(...).hexdigest()

# Alembic migration count (root CLAUDE.md's number rots fast — trust this, not prose)
ls flowfile_core/flowfile_core/alembic/versions/ | sort

# FastAPI revert incident
git log --all --oneline --grep="Reverting upgrade Fastapi"        # expect eff7287b on feature/LLM-security-patches

# CodeQL dual-state (legacy workflow broken, default setup live)
cat .github/workflows/codeql.yaml | grep config-file
git ls-files .github/codeql/                                      # expect empty (missing config)
gh api repos/Edwardvaneechoud/Flowfile/code-scanning/default-setup   # expect "state":"configured"

# Branch protection reality (ghost required checks, no CODEOWNERS)
gh api repos/Edwardvaneechoud/Flowfile/branches/main/protection
git ls-files | grep -i codeowners                                 # expect empty
gh api repos/Edwardvaneechoud/Flowfile/rulesets                   # expect ruleset id 2660650 "Only admin commits"

# Tag hygiene facts
git tag | grep -E "^V[0-9]"                                       # expect capital-V tags exist (e.g. V0.12.3)
git tag | grep -viE "^v?[0-9]"                                    # expect stray branch-named tags incl. "main"

# v*/wasm-v* release triggers
grep -n "check_version_sync" .github/workflows/pypi-release.yml .github/workflows/release.yaml
grep -n "on:" -A24 .github/workflows/docker-publish.yml           # confirm push(main kernel paths + v* tags) + dispatch(publish_app/force_kernel)
git tag | grep -i "^wasm-v"                                       # expect empty (no wasm-v* tag ever pushed)

# latest.json updater manifest (generated by the release job)
grep -n "make_latest_json" .github/workflows/release.yaml                                      # expect the generate step + latest.json in files:
gh release view <newest tag> --repo Edwardvaneechoud/Flowfile --json assets | grep -i latest.json  # expect a match on releases built since it landed
```

Facts that will rot fastest (re-check on every use of this skill, don't trust cached numbers): current app version (`grep version pyproject.toml`), migration count (`ls` the versions dir), workflow file count and names, and the exact required-check-context list from `branches/main/protection` — all four have already drifted once from what root `CLAUDE.md` claims.
