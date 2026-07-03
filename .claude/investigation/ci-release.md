# Discovery Dossier — KEY=ci-release
## CI/CD & Release Discipline for the Flowfile monorepo

Investigated: 2026-07-03, repo `/Users/edwardvaneechoud/flowfile_backup/Flowfile`, HEAD branch `feature/claude-skills` (git-status snapshot said `improvement/improve-naming-unnamed-flows` at session start; local `main` == `origin/main` == `f6963c77`). All claims verified by reading files, git history, or live read-only `gh api` / `gh run` calls unless marked **inferred**.

---

## 1. Workflow inventory — 15 files, not 12

`git ls-files .github/workflows/` returns **15 tracked workflow files**. Root `CLAUDE.md` says "12 workflows" and claims the legacy `codeql.yaml` "was removed" — **both statements are stale**. The three files CLAUDE.md doesn't list: `claude.yml`, `claude-pr-review.yml`, and `codeql.yaml` (still tracked on `origin/main`, verified via `git cat-file -e origin/main:.github/workflows/codeql.yaml` → EXISTS).

| File | Name | Triggers | What it gates |
|---|---|---|---|
| `test.yaml` | Run Tests | push/PR → main (no path filter) + `workflow_dispatch` (input `run_all_tests`) | Primary CI: backend matrix, coverage, Windows, kernel, stubs, formula docs, web build, docs build |
| `e2e-tests.yml` | E2E Tests | push/PR → main, paths `flowfile_frontend/**`, `flowfile_core/**`, self | Playwright web E2E |
| `test-docker-auth.yml` | Docker Authentication E2E Tests | push/PR → main, auth-code paths | auth unit + docker E2E |
| `test-docker-kernel-e2e.yml` | Docker Kernel E2E Tests | push/PR → main, kernel/compose paths | full-compose kernel E2E (`-m docker_integration`) |
| `test-kernel-integration.yml` | Kernel Integration Tests | push/PR → main, kernel/artifacts paths | kernel container tests (`-m kernel`) |
| `test-kafka-integration.yml` | Kafka Integration Tests | push/PR → main, kafka paths | kafka unit (mocked) + real Redpanda broker |
| `flowfile-wasm-build.yml` | Flowfile WASM Build | push/PR → main, `flowfile_wasm/**` + self | wasm build, lib build, CDN-import guard, vitest, CPython engine tests, Pyodide smoke |
| `documentation.yml` | Documentation | push/PR → main, docs paths | mkdocs build (PR) + `mkdocs gh-deploy --force` (main push only) |
| `docker-publish.yml` | Build and Push Docker Images to Docker Hub | push → main (paths), `release: published`, dispatch | 6 images × 2 arch → Docker Hub |
| `pypi-release.yml` | Build and Release Python Package | tags `v*`, dispatch | PyPI publish (trusted publishing) |
| `release.yaml` | Build and Release Desktop (Tauri) | tags `v*`, dispatch | 4-platform desktop installers + GitHub Release |
| `npm-publish-wasm.yml` | Publish flowfile-editor to npm | tags `wasm-v*`, dispatch | npm publish of `flowfile-editor` |
| `claude.yml` | Claude Code | issue/PR comments/reviews containing `@claude`, issues opened/assigned | interactive Claude agent (uses `anthropics/claude-code-action@v1`, secret `CLAUDE_CODE_OAUTH_TOKEN`) |
| `claude-pr-review.yml` | Claude PR Review | `pull_request: [opened, synchronize, ready_for_review, reopened]`, skips drafts | automated Claude review of every PR; prompt tells it to read CLAUDE.md files; allowed tools: inline-comment MCP + `gh pr comment/diff/view` |
| `codeql.yaml` | CodeQL | `schedule: '0 6 * * 1'` (Mon 06:00 UTC) + dispatch | **LEGACY, BROKEN** — references `./.github/codeql/codeql-config.yml` which does not exist (`.github/codeql/` is an empty dir); verified failing weekly in 16–18s (runs 2026-06-22, 2026-06-29) |

Note file-extension mix: `test.yaml`, `release.yaml`, `codeql.yaml` use `.yaml`; the rest `.yml`.

**CodeQL reality (live API):** `gh api repos/Edwardvaneechoud/Flowfile/code-scanning/default-setup` → `{"state":"configured","languages":["actions","javascript","javascript-typescript","python","rust","typescript"],"query_suite":"default","schedule":"weekly","updated_at":"2026-05-30T06:48:41Z"}`. So **default setup IS live** (runs show event `dynamic`, e.g. "Push on main / CodeQL / dynamic" succeeded 2026-07-03), while the legacy `codeql.yaml` still exists and fails every Monday. CLAUDE.md's claim that it was removed describes intent, not current state.

---

## 2. `test.yaml` — the primary CI gate (deep dive)

File: `.github/workflows/test.yaml` (617 lines).

- **Permissions:** `contents: read` (lines 3–4).
- **Concurrency (lines 21–23):** group `${{ github.workflow }}-${{ github.ref }}`, `cancel-in-progress: ${{ github.event_name == 'pull_request' }}` — PR runs cancel superseded runs; **main-branch runs are never cancelled** (comment at lines 19–20: "docker-publish / release pipelines key off completed main builds"). This is the ONLY workflow with a concurrency group (`grep -rn concurrency .github/workflows/` → only test.yaml:21).
- **`detect-changes` job (lines 27–86):** `dorny/paths-filter@v3` computes outputs `backend_core`, `backend_worker`, `backend_frame`, `backend_flowfile`, `kernel`, `frontend`, `docs`, `formula_docs`, `shared`, `any_backend`, `test_workflow`. `shared` filter includes `shared/**`, `test_utils/**`, `pyproject.toml`, `poetry.lock` → touching root `pyproject.toml`/lock triggers everything backend. `test_workflow` = changes to `.github/workflows/test.yaml` itself → runs everything.
- **`version-sync` job (lines 89–94):** unconditional, runs `python3 tools/check_version_sync.py`. **NOT listed in `test-summary`'s `needs` (line 572) nor its failure checks (602–609)** — a version-sync failure fails the run overall but the summary job's own gate ignores it, and it is not in the branch-protection required contexts either.
- **`backend-tests` matrix (lines 103–117):** `fail-fast: false`; ubuntu-latest × py 3.10/3.11/3.12/3.13 + macos-latest × 3.11. Poetry installed via `curl -sSL https://install.python-poetry.org` and `$HOME/.poetry/bin` added to PATH. `actions/setup-python@v5` with `cache: 'pip'` (of limited value for poetry venvs — no poetry-venv cache in this workflow, unlike the kernel/kafka workflows which cache `.venv` keyed on `poetry.lock`).
- Spins up 5 Docker-backed fixtures via poetry scripts (verified in root `pyproject.toml` lines 82–100): `start_postgres`, `start_mysql`, `start_minio`, `start_azurite`, `start_gcs` (+ matching `stop_*`). These commands **gracefully skip if Docker is unavailable** (`test_utils/postgres/commands.py:33-37` prints "SKIPPING: Docker is not available") — which is how `backend-tests-windows` passes in ~4 min on windows-latest.
- Per-package pytest steps are conditional on the filter outputs; core runs `-m "not kernel"` (line 182). Shared tests ignore `shared/tests/kafka` (broker tests live in the kafka workflow, line 165–166).
- **`coverage` job (lines 216–296):** dedicated ubuntu 3.12 runner, `COVERAGE_CORE: sysmon` (PEP-669 tracer; comment lines 216–219 & 261–264 explains the old C-tracer doubled the 3.12 job to ~56 min — matches memory note "CI test-speed work"). Runs core `-m "not kernel"` then worker with `--cov --cov-append --cov-report=`. Coverage report goes into `$GITHUB_STEP_SUMMARY`; `coverage xml`; uploads artifact `coverage-report`; uploads to Codecov via `codecov/codecov-action@v5` with `flags: backend`, `fail_ci_if_error: false`, `token: ${{ secrets.CODECOV_TOKEN }}`. Coverage source/omit config in root `pyproject.toml` lines 141–160 (`fail_under = 0`; sources = core + worker package dirs only). **There is no `codecov.yml` in the repo.**
- **`backend-tests-windows` (lines 299–401):** windows-latest, py3.11 only, pwsh shell, poetry via `Invoke-WebRequest`; same conditional pytest steps.
- **`kernel-tests` (lines 404–443):** ubuntu, py3.11, `timeout-minutes: 15`; builds `kernel_runtime/Dockerfile` as `flowfile-kernel`; runs `pip install -e "kernel_runtime/[test]"` + kernel_runtime unit tests; then `poetry run pytest flowfile_core/tests -m kernel`.
- **`check-stubs` (lines 447–472):** runs `make check_stubs` when `backend_frame` changed — Makefile target (lines 269–276) regenerates stubs then `git diff --exit-code` on `flowfile_frame/flowfile_frame/**/*.pyi`; fails with "Run 'make stubs' and commit the result."
- **`check-formula-docs` (lines 477–502):** `make check_formula_docs` (Makefile 285–291) — regenerates `docs/users/formulas/functions.md` from polars-expr-transformer docstrings, fails on diff. Triggered by filter `formula_docs` (generator script, functions.md, pyproject, lock).
- **`test-web` (lines 505–544):** Node 22 (`cache: npm`, keyed on `flowfile_frontend/package-lock.json`), `npm ci`, `npm run test:unit` (vitest), `npm run build:web`, then boots `npm run preview:web` and curls `http://localhost:4173` expecting `200 OK`.
- **`docs-test` (lines 547–568):** `pip install poetry`, `poetry install --with dev`, `poetry run mkdocs build`.
- **`test-summary` (lines 571–616):** `if: always()`, needs all jobs **except `version-sync`**; echoes per-job results; exits 1 if any non-skipped job reports `failure`. Designed as an aggregate, but is NOT in the required-checks list (see §5).

---

## 3. Release process, reconstructed end-to-end

### 3a. Version bump (single source of truth machinery)

Commit `b21f518c` "Centralize version management across all manifests (#547)" (2026-06-24) built this after a real incident: version read in 8 places with drifted hardcoded fallbacks (0.5.0 / 0.12.0 / 0.12.3 / "unknown"); in frozen PyInstaller sidecars `importlib.metadata.version("Flowfile")` failed and the fallback could write a non-version into `db_info.app_version` (NOT NULL), **breaking desktop startup**. Fix: explicit literal `shared/_version.py::__version__` + bump/check tooling.

- **Bump:** `python tools/bump_version.py X.Y.Z` or `make bump-version VERSION=X.Y.Z` (Makefile lines 295–298). Rewrites exactly 5 files (`tools/bump_version.py:50-61`):
  1. `pyproject.toml` `[tool.poetry] version` (line 3, currently `0.12.7`)
  2. `shared/_version.py` `__version__` (line 3)
  3. `flowfile_frontend/src-tauri/Cargo.toml` `[package] version` (line 3)
  4. `flowfile_frontend/package.json` `"version"` (line 3)
  5. `flowfile_frontend/src-tauri/tauri.conf.json` `"version"` (line 4)
  Script's last line prints: **"Done. Refresh Cargo.lock (cargo update -p flowfile) and commit."** — Cargo.lock refresh is a manual follow-up the script does NOT do.
- **Check:** `python3 tools/check_version_sync.py` (`make check-version`). Prints all 5 versions; exits 1 with "Version drift detected … Run: python tools/bump_version.py <version>" on mismatch. `--expect X.Y.Z` additionally asserts the canonical version equals a value — used by both v* release workflows to match the tag. Verified locally: all 5 files at `0.12.7`, exit 0.
- **Independent versions (deliberately NOT synced):** `kernel_runtime/pyproject.toml` = `0.4.0` (kernel image version, own cadence); `flowfile_wasm/package.json` = `0.1.0` (`flowfile-editor` npm package, `wasm-v*` tags).

### 3b. Pushing a `v*` tag fires TWO workflows simultaneously

**`pypi-release.yml`** (jobs `build` → `release`):
1. `python3 tools/check_version_sync.py --expect "${GITHUB_REF#refs/tags/v}"` (line 21) — tag must equal the manifest version or the run dies immediately.
2. Node 20, `cd flowfile_frontend && npm install && npm run build:web`; copies `build/renderer/*` into `flowfile/flowfile/web/static/` (so the pip package serves the UI).
3. `poetry install`, `poetry build`, upload `dist/` artifact.
4. `release` job: `environment: pypi`, workflow has `id-token: write` → **PyPI Trusted Publishing (OIDC)** via `pypa/gh-action-pypi-publish@release/v1` with `skip-existing: true`, `packages-dir: dist/`. No API-token secret involved.
   - `skip-existing: true` gotcha: re-pushing the same version tag re-runs green but **PyPI keeps the first artifact** (uploads of existing filenames are skipped, not replaced).
5. Typical duration: ~3–6 min (verified from run list).

**`release.yaml`** ("Build and Release Desktop (Tauri)", jobs `build` (matrix) → `release`):
- Matrix (`fail-fast: false`): `macos-14`/aarch64-apple-darwin, `macos-15-intel`/x86_64-apple-darwin, `windows-latest`/x86_64-pc-windows-msvc, `ubuntu-22.04`/x86_64-unknown-linux-gnu.
- Per-platform steps: Node 20; Python 3.11; **version/tag guard** `python tools/check_version_sync.py --expect "${GITHUB_REF#refs/tags/v}"` (line 46, only on tag refs); Rust stable via `dtolnay/rust-toolchain@stable` with matrix target; `Swatinem/rust-cache@v2` scoped to `flowfile_frontend/src-tauri`; Linux installs WebKit deps (libwebkit2gtk-4.1-dev etc., lines 58–71).
- **macOS non-framework-Python dance (lines 76–101, load-bearing):** `astral-sh/setup-uv@v5`, `UV_PYTHON_PREFERENCE=only-managed`, `uv python install 3.11`, `poetry env use "$(uv python find 3.11)"`, plus a fail-fast Python one-liner that **aborts if `sysconfig.get_config_var('PYTHONFRAMEWORK')` is non-empty**. Why: framework CPython → PyInstaller bundles `Python.framework` with symlink-based `_CodeSignature`; Tauri drops symlinks when copying resources (tauri-apps/tauri#13219) → broken seal → **notarization fails** ("The signature of the binary is invalid"). Full local repro recipe in `flowfile_frontend/src-tauri/SIGNING.md` ("Build the sidecars against a non-framework CPython" section).
- `poetry install --with build` → `poetry run build_backends` (PyInstaller) → `poetry run python tools/rename_sidecar.py --triple <target>` (stages `services_dist/` into `src-tauri/binaries/<name>-<triple>`).
- macOS signing: `apple-actions/import-codesign-certs@v3` **guarded by `env.APPLE_CERTIFICATE != ''`** (job-level env at line 15; unsigned builds proceed if secrets absent — deliberate, from commit `a812a326`); `echo "/usr/bin" >> $GITHUB_PATH` to prefer system `xattr` over PyPI/Homebrew one; `bash tools/sign_macos_sidecars.sh` signs ~276 Mach-O sidecar files with Developer ID + hardened runtime + entitlements (commit `3082b182`: Tauri signs the shell but NOT resource binaries — unsigned sidecars fail notarization).
- `npx tauri build --target <triple>` with env: `TAURI_SIGNING_PRIVATE_KEY(_PASSWORD)` (updater signatures, all platforms), `APPLE_SIGNING_IDENTITY/APPLE_ID/APPLE_PASSWORD/APPLE_TEAM_ID` (notarization), `GH_TOKEN`. (The inline comment at line 144 — "The repo's master_key for embedded Fernet auth" — sits above `GH_TOKEN` and appears misplaced/wrong; treat as noise.)
- **macOS updater artifact rename (lines 154–168):** Tauri emits `Flowfile.app.tar.gz(.sig)` with no arch in the name → arm64 and x64 collide as release assets (symptom: "misleading 404 from action-gh-release on the 2nd upload"; fixed in commit `df1f6853`). Renamed to `Flowfile_aarch64.app.tar.gz` / `Flowfile_x64.app.tar.gz`; safe because the signature signs contents, not filename.
- Uploads: installers `bundle-<target>` (`dmg/Flowfile_*.dmg`, `macos/Flowfile_*.app.tar.gz`, `nsis/Flowfile_*-setup.exe`, `deb/Flowfile_*.deb` — note **no AppImage/msi in the globs**) `if-no-files-found: error`; signatures `signature-<target>` (`**/*.sig`) `if-no-files-found: warn`.
- `release` job (lines 188–224): only `if: startsWith(github.ref, 'refs/tags/v')` (dispatch runs exercise the matrix without cutting a release — commit `a812a326`). `softprops/action-gh-release@v1` with `prerelease: ${{ contains(github.ref_name, '-') }}` (so `v0.10.1-rc.1`, `-test`, `-beta` never become "Latest"), `generate_release_notes: true`, `GITHUB_TOKEN: ${{ secrets.GH_TOKEN }}` (a PAT, not the default token — **inferred reason:** so the `release: published` event can trigger `docker-publish.yml`; events from the default GITHUB_TOKEN don't trigger other workflows).
- Final step is a `::notice` reminder: **"Attach latest.json to the release after this workflow completes"** — see 3d. The comment references `tools/make_latest_json.py` "(or equivalent)" — **that script does not exist** (`ls tools/` → only `bump_version.py`, `check_version_sync.py`, `generate_formula_docs.py`, `migrate/`, `rename_sidecar.py`, `sign_macos_sidecars.sh`, `__init__.py`).
- Typical duration: 35–58 min (verified run list).

### 3c. `release: published` chains into docker-publish

When `release.yaml` publishes the GitHub Release, `docker-publish.yml` fires again on the `release` event (verified: "Release v0.12.5/v0.12.7 … Triggered via release"). So each desktop release also re-publishes Docker images at whatever version the manifests hold.

**`docker-publish.yml` details:** jobs `prepare` → `build` (12-row matrix) → `merge` → `summary`.
- `prepare` extracts root app version via `poetry version -s` and kernel version via `poetry version -s` in `kernel_runtime/` — Docker tags come **from pyproject, not from the git tag**.
- `build`: 6 images × 2 platforms, all on **native runners** (`ubuntu-latest` amd64, `ubuntu-24.04-arm` arm64 — no QEMU). Images: `flowfile-core`, `flowfile-frontend`, `flowfile-worker` (context `.`, app version) and `flowfile-kernel-base`, `flowfile-kernel-ml` (build-arg `EXTRAS=ml`), `flowfile-kernel-lite` (`SLIM_CONSTRAINTS=true`) (context `./kernel_runtime`, kernel version).
- Pushes **by digest** (`push-by-digest=true`), exports digest files as artifacts (`retention-days: 1`); `merge` job downloads digests and runs `docker buildx imagetools create` to assemble the multi-arch manifest with tags from `docker/metadata-action@v5`: `latest` (default branch only), `<version>`, `test` (non-main), `sha-<sha>` (prefix fixed by commit `8a9b6d52` — `{{branch}}-` prefix produced invalid tags for `feature/xyz` branches since `/` is illegal in Docker tags).
- Caching: `cache-from/to: type=gha, scope=<image>-<platform>, mode=max`.
- **No concurrency group** → overlapping main pushes each build; operator manually cancels redundant runs (verified: several "cancelled" runs on 2026-07-01/03).
- Timeout 60 min/job; secrets `DOCKERHUB_USERNAME`/`DOCKERHUB_TOKEN`.

### 3d. The auto-updater gap (live-verified)

`tauri.conf.json` lines 84–92: updater endpoint `https://github.com/Edwardvaneechoud/Flowfile/releases/latest/download/latest.json`, committed pubkey, `windows.installMode: passive`. SIGNING.md documents the Tauri-2 `latest.json` schema and says "The release workflow assembles this file from `.sig` artifacts" — **it does not**; the workflow only prints a reminder. Verified via `gh release view` on v0.12.7, v0.12.6, v0.12.5, v0.12.0: **no release has a `latest.json` asset**. Consequence: the desktop auto-updater endpoint 404s; auto-update is effectively inert. Release assets present per release (v0.12.7): `Flowfile_0.12.7_aarch64.dmg`, `Flowfile_0.12.7_x64.dmg`, `Flowfile_0.12.7_amd64.deb(+.sig)`, `Flowfile_0.12.7_x64-setup.exe(+.sig)`, `Flowfile_aarch64.app.tar.gz(+.sig)`, `Flowfile_x64.app.tar.gz(+.sig)`.

### 3e. `wasm-v*` → npm

`npm-publish-wasm.yml`: `test` job (npm ci, `npx vue-tsc --noEmit`, `npm run test:run`) → `publish` job (`environment: npm`, verifies `package.json` version equals `${GITHUB_REF#refs/tags/wasm-v}`, `npm run build:lib`, `npm publish --provenance --access public` with `NODE_AUTH_TOKEN: secrets.NPM_TOKEN`; `id-token: write` for provenance). **No `wasm-v*` tag exists** (`git tag | grep -i wasm` → empty); all 5 historical runs were `workflow_dispatch` (Feb 2026, 4 of 5 failed; last run failed) — the npm channel is stalled/experimental.

### 3f. The actual release checklist (as reconstructable from CI)

1. `make bump-version VERSION=X.Y.Z` (rewrites the 5 synced manifests).
2. `cd flowfile_frontend/src-tauri && cargo update -p flowfile` to refresh `Cargo.lock` (bump script tells you; nothing in CI checks Cargo.lock — **inferred** from script output + bump PRs like #558/#560 titled "bump versions").
3. `make check-version` (or `python3 tools/check_version_sync.py`) — must print "All versions in sync".
4. Open PR (branch protection blocks direct pushes; PRs like "bump versions to 0.12.7 (#560)"), merge to main.
5. Tag **lowercase** `vX.Y.Z` on the merge commit and push the tag. This fires `pypi-release.yml` + `release.yaml`; both die instantly if tag ≠ manifest version.
6. `release.yaml` publishes the GitHub Release (auto release notes; `-suffix` ⇒ prerelease), which fires `docker-publish.yml` (release event).
7. **Manual:** assemble and attach `latest.json` per SIGNING.md schema (this step has never been done — see 3d).
8. Per `docs/community.md` each release gets a Discussion thread (**inferred** from CLAUDE.md/community docs, not CI).

Cadence (from `git for-each-ref --sort=-creatordate refs/tags`): roughly every 2–7 days at the 0.12.x stage — v0.12.0 (06-12), .1 (06-13), .2 (06-19), V0.12.3 (06-23), .4 (06-24), .5 (06-26), .6 (06-30), .7 (07-01). 72 tags total.

---

## 4. Branch protection & required checks (live `gh api` data — the biggest gotcha)

`gh api repos/Edwardvaneechoud/Flowfile/branches/main/protection`:
- **Required status checks (strict=true, i.e. branch must be up to date):** `electron-tests-macos`, `electron-tests-windows`, `test-web`, `backend-tests-windows`, `backend-tests (macos-latest, 3.11)`, `backend-tests (ubuntu-latest, 3.10)`, `backend-tests (ubuntu-latest, 3.11)`, `backend-tests (ubuntu-latest, 3.12)`.
- **`electron-tests-macos` / `electron-tests-windows` no longer exist** — removed in the Electron→Tauri migration (`3777c661`). A required context that never reports = the PR can never satisfy protection.
- `backend-tests (ubuntu-latest, 3.13)`, `coverage`, `kernel-tests`, `check-stubs`, `check-formula-docs`, `docs-test`, `test-summary`, `version-sync`, E2E, Claude review: **not required**.
- Reviews: `required_approving_review_count: 1`, `require_code_owner_reviews: true` — but **there is no CODEOWNERS file** (`git ls-files | grep -i codeowners` → empty), so the code-owner requirement is a no-op.
- `enforce_admins: false`, `allow_force_pushes: false`, `allow_deletions: false`.

Additionally, repo ruleset **id 2660650 "Only admin commits"** (active, targets `refs/heads/main`): rules = deletion-block, non-fast-forward-block, creation-block, `required_linear_history`, and the **same stale required-checks list** (incl. both electron contexts). `bypass_actors`: OrganizationAdmin (always) + RepositoryRole id 2 (always); `current_user_can_bypass: "always"`.

**Practical consequence:** the maintainer merges via admin/ruleset bypass; a non-admin contributor's PR can never turn green-mergeable because two required checks are permanently missing and skipped-vs-required semantics don't matter for contexts that no workflow produces. Any skill/document telling an agent "wait for required checks" must know `test-summary` is the real aggregate signal and the electron contexts are ghosts.

Also: since backend-tests jobs are conditional (paths-filter), on frontend-only PRs the required `backend-tests (...)` contexts report **skipped**, which GitHub treats as satisfying protection (standard GH behavior — **inferred**, not repo-verified).

---

## 5. Supporting-config census

- **Dependabot config:** none (`git ls-files | grep -i dependabot` → empty). Yet Dependabot-style PRs exist ("Bump form-data 4.0.5→4.0.6 (#530)", "Bump vite/vitest in /flowfile_wasm (#531)", "Bump axios (#511)") — these come from GitHub's **security updates** (alert-driven), which need no config file (**inferred**).
- **CODEOWNERS:** none.
- **codecov.yml:** none — Codecov behavior is defaults + `flags: backend`, `fail_ci_if_error: false` in test.yaml (coverage never blocks CI).
- **`.github/` extras:** `ISSUE_TEMPLATE/bug_report.md`, `images/` (README GIFs), empty `codeql/` dir (the missing-config-file that breaks legacy codeql.yaml).
- **Environments:** `pypi` (pypi-release), `npm` (npm-publish). Trusted publishing for PyPI (OIDC, `id-token: write`); npm uses `NPM_TOKEN` + `--provenance`.
- **Secrets referenced across workflows:** `CODECOV_TOKEN`, `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN`, `TAURI_SIGNING_PRIVATE_KEY(_PASSWORD)`, `APPLE_CERTIFICATE(_PASSWORD)`, `APPLE_SIGNING_IDENTITY`, `APPLE_ID`, `APPLE_PASSWORD`, `APPLE_TEAM_ID`, `GH_TOKEN` (PAT for release creation), `NPM_TOKEN`, `CLAUDE_CODE_OAUTH_TOKEN`. Windows Authenticode secrets (`WINDOWS_CERTIFICATE*`) are documented in SIGNING.md but **not wired into any workflow** — Windows installers ship signed only with the Tauri updater key, and will trip SmartScreen (SIGNING.md says unsigned installers are acceptable for dev).

---

## 6. Tag-hygiene incidents (verified from `git for-each-ref`)

1. **Capital-V tags:** `V0.10.1` (2026-05-11) and `V0.12.3` (2026-06-23) exist. GitHub tag filters are case-sensitive: `v*` does NOT match `V*`, so a capital-V tag fires nothing. Lowercase `v0.10.1`/`v0.12.3` don't exist locally — yet `gh run list` shows three successful release runs for tag `v0.12.3` on 2026-06-23, meaning the lowercase tag was **pushed, re-pointed/re-pushed twice (3 release builds in one day: 10:13, 13:27, 14:44), then apparently replaced by the capital-V tag** in this clone. Same-version re-tagging + PyPI `skip-existing: true` means PyPI kept the FIRST v0.12.3 build while desktop users got the LAST — intra-version drift between channels.
2. **Stray non-version tags:** `main`, `feauture/kernel-implementation` (typo preserved), `refinement/improve_fuzzy_matching` — accidental tags named like branches. The `main` tag (at old commit `f6cb5f27`) makes every `git <cmd> main` print `warning: refname 'main' is ambiguous` and can silently resolve to the **tag** (tags win over branches in ref resolution) — e.g. `git ls-tree main` here returned the 2025-era workflow set. Always use `origin/main` or `refs/heads/main`.
3. `v0.10.1-rc.1` demonstrates the prerelease path (`contains(ref_name,'-')` ⇒ GitHub prerelease).

---

## 7. Historical incidents (symptom → root cause → fix → status)

1. **Desktop app broke at startup from version fallbacks** → `importlib.metadata.version("Flowfile")` unresolvable in PyInstaller sidecars; drifted hardcoded fallbacks; non-version written to NOT NULL `db_info.app_version` → commit `b21f518c` (#547): explicit `shared/_version.py` literal, `bump_version.py`, `check_version_sync.py`, CI drift guard + tag-match gates → **fixed & gated** (version-sync job + `--expect` in both release workflows).
2. **macOS notarization rejected the bundle ("signature of the binary is invalid" on `Python.framework/Python`)** → framework CPython from actions/setup-python; PyInstaller bundles symlinked Python.framework; `tauri build` drops symlinks (tauri-apps/tauri#13219), breaking the `_CodeSignature` seal → uv/python-build-standalone non-framework interpreter + fail-fast PYTHONFRAMEWORK check in `release.yaml:83-101`; repro runbook in SIGNING.md → **fixed & guarded**.
3. **Notarization rejected unsigned sidecars** → Tauri signs the app shell but not resource binaries; ~276 Python Mach-O files unsigned → `tools/sign_macos_sidecars.sh` (commit `3082b182`) wired into Makefile + release workflow between cert-import and tauri build → **fixed**.
4. **2nd macOS release upload 404'd in action-gh-release** → both mac arches emit identically-named `Flowfile.app.tar.gz` updater bundles that collide as release assets → per-arch rename step (commit `df1f6853`, release.yaml lines 154–168); same commit added the prerelease flag for `-` tags → **fixed**.
5. **Release run #83 failed on every matrix job** (commit `a812a326`): (a) Windows/Linux upload paths missed `target/<triple>/release/bundle/`; (b) `{appimage,deb}` brace expansion unsupported by @actions/glob; (c) linuxdeploy AppImage failed on libfuse3-only runners; (d) hard failure when `APPLE_CERTIFICATE` unset; (e) workflow_dispatch runs cut branch-named releases → derived paths from matrix target, libfuse2 + `APPIMAGE_EXTRACT_AND_RUN=1`, guarded cert import, gated release job on `refs/tags/v` → **fixed** (note: AppImage no longer in today's upload globs; only `.deb` ships for Linux).
6. **x86_64 mac job never picked up** → `macos-13` runner retired 2025-12-04 → replaced with `macos-15-intel` (commit `b30300f0`) → **fixed** (runner labels rot; watch for retirements).
7. **Electron-era codesign failure** ("bundle format is ambiguous") → hardenedRuntime signing of PyInstaller Python.framework under electron-builder → signIgnore + `CSC_IDENTITY_AUTO_DISCOVERY=false` in CI (commit `97808864`) → **obsolete** (Electron removed) but explains the fossil `electron-tests-*` required checks.
8. **Invalid Docker tags on feature branches** → `type=sha,prefix={{branch}}-` with `/` in branch names → `prefix=sha-` (commit `8a9b6d52`) → **fixed**.
9. **3.12 matrix job ~56 min** → coverage C-tracer ~2× overhead + serial layout → coverage split into dedicated job with `COVERAGE_CORE=sysmon`, frontend builds dropped from backend jobs, PR concurrency cancellation (commit `093ead72`, #539) → **fixed**; xdist deferred (needs per-worker DB isolation).
10. **`docker-publish` release build of kernel-ml/amd64 failed on v0.12.5** → GHA cache backend `BlobNotFound` while resolving `cache-from: type=gha` → transient; rest of matrix succeeded (`fail-fast: false`), so **that release shipped without a fresh amd64 kernel-ml manifest member** unless re-run → **recurring flake class, no guard**.
11. **Weekly CodeQL failure every Monday** → legacy `codeql.yaml` references non-existent `.github/codeql/codeql-config.yml`; CodeQL default setup (configured 2026-05-30, incl. rust+actions) coexists and succeeds as `dynamic` runs → **open**: the broken workflow file is still tracked on origin/main (verified failures 2026-06-22, 2026-06-29 in 16–18s).
12. **`coverage` job failing on recent main pushes while plain backend-tests pass** → e.g. run 28644693819 (2026-07-03): `FAILED flowfile_core/tests/flowfile/test_code_generator.py::test_join_operation[outer_right_rename_right_no_keep-join_scenario58-flowframe]`; several "Run Tests" failures on main 06-23 → 07-03 → **open / live flake or coverage-only failure** — the last several main pushes have red "Run Tests" runs and merges proceed anyway (admin bypass).

---

## 8. What commonly breaks CI (ranked by observed frequency)

1. Real test failures reaching main anyway (required-checks list doesn't include coverage/test-summary; admin bypass) — main has had 6 failing "Run Tests" runs since 06-23.
2. Coverage-job-only failures (same tests pass in the plain matrix) — currently `test_code_generator.py` join scenarios.
3. Transient GHA cache/backend errors in docker buildx (`BlobNotFound`).
4. Stub / formula-docs drift: editing `flowfile_frame` public API without `make stubs`, or bumping polars-expr-transformer without `make formula_docs` (both produce a git-diff failure with explicit fix instructions).
5. Version drift: touching any of the 5 synced manifests by hand → `version-sync` fails; tagging without bumping → both release workflows die at the `--expect` gate.
6. Poetry lock drift → `poetry check --lock` gate in `e2e-tests.yml:53`.
7. Runner-image rot (macos-13 retirement) and action-glob quirks (no brace expansion) — release-workflow-specific.
8. macOS-only: framework Python creeping in (guarded), leftover DMG mounts locally (`make clean_dmg_mounts` handles).

---

## 9. Verified commands (all run during discovery, or verbatim from CI)

```bash
# Version discipline
python3 tools/check_version_sync.py                      # prints 5 manifest versions; exit 0 = in sync (verified: all 0.12.7)
python3 tools/check_version_sync.py --expect 0.12.7      # release-gate form (tag match)
python tools/bump_version.py X.Y.Z                       # rewrites the 5 manifests (DO NOT run in discovery; modifies files)
make bump-version VERSION=X.Y.Z                          # same via Make
make check-version                                       # poetry-run wrapper of the check

# Drift gates exactly as CI runs them
make check_stubs                                          # regenerate .pyi stubs, fail on git diff
make check_formula_docs                                   # regenerate docs/users/formulas/functions.md, fail on git diff
poetry check --lock                                       # lock-file freshness (e2e-tests.yml gate)

# Test invocations copied from workflows
poetry run pytest flowfile_core/tests -m "not kernel" --disable-warnings
poetry run pytest flowfile_worker/tests --disable-warnings
poetry run pytest flowfile_core/tests -m kernel -vv --tb=long --log-cli-level=INFO
poetry run pytest tests/integration -m docker_integration -vv
poetry run pytest tests/kafka -m kafka -vv                # needs start_redpanda + flowfile_worker running
COVERAGE_CORE=sysmon poetry run pytest flowfile_core/tests -m "not kernel" --cov --cov-append --cov-report=

# Release/tag forensics
git tag --sort=-creatordate | head -30                    # newest tags (v0.12.7 … V0.12.3 … v0.10.1-rc.1)
git for-each-ref --sort=-creatordate --format='%(refname:short) %(creatordate:short)' refs/tags | head -25
git show-ref | grep -E '/main$'                           # exposes the stray `main` TAG (refs/tags/main) causing ambiguity

# Live CI/protection state (read-only gh)
gh run list --repo Edwardvaneechoud/Flowfile --limit 30
gh run list --repo Edwardvaneechoud/Flowfile --workflow=release.yaml --limit 10
gh run view <run-id> --repo Edwardvaneechoud/Flowfile --log-failed
gh api repos/Edwardvaneechoud/Flowfile/branches/main/protection
gh api repos/Edwardvaneechoud/Flowfile/rulesets            # ruleset 2660650 "Only admin commits"
gh api repos/Edwardvaneechoud/Flowfile/code-scanning/default-setup
gh release view v0.12.7 --repo Edwardvaneechoud/Flowfile --json assets
```

---

## 10. Load-bearing exact quotes

- `test.yaml:19-20`: "Cancel superseded in-progress runs for the same PR. Main-branch runs are never cancelled (docker-publish / release pipelines key off completed main builds)."
- `release.yaml:76-82`: "macOS release sidecars MUST be built against a non-framework CPython… Tauri drops symlinks — tauri-apps/tauri#13219 — failing notarization."
- `release.yaml:148-153`: "Tauri names the macOS updater bundle `Flowfile.app.tar.gz(.sig)` with NO architecture in the filename… the symptom is a misleading 404 from action-gh-release on the 2nd upload."
- `release.yaml:217-220`: "Operators must run `tools/make_latest_json.py` (or equivalent) to assemble `latest.json`… We do not automate that here yet — Tauri's updater only activates once both files are present and signed." (Script doesn't exist; no release has the asset.)
- `release.yaml:208-209`: "Tags with a suffix (v0.10.1-rc.1, -test, -beta) are flagged as GitHub pre-releases so test builds never become the public 'Latest'."
- `tools/bump_version.py:63`: "Done. Refresh Cargo.lock (cargo update -p flowfile) and commit."
- `CONTRIBUTING.md:144`: "**Don't force-push to `main`.** Releases build from it." and :143 "**CI must be green** before merge. If a check is flaky, say so in the PR — don't just re-run silently."
- `test.yaml:216-219`: "COVERAGE_CORE=sysmon selects the PEP-669 sys.monitoring tracer (near-zero line-coverage overhead on 3.12, vs the ~2x C-tracer tax that previously doubled the 3.12 matrix job)."

## 11. CLAUDE.md drift found (root CLAUDE.md vs reality)

- Says "12 workflows" → 15 tracked files.
- Says legacy `codeql.yaml` "was removed" → still tracked on origin/main and failing weekly.
- Doesn't mention `claude.yml` / `claude-pr-review.yml` (added `d996e846`, #552).
- Version listed as "0.11.0" in the header → manifests are at 0.12.7.
- CONTRIBUTING.md still claims Windows Polars pin `<=1.25.2`; root CLAUDE.md says that ceiling was removed (pin now `>=1.8.2,<1.40`).
