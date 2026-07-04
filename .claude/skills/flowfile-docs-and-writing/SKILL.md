---
name: flowfile-docs-and-writing
description: How the Flowfile docs site (MkDocs + Material, the docs/ tree, mkdocs.yml nav) is organized, built by documentation.yml, and deployed to GitHub Pages; the repo-wide comment doctrine for code (and its two verbose exceptions); and the CLAUDE.md maintenance protocol across all 9 package guides. Use when writing or moving a page under docs/, editing mkdocs.yml's nav, linking between docs pages, regenerating docs/users/formulas/functions.md, debugging a Documentation workflow that's silently missing a page or won't build, deciding whether a line of code needs a comment, or updating any CLAUDE.md file after a feature lands.
---

# Flowfile docs & writing conventions

## When NOT to use this skill

- Editorial standard for docs pages (house style/voice rules, the persona nav map, fact-checking a docs claim against code, the tested-examples contract and add-an-example recipe) → `flowfile-docs-review` (this skill owns *how the site builds*; that one owns *what good pages say and how to verify it*).
- CI gates that happen to touch docs (`check-formula-docs`, `check-stubs` jobs, version-sync) and release mechanics → `flowfile-change-control` (this skill explains *what* the formula-docs generator does and the doc-authoring traps; that skill owns *why the CI job exists and what blocks a merge*).
- flowfile_frame public API design, `.pyi` stub authoring, expression codegen → `flowfile-frame-and-codegen`.
- Writing/reviewing a custom node's `NodeSettings` docstrings for the Node Designer UI → `flowfile-node-development`.
- Vue/TS component conventions, ESLint/Prettier setup → `flowfile-frontend-conventions`.
- AI prompt-string authoring (system prompts, tool descriptions) → `flowfile-ai-subsystem` (this skill only tells you prompt strings are exempt from the comment doctrine, not how to write them).
- Env var / feature-flag reference tables → `flowfile-config-and-flags`.
- Chasing a specific historical bug for its own sake → `flowfile-failure-archaeology`.

---

## 1. Docs inventory — what exists, and where a new page goes

Two documentation surfaces exist; don't confuse them:

| Surface | Source | Audience |
|---|---|---|
| **MkDocs site** (`docs/`) | Markdown pages + `docs/index.html` (count them: `find docs -name '*.md' | wc -l`), built by `mkdocs.yml`, deployed to `https://edwardvaneechoud.github.io/Flowfile/` | End users (visual editor, Python API) and external contributors |
| **CLAUDE.md files** (9: root + 8 packages) | Not part of the MkDocs build; plain files read by AI agents and human contributors | Anyone working *inside* the repo — see §6 |

The `docs/` tree structure mirrors `mkdocs.yml`'s `nav:` block almost exactly (verify by diffing them — see §3.3 for the one place they currently disagree). Top-level layout:

| Path | What goes there |
|---|---|
| `docs/index.html` | The site home — hand-written HTML, not Markdown. See the trap in §3.1 before touching it. |
| `docs/quickstart.md` | The single "get started in 5 minutes" page — both the no-code and Python-API paths live here. |
| `docs/community.md` | Discussions-vs-Issues routing, contribution ways, Contributor Covenant. |
| `docs/users/` | **End-user guides**: how to use a shipped feature. `formulas/` (formula language + generated function reference), `visual-editor/` (nodes, catalog, kernels, tutorials), `python-api/` (concepts, reference, tutorials), `deployment/` (Lite/desktop/pip/Docker). New user-facing feature docs go here. |
| `docs/ai/` | The AI Assistant feature catalog + BYOK provider setup. AI *architecture* docs (for contributors) go under `for-developers/`, not here. |
| `docs/for-developers/` | **Contributor/internals docs**: architecture, `FlowGraph`/`FlowNode`/`FlowDataEngine` internals, kernel architecture, AI architecture, mkdocstrings-driven internal API reference, custom-node tutorials. New "how this subsystem works" docs go here. |

Decision rule: if the reader is *using* Flowfile (drag-drop or `flowfile_frame`), write under `users/`. If the reader is *extending or debugging* Flowfile, write under `for-developers/`. If in doubt, look at which register the neighboring pages use (§4) — that tells you the audience faster than the folder name does.

Every new page must be added to `mkdocs.yml`'s `nav:` block by hand — MkDocs does not auto-discover nav entries from the `docs/` tree (§3.3 has a live example of what happens if you forget).

---

## 2. How the docs site builds and deploys

- **Engine**: MkDocs 1.6.1 + Material theme (deps come from the root Poetry **dev group** — `pyproject.toml`'s `mkdocs`, `mkdocs-material`, `mkdocstrings`, `mkdocstrings-python`, `griffe`, `griffe-pydantic`. `docs/requirements.txt` is a 0-byte leftover — nothing reads it).
- **Plugins**: `search`; `mkdocstrings` (Python handler, `griffe_pydantic` extension with `schema: true` — this is what renders Pydantic "Fields"/"Validators" sections on `for-developers/python-api-reference.md`, which documents `flowfile_core` classes like `FlowGraph`/`FlowNode`/`FlowDataEngine` and the Pydantic schemas).
- **Markdown extensions**: `attr_list`, `md_in_html`, `admonition`, `pymdownx.details` (the `<details markdown="1">` collapsibles you'll see all over `quickstart.md`), `pymdownx.superfences` with a custom `mermaid` fence, `footnotes`.
- **CI**: `.github/workflows/documentation.yml`, triggered on push/PR to `main` touching `docs/**`, `mkdocs.yml`, `flowfile_frame/**/*.py`, or `tools/generate_formula_docs.py` (plus `workflow_dispatch`).
  - `build` job: Python 3.11 → `poetry install --with dev` → **`make check_formula_docs`** (drift gate, see §3.4) → `poetry run mkdocs build` → uploads `site/` as an artifact.
  - `deploy` job (push-to-`main` only): `poetry run mkdocs gh-deploy --force` → publishes to the `gh-pages` branch → GitHub Pages. There is no staging environment; a merge to `main` that touches docs paths goes live on the next `deploy` run.
- **Build locally**: `poetry run mkdocs build` — this is exactly what CI runs and reproduces every warning CI would see. Live-reload with `poetry run mkdocs serve`.

**Gotcha — local build touches your live catalog DB.** `for-developers/python-api-reference.md` uses mkdocstrings to introspect `flowfile_core` classes, which means `mkdocs build` **imports `flowfile_core`** as a side effect of building the docs. Importing `flowfile_core` runs `validate_setup()` + `init_db()` at import time, including the Alembic startup migration, against whatever DB `get_database_url()` resolves to — your real local `flowfile_catalog.db` if you haven't set anything. Set `FLOWFILE_SKIP_STARTUP_MIGRATION=1` before running `mkdocs build`/`mkdocs serve` locally, same as any other diagnostic import of `flowfile_core`.

**Gotcha — the build does not fail on warnings.** `documentation.yml` runs plain `poetry run mkdocs build`, with no `--strict` flag. Orphaned nav pages, broken in-page anchors, and griffe docstring-parsing warnings all print as `INFO`/`WARNING` lines and the job still exits 0. A green Documentation workflow run is *not* proof the docs are internally consistent — read the log, don't just trust the check mark.

---

## 3. Traps — read before touching `docs/`

### 3.1 The site home is `docs/index.html`, not `index.md`

`mkdocs.yml`'s `nav:` starts with `- Home: index.html`. The home page is a hand-authored HTML file (raw `<a href>` tags, inline CSS using Material's CSS custom properties) — MkDocs supports mixing static HTML pages into the nav alongside Markdown, and this repo uses that for the landing page. If you "fix" this by adding a `docs/index.md` and repointing the nav, you change the entire site's home page and lose all the hand-tuned hero/feature-grid markup in one edit. If the home page needs a content change, edit `docs/index.html` directly.

### 3.2 `use_directory_urls: false` — know which links need `.html`

`mkdocs.yml` sets `use_directory_urls: false`, so **every non-index page publishes as a flat `<name>.html` file**, not a directory with an `index.html` inside (e.g. `quickstart.md` → `quickstart.html`, reachable at `.../quickstart.html`, *not* `.../quickstart/`). Two different rules apply depending on where the link lives:

- **Inside a `.md` page**, write relative links the normal way, ending in `.md` (e.g. `[Provider Setup](providers.md)`, `[Visual Editor](visual-editor/index.md)`) — MkDocs' Markdown processor rewrites these to the correct published extension at build time regardless of the `use_directory_urls` setting. This is why the existing pages are full of `.md`-suffixed relative links and they all work.
- **Inside `docs/index.html`** (raw HTML, not run through the Markdown link-rewriter) **or any hardcoded absolute URL outside the docs build** (README.md, CONTRIBUTING.md, a package README linking to the hosted site), you must write the *actual published path* by hand: flat pages need the literal `.html` suffix (`href="quickstart.html"`, not `href="quickstart/"` — the latter 404s because no such directory exists), while section index pages can use a trailing-slash directory form (`href="users/"`, `href="for-developers/"`) because `index.html` is always served as the directory default regardless of `use_directory_urls`. `docs/index.html` itself mixes both forms correctly today — use it as the reference. `CONTRIBUTING.md` links to `https://edwardvaneechoud.github.io/Flowfile/community.html` for the same reason.

### 3.3 Forgetting the `nav:` entry makes a page invisible, not broken

MkDocs builds every `.md` file under `docs/` whether or not it's listed in `mkdocs.yml`'s `nav:` — a missing nav entry does **not** fail the build (§2's "no `--strict`" gotcha) and does **not** stop the page from existing at its URL if something else links to it directly. It just means the page is absent from the sidebar and site search weighting. The build prints an `INFO` line ("The following pages exist in the docs directory, but are not included in the 'nav' configuration") and still exits 0. If you add a new page and it doesn't show up in the sidebar after a build, this is almost certainly why — check `mkdocs.yml`'s `nav:` block before assuming something is actually broken. (The long-standing live example, an orphaned `for-developers/docker-deployment.md`, was merged into `users/deployment/docker.md` in the 2026-07 docs rebuild, with a `redirects`-plugin mapping preserving its URL — as of that rebuild the nav is orphan-free, and the redirect map in `mkdocs.yml` is where moved pages get recorded.)

### 3.4 The formula reference is generated — never hand-edit it

`docs/users/formulas/functions.md` starts with `<!-- AUTO-GENERATED by tools/generate_formula_docs.py — do not edit. Run 'make formula_docs'. -->` and documents all 95 functions of the Flowfile formula language, sourced from **`polars-expr-transformer`'s own docstrings** (the library is upstream of this repo; bumping its pin is what usually triggers drift here).

```bash
make formula_docs         # regenerate docs/users/formulas/functions.md from the current polars-expr-transformer pin
make check_formula_docs   # formula_docs + `git diff --exit-code` on that file — what CI runs
```

If you bump the `polars-expr-transformer` version pin (or edit `tools/generate_formula_docs.py`) and forget to run `make formula_docs`, `documentation.yml`'s `check-formula-docs` gate fails on a diff in a file you never touched by hand. This is CI-gate *mechanics* — for how the gate fits into the broader change-control picture (required checks, what blocks a merge), see `flowfile-change-control`.

### 3.5 Sphinx leftovers are dead weight — the site is MkDocs only

`docs/conf.py` (Sphinx config, `release = "0.1.2"`, `sphinx.ext.autodoc`/`napoleon`/`sphinx_rtd_theme`), `docs/MakeFile` (note the capital-F — a Sphinx makefile, not GNU Make's `Makefile`), and `docs/requirements.txt` (0 bytes) are all vestigial from before the project switched to MkDocs. Nothing in CI or the Makefile invokes `sphinx-build` or `docs/MakeFile`. Don't run them, don't "fix" `conf.py`'s stale version, and don't add Sphinx-flavor `.rst` files expecting them to build — they won't be picked up by anything.

---

## 4. House style for docs pages

Since the 2026-07 docs rebuild there is **one register**: tight declarative prose, Material admonitions (`!!! info`, `!!! tip`, `!!! warning`, with a quoted title), comparison tables, exact file paths and symbol names, explicit invariant statements, mermaid diagrams where a picture beats prose. The old marketing register (emoji headers, gradient `<div>` cards, `clamp()` inline CSS, hype prose) was eliminated page-by-page — do not reintroduce it. The full editorial standard — voice rules (including the ban on generic "Tips for Success" filler), the persona nav map, the claim→source verification index, and the tested-examples contract — lives in `flowfile-docs-review`; read that skill before writing or reviewing any page.

Admonition syntax (register 2's signature move):

```markdown
!!! note "Registered Flows Required"
    `publish_global` requires the flow to be registered in the catalog. It is not available in interactive (cell) mode.

!!! warning "Docker Socket"
    The kernel manager needs access to the Docker socket to spawn containers.
```

Package-level READMEs (`flowfile_core/README.md`, `flowfile_worker/README.md`, `flowfile_frontend/README.md`, `flowfile_wasm/README.md`) use their own feature-list-with-emoji register — that's a separate, narrower convention scoped to "here's what this package does," not the MkDocs site's house style. Don't import docs-site register 2 conventions into a package README wholesale; keep it short.

---

## 5. Comment doctrine for code (not docs prose)

This governs comments **inside source files** (Python, TS/Vue, Rust) — it is unrelated to the docs-site house style in §4, which is about prose on rendered pages.

Root `CLAUDE.md`'s rule, verbatim: *"Keep comments minimal. Prefer self-explanatory code; add a comment only for non-obvious *why*. No long explanatory blocks or multi-line header comments — one short line at most. This applies to all languages."*

This is maintainer-enforced, not aspirational — commit `81322109` ("Remove the comments (#492)", 2026-06-04) deleted comments across **530 files, net ~6,800 lines removed** (7,309 deletions vs 533 insertions), and smaller follow-up commits (`09f5d8c5` "Tighten comments", `2cf1c6ad` "cleanup comments") have kept trimming since. Treat any newly-added comment through this lens:

| Keep | Cut |
|---|---|
| A non-obvious **why** ("cursor advances to now, never backfill — DST/catch-up tests pin this") | Restating what the next line already says |
| A **cross-service contract** ("must stay byte-for-byte in sync with worker's `secrets.py`") | Step-N narration ("# Step 1: validate input") |
| An **intentionally-NOT** invariant ("NOT a KDF — deliberate for 256-bit tokens, see CLAUDE.md") | Banner/header comment blocks |
| A **gotcha** a future editor would otherwise rediscover the hard way | A history essay about how the code got this way |

One short line, max. If you need more than one line to explain something, that's a signal the explanation belongs in the relevant `CLAUDE.md`'s Gotchas section (§6), not inline in the source.

**Two deliberate exceptions stay verbose — do not apply the trim-it doctrine to these:**

- **`flowfile_frame` public docstrings** (`FlowFrame`, `Expr`, and their public methods). These feed two downstream consumers: the `.pyi` stub generators lift each method's first docstring line verbatim into the committed stub as a comment (`expr_stub_generator.py`/`flow_frame_stub_generator.py` read `method.__doc__`), and they're the runtime `help()`/IDE-hover surface for anyone using the library directly. Full multi-paragraph docstrings here are the contract, not narration to be pruned.
- **AI prompt strings** (`flowfile_core/flowfile_core/ai/prompts/`). Long, example-laden system prompts are load-bearing scaffolding for the LLM, not comments — cutting worked examples to "tighten" a prompt measurably degrades agent behavior. This is a distinct subject from source-code comments; see `flowfile-ai-subsystem` for prompt-authoring guidance.

---

## 6. CLAUDE.md maintenance protocol

There are exactly **9 CLAUDE.md files**: root, plus one in each of `flowfile_core/`, `flowfile_worker/`, `flowfile_frame/`, `flowfile_frontend/`, `flowfile_scheduler/`, `flowfile_wasm/`, `kernel_runtime/`, `shared/`. (`flowfile/`, `test_utils/`, `tools/`, and `build_backends/` have no CLAUDE.md — they're covered only by the root file and their own code.)

Every package file follows the same template — match it when adding or editing one:

```
# CLAUDE.md - <package>
<one-sentence role> — "Package-specific notes; see the root /CLAUDE.md for monorepo-wide …"

## Role
## Layout
## Key patterns & conventions
## Running / entry points
## Testing
## Gotchas
## Key files
```

Style: dense, imperative, bold-lead the load-bearing rules, cite exact symbols and `file.py:line`, no fluff, no restating what's in the root file. The **Gotchas** section is where a non-obvious *why* that's too long for a source comment (§5) belongs — CLAUDE.md is the doc surface for exactly that content.

**These files ARE actively maintained per feature** — this isn't a stale artifact. `git log -- '*/CLAUDE.md'` shows updates tied to real merges: RBAC (#502), explicit-only execution (#521), project git-tracking (#524), notebook environment (#538), object-storage-in-catalog (#555). When you land a feature that changes a package's contracts, patterns, or gotchas, update its CLAUDE.md in the same PR — that's the working norm here, not optional busywork.

**What rots, and the fix:** exact literal counts and versions age out within weeks even with per-feature maintenance, because they require a *separate*, cross-cutting sweep that per-feature edits don't naturally trigger (root `CLAUDE.md`'s app version, workflow-file count, Alembic migration range, WASM node count, and several file line-counts were all stale as of 2026-07-03, some by months). When writing or editing a CLAUDE.md:

- **Prefer a pointer over a literal count.** "See `pyproject.toml` for the pinned version" beats "version 0.12.7." "`ls flowfile_core/flowfile_core/alembic/versions/`" beats "28 migrations." "See `.github/workflows/`" beats "15 workflow files."
- If a literal count is genuinely useful inline (e.g. "6 pytest markers"), date-stamp it ("as of 2026-07-03") so a reader knows to re-verify rather than trust it silently.
- Numbers that appear in **multiple places and disagree** are a strong signal something needs a pointer instead of a copy: this repo's WASM node count is quoted differently in four separate places (root `CLAUDE.md`, `docs/users/deployment/lite.md`, the top-level `README.md`, and `flowfile_wasm/CLAUDE.md`) — none of them agree, and an agent trusting any single one of them in isolation will be wrong. Verify counts against code (`grep`/`ls`/`wc -l`), never against another prose doc.

---

## 7. What the docs site does not tell contributors

Gaps worth knowing so you don't assume something is documented when it isn't, and don't accidentally treat an undocumented-but-real workflow as broken:

- **No PR template, no feature-request issue template.** The only issue template (`.github/ISSUE_TEMPLATE/bug_report.md`) is GitHub's unmodified default — it still asks for "Smartphone… Device: [e.g. iPhone6]," which makes no sense for a desktop/server ETL tool. `CONTRIBUTING.md`'s prose ("what changed, why, how you tested it; screenshots for UI changes") is the only PR-description guidance that exists.
- **No `SECURITY.md`** at the repo root or in `.github/`. The private-vulnerability-reporting policy (GitHub's private "Report a vulnerability" form) is documented only inside `CONTRIBUTING.md`'s "Reporting security issues" section.
- **`claude.yml` and `claude-pr-review.yml`** (interactive `@claude`-mention agent, and an automatic Claude code review posted on every non-draft PR) are real, tracked workflow files in `.github/workflows/` but are not mentioned in any docs page or in `CONTRIBUTING.md`'s workflow list — a first-time contributor gets an automated review with no explanation of where it came from.
- ~~Flow-in-flow, project git-tracking, group-based sharing undocumented~~ — closed by the 2026-07 rebuild: `users/visual-editor/subflows.md`, `users/projects.md`, `users/deployment/sharing.md`, plus `users/deployment/cli.md` (headless runs), `users/connect/{index,kafka,apis}.md`, and `users/coming-from-excel.md` now exist and are in nav.

If you're asked to fill one of these gaps, write it in register 2 (§4), put user-facing content under `users/` and internals under `for-developers/`, and don't forget the `nav:` entry (§3.3).

---

## Provenance and maintenance

All facts above were spot-verified in this repo on 2026-07-03 against app version 0.12.7. Re-run before trusting a stale copy of this skill:

```bash
# Docs site config and orphan-page check (reproduces the exact CI build)
poetry run mkdocs build 2>&1 | grep -E "not included in the|^ERROR"
# expect: no orphan pages and no line starting "ERROR" (orphans were eliminated in the 2026-07 rebuild)

# Confirm the formula reference is still generated (not hand-edited) and current
head -1 docs/users/formulas/functions.md   # expect the AUTO-GENERATED header
make check_formula_docs                    # expect "Formula docs are in sync."

# Confirm use_directory_urls and the raw-HTML home page are unchanged
grep -n "use_directory_urls\|- Home:" mkdocs.yml

# Confirm Sphinx leftovers are still untouched/unused
grep -rn "sphinx-build\|docs/MakeFile\|docs/conf.py" Makefile .github/workflows/  # expect no hits

# Re-check the comment-doctrine purge and current wording
git show --shortstat 81322109                      # "Remove the comments (#492)"
grep -n -A4 "^### Comments" CLAUDE.md

# Re-verify the 9 CLAUDE.md files and per-package template headings
find . -maxdepth 2 -name CLAUDE.md | sort
grep -n "^## " flowfile_worker/CLAUDE.md            # expect Role/Layout/Key patterns & conventions/Running / entry points/Testing/Gotchas/Key files

# Re-check the docs contributor-facing gaps
ls .github/ISSUE_TEMPLATE/                          # expect only bug_report.md (default template)
find . -maxdepth 2 -iname SECURITY.md                # expect no hits
grep -rli "flow-in-flow\|subflow" docs/              # expect no real hits (verify any match isn't a false positive)
```

Facts that will rot fastest — re-check every time, don't trust a cached number: current app version, the exact `nav:`/orphan-page state (someone may have fixed §3.3's example), the CLAUDE.md file count if a new package is added, and any node/workflow/migration count quoted in prose anywhere in the repo (this repo's numbers disagree with each other more often than not — always verify against code, never against another doc).
