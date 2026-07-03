---
name: flowfile-docs-review
description: The editorial standard for Flowfile's docs site — the house style (register-2 voice rules), the persona-based nav map (which tab serves which arriving audience), the claim-type→source-of-truth verification index for fact-checking any docs statement against code, and the zero-drift examples contract (how tested .py and .yaml examples are structured, included via snippets, and auto-tested). Use when writing or reviewing any page under docs/, fact-checking a docs claim (node counts, Lite availability, API signatures, ports, providers, defaults), adding a worked example or tutorial, deciding where a new page belongs in the persona nav, or running an editorial/lint pass over docs changes.
---

# Flowfile docs review — style, personas, verification, examples

## When NOT to use this skill

- Site build mechanics, `mkdocs.yml` traps (`use_directory_urls`, nav invisibility, the raw-HTML home page), deploy pipeline, formula-docs regeneration, comment doctrine for source code, CLAUDE.md maintenance → `flowfile-docs-and-writing` (this skill owns *what good pages say and how to verify it*; that one owns *how the site is built*).
- CI gates and release mechanics → `flowfile-change-control`.
- Per-package test commands and Docker fixture mechanics beyond the examples contract → `flowfile-testing-and-validation`.

## 1. House style (register 2 — the only register for new/edited pages)

The canonical exemplar is `docs/ai/index.md`. Rules, all checkable:

1. **Declarative present tense, second person where needed.** No first-person-plural marketing ("we've got you covered"), no hype adjectives (powerful, seamless, blazing, incredible, professional-grade), no exclamation-mark enthusiasm, no unverifiable stats ("thousands of users").
2. **No emoji** in headings, bullets, or feature lists. No `## **Bold-in-heading**` markdown.
3. **No inline-styled HTML blocks** in `.md` pages (gradient divs, `clamp()` fonts, `style=` attributes). Visual variety is wanted — plain-markdown-only pages read as flat — but it comes exclusively from the shared brand layer: Material **grid cards** (`<div class="grid cards" markdown>`, brand accent bar styled globally in `docs/stylesheets/extra.css`), **content tabs** (`=== "Label"` — use for install methods and platform variants), **icon shortcodes** (`:material-*:` / `:octicons-*:` via `pymdownx.emoji`), the `ff-paths`/`ff-path-teal`/`ff-path-purple` chooser classes from `extra.css`, admonitions, and mermaid. New visual components get a class in `extra.css` (both color schemes), never a `style=` attribute. Section indexes and audience routers should use cards; high-traffic pages (quickstart, guides-by-audience) carry the most visual weight, deep reference pages the least.
4. **No "coming soon" / roadmap promises.** Document what ships. Aspirational notes only as a short "Future direction" admonition on developer pages, clearly labeled.
5. **Material admonitions** (`!!! note "Quoted Title"`, `tip`, `warning`, `info`) for asides; roughly ≤2 per screenful.
6. **Exact names everywhere**: UI labels as the component renders them (Group By's average is **Mean**, `GroupBy.vue`), API symbols as actually exported, file paths and env vars in backticks. When docs and UI disagree, the Vue component is the truth.
7. **Numbers rot — prefer pointers.** "See `configs/node_store/nodes.py`" beats "46 nodes". A literal count needs an entry in §3's index and, if fast-rotting (versions, model names, node counts), a date stamp ("as of 2026-07").
8. **Runnable code is never hand-written in a page.** Any Python block presented as runnable is a `--8<--` include from `docs/examples/` (§4). Inline fragments are allowed only for formulas, UI config values, or shell one-liners — with verified syntax.
9. **Every page opens with one orientation paragraph**: who it's for, what they'll be able to do after reading. Then short sections; tables for enumerable facts with the explanation in surrounding prose.
10. **Links**: relative `.md` links between pages (MkDocs rewrites them); raw `.html`/directory forms only inside `docs/index.html`. Every new page gets a `nav:` entry; moved/merged URLs get a `redirects` plugin mapping in `mkdocs.yml`.
11. **Images**: never create or edit image files; never block on one. Reuse an existing `docs/assets/images/` asset if apt, else leave `<!-- IMAGE-PLACEHOLDER-TO-CHANGE: what the shot should show -->` at the spot. In step-by-step walkthroughs, per-step screenshots go in fold-outs so they don't break the reading flow (maintainer-preferred pattern): `<details markdown="1"><summary>See it: …</summary>` wrapping the image plus its placeholder/refresh comment.
12. **Cross-page consistency beats local polish**: one canonical statement per fact, siblings link to it (the join-type list, the Lite node inventory, and the custom-node `process()` signature have each previously diverged across three pages).
13. **No filler advice — maintainer-rejected pattern.** Generic "Tips for Success" bullet lists ("Start simple", "Save regularly", "Preview often", "Use descriptions", "Try both modes") are banned. If a tip isn't specific to the page's subject and non-obvious, cut it. Never state the obvious ("click Run to run the flow").
14. **Length is a quality dimension.** Each page earns its length: getting-started/how-to pages are short concrete numbered steps; reference pages are complete tables; concept pages explain once and stop. If a section restates what an adjacent section or page already says, cut or link.
15. **Model division of labor**: subagent drafts are raw material. The final shipped text of user-facing pages is written/edited by the lead (Fable) after a dedicated scan for filler, obviousness, length, and cross-page consistency.

## 2. Persona nav map (target IA)

One tab per arriving audience; the Home page routes with persona cards (one hop).

| Tab | Arriving reader | Owns |
|---|---|---|
| Home (`index.html`) | everyone | value prop, sales-pipeline showcase (tested flow download + demo.flowfile.org), persona router |
| Get Started | new user, any kind | `quickstart.md` (install + first visual flow + first Python pipeline), Coming from Excel, Flowfile Lite |
| Visual Editor | analysts building flows | overview, building flows, formulas (+ generated function reference), node reference (6 categories), kernels, node designer, worked examples |
| Connect Your Data | "my data lives elsewhere" | connector matrix, connections & secrets, databases, cloud storage (S3/ADLS/GCS), Kafka, REST APIs & Google Analytics |
| Catalog & Automation | analyzing/operating data | catalog, virtual tables, SQL editor, visualizations, schedules, subflows, projects & git |
| Python API | Python developers | quickstart, concepts, reference, tutorials (all examples tested) |
| AI Assistant | any | feature catalog, provider setup (BYOK) |
| Deploy & Operate | admins | desktop, pip, Docker (single merged page), users/groups/sharing, headless runs & CLI |
| For Developers | contributors | architecture, internals, kernel, AI architecture, custom nodes |

The **analysis** journey (analyze data without building pipelines) is deliberately multi-hooked: Home persona card → Catalog tab; quickstart's visual track ends in Catalog Writer → SQL editor → visualization; `flowfile seed-demo` documented as the one-command populated catalog.

## 3. Claim-type → source-of-truth index

Verify against code, never against another prose doc (READMEs and CLAUDE.md drift too). The most drift-prone claim types and where each is decided:

| Claim about | Source of truth |
|---|---|
| Core node types, categories, laziness, narrow/wide | `flowfile_core/flowfile_core/configs/node_store/nodes.py` (`get_all_standard_nodes`; plus dict-only `polars_lazy_frame`) |
| Lite/WASM node availability | `flowfile_wasm/src/config/nodeDescriptions.ts` + `flowfile_wasm/src/components/Canvas.vue` `nodeCategories` (`available: false` flags). 23 usable nodes as of 2026-07 — historic "18" was wrong |
| Join strategies, fuzzy algorithms, group-by agg options | `flowfile_core/.../schemas/transform_schema.py` (`JoinKeyStrategy` = inner/left/right/full/semi/anti/outer; `FuzzyTypeLiteral` = 6 algorithms); UI labels in `GroupBy.vue` (`mean` → "Mean") |
| Node settings fields, file formats, write modes | `flowfile_core/flowfile_core/schemas/input_schema.py`; cloud: `cloud_storage_schemas.py` (`CloudStorageType` s3/adls/gcs; `AuthMethod` — the CLI literal is `aws-cli`, hyphen; read formats include `iceberg`) |
| `ff.*` availability | `flowfile/flowfile/__init__.py` — NOT the same as `flowfile_frame/flowfile_frame/__init__.py` (e.g. `read_ipc`/`read_ndjson`/`read_avro` exist in frame but are not re-exported as `ff.*`) |
| Expression methods | `flowfile_frame/flowfile_frame/expr.py` + pinned Polars (renames: `cum_sum` not `cumsum`, `weekday` not `day_of_week`; no `FlowFrame.drop_duplicates`/`vstack`/`__len__`) |
| AI providers and models | `flowfile_core/flowfile_core/ai/providers/registry.py` (6 BYOK + local pseudo-provider); per-provider `default_model` vs per-surface models differ — Groq default is `qwen/qwen3-32b` |
| CLI verbs and flags | `flowfile/flowfile/__main__.py` (run ui/core/worker/flow, seed-demo, remove-demo, project init/open/save) |
| Ports | `flowfile/flowfile/api.py`, `shared/storage_config.py`; the web UI is hard-locked to 63578 (`flowfile/web/__init__.py` raises on any other port — `FLOWFILE_PORT` does not move it) |
| Health probes | `flowfile_core/routes/public.py` — `/health/status`; there is no `/health` on core or worker |
| Kernel images and defaults | `flowfile_core/flowfile_core/kernel/manager.py` (image tags), `kernel/models.py` (default memory 4 GB, CPU 2) |
| Password/auth policy | `flowfile_core/auth/password.py` (8 chars + number + special; no case rules) |
| Storage paths per mode | `shared/storage_config.py` + `docker-compose.yml` overrides (`FLOWFILE_USER_DATA_DIR=/app/user_data` in shipped compose) |
| Docker deployment facts | `docker-compose.yml` itself (volume `flowfile-internal-storage`, `shm_size`, scheduler enabled in shipped compose, `FLOWFILE_INTERNAL_TOKEN` required) |
| Catalog internals | `flowfile_core/flowfile_core/catalog/` (services/, `constants.py` — thumbnail cap 500 KB, SQL recursion limit 5) |
| Flow save format | `.yaml` default (`.yml`/`.json` accepted; `.flowfile` is legacy pickle, open-only) — `flowfile/manage/io_flowfile.py` |
| Formula functions | generated `docs/users/formulas/functions.md` (never hand-edit; `make formula_docs`) |
| App version | root `pyproject.toml` only — never hardcode in prose |

## 4. Zero-drift examples contract

Two example kinds, two automatic gates each (pytest at runtime, `pymdownx.snippets check_paths: true` at build time).

**Visual flow examples** — committed at `data/templates/flows/<id>.yaml`:
- Carry `_template_meta` (template_id, name, category Beginner/Intermediate/Advanced, tags, node_count, icon) and `_required_csv_files`; read nodes use `__TEMPLATE_DATA_DIR__/<file>.csv`.
- Auto-tested with zero new code: `flowfile_core/tests/templates/test_template_flows.py` globs the directory, validates, substitutes the placeholder, opens via `open_flow`, runs with `execution_location="local"` (no worker), asserts success.
- Auto-surfaced in the in-app template browser (`GET /templates/`, `POST /templates/{id}/create`).
- Author flows by building them in-process and `FlowGraph.save_flow()` — never hand-write node bodies.

**Python examples** — committed at `docs/examples/<name>.py` (Docker-dependent: `docs/examples/integrations/`):
- One-line docstring; doc-visible code between `# --8<-- [start:example]` / `# --8<-- [end:example]`; real assertions BELOW the end marker (tests run them, pages never show them).
- Use `import flowfile as ff` and only `ff`-namespace exports; repo-root-relative data paths (`data/templates/...`); runner pins CWD to repo root.
- Runner: `flowfile_core/tests/docs_examples/test_docs_examples.py` (glob-parametrized; integrations gated on `test_utils` fixture availability — real Postgres/MinIO/etc., no mocks).
- Pages include with `--8<-- "docs/examples/<name>.py:example"`.

**Data**: only committed, seeded datasets (`data/templates/*.csv` via `generate_template_data.py`; new generators use their own `random.Random(n)` and are called last so existing CSVs stay byte-identical — verify with `git diff`). Assert exact values for deterministic transforms; schema/shape only for ML and fuzzy outputs.

**Add-a-worked-example recipe** (the "blog post" flow — no new test code, ever):
1. Pick/extend a committed dataset.
2. Build the flow in-process, save, placeholder-ize paths, add `_template_meta` → drop into `data/templates/flows/`.
3. Optional Python twin in `docs/examples/`.
4. Run `pytest flowfile_core/tests/templates/ flowfile_core/tests/docs_examples/ -q`.
5. Write the page from the fixed skeleton (§5) under the owning persona tab; add the `nav:` entry and a gallery-index row.

## 5. Worked-example page skeleton (fixed)

```markdown
# <Outcome-phrased title, e.g. "Deduplicate and summarize sales data">

<One paragraph: what you'll build, which reader this serves.>

**Flow:** [`<id>.yaml`](https://github.com/edwardvaneechoud/Flowfile/blob/main/data/templates/flows/<id>.yaml) ·
In-app: Create → From template → "<name>" · Data: `data/templates/<file>.csv`

<!-- IMAGE-PLACEHOLDER-TO-CHANGE: finished flow on the canvas -->

## The data          <!-- column table + one line on shape -->
## The flow          <!-- numbered node walkthrough, exact config values -->
## Run it            <!-- template browser; download; headless: flowfile run flow -->
## The result        <!-- expected output rows/schema, quoted from the tested run -->
## In Python         <!-- optional: --8<-- include of the tested twin -->
## Variations        <!-- 2–3 pointers: swap your data, next nodes -->
```

## 6. Review checklist (run per touched page)

0. Filler scan: zero generic tip lists, zero stating-the-obvious lines, no section that merely restates a sibling (§1.13–14). How-to pages read as short concrete steps.
1. Every factual claim is timeless or verified against §3 (spot-check at least the counts, labels, signatures, defaults).
2. Every runnable code block is a snippet include; inline fragments use verified syntax/labels.
3. Zero register-1 markers: emoji headings/bullets, hype adjectives, inline-styled divs, "coming soon", unverifiable stats.
4. Links resolve; anchors exist; page is in `nav:`; moves have redirect mappings.
5. Numbers are pointers or date-stamped.
6. Image spots are placeholders or existing assets — no image files created/edited.
7. `FLOWFILE_SKIP_STARTUP_MIGRATION=1 poetry run mkdocs build` exits 0 with no new WARNINGs (snippets `check_paths` makes missing includes fatal).
8. If examples were touched: `poetry run pytest flowfile_core/tests/templates/ flowfile_core/tests/docs_examples/ -q` green.

## Provenance

Distilled from a full-site audit + adversarial fact-check (361 claims verified against source) and a product-surface sweep, 2026-07-03, app version 0.12.7. The §3 index entries are the exact locations that resolved those claims. Re-verify fast-rotting values (counts, versions, model names) against their §3 source before quoting them in new prose.
