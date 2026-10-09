# Plan: Simple build writes FlowFrame code, core turns it into nodes

## Context

Simple build (`POST /ai/generate`, `ai/local_model/oneshot.py`) asks the model for a bespoke
`{nodes, edges}` JSON object whose `settings` mirror Flowfile's Pydantic node schemas. Measured on
2026-10-08 with the on-device Qwen3.5-4B (thinking off), three representative requests:

| Prompt style | Latency per request | Result |
|---|---|---|
| JSON spec (today) | 6–8 s | 3/3 parsed, correct node chains |
| FlowFrame code (`import flowfile as ff`) | ~2.7 s | 3/3 correct and readable; one invented call (`ff.read_json`) |

LLMs have seen far more Polars-shaped code than Flowfile settings JSON, so the code is faster, better
structured and self-explaining. Flowfile already has the machinery to turn that code into canvas nodes
**without executing it**: the canvas notebook's clean run (`notebook/interpret.py` reads a cell with
`ast` + `symtable` and only calls what `notebook/allowlist.py` names; `tests/notebook/test_contract.py`
proves no cell text reaches `exec`/`eval`/`compile`). The notebook already renders every flow back as
exactly this dialect, so the model's output is the same language the user sees in the Notebook pane.

Decisions taken:

- Simple build gains a third mode, `code`, which becomes the default of the chat drawer's Simple build.
  The JSON `simple` and the validating `one_shot` modes stay reachable through `mode=` on the request.
- The generated code is shown in the chat bubble (collapsed) above the "Add to canvas" button.
- Nothing the model writes ever runs in core. The clean run is the only reader of the code, and every
  node that would carry executable text (`polars_code`, `python_script`, `sql_query`) or reach a
  stored resource (catalog, database, cloud, Kafka, REST, subflows, custom nodes) is refused up front.

Constraints:

- Prompt doctrine: new prompt file, additive only to existing ones (`local_oneshot.md` is untouched).
- `require_notebook_sync` makes plan/push admin-only in docker/package because the frame's catalog
  lookups do not check the caller's grants. This feature must not reopen that gap (see Part 2).
- No `git commit` by the agent; commands are handed over at the end.

## Part 1: the code prompt

1. **New prompt** `prompts/code_build.md` (provider-agnostic; the on-device model is the hardest
   customer, so it is written for a 4B): the user describes a pipeline, the model outputs one fenced
   `python` block, `import flowfile as ff`, one assignment per step, no `collect()`, no writes, no
   `def`, no loops, plain string literals for paths and column names. Worked examples for read →
   filter → group_by → sort, a two-input join, `with_columns` with an `.alias`, pivot/unpivot, and the
   same `{"answer": …}` escape hatch Simple build has today for a non-pipeline message (keep it as a
   fenced ```json``` block so one extractor handles both).
2. **Dialect block generated from the allowlist, not hand-written.** A `_render_dialect_block()` in
   `oneshot.py` (or a new `ai/local_model/code_build.py`) lists, from `notebook/allowlist.py`:
   `FL_VERDICTS` entries with `ALLOW` minus the refused families from Part 2 (readers:
   `read_csv`, `scan_csv`, `scan_parquet`, `read_excel`, `from_raw_data`, `col`, `lit`, `when`, `len`,
   `concat`); `ALLOWLIST["FlowFrame"]` minus writers/code nodes; `GroupByFrame.agg`; the `Expr`,
   `StringNS` and `DateTimeNS` methods. Appended to the prompt at assembly time, so the prompt can never
   offer a call the interpreter refuses (today's `ff.read_json` slip becomes impossible to suggest) and
   there is no drift gate to maintain. Signatures and one-line docs come from
   `components/notebook/flCompletions.json`'s generator (`make fl_completions`) where available.
3. **Budget.** Measure the assembled prompt; target ≤ 2.5k tokens so it fits the on-device window with
   the flow context. Unlike chat, Simple build sends no subgraph today; keep it that way in this change.

## Part 2: code → nodes in core

1. **Extractor.** `extract_code(text) -> str | None`: strip `<think>…</think>`, take the first fenced
   `python` block (or the whole text when it starts with `import flowfile`), `None` otherwise;
   `extract_answer` keeps handling the escape hatch and prose.
2. **Pre-scan before the clean run** (`ast` only, no execution): refuse the code with a one-line reason
   when it contains a `def`, `lambda`, `class`, `import` other than `flowfile as ff` / `polars as pl` /
   `datetime`, a call to any name in a `REFUSED_FOR_BUILD` set (`read_catalog_table`, `read_catalog_sql`,
   `read_database`, `read_from_cloud_storage`, `read_kafka`, `read_api`, `flow_ref`, `RunFlow`,
   `PythonScript`, `polars_code`, `sql`, `canvas_node`, `custom_nodes`, every `write_*`/`sink_*`,
   `Gate`, `Parameter`, `add_flow_parameter`), or a `collect()`. This is what keeps the route JWT-only
   in every mode: nothing left can touch the catalog, a connection or a kernel, so the
   `require_notebook_sync` admin gate is not needed and the frame never runs a grant-less lookup.
3. **Clean run.** `CleanRunRequest(cells=[("build", code)], ceiling=flow.node_id_ceiling,
   snapshot=push.seed_snapshot(flow))` through `get_clean_runner().clean_run(user.id, flow.flow_id, …)`
   (the installed `NotebookRunner`, never a kernel). The snapshot seeds the session with the live flow
   so new ids land above the canvas's; the model's cell references none of the live nodes, and the
   result is pruned to what the cell bound.
4. **Result → GraphDiff.** Convert `CleanRunResult.flowfile_data` (save-format nodes with `type` and
   `setting_input`, plus `node_connections`) into the `{nodes: [{id, type, settings}], edges}` spec
   `_build_simple_diff` already consumes, then stage through it unchanged. Its writer/unknown-type
   drop with warnings stays as a second safety net. Response adds `code` (the accepted code) next to
   today's fields.
5. **One repair round.** A clean-run failure returns `{message, line, kind}`; send the model one
   follow-up turn ("line 4: `ff.read_json` is not available; use …") and retry once. A second failure
   answers like a non-build message: the chat shows the error, the line and the code, nothing staged.
   Pin the retry at one so a 4B cannot loop.
6. **Route.** `GenerateFlowRequest.mode` gains `"code"`; the route passes it through; the 422 detail
   for an interpreter failure carries `{message, line, kind, code}` so the UI can render the line.

## Part 3: frontend

1. `localModelApi.generateFlow` sends `mode: "code"` from the composer; `GenerateFlowResult` gains
   `code: string | null`.
2. The build bubble in `AiMessage.vue` shows the code in a collapsed `<details>`-style block (CodeMirror
   read-only, the notebook's Python mode) above "Add to canvas"; on an interpreter failure the bubble
   shows the message, the line, and the code with that line highlighted.
3. Settings → AI gets a "Simple build output" radio (`code` default / `json`) persisted in the AI
   settings bucket, for users on a cloud model who prefer today's behaviour.

## Part 4: safety, gating, modes

- The no-exec contract: extend `tests/notebook/test_contract.py`'s recorder to a `/ai/generate` call
  with hostile code (`import os`, `def`, `__import__`), asserting nothing reaches `exec`/`eval`/
  `compile` and the pre-scan refuses before the clean run.
- Docker/package mode: with Part 2.2 the route stays `get_current_active_user` only. Document why in
  `routes/notebook.py`'s gate docstring and the root `CLAUDE.md` notebook entry, so a later widening
  of the dialect (catalog readers) knows it must add the grant check or the admin gate first.
- Writers never staged (unchanged); `polars_code`/`python_script`/`sql_query` never staged (new).

## Docs and notes

- `docs/ai/index.md` Simple build section: what the mode generates, that the code is shown, the
  refused families, and that the output matches the Notebook pane's language.
- `docs/ai/providers.md` on-device section: Simple build now writes FlowFrame code; expected latency.
- `.claude/skills/flowfile-ai-subsystem/SKILL.md` §6 and a new §10 on the code build pipeline.
- Root `CLAUDE.md` AI bullet and `flowfile_core/CLAUDE.md` `ai/` entry.

## Tests

- `test_local_oneshot.py`: `extract_code` (fenced, bare, think-tag, answer object, prose); the pre-scan
  refusals, one test per refused family plus `def`/`lambda`/foreign import/`collect`; the
  `flowfile_data` → spec adapter on a three-node payload with a join; end-to-end with a stub provider
  through the **real** interpreter (no mocks: `clean_run` is in-process and exec-free) for a linear
  flow, a join, and the repair round (first reply uses `ff.read_json`, second is fixed); the
  JSON `simple` mode byte-for-byte unchanged (snapshot of the stub test that exists today).
- `test_generate_routes` (new): `mode="code"` happy path, interpreter 422 shape, non-admin user in
  docker mode is allowed (`FLOWFILE_MODE=docker` fixture from `tests/sharing/`).
- Frontend `ai-store.test.ts`: `code` reaches the bubble; failure renders message + line.
- Real-model dogfood with Qwen3.5-4B (the harness used on 2026-10-08 in this session spawns the server
  with `manager._spawn` and posts to its OpenAI endpoint): ten prompts covering every example family
  plus three deliberately out-of-dialect requests (a UDF, a database read, a write). Pass: ≥ 9/10 stage
  on the first or repair round, every refusal names the line, no output exceeds 4 s on this machine.
  Then one round on OpenRouter Sonnet 5.5 and one on Ollama `qwen3.5:9b`.

## Not in this change

- Editing an existing flow from code (that is the notebook push, already present).
- Catalog, database, cloud or Kafka sources in Simple build. Needs the grant-aware lookup the
  notebook gate is waiting for; until then those stay refused with a message naming the Read node.
- Flow context in the Simple build prompt (column names of the open flow). Worth a follow-up: the
  chat's `render_prompt_context(local=True)` already produces a small enough block.
- Replacing the JSON path entirely. It stays as `mode="simple"` until the code path has a release of
  dogfooding.

## Order of work

Part 1 (prompt + dialect block, measured) → Part 2 (extractor, pre-scan, clean run, adapter, repair
round, route) with its tests and the no-exec contract → Part 3 (frontend) → Part 4 docs. Parts 1 + 2
are shippable on their own behind `mode="code"`; Part 3 flips the default.
