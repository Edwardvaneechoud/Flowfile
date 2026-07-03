---
name: flowfile-ai-subsystem
description: Architecture and safe-extension guide for flowfile_core's /ai/* subsystem — the assist/copilot/planner surface map, the FEATURE_FLAG_AI router gate, the litellm lazy-import contract and its enforcing tests, BYOK provider resolution, per-process rate limiting, the prompt-log debugging runbook, the local llama.cpp model, and two maintainer-validated doctrines (prompt edits must be additive not subtractive; wrong-but-recoverable LLM payload shapes get normalized at the executor seam, not the schema or the prompt) — use when adding or debugging an AI route/agent/provider, when an agent tool call is being rejected or looping, when asked to "make the prompt more direct" or tighten AI/system prompts, when the LLM keeps emitting a wrong JSON shape for a tool call, or when investigating what a Flowfile LLM call actually saw/said.
---

# Flowfile AI subsystem

Everything under `flowfile_core/flowfile_core/ai/` — the `/ai/*` HTTP surface,
the three agent tiers (assist / copilot / planner), the litellm provider
seam, BYOK credentials, rate limiting, the prompt log, and the local model
manager. Read this before touching any file under `flowfile_core/flowfile_core/ai/`
or `flowfile_core/tests/ai/`.

## When NOT to use this skill

- **The full `FEATURE_FLAG_AI` / `FLOWFILE_AI_LOG_PROMPTS*` / rate-limit env-var
  table with exact `settings.py` line numbers** → `flowfile-config-and-flags`
  (this skill states only what's needed to reason about AI behavior).
- **Step-by-step "an agent is misbehaving, what do I check first" triage** →
  `flowfile-debugging-playbook` (this skill explains *why* the prompt log
  works the way it does; the playbook is the symptom-first runbook).
- **Adding a brand-new node type** (Pydantic settings, `add_<type>` on
  `FlowGraph`, frontend node UI) → `flowfile-node-development`. The AI tool
  catalog auto-derives from those same settings classes — get the node right
  first, the AI surfaces it for free.
- **Core/worker/kernel dataflow contract** (why core never `.collect()`s) →
  `flowfile-architecture-contract`.
- **Frontend chat/copilot UI components** (`AiAssistant.vue`, ghost-node
  suggestions, `Cmd+K` palette rendering) → `flowfile-frontend-conventions`.
- **Test markers and how to run `flowfile_core/tests/ai/`** → `flowfile-testing-and-validation`.

---

## 1. Surface map — three agent tiers, one prompt stack

All AI code lives under `flowfile_core/flowfile_core/ai/`. The package
docstring (`ai/__init__.py`) is the map: `providers/` (litellm seam + BYOK
key load), `tools/` (tool catalog + executor), `context/` (subgraph/schema
serialization), `agents/` (surface implementations), `prompts/` (layered
system prompts), plus `streaming`, `scheduler`, `sessions`, `diff`, `safety`,
`metrics`. 17 `*_routes.py` route modules (as of 2026-07-03; re-count with
`ls flowfile_core/flowfile_core/ai/*_routes.py`) are mounted onto one `router`,
which is the package's **only** public export.

| Tier | Entry point | Where the logic actually lives | Behavior |
|---|---|---|---|
| **Assist** | `POST` via `chat_routes.py` (chat drawer, inline ✨ menu, Fix-with-AI) | Route modules directly — `agents/assist.py` is a **9-line docstring-only stub** ("Stub.") | Single-turn, read-only. No tool calls, no graph mutation. |
| **Copilot** | `autocomplete_routes.py`, `command_palette_routes.py`, edge ghost-node suggestions | Route modules directly — `agents/copilot.py` is a **10-line docstring-only stub** | Suggestive, in-flow. One tool call per surface; applied immediately, undoable. |
| **Planner** | `POST /ai/agent/start` (`agent_routes.py`) | `agents/planner/` — a real package (`_internal.py`, `catalog.py`, `coercions.py`, `insertion.py`, `llm_replies.py`, `loop.py`, `messages.py`, `rationale.py`, `recovery.py`, `staged_schemas.py`) | Multi-turn, tool-calling, diff-staged. |

**Do not** look for assist/copilot implementation inside `agents/assist.py`
or `agents/copilot.py` — they only point you at the real route modules and
at `prompts/base.md` + `prompts/assist.md` / `prompts/copilot.md`. All three
tiers share `prompts/base.md` as a common layer.

### Planner surfaces (verify before quoting elsewhere — this drifted once already)

`AgentStartRequest.surface` (`agent_routes.py`) is a `Literal["agent_complex",
"agent_staged", "agent_live"]`, **default `"agent_staged"`** — not a bare
`"agent"`. `agents/planner/_internal.py` defines
`_STAGED_STATE_MACHINE_SURFACES = frozenset({"agent_staged", "agent_live"})`:

- **`agent_staged`** (default) — the two-stage state machine: `classify` →
  `pick_type` → `pick_upstream` → `fill_settings`. Every tool call dispatches
  with `mode="stage"`; nothing touches the live graph until the user accepts.
- **`agent_live`** — same state machine, but applies each step immediately
  (lives in `flow.nodes` right away) instead of staging, and includes an
  observation step. Every `agent_staged` gate in the planner code must also
  include `agent_live` (see the comment at `_internal.py:65-69`).
- **`agent_complex`** — single-shot, full tool catalog (including codegen
  helpers hidden from the staged surfaces). Requires a provider with
  `supports_tools=True`, or the route returns `422`.

Planner contract, verified against `agents/planner/__init__.py`'s module
docstring:

- Session opens via `POST /ai/agent/start`; every LLM tool call dispatches
  through `execute_tool_call` with **`mode="stage"`** — the live graph is
  never mutated mid-run (staged surfaces) or mutated one step at a time with
  full auditing (`agent_live`).
- The graph is snapshotted before every dispatch. If the user edits the
  canvas mid-run, the planner yields `drift_detected` + `paused` and exits
  cleanly; the session waits for `POST /ai/agent/{session_id}/resume`
  (`{"action": "continue"}` re-snapshots and resumes; `{"action": "discard"}`
  frees the staged diff).
- A rejected step retries up to `max_retries_per_step` times by feeding the
  executor's `refusal_detail` back to the LLM as a `role="tool"` message.
- On completion, per-step results bundle into one
  `flowfile_core.ai.diff.GraphDiff` via `bundle_staged_results`, reviewed and
  accepted atomically in the UI (`AiDiffPreview`).
- The planner generator **never raises** — every failure becomes a
  `PlannerEvent` (`"error"` / `"tool_call_rejected"` / `"drift_detected"` /
  `"abort"`). SSE `id:` headers are `f"{session_id}.{step_count}"` so
  `Last-Event-ID` reconnect works against the replay buffer.
- Sessions are namespaced by `user_id`; cross-user access on
  `{session_id}/*` returns `404` (not `403`) to avoid leaking session
  existence.

---

## 2. Router gating — `FEATURE_FLAG_AI`

The entire `/ai/*` router is behind one dependency:
`ai/feature_flag.py::require_ai_enabled()`, wired as
`APIRouter(dependencies=[Depends(require_ai_enabled)])` in `ai/routes.py`.
Off → every AI endpoint returns `503` with detail `"AI features are
disabled. Set FEATURE_FLAG_AI=true to enable."` (`DISABLED_DETAIL`,
`feature_flag.py:36`).

- Backing store: `settings.FEATURE_FLAG_AI`, a `MutableBool`
  (`configs/settings.py:25-27`), default **on**
  (`os.environ.get("FEATURE_FLAG_AI", "1")`, truthy strings
  `true`/`1`/`yes`/`on`, case/whitespace-insensitive).
- `is_ai_enabled()` reads the live `MutableBool` value on **every** call — no
  restart, no reload needed to see a flip.
- Live flip: `POST /system/feature_flags/ai` (`ai/admin_routes.py`, admin-only
  via `get_current_admin_user`) — mounted under **`/system`**, deliberately
  outside the gated `/ai` router (so you can turn AI back on while it's
  reporting 503). It sets **both** `settings.FEATURE_FLAG_AI.set(enabled)`
  and `os.environ["FEATURE_FLAG_AI"]`, but the response always carries
  `persisted: false` — surviving a process restart is the operator's `.env`
  file, not this endpoint's job.
- Full env-var table (defaults, truthy-parse rule, every other
  `FLOWFILE_AI_*` flag) → `flowfile-config-and-flags`.

---

## 3. The litellm lazy-import contract

**Rule:** no module under `ai/` imports `litellm` at module (top) level, so
the package can be imported — including by cheap classification/dry-run/
scheduling code paths that never need a real LLM call — without paying
litellm's import cost or triggering its network calls.

Verified (grep for `import litellm` / `from litellm import` under
`flowfile_core/flowfile_core/ai/`) — there are exactly two real imports,
both function-local:

1. `ai/providers/_litellm_base.py`, inside `_lazy_litellm()` — `import
   litellm` then `litellm.suppress_debug_info = True` (idempotent; silences
   litellm's stdout banners on every call).
2. `ai/scheduler.py`, inside a function — `from litellm import exceptions as
   lle  # lazy`, guarded by `try/except` so a missing/broken litellm install
   degrades to bare `TimeoutError`/`ConnectionError` retry matching instead
   of crashing.

`ai/__init__.py` also does `os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP",
"True")` **before** importing routes — a write, not an import — guaranteeing
litellm's eventual first import uses the bundled cost map instead of
fetching one from `raw.githubusercontent.com`.

**Correction to a common paraphrase of the root `CLAUDE.md` line** ("keep the
package litellm-import-free except `ai/byok.py`"): read literally that's
wrong — `byok.py` contains **no litellm import at all** (verified). What
`byok.py`'s own docstring actually describes is that it keeps
`ai/credentials.py` (the DB CRUD layer) **provider-class-import-free**, so
`import flowfile_core.ai.credentials` never pulls in any `providers/*`
module. The real litellm rule is simpler and package-wide: *no module under
`ai/` imports litellm at load time*; the two lazy sites above are it.

**Enforcing tests** (`flowfile_core/tests/ai/`) — re-adding an eager
`import litellm` anywhere under `ai/` breaks these:

- `test_classification.py::test_classify_lazy_litellm` — asserts no
  `litellm`/`litellm.*` module is in `sys.modules` after importing the
  classification module.
- `test_dry_run.py::test_dry_run_lazy_litellm_contract` — same assertion for
  the dry-run module.
- `ai/scheduler.py`'s own docstring states the same invariant for
  `flowfile_core.ai.scheduler`, but no dedicated test for it was found
  beyond the two above — don't assume a third exists.

`LiteLLMProvider.chat()` / `.stream()` (`ai/providers/_litellm_base.py`) are
the single dispatch seam every vendor subclass shares: translate Pydantic
`Message`/`ToolSpec` to litellm's dict shapes, aggregate streamed tool-call
fragments in `_PartialToolCall` buffers (a tool call surfaces only once it
has an id + name and its arguments parse as JSON), and in `finally` blocks
run two never-crash hooks — `_record_call_telemetry` (always-on
`flowfile_ai_provider_call_total` counter) and `_record_chat_log`/
`_record_stream_log` (prompt log, only when `FLOWFILE_AI_LOG_PROMPTS` is
on). Both import their dependencies function-locally and swallow + log
failures — a logging bug must never take down an LLM call.

---

## 4. Providers, BYOK, and model resolution

`ai/providers/registry.py::PROVIDERS` maps six litellm-backed vendors —
`anthropic, openai, google, groq, openrouter, ollama` — each a config-only
subclass of `LiteLLMProvider` (name, `default_model`, `model_prefix`,
capability flags, `surface_models`, `default_api_base`).

The **local** llama.cpp pseudo-provider (`providers/local.py`,
`LOCAL_PROVIDER_ID = "local"`) is deliberately **not** in `PROVIDERS` — it
never appears in BYOK credential CRUD. `is_resolvable_provider(name)` =
`PROVIDERS ∪ {"local"}`, used by read-only routes; the tool-calling agent
route (`agent_routes.py`) rejects `provider == LOCAL_PROVIDER_ID` explicitly
— a local model has no reliable tool-calling support to build a planner run
on.

**BYOK** (`ai/byok.py` + `ai/credentials.py`): API keys are `Secret` rows
via the normal `secret_manager` (`encrypt_secret`/`decrypt_secret`), name
convention `ai:{provider}:api_key:{user_id}:{credential_id}`. Rotation
mutates `Secret.encrypted_value` in place (id stable).

`byok.get_configured_provider(db, user_id, provider, *, surface, model)` —
model resolution order (its own docstring, `byok.py:1-31`): (1) explicit
`model=` argument → (2) credential row's `default_model` → (3) provider
class `surface_models[surface]` **if** it's in the user's curated `models`
list → (4) first entry of the user's curated `models` list → (5) provider
class `surface_models[surface]` (plain fallback) → (6) provider class
`default_model` (terminal fallback).

Env-var fallback when no credential row exists — `_PROVIDER_ENV_VARS`
(`byok.py`): `anthropic`→`ANTHROPIC_API_KEY`, `openai`→`OPENAI_API_KEY`,
`google`→`GEMINI_API_KEY`/`GOOGLE_API_KEY`, `groq`→`GROQ_API_KEY`,
`openrouter`→`OPENROUTER_API_KEY`, `ollama`→none (never needs a key —
`unconfigured` until a row exists, `configured` only once one is saved).
`get_configured_provider` raises `ProviderNotConfiguredError` only when
**all** of: no credential row, no env var, no class `default_api_base`, and
`provider != "ollama"`.

**Rate limiting** (`ai/scheduler.py`) — opt-in, not automatic. Per-provider
sliding-window RPM/RPD from `FLOWFILE_AI_<PROVIDER>_RPM` / `_RPD` (unset ⇒
unenforced; `ollama` is always in `_UNLIMITED_PROVIDERS`, never limited).
Honors `Retry-After` on `429`; exponential backoff `2s, 4s, 8s, 16s`, max 4
retries, ±25% jitter — server `Retry-After` always wins when longer.
`byok.get_configured_provider` is **NOT** auto-wrapped; callers opt in via
`with_provider_retry(provider, ...)`.

**Load-bearing caveat:** state is **per-process, in-memory** (plain deques,
no persistence, nothing shared across workers). Under `gunicorn -w N` (or
any multi-worker deploy), the effective aggregate budget is **≈ N × the
configured RPM/RPD** — each worker enforces its own window independently.
For a hard global ceiling, pin to a single worker or divide the configured
limit by the worker count. No audit writes happen on retries ("failure is
free" — retries are invisible to user-facing quota counters).

---

## 5. Prompt-log debugging runbook

The prompt log is the "what did the model actually see and say" hatch —
prefer it over guessing from output when an agent loops, hallucinates a tool
name, references a nonexistent column, or otherwise misbehaves.

1. Set `FLOWFILE_AI_LOG_PROMPTS=true` in the environment (accepts
   `true`/`1`/`yes`/`on`) and re-run the failing flow/chat/agent session.
2. Tail the latest entries: `python -m flowfile_core.ai.prompt_log tail 20`.
3. Or search: `python -m flowfile_core.ai.prompt_log grep PATTERN [SURFACE]`
   — regex over the serialized JSON line, optionally scoped to one AI
   surface (e.g. `agent_staged`).
4. Or open the file directly:
   `{storage.base_directory}/ai_prompts/YYYY-MM-DD.jsonl` —
   `~/.flowfile/ai_prompts/...` locally, `$FLOWFILE_STORAGE_DIR/ai_prompts/...`
   when set, or `/app/internal_storage/ai_prompts/...` in Docker. One
   `jq`-parseable JSON line per LLM round-trip.

**Gotcha — `tail`/`grep` only read *today's* file.** Both `tail(n, date=None)`
and `grep(pattern, surface=None, date=None)` default `date` to
`datetime.now(timezone.utc).date().isoformat()` — **UTC**, not local time —
and the CLI (`_cli_main` in `prompt_log.py`) never exposes a `date` flag. To
inspect a prior day, either run just after UTC midnight rollover expecting
"yesterday", or drop into Python and call `tail(20, date="2026-07-02")` /
`grep(pattern, date="2026-07-02")` directly.

**Truncation** (keeps every line `jq`-parseable regardless of agent-loop
depth): entries over `MAX_MESSAGES_BYTES = 256 * 1024` (256 KiB) get their
older message bodies replaced with `[...truncated, len=N chars]` and
`truncated: true` set, while the system prompt plus the most-recent
`KEEP_RECENT_TURNS = 5` messages are kept verbatim.

**Sharing transcripts externally:** set `FLOWFILE_AI_LOG_PROMPTS_SCRUB=true`
first. Scrub masks **user and tool** message bodies only through the regex
PII scrubber in `ai/safety.py` — system and assistant content stay verbatim,
because that's the half you actually need to debug compliance against the
prompt. Both flags default **off**; production stays silent unless an
operator opts in.

**Failure isolation:** a prompt-log write failure (disk full, serialization
bug) never crashes the LLM call — the wrapper in `_litellm_base.py` swallows
and warns.

---

## 6. Local model (`ai/local_model/manager.py`)

On-demand `llama-server` (llama.cpp) + a GGUF model, downloaded **only when
the user opts in** via `/ai/local-model/*` routes — the download directory
(`storage.local_model_directory`, `~/.flowfile/local_model`) is deliberately
**excluded** from `shared.storage_config`'s `_ensure_directories()` so it's
never created for users who never touch the feature.

- Pinned llama.cpp build: `LLAMACPP_BUILD = "b9305"` from `ggml-org/llama.cpp`
  releases (platform-specific archive per OS/arch). Default model:
  Qwen2.5-Coder-3B-Instruct GGUF (~2 GB); a curated list also offers a 1.5B
  (lighter) and a 7B (heavier, non-coder) option.
- Module-level singleton — **at most one server runs at a time**; `_lock`
  guards it. `LocalProvider` resolves the live port lazily on the first
  `.stream()` call so constructing it stays synchronous/non-blocking.
- Shut down from core's FastAPI lifespan (`main.py`'s `_shutdown_local_model`)
  so a killed core process doesn't orphan the `llama-server` subprocess.
- Kept out of `providers.PROVIDERS` (see §4) — never appears in BYOK CRUD,
  and the tool-calling agent route rejects it outright.

---

## 7. How to extend safely

- **New vendor provider:** add a config-only subclass under
  `ai/providers/` (mirror `anthropic.py`/`groq.py` — name, `default_model`,
  `model_prefix`, `surface_models`, `default_api_base`), register it in
  `registry.PROVIDERS`, and add its API-key env var(s) to
  `byok._PROVIDER_ENV_VARS`. **Never** import litellm at module scope — the
  vendor subclass inherits the lazy `_lazy_litellm()` call from
  `LiteLLMProvider`.
- **New AI route:** put it under the `/ai` router (`ai/routes.py`) so it
  inherits `require_ai_enabled` for free — don't hand-roll the 503 check.
  Add auth per-route (`Depends(get_current_active_user)`, or
  `get_current_admin_user` for admin-only) — the feature flag and auth are
  orthogonal gates, both required.
- **New agent tool:** the tool catalog auto-derives from the same Pydantic
  node-settings classes used by the visual graph (see
  `flowfile-node-development`) — get the node's settings schema right first
  and most of the catalog wiring falls out for free. Route it through
  `execute_tool_call` in `ai/tools/executor/`; don't bypass the dispatch seam.
- **New prompt content:** see the doctrine in §8 before editing anything
  under `ai/prompts/`.
- **A tool call keeps arriving in a wrong-but-fixable shape:** see the
  doctrine in §9 before touching the prompt or the Pydantic schema.

---

## 8. PROMPT DOCTRINE (maintainer-validated — do not relitigate without re-reading this)

Flowfile's AI prompts (`ai/prompts/base.md`, `planner.md`, `assist.md`,
`copilot.md`, and the `stage_*.md` files) are written for **cheap,
small-model-class agents**, not just top-tier models. That constraint shapes
what "improving" a prompt actually means:

- **Worked examples and repeated imperatives are load-bearing scaffolding,
  not padding.** A concrete "customers per city → look for a node with
  customer and city columns"-style example teaches pattern-matching that a
  terser rule statement doesn't reliably transfer to a smaller model, even
  when the rule text is technically equivalent.
- **A ~40-50% "more direct" rewrite of the planner prompt caused an
  immediate, cascading regression in production dogfooding and was reverted
  within minutes (2026-05).** The rewrite led with the rule, cut hedges and
  worked examples, and shortened rationale paragraphs — and the very next
  live session produced cascading retry failures across `add_group_by` /
  `connect` / `delete_node` / `read_node_schema` tool calls plus an
  unrequested node staged. The fix was reverting, not iterating on the
  shorter version.
- **Prefer additive clarity over subtractive trimming.** If a prompt section
  feels verbose, the safe move is adding one more worked example or
  imperative restatement — not deleting the ones already there. Treat any
  PR that *removes* worked examples or repeated imperatives from
  `base.md`/`planner.md`/`assist.md`/`copilot.md` as a regression risk
  requiring dogfood verification, not a routine cleanup.
- **Tone-level polish is fine and welcome:** fixing typos, sharpening a verb,
  stripping a genuinely dead conversational hedge, or improving formatting —
  as long as no rule, imperative, or worked example is removed in the same
  edit.
- **If asked to "make the prompt more direct" or "tighten" it:** do not
  interpret that as "shorten." Rewrite for clarity and force per line
  without deleting content. If real compression is wanted, that's a planned,
  test/dogfood-gated editorial pass — not a quick edit.

---

## 9. EXECUTOR-SEAM DOCTRINE (maintainer-validated — do not relitigate without re-reading this)

When the LLM **consistently** emits a wrong-but-structurally-recoverable
payload shape for a tool call, the fix goes in the **executor**, not the
prompt and not the Pydantic schema.

**Where:** `flowfile_core/flowfile_core/ai/tools/executor/` — a package (it
used to be a single `executor.py`; the public API is re-exported verbatim
from `executor/__init__.py` so `from flowfile_core.ai.tools.executor import
...` still works). Coercers live in `executor/coercions.py` and
`executor/manual_input.py`; the dispatcher that calls them is
`executor/dispatch.py` and `executor/handlers/add.py`.

**Two live examples to copy the shape of:**

- `coercions.py::_coerce_connection_id_to_flat` — the LLM occasionally emits
  `{"connection_id": "1→2"}` (or `"1->2"` / `"1:2"`) for a delete-connection
  call instead of the structured `{from_node_id, to_node_id}` shape. Called
  first thing inside the connection-delete handler
  (`executor/handlers/connections.py`), before validation.
- `manual_input.py::_normalize_manual_input_args` (+
  `_normalize_manual_input_columns` / `_normalize_manual_input_data`) — the
  `manual_input` node's `RawData` schema (`schemas/input_schema.py`) demands
  strictly columnar `data` (`data[i]` = all values for `columns[i]`), but
  LLMs consistently emit row-oriented data and `columns` as a `{name:
  dtype}` dict. Root cause noted in the module docstring: Pydantic's
  JSON-Schema rendering of `data: list[list]` produces unconstrained `items:
  {}` at the inner level, so the schema itself can't teach the columnar
  invariant — prompt examples alone weren't enough. Gated on `node_type ==
  "manual_input"` inside `_handle_add_node`
  (`executor/handlers/add.py`), called before `settings_cls.model_validate`.

**The pattern to follow:**

1. Place the normalizer alongside the existing coercers (`coercions.py` for
   general shape fixes, or a dedicated module like `manual_input.py` for a
   node-type-specific one).
2. Gate the call on the specific `node_type` or tool name it applies to —
   never apply a coercion globally to every payload.
3. Write it **identity-preserving on no-op**: if the input is already
   canonical or doesn't match the recognized bad shape, return the original
   object unchanged (not a copy) so callers can detect "did anything change"
   via `is`.
4. When the shape is **structurally ambiguous** — e.g. a square manual_input
   matrix where `len(data) == len(columns)` and every inner list also has
   that length, so both orientations are valid `list[list]` — **leave it
   untouched**. The schema is canonical in the ambiguous case; guessing
   wrong silently corrupts data, which is worse than a refusal.
5. Add a unit test per shape variant (see
   `flowfile_core/tests/ai/test_executor.py`'s
   `test_normalize_columns_*` / `test_normalize_data_*` /
   `test_normalize_manual_input_args_*` / `test_executor_accepts_*_for_manual_input`
   cluster) plus an integration-style test asserting the previously-rejected
   payload now applies successfully.

**What NOT to do:**

- **Do not keep tightening the prompt** for a shape error once it's shown up
  more than once — in-prompt examples were already tried for the
  `manual_input` case and the LLM still fell back to row-oriented thinking,
  because the JSON-Schema the LLM actually sees doesn't encode the
  constraint the prose does.
- **Do not add a schema-level `@model_validator(mode="before")`** to make
  the Pydantic class itself accept the sloppy shape. Node settings classes
  like `RawData` have **non-AI consumers** — the UI's manual-input dialog,
  the programmatic `flowfile_frame` API — that must not pay the
  LLM-tolerance cost of a permissive schema. The schema stays the strict,
  canonical contract; leniency is the executor's job, scoped to the AI path
  only.

---

## Provenance and maintenance

Facts below are dated **2026-07-03 (v0.12.7)**; the AI package moves fast
(new route modules, occasional file-to-package splits like
`executor.py` → `executor/`) — re-run these before trusting a specific
line number or file path.

```bash
# Surface map / stub confirmation + planner surface literal (watch for drift off
# agent_staged/agent_live/agent_complex)
sed -n '1,20p' flowfile_core/flowfile_core/ai/agents/assist.py flowfile_core/flowfile_core/ai/agents/copilot.py
ls flowfile_core/flowfile_core/ai/agents/planner/
grep -n 'surface: Literal' flowfile_core/flowfile_core/ai/agent_routes.py

# FEATURE_FLAG_AI gate + admin flip endpoint
grep -n 'FEATURE_FLAG_AI' flowfile_core/flowfile_core/configs/settings.py
sed -n '1,95p' flowfile_core/flowfile_core/ai/admin_routes.py
grep -n 'ai_admin_router\|prefix="/system"' flowfile_core/flowfile_core/main.py

# Lazy-litellm contract + its tests. Expect exactly two code hits, both
# function-local: providers/_litellm_base.py (import litellm) and
# scheduler.py (from litellm import exceptions). A bare `grep 'import litellm'`
# also matches comments/log strings and misses the `from litellm import` form.
grep -rnE '^\s*(import litellm|from litellm import)' flowfile_core/flowfile_core/ai/
poetry run pytest flowfile_core/tests/ai/test_classification.py::test_classify_lazy_litellm \
  flowfile_core/tests/ai/test_dry_run.py::test_dry_run_lazy_litellm_contract -q

# Provider registry + BYOK env-var fallback map + rate-limit per-process caveat
sed -n '1,45p' flowfile_core/flowfile_core/ai/providers/registry.py
grep -n '_PROVIDER_ENV_VARS' -A 9 flowfile_core/flowfile_core/ai/byok.py
grep -n 'per-process\|_UNLIMITED_PROVIDERS\|gunicorn' flowfile_core/flowfile_core/ai/scheduler.py

# Prompt log constants + CLI (today-only date default) + local model manager pin/singleton
grep -n 'MAX_MESSAGES_BYTES\|KEEP_RECENT_TURNS\|LOG_SUBDIR\|def tail\|def grep\|def _cli_main' \
  flowfile_core/flowfile_core/ai/prompt_log.py
grep -n 'LLAMACPP_BUILD\|local_model_directory\|_ensure_directories' \
  flowfile_core/flowfile_core/ai/local_model/manager.py shared/storage_config.py

# Executor-seam doctrine: coercers, their gating call sites, and no before-validator on RawData
grep -n 'def _coerce_connection_id_to_flat\|def _normalize_manual_input_args' \
  flowfile_core/flowfile_core/ai/tools/executor/coercions.py \
  flowfile_core/flowfile_core/ai/tools/executor/manual_input.py
grep -n 'node_type == "manual_input"' flowfile_core/flowfile_core/ai/tools/executor/handlers/add.py
grep -n 'class RawData' -A 20 flowfile_core/flowfile_core/schemas/input_schema.py | grep -i model_validator
```

The prompt doctrine (§8) and executor-seam doctrine (§9) are process
knowledge from lived incidents, not something you can `grep` back out of
the current tree (the reverted prompt diff and the original bug report
aren't preserved as commits to inspect). Treat them as standing policy
unless a maintainer explicitly revisits them — don't rediscover them the
hard way.
