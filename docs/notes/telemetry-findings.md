# Telemetry delivery — ground-truth findings

Branch `feature/add-telemetry`, 2026-09-01. Phase 1/2 working notes for the
"CLI spool vs inline delivery" decision. Written to stand alone across a model
switch: everything here was verified against the code on this machine, with
citations. Verification was double-checked by an independent adversarial pass;
benchmarks were run three times on this machine and agreed.

**Correction to the record before anything else:** the brief this session was
pointed at, `docs/notes/cli-telemetry-buffering.md`, **does not exist** in the
repo or anywhere findable on this machine (the `docs/notes/` directory itself
did not exist; repo-wide searches for its name and content found nothing). The
eight claims below were verified from the phase-1 checklist in the session
prompt. The brief's option definitions A–D and its "three fork questions" are
referenced by that checklist but defined nowhere readable — the decision brief
reconstructs them explicitly and says so.

## 1. Claim-by-claim verification

**C1 — `emit()` is non-blocking on the caller thread under all four gate
outcomes: CONFIRMED, with a measured cold-start nuance.**
`emit()` → `_enqueue()` ([shared/telemetry.py:461-466](shared/telemetry.py:461),
[440-458](shared/telemetry.py:440)). Gate-closed paths return after env-var
reads (plus one *cached* consent-file read). The enabled path builds the
envelope and `put_nowait`s onto a 256-slot queue — full queue drops silently
([:451-454](shared/telemetry.py:451)). No network ever happens on the caller
thread: `_post` is reached only from the daemon `_loop` and from `flush`.
Lock audit (complete): `_cached_state`, `_invalidate_state_cache`, `_enqueue`,
`emit_once`, `_reset_for_tests` are the only `_lock` holders; the daemon
thread and `flush` are lock-free, and `persist_state`'s file write is *outside*
the lock — nothing slow is ever held under it.
Measured on this machine: steady-state emit **5–7µs p50 / 7–13µs p99**;
gate-closed emit **~1–2µs**; **first enabled emit ~22–28ms** (lazy `yaml`
import ≈8ms + `importlib.metadata` version ≈2.4ms + consent read + queue/thread
creation). Where that first-emit cost lands matters: route events are emitted
inside the ASGI send wrapper ([flowfile_core/telemetry.py:294-301](flowfile_core/flowfile_core/telemetry.py:294)),
i.e. **on the event loop** — the first mapped route after a consent grant pays
the ~25ms there. Bounded, one-time, but real.

**C2 — gate order kill switch → `TESTING` → endpoint → consent, each
short-circuiting: CONFIRMED in code; the *order* is only partly test-pinned.**
`is_enabled()` = `is_available() and consent() is True`;
`is_available()` = `not _kill_switch_engaged() and not _testing_disabled() and
_endpoint() is not None` ([shared/telemetry.py:245-258](shared/telemetry.py:245);
gates at [:219-242](shared/telemetry.py:219)). Python `and` short-circuits.
Correction: the test suite pins only ONE ordering fact —
`test_kill_switch_short_circuits_before_any_file_io`
([test_telemetry_gates.py:61-70](shared/tests/telemetry/test_telemetry_gates.py:61));
every other gate test pins effect, not order (swapping gates 1–3 would stay
green). Adjacent: `get_status()` does **not** short-circuit — it reads the
consent file even under the kill switch ([:267-273](shared/telemetry.py:267)).

**C3 — `flush(2.0)` sites: WRONG as stated.** The brief names "core shutdown,
CLI `run`". Reality — every product-code call site (repo-wide grep, all file
types):
1. [flowfile_core/main.py:438](flowfile_core/flowfile_core/main.py:438) —
   the `--run-flow` in-process CLI path (PyInstaller/frozen spawns).
2. [flowfile/\_\_main\_\_.py:146](flowfile/flowfile/__main__.py:146) — the
   `flowfile run flow` CLI path.
3. The implicit site the brief misses: `atexit.register(flush)` at
   [shared/telemetry.py:429](shared/telemetry.py:429), registered lazily on
   first successful enqueue, default `timeout=2.0`.

There is **no flush in the server lifespan/shutdown path** (its `finally` does
scheduler/kernels/local-model only). SIGTERM/SIGINT → uvicorn graceful exit →
atexit **was verified empirically** (marker-file experiment with this venv's
uvicorn); only SIGKILL loses the queue (≤5s of events given the daemon drain
tick). Two adjacent facts: (a) the atexit flush adds up to ~2s to process exit
when the collector is unreachable and the queue non-empty; on the CLI paths
the explicit flush + atexit flush **stack to ≈4s worst case**; (b) the atexit
site is pinned by **no test at all** (`_atexit_registered` is not even reset by
`_reset_for_tests`).

**REFUTED BY LATER REVIEW — the "only SIGKILL loses the queue" reading above
was wrong.** `flush()` drains only what is still *in the queue* and never joins
the daemon thread, but `_enqueue` wakes the daemon immediately, so the daemon
usually takes the terminal batch first; `flush` then finds an empty queue and
returns instantly, and process exit kills the in-flight POST. Empirically
verified (0.5–50ms emit→flush gaps: event lost in every run): **a CLI run's
terminal event is systematically lost even against a healthy collector.** Fix
shape: `flush` joins `_thread` for its remaining budget (or an in-flight
flag). See [telemetry-implementation-review.md](telemetry-implementation-review.md),
report 4 finding 1. **FIXED in the working tree (2026-09-01):** `flush` now
drains the queue and then waits out any in-flight daemon send via a
`_send_guard` lock the daemon holds across its take-and-send cycle, capped by
the flush budget; race pinned by `TestFlushWaitsForTheBackgroundThread` in
`test_telemetry_client.py` (both tests verified failing pre-fix).

**C4 — envelope is exactly `event, install_id, app_version, platform, mode,
ts, props`, no unique id, `ts` at one-second granularity: CONFIRMED.**
[shared/telemetry.py:361-370](shared/telemetry.py:361);
`ts = strftime("%Y-%m-%dT%H:%M:%SZ")`. Pinned five ways: client WIRE_KEYS
([test_telemetry_client.py:15,74](shared/tests/telemetry/test_telemetry_client.py:74)),
roundtrip ([test_telemetry_roundtrip.py:115](shared/tests/telemetry/test_telemetry_roundtrip.py:115)),
parity `cleaned == envelope`
([test_telemetry_schema_parity.py:93](shared/tests/telemetry/test_telemetry_schema_parity.py:93)),
and two hardcoded key-sets in
[test_telemetry_events.py:237,349](flowfile_core/tests/test_telemetry_events.py:237).
The only per-event uniqueness anywhere is the collector's server-side
`received_at`. Today the client can never double-send one envelope
(`_take_batch` uses `get_nowait`; a taken-then-failed batch is gone), so the
system is at-most-once end to end.

**C5 — spawned children have the kill switch injected, so they emit nothing:
WRONG as stated.** `spawn_flow_subprocess` injects `FLOWFILE_TELEMETRY=0` only
when `suppress_telemetry=True`
([shared/subprocess_utils.py:49-50](shared/subprocess_utils.py:49)); the only
`True` caller in the repo is the demo seeder
([demo_seed.py:176](flowfile_core/flowfile_core/catalog/demo_seed.py:176)).
The scheduler ([engine.py:555-557](flowfile_scheduler/flowfile_scheduler/engine.py:555))
and every user-triggered spawn pass no suppression — **deliberately**: both
child CLI paths call `install_headless()`
([main.py:389](flowfile_core/flowfile_core/main.py:389),
[\_\_main\_\_.py:96](flowfile/flowfile/__main__.py:96)) so those runs emit
`flow_run_*` events. App-driven noise is silenced subscriber-side instead
(`_subflow_depth`/`_system_run`,
[flowfile_core/telemetry.py:205-207](flowfile_core/flowfile_core/telemetry.py:205)).
`flowfile_worker` has zero telemetry imports; the scheduler imports
`shared.telemetry` **transitively** (module-level constant import in
[subprocess_utils.py:16](shared/subprocess_utils.py:16)) but never emits.

**C6 — no module-level `httpx`/`yaml` in `shared/telemetry.py`: CONFIRMED,
runtime-verified.** `httpx` under `TYPE_CHECKING` ([:31-32](shared/telemetry.py:31))
and inside `_post` ([:378](shared/telemetry.py:378)); `yaml` inside
`load_state` ([:172](shared/telemetry.py:172)). After `import shared.telemetry`
plus 1000 disabled emits, neither module is in `sys.modules`. Not re-exported
from `shared/__init__.py`. Because of the `subprocess_utils` transitive import,
this purity is what every scheduler/CLI spawn path relies on — a constraint
any new module inherits.

**C7 — declining consent drops the install id and retains nothing on disk:
HALF RIGHT — needs precision.** The id is dropped and unrecoverable
(`persist_state(False, None)` writes header + `consent: false` only,
[:183-199](shared/telemetry.py:183); re-grant mints a fresh UUID; pinned by
[test_telemetry_state.py:62-90](shared/tests/telemetry/test_telemetry_state.py:62);
exact post-decline file content verified by running it). But `telemetry.yaml`
itself **is** retained with `consent: false` — deliberately, it is what makes
"declined" distinct from "never asked" for the one-time modal
([telemetryConsent.ts:59-68](flowfile_frontend/src/renderer/app/components/settings/telemetryConsent.ts:59)).
No other telemetry data exists on disk today (queue is memory-only). One edge:
the tmp-then-`os.replace` write can leave a **`telemetry.yaml.tmp` containing
the install id** if `os.replace` fails on a grant — never cleaned up. Any
spool design inherits the obligation that `set_consent(False)` also deletes
the spool.

**C8 — collector-side schema parity tests exist: CONFIRMED.**
[test_telemetry_schema_parity.py](shared/tests/telemetry/test_telemetry_schema_parity.py)
pins event-name sets, per-event prop allowlists, value sets, caps
(`BATCH_SIZE ≤ MAX_BATCH_SIZE`, line 78), and round-trip envelope equality.
Full break inventory for an added field is in §5.

## 2. Additional load-bearing facts (not in the checklist)

- **Telemetry has never shipped.** The commits exist only on
  `feature/add-telemetry`; 0.16.0 is unreleased. There is no fleet.
- **The production collector is live but its data is one smoke test.**
  `https://events.flowfile.app/health` → `{"status":"ok"}` (Cloudflare). Its
  data store is not on this machine. The only events file anywhere is
  [tools/telemetry_collector/data/events.jsonl](tools/telemetry_collector/data/events.jsonl):
  one `app_started`, hand-round `ts` 16:30:00Z, `received_at` 41 minutes later
  — a smoke test, not usage.
- **The collector silently drops unknown top-level envelope fields**
  ([app.py:97-138](tools/telemetry_collector/app.py:97) rebuilds a 7-key dict;
  verified empirically — extra `seq`/`event_id`/junk keys → 202, 7 canonical
  keys stored). Unknown *prop* keys reject the whole event
  ([:126-129](tools/telemetry_collector/app.py:126)). Consequence: client-first
  rollout of a new field is safe-and-lossy (field dropped, event kept); no
  flag day. What breaks on day one is CI (parity test), not production.
- **The funnel is duplicate-tolerant by construction.** All five reported
  metrics are distinct-install booleans / set cardinalities
  ([funnel.py:86-108](tools/telemetry_collector/funnel.py:86)); `week2_return`
  is doubly idempotent (min over multiset + `any()`); the `--days` cutoff uses
  `max(ts)`, also duplication-proof. Only the stderr `malformed` count is a
  raw count. At-least-once redelivery cannot distort anything reported today.
- **`emit_once` is per-process, and cron children are fresh processes** —
  `activation`, `catalog_used`, `kernel_used` re-emit on *every* scheduled/CLI
  run (720×/month for an hourly cron). Distinct-install analyses are safe;
  any future event-count analysis is already skewed by this, spool or not.
- **An empty `FLOWFILE_TELEMETRY_ENDPOINT` does not disable telemetry** —
  [:242](shared/telemetry.py:242) falls through to the baked default. Only the
  kill switch or `TESTING=True` disables. **Five doc locations still claim the
  opposite** ("no default ships today, unset = disabled"):
  `docs/users/telemetry.md:16` and `:130`, `docs/users/deployment/docker.md:142`,
  `.env.example:161`, and the root `CLAUDE.md` env-var row. That page is what
  the consent modal links to. Pre-existing drift, not caused by this decision,
  but it must be fixed by whoever next touches these docs.
- **Delivery caps:** client batches ≤100/POST; collector `MAX_BATCH_SIZE=100`,
  `MAX_BODY_BYTES=256KiB`. See §6 for which cap binds.
- **In-memory queue:** 256 events, overflow dropped
  ([:42](shared/telemetry.py:42), [:451-454](shared/telemetry.py:451)).
- **`_send` discards the response entirely** ([:385-395](shared/telemetry.py:385)) —
  the client cannot distinguish 202 from 413 from 500 from a dead socket. A
  spool with delete-on-success semantics is unimplementable without changing
  this seam.
- **Latent liveness edge:** `_ensure_worker` runs only after a *successful*
  `put_nowait`; if the daemon thread ever died with a full queue, no new
  worker starts and emits drop until a `flush()` happens. Low risk, adjacent.

## 3. Benchmarks (this machine, three agreeing runs)

Harness (rerunnable, uncommitted): `scratch/telemetry_bench/bench_emit_latency.py`
— `poetry run python scratch/telemetry_bench/bench_emit_latency.py` from the
repo root. Each scenario runs in a fresh subprocess with
`FLOWFILE_STORAGE_DIR` under `scratch/`, `TESTING` popped, and an endpoint
guard that hard-aborts on any non-loopback/non-TEST-NET host. It never touches
`~/.flowfile` and never posts to the real endpoint.

`emit()` latency on the calling thread (µs):

| scenario | n | p50 | p90 | p99 | max |
|---|---|---|---|---|---|
| gates closed (kill switch) | 20000 | 0.58 | 0.63 | 2.6 | 30 |
| healthy local collector | 5000 | 6.1 | 7.2 | 11.3 | 659 |
| closed port (RST) | 5000 | 5.0 | 5.9 | 7.1 | 283 |
| blackhole (TEST-NET-3, 2s connect hang) | 5000 | 6.1 | 9.0 | 13.0 | 637 |

Cold start (fresh process, consent pre-granted, 10 processes): `import
shared.telemetry` ≈ 15–29ms; **first emit ≈ 20–46ms (median 21.7ms)**; second
emit ≈ 22µs. With the kill switch: first emit ≈ 7µs (the gate short-circuit is
what you pay, nothing else).

`flush(2.0)` with 150 queued events: healthy **0.055s**, closed port
**0.027s**, blackhole **2.016s** — correctly budget-capped. **Caveat added
after review:** these flush scenarios ran with the background daemon stubbed,
so they measure flush's own budget behavior, not production. With the daemon
live, flush races it and usually loses (see the refutation under C3) — the
production exit path delivers the terminal batch far less reliably than these
numbers suggest. Note also the mechanics: against the blackhole it took one 100-event batch out of the queue,
burned the whole budget on its doomed connect, **dropped those 100 events**,
and left 50 in the queue (which the atexit flush then burns another ~2s
failing to send).

**The brief's shape holds and is now measured: on the calling thread a dead
collector is indistinguishable from a healthy one.** Across three runs the
healthy/closed/blackhole orderings flipped (scheduling noise); the only place
liveness is observable is `flush()` wall time — 27–55ms alive vs a hard 2.0s
dead. So *interactive* use never feels collector death; a *CLI run* eats up to
~4s of exit delay (explicit + atexit flush) and silently loses whatever the
doomed batches carried.

## 4. Phase 2 — the two unmeasured facts

**Loss rate: cannot be bounded from existing data — there is no data.**
Telemetry is unshipped; the one stored event is a smoke test. Nothing exists
to find gaps in. Additionally, the client's silent-failure design means there
will never be a client-side loss signal without a protocol change, and
collector-side inference can only ever see installs that deliver at least
once — an install that never reaches the collector is invisible forever. This
is the finding the session prompt said makes option D live.

**Headless vs interactive share: unanswerable from existing data (n=1), and —
correction to the brief's premise — `mode` could not answer it even with
data.** `mode` is deployment shape, not run shape, and nothing in shipped code
ever sets `package` (its only consumer is `is_package_mode()`; it is
operator-supplied). Measured reality of who reports what: desktop app →
`electron`; scheduler child → inherits the parent (docker under compose,
electron on desktop/pip); **pip `flowfile run flow` → `electron`** (the core
import chain defaults it); pip `flowfile ui` → `electron`. Three of four
scenarios say `electron`. Distinguishing headless needs a new signal (a new
envelope field or prop — cost in §5), and reusing `mode` would corrupt the
meaning of historical events and break the mode allowlist tests.

## 5. Costings

**Envelope sizes (measured, compact JSON):** minimal `app_started` **175B**
(all 12 event names: 174–182B); typical `flow_run_succeeded` (5 node types)
**322B**; maximal legal `flow_run_succeeded` (60×64-char node types) **4,296B**
(absolute worst across fields 4,323B). General form: `envelope(k max-length
node types) ≈ 276 + 67k`.

**Spool extremes.** One headless CLI run emits: `flow_run_started` +
(`flow_run_succeeded` | `flow_run_failed`), plus per-condition
`catalog_used`/`activation`/`kernel_used` — and because `emit_once` is
per-process, the "once" events repeat every run. `app_started` and all route
events never fire on the CLI path (no lifespan, no middleware). Hourly cron ×
30 days, collector down throughout: 720 runs → 1,440–3,600 events → **0.24–0.6
MiB at minimal, ~0.47 MiB realistic (3 events/run), 8.9–14.8 MiB at
pathological maximal envelopes**. Every-minute cron × month: 43,200 runs →
**~28 MiB realistic**, 531–885 MiB at maximal. Recommended caps from the
arithmetic: **size cap 16 MiB FIFO-drop-oldest** (swallows the entire
legitimate hourly worst case; clips the every-minute-maximal case to 3%),
**age cap 30 days** (>2× the widest funnel window, `week2_return`'s
`first_ts+14d`; covers the month-long outage exactly).

**Sequence number vs event id.**
- `event_id` (uuid4): **one line** in `_envelope` (uuid already imported),
  stateless, +50B/event (+38B as base64-22). No consent-file change.
- `seq`: needs new module state under `_lock` (envelope is currently built
  *outside* the lock — a real design decision), reset in `_reset_for_tests`,
  and to mean anything across cron runs it must persist in `telemetry.yaml` —
  a consent-file schema change. +13B/event. Only buys gap-detection/ordering.
- Collector: validation clause + add the key to the rebuilt 7-key literal at
  [app.py:130-138](tools/telemetry_collector/app.py:130) (or it validates then
  discards). Optional-field rollout is safe in both orders; mandatory breaks
  every pre-upgrade client.
- Tests that fail on an added field: parity `:93` (the load-bearing one),
  client WIRE_KEYS `:74`, roundtrip `:115`, `test_telemetry_events.py:237` and
  `:349`. Plus doc/example drift: parity `EXAMPLE_EVENT`, consent-modal
  `telemetryConsent.ts:11-24` + its test (`ENVELOPE_KEYS`), README stored-line
  example, `docs/users/telemetry.md:28-45`.
- Stored-data migration: **none needed.** `funnel.py` reads only
  `install_id`/`event`/`ts` via `.get`; it is the only reader of
  `events.jsonl` in the repo. Mixed 7/8-key files are fine for every consumer.
  Pre-cutover lines simply have no id (un-dedupable segment).
- Semantic core: with 1-second `ts` and no id, two *legitimately distinct*
  events collide byte-for-byte whenever same install/second/event/props —
  empirically demonstrated. Realistic colliders: `flow_created` (props always
  `{}` — rapid quick-create), `export_code_used` (same target twice),
  `app_started` under a restart loop (where the count IS the signal),
  concurrent `flow_run_started`. So content-hash dedup silently undercounts
  exactly the bursty behavior worth measuring, permanently and biased against
  high-frequency installs. Since nothing reported today is duplicate-sensitive
  (§2), dedup is not needed now; if at-least-once redelivery ships,
  `event_id` is the correct field, `seq` only if loss-detection becomes a
  requirement.

## 6. Drain-protocol constraints (for any spool design)

- Two simultaneous limits, **byte cap binds first**: 100 maximal envelopes =
  429,712B > `MAX_BODY_BYTES` 262,144B. Largest all-maximal batch: **61
  events**. A full 100-batch stops being legal at ≥35 max-length node types
  per event. A drain loop must cut batches on `min(100 events, serialized
  bytes ≤ cap−margin)` — measure, don't estimate (25× size spread).
- Oversize is all-or-nothing and silent: 413 rejects the whole request before
  streaming; there is no partial acceptance. A count-only drain of maximal
  envelopes loses 100% of what it sends, invisibly, forever.
- No poison-pill events exist: max envelope is 1.6% of the body cap; one-at-a-
  time progress is always possible.
- `_send` must learn to report success and to separate permanent failures
  (413/422 — reshape or drop, never retry as-is) from transient ones
  (connect/5xx — keep spooled, back off). Today it swallows everything.
- Any new envelope field shrinks the max batch (seq → 60 events at maximal).

## 7. Test-break inventory by mechanism

(Option letters A–D from the missing brief could not be verified; mechanisms
below cover any mapping.)

**M-field (new envelope field):** fails the 5 shape tests listed in §5;
collector app tests fail only if the field is mandatory; frontend
`ENVELOPE_KEYS` test + consent-modal example drift silently — update anyway.

**M-spool (durable client-side buffer):** the most invasive. Direct design
contradictions — [test_telemetry_roundtrip.py:149-165](shared/tests/telemetry/test_telemetry_roundtrip.py:149)
(`test_events_dropped_while_dead_never_resurface` — "No retry queue by
design"; **this test IS the current decision and must be rewritten/deleted**),
`test_a_full_queue_drops_without_raising_or_blocking` ("overflow is dropped,
not buffered"), `test_flush_with_no_budget_left_sends_nothing` (`qsize()==1`),
`test_flush_on_an_untouched_module_is_a_no_op`, and the kill-switch
short-circuit test if any spool I/O sits ahead of the gates; a `.tmp`-staged
spool in the settings dir also trips `test_no_temp_file_is_left_behind`.
Systemic risk: **test-isolation collapse** — all three suites' fixtures
redirect only `_settings_file` (and `storage_config` memoizes `_base_dir` on
first access, so late env monkeypatching cannot redirect a
`storage.base_directory`-derived spool path), and `_reset_for_tests` knows
nothing about a spool file, so a spool would leak state across tests in
`shared/tests/telemetry`, `flowfile_core/tests/test_telemetry_events.py`, and
`test_telemetry_routes.py` (which also monkeypatches `os.replace` globally in
one test). A spool needs: its own monkeypatchable path seam beside
`_settings_file`, `_reset_for_tests` awareness, purge in `set_consent(False)`
+ a new purge test, and placement under `base_directory` (NOT
`temp_directory`/`cache_directory` — those are swept at 24h/1h by
`cleanup_directories`).

**M-inline (retry / longer flush at existing sites):** changing only the
timeout argument breaks **nothing** (no test pins 2.0). Adding retry inside
`_send`/`flush` fails `test_a_rejecting_collector_is_ignored` (`len==1` with
status 500), `test_batches_never_exceed_the_cap` (`[100, 50]`), and the
queue-drop count test; an unbounded retry loop hangs suites against their
timeouts. Both CLI sites carry the comment "This process exits right after;
drain after the summary so nobody waits on it" — a longer budget contradicts
it. `docs/users/telemetry.md` makes no retry claim; only the collector README
does.

**M-status-quo:** statically consistent; suite passes as of this branch (142
tests in `shared/tests/telemetry` were run during verification: all green).

## 8. Doc lines a spool (or retry) falsifies

- [tools/telemetry_collector/README.md:34-38](tools/telemetry_collector/README.md:34):
  "the Flowfile client fails silently by design (telemetry must never produce
  user-facing errors or retries), collector downtime means silently lost
  events — there is no client-side retry queue" — plus its consequences at
  `:42-45` ("missed events are simply gone") and `:50-52` (the VPS-move
  recommendation's premise).
- [test_telemetry_roundtrip.py:150](shared/tests/telemetry/test_telemetry_roundtrip.py:149)
  docstring: "No retry queue by design: a batch that failed to send is gone
  for good."
- `shared/telemetry.py` module + `emit`/`flush` docstrings (delivery
  description), and the `shared/CLAUDE.md` telemetry bullet.
- `docs/users/telemetry.md` makes no delivery-loss claim, but its on-disk
  footprint story (`:98-112`: one path, whole-file listing, "no database
  record behind it", "Turning telemetry off deletes it") must absorb a second
  artifact + purge-on-decline.
- Root `CLAUDE.md` telemetry rows ("Consent + the random install id live in
  `telemetry.yaml` … — no DB").

## 9. Build-pass addendum (2026-09-01, later the same day)

The decision in [docs/decisions/telemetry-delivery.md](../decisions/telemetry-delivery.md)
is now implemented in the working tree, along with the frontend consent fixes
and worker-side error-class prefixes. Delivery is at-least-once: transient
send failures append to `telemetry_spool.jsonl` (16 MiB FIFO / 30-day caps,
purged by `set_consent(False)`), the daemon drains the spool before the live
queue, the envelope carries a uuid4 `event_id` (8 wire keys now), and the CLI
exit budget is `flush(0.3)`. Benchmark after the build: steady-state emit
**~10.7µs p50** vs the pre-`event_id` 5–7µs band — attributed by controlled
A/B to `str(uuid.uuid4())` (~2µs isolated), not the spool (a constant-id
control run sat back inside the band). Accepted: the band is not reachable
with a per-event CSPRNG id. Worst-case exit delay on a *black-holed* collector
is now ~2s (the atexit flush waiting out the daemon's doomed send, which then
spools the batch — zero loss), down from ~4s with total loss; refused/healthy
exits are sub-second. Remaining known gaps: events still *in the queue* when
the atexit budget expires die unspooled (only reachable with >100 queued
events at exit), and collector auth/rate-limiting/rotation remain open.

## 10. Sources for the next phase

The decision brief is [telemetry-decision-brief.md](docs/notes/telemetry-decision-brief.md).
The two files it depends on most: **this file** and
**[shared/telemetry.py](shared/telemetry.py)** (the entire client, ~520
lines; the collector counterpart is
[tools/telemetry_collector/app.py](tools/telemetry_collector/app.py)).
Benchmark harness: `scratch/telemetry_bench/bench_emit_latency.py`
(uncommitted; keep for the phase-4 before/after comparison).
