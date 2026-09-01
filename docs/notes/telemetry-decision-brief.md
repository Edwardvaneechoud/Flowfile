# Telemetry delivery — decision brief

Input to the phase-3 decision. Evidence base:
[telemetry-findings.md](telemetry-findings.md) (all claims there carry
file:line citations and measured numbers; read it for any "why"). Scope
guard: consent semantics, gate order, the twelve events and their props, and
collector hosting are settled and are not reopened here.

**Provenance caveat:** the original brief file
(`docs/notes/cli-telemetry-buffering.md`) does not exist anywhere readable, so
its options A–D and its "three fork questions" could not be verified. The
options and forks below are **reconstructed** from the phase task list and the
mechanism space; whoever decides should re-map letters if the original
definitions differ.

## Verified facts (each checked against code, most also measured)

1. `emit()` never blocks, raises, or does network on the caller: 5–7µs p50
   steady-state, ~1–2µs gates-closed, one-time ~22ms first enabled call. A
   dead collector is **indistinguishable from a healthy one on the calling
   thread** — measured across healthy / connection-refused / black-holed.
2. Delivery is at-most-once today, and loss is structural: the in-memory
   queue (256) is the only buffer; a batch taken for a failed send is gone;
   `flush(2.0)` against a black-holed collector burns its whole budget on one
   doomed 100-event batch, drops it, and leaves the rest — then the atexit
   flush repeats the ritual (~4s total CLI exit delay, all events lost).
3. The two explicit `flush(2.0)` sites are both CLI run paths; the server has
   no shutdown flush and relies on a lazily-registered, entirely untested
   atexit hook. "Core shutdown" as a named flush site was wrong.
4. Envelope = exactly 7 fields, 1-second `ts`, no unique id. The collector
   adds `received_at` server-side — late delivery is already representable in
   storage without any schema change.
5. Child suppression is opt-in and demo-seeder-only; scheduled/CLI children
   deliberately emit. `emit_once` is per-process, so cron children re-emit
   `activation`/`catalog_used`/`kernel_used` every run — event *counts* are
   already unreliable pre-spool; distinct-install analyses are the only sound
   ones, and the shipped funnel is exactly that (fully duplicate-tolerant).
6. `set_consent(False)` drops the install id irrecoverably but retains
   `consent: false` on disk (deliberate). A spool inherits purge-on-revoke.
7. Telemetry has never shipped. The collector is live; its total recorded
   history is one smoke-test event.

## The unknowns, closed and open

- **Loss rate: unboundable from existing data — there is no data** (unshipped
  feature, one smoke event). Structurally: the client will never self-report
  loss without a protocol change, and collector-side inference can never see
  installs that fail to deliver at all. **Closed in the negative — this is
  what makes "instrument first" live.**
- **Headless vs interactive share: unanswerable, and `mode` can never answer
  it.** Nothing in shipped code sets `package`; pip `flowfile run flow`
  reports `electron`, same as the desktop app. Answering this question
  requires a new signal on the envelope — it is downstream of fork F2, not of
  more data.

## Options, with what each actually costs

**A — status quo (accept loss).** Cost: zero code. What you accept, measured:
every CLI run against a down collector loses its 2–5 events and pays ~4s exit
delay; a month-long outage on an hourly cron box silently loses ~2,160 events
(~0.5 MiB worth). The funnel's five metrics survive loss of any *subset* of an
install's events except total loss of the install (invisible forever). The
collector README already documents this posture in three sentences.

**B — harden inline delivery (no persistence).** Timeout tuning at the two
CLI sites breaks zero tests; adding retry inside `_send`/`flush` breaks three
delivery tests and contradicts the "process exits right after" comments —
and buys nothing against the measured failure mode (a black-holed collector
consumes any budget you give it, one doomed batch at a time). Retry within a
dead window is a longer way to lose the same events.

**C — durable spool + drain.** The real feature. Change surface (all verified
against the suite): a new path seam beside `_settings_file` + placement under
`storage.base_directory` (NOT temp/cache — swept), `_reset_for_tests`
awareness, three test fixtures gain a spool redirect (isolation otherwise
collapses across three suites — the `storage_config` memoized-base-dir trap
makes env monkeypatching insufficient), purge in `set_consent(False)` + new
test, `_send` must start reporting success and distinguishing permanent
(413/422) from transient failures, drain batches must be byte-capped (the
256KiB body cap binds before the 100-event cap at ≥35 max-length node types;
oversize is rejected whole and silently), and one design-statement test
(`test_events_dropped_while_dead_never_resurface` — "no retry queue by
design") must be deliberately rewritten: that test is the current policy.
Docs: collector README paragraph + `docs/users/telemetry.md` on-disk-footprint
section + two CLAUDE.md bullets. Sizing (measured arithmetic): hourly cron ×
month-long outage ≈ 0.5 MiB realistic, 8.9 MiB adversarial-maximal; caps of
**16 MiB / 30 days** cover the legitimate worst case whole with wide margin.
Spool drain also introduces at-least-once delivery → fork F2.

**D — instrument first, decide later.** Ship as-is (or with the one-line
`event_id`), stand up the funnel over real data, and revisit buffering when
the collector can show what loss looks like. Grounding: the funnel is already
loss-shaped (distinct-install booleans degrade gracefully); `received_at` −
`ts` already measures delivery lag with zero schema change; and the strongest
loss signal available without a spool is an install whose `flow_run_started`
stream has gaps — detectable once there is any fleet at all. Cost: the
blind spot stays (never-delivering installs are invisible), which is
acceptable only while the question is "how much loss" rather than "which
installs".

## The three forks, as sharply as the evidence allows

**F1 — buffer at all?** The measured facts cut both ways and the decision is
genuinely open: loss is real and permanent (2), but nothing reported today is
loss-fragile (5), the product has zero users on this pipeline (7), and the
data to size the problem does not exist (unknowns). If the answer is "not
yet", D is the disciplined form of A.

**F2 — duplicates accepted, or schema change?** Sharpened to a point: the
five shipped metrics cannot be distorted by duplicates, so *today* duplicates
are free. But content-based dedup as a future escape hatch is provably lossy
— with 1-second `ts` and no id, legitimately distinct events collide
byte-for-byte precisely in the bursty cases worth measuring (`flow_created`
double-click, `app_started` restart loop) — and the loss is permanent and
biased. If any redelivery mechanism ships, the correct field is a stateless
`event_id` uuid4: one line client-side, +50B/event, no consent-file change,
collector validates-and-copies (its validator currently *drops* unknown
top-level fields, so client-first rollout is safe-and-lossy; the flag day is
CI's parity test, not production). A durable `seq` costs a consent-file
schema change and buys only gap detection — a separate, later decision.

**F3 — if a spool: lifecycle details.** Constraints the design must satisfy,
from code: behind all four gates (the kill-switch short-circuit test will
catch a spool touched too early); sited under `storage.base_directory` beside
`telemetry.yaml`; purge on `set_consent(False)` (and mind the pre-existing
`.tmp` install-id leak edge if the spool write is also tmp-then-replace);
FIFO drop-oldest at 16 MiB, expire at 30 days (both caps justified by
arithmetic in the findings, both larger than any legitimate scenario needs);
drain byte-aware (≤256KiB body, ≤100 events, permanent-vs-transient failure
split); corrupt or unwritable spool must degrade to exactly today's behavior
(silent, at-most-once); and the retention line for the README is fork F3's
output, of the shape: "buffered events are kept at most N days / M MiB and
are deleted when telemetry is turned off."

## What phase 3 should read

This file, then [telemetry-findings.md](telemetry-findings.md) and
[shared/telemetry.py](../../shared/telemetry.py). (Collector counterpart, if
needed: [tools/telemetry_collector/app.py](../../tools/telemetry_collector/app.py).)
