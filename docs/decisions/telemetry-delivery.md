# Decision: telemetry delivery — spool at exit, drain in background

2026-09-01, branch `feature/add-telemetry`. Decision record for the CLI
buffering question. Evidence: `docs/notes/telemetry-findings.md` and
`docs/notes/telemetry-implementation-review.md`. Consent semantics, the four
gates and their order, and the twelve events with their prop allowlists are
settled and unchanged.

## The decision

Adopt the narrow spool (option C in its minimal form) plus a stateless
`event_id`, driven by two measured facts: the only delay a dead collector
causes lives at process exit (up to ~4s pre-fix, buying nothing), and exit-time
delivery was unreliable even against a healthy collector until the flush race
fix. The goal is near-zero exit delay with eventual delivery, not prompt
delivery.

1. **Failed sends spool instead of vanishing.** `_send` learns success: on
   transient failure (connection error, timeout, 5xx) the batch is appended to
   the spool; on permanent rejection (413, 422) it is dropped as today. This
   covers the daemon, `flush`, and the atexit hook with one mechanism.
2. **CLI exit budget shrinks to 0.3s.** The two CLI call sites pass
   `flush(0.3)`: enough for the measured healthy case (27–55ms), and a dead
   collector costs at most ~0.3s + a sub-millisecond spool append instead of
   ~4s of doomed retries. The atexit default stays 2.0 (server/desktop exit,
   where the queue is normally already drained).
3. **Drain on process start, in the daemon thread, behind all four gates.**
   When the daemon starts (first enqueue), it first drains the spool —
   oldest first, then continues with the live queue. Never on the caller
   thread, never before the gates pass. A cron box therefore delivers last
   run's events during this run; any later app/server start sweeps up the rest.
4. **Envelope gains `event_id`** (uuid4, stateless, one line in `_envelope`).
   The spool introduces at-least-once redelivery; with 1-second `ts` and no
   id, content dedup would silently merge legitimately distinct events
   (`flow_created` double-click, restart-loop `app_started`). The collector
   validates it as an optional UUID and stores it. No `seq` — gap detection is
   not a requirement and would force a consent-file schema change.

## Spool specification

- **File:** `telemetry_spool.jsonl`, beside `telemetry.yaml` under
  `storage.base_directory` (never `temp/`/`cache/` — those are swept). One
  envelope per line, compact JSON, append-only between compactions.
- **Gates:** every spool read/write/drain sits behind `is_enabled()` — the
  kill switch, `TESTING`, endpoint, and consent all short-circuit ahead of any
  spool I/O (the existing kill-switch short-circuit test must stay green).
- **Caps:** 16 MiB size, FIFO drop-oldest, enforced on append (compact when
  over); 30-day age, enforced on drain (older lines discarded). Both are far
  above the legitimate worst case (hourly cron, month-long outage ≈ 0.5 MiB).
- **Drain protocol:** batches of ≤100 events AND ≤ ~250 KiB serialized body,
  whichever trips first (the collector's 256 KiB body cap binds before the
  count cap at ≥35 max-length node types). Transient failure → surviving
  lines stay spooled, drain stops until the next trigger. Permanent rejection
  → that batch is dropped. Successful lines are removed (compaction rewrite,
  tmp-then-replace).
- **Failure behavior:** corrupt lines are skipped and discarded at the next
  compaction; an unreadable or unwritable spool degrades silently to exactly
  the pre-spool behavior (events drop; never raise, never block, never log
  above debug).
- **Purge on revoke:** `set_consent(False)` deletes the spool file along with
  the install id — buffered events carry the old id and must not outlive it.
  Tested.
- **Isolation:** the spool path is a monkeypatchable seam beside
  `_settings_file`; `_reset_for_tests` clears/redirect-resets it; the three
  telemetry test suites' fixtures redirect it alongside the consent file.

## Retention policy (README line)

> Undelivered events are buffered locally for at most 30 days and 16 MiB
> (oldest dropped first), and the buffer is deleted immediately if telemetry
> is turned off.

## Out of scope

No `seq`/ordering, no delivery receipts, no collector-side dedup enforcement
(consumers that count events must filter on `event_id` themselves), no change
to what is collected. The collector's own durability stays a separate
question.
