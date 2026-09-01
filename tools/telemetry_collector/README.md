# Flowfile telemetry collector

A tiny, self-contained FastAPI service that receives Flowfile's opt-in
anonymous telemetry events and appends them as JSON lines to
`events.jsonl`. No database, no monorepo imports — the whole deployment is
this directory.

## Running it

```bash
cd tools/telemetry_collector
docker compose up -d          # collector on port 8300, data in ./data/
curl http://localhost:8300/health
```

Point a Flowfile install at it — this replaces the project's built-in default
collector for that install, and telemetry stays off until the user also opts
in from the UI:

```bash
export FLOWFILE_TELEMETRY_ENDPOINT=http://<host>:8300/events
```

Funnel report over the collected data:

```bash
python -m tools.telemetry_collector.funnel data/events.jsonl [--days 30]
```

It prints installs / launched / run_attempted / activated / week2_return
with conversion percentages; malformed lines are skipped and reported to
stderr.

## Where to run it

The Flowfile client still fails silently by design (telemetry must never
produce a user-facing error), but downtime no longer means lost events: a
batch the client cannot deliver is written to a local spool file and re-sent
the next time the app, the server or a CLI run starts. Delivery is therefore
**at-least-once and eventual, not prompt** — a batch can arrive hours or days
after its `ts`, which is why every event carries a unique `event_id` for
consumers that need to count. The retention policy:

> Undelivered events are buffered locally for at most 30 days and 16 MiB
> (oldest dropped first), and the buffer is deleted immediately if telemetry
> is turned off.

Downtime longer than that window, or a machine that never runs Flowfile
again, still loses events. Pick a deployment with that in mind:

- **Local machine behind a tunnel** (Cloudflare Tunnel / Tailscale Funnel)
  or dynamic DNS + port-forward: zero cost and the data stays home.
  Trade-offs: uptime is tied to your machine being awake and online (events
  buffer on the sender meanwhile and arrive late), TLS is solved by the
  tunnel but is DIY with raw port-forwarding, and dynamic DNS exposes your
  home IP.
- **Small VPS (~$4–6/mo)**: always-on, stable DNS, TLS via Caddy or nginx in
  front. Trade-off: another box to patch, and the data now lives off-site.
- **Hosted/managed ingestion**: only worth considering at volume; overkill
  for a single-project funnel.

**Recommendation:** start on the local machine behind a Cloudflare Tunnel —
free, TLS included, no open ports. Move the same compose file to a VPS once
delivery latency, or outages longer than the 30-day buffer, start to matter
more than the hosting cost.

## Data handling

Each accepted event is stored as one line, with a server-side `received_at`
added:

```json
{"event":"flow_run_succeeded","install_id":"3f6b1c2e-8a94-4c50-9d0e-2f7a61b8c4d1","app_version":"0.12.7","platform":"darwin","mode":"electron","ts":"2026-08-29T12:00:00Z","props":{"node_count_bucket":"4-7","node_types":["filter","output","read"],"duration_bucket":"1-10s","used_sample_data":false},"event_id":"b7a1d9c4-3e52-4f18-9a6b-0c5d2e7f8a13","received_at":"2026-08-29T12:00:01.481275Z"}
```

The two timestamps differ in precision: the client's `ts` is whole seconds,
while `received_at` is stamped with microseconds. Parse them with separate
formats, or with something that accepts both. Only `received_at` is
trustworthy for windowing — `ts` is client-supplied — which is what the funnel
script uses, falling back to `ts` for lines written before it was stamped.

`event_id` is optional and only present for clients new enough to send it. It
is the field to deduplicate on: because a spooled batch is re-sent until the
collector accepts it, a batch that was stored but whose response never reached
the sender arrives twice. Nothing the funnel reports is duplicate-sensitive
(every number is a distinct-`install_id` count), so no de-duplication happens
on this side; a consumer that counts *events* must do it itself.

Install ids are random UUIDs generated on the user's machine at opt-in; they
carry no identifying information, and events never contain data values,
paths, names, or anything user-typed.

The collector re-validates every event before it is stored, and rejects the
whole event on the first thing that fails. How strict that is depends on the
field. Event name, platform, mode, the count/duration buckets and the export
target must match a frozen value set; `install_id`, and `event_id` when it is
present at all, must parse as a UUID; `error_class` and each entry of
`node_types` must be a Python identifier
within the length cap. One field cannot be enumerated: `app_version` is a
version string, so it is capped in shape and length rather than checked
against a list of known releases. Treat it as a short constrained string, not
as a closed value set.

To rotate or archive, stop the collector (or just accept a seam), move
`events.jsonl` aside (e.g. `events-2026-08.jsonl`), and restart — the file
is recreated on the next batch, and the funnel script can be run over any
archived file.
