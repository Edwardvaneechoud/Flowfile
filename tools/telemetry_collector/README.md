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

Point a Flowfile install at it (telemetry stays off until the user also
opts in from the UI):

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

Because the Flowfile client fails silently by design (telemetry must never
produce user-facing errors or retries), **collector downtime means silently
lost events** — there is no client-side retry queue. Pick a deployment with
that in mind:

- **Local machine behind a tunnel** (Cloudflare Tunnel / Tailscale Funnel)
  or dynamic DNS + port-forward: zero cost and the data stays home.
  Trade-offs: uptime is tied to your machine being awake and online (missed
  events are simply gone), TLS is solved by the tunnel but is DIY with raw
  port-forwarding, and dynamic DNS exposes your home IP.
- **Small VPS (~$4–6/mo)**: always-on, stable DNS, TLS via Caddy or nginx in
  front. Trade-off: another box to patch, and the data now lives off-site.
- **Hosted/managed ingestion**: only worth considering at volume; overkill
  for a single-project funnel.

**Recommendation:** start on the local machine behind a Cloudflare Tunnel —
free, TLS included, no open ports. Move the same compose file to a VPS once
gaps in the data start to matter more than the hosting cost.

## Data handling

Each accepted event is stored as one line, with a server-side `received_at`
added:

```json
{"event":"flow_run_succeeded","install_id":"3f6b1c2e-8a94-4c50-9d0e-2f7a61b8c4d1","app_version":"0.12.7","platform":"darwin","mode":"electron","ts":"2026-08-29T12:00:00Z","props":{"node_count_bucket":"4-7","node_types":["filter","output","read"],"duration_bucket":"1-10s","used_sample_data":false},"received_at":"2026-08-29T12:00:01Z"}
```

Install ids are random UUIDs generated on the user's machine at opt-in; they
carry no identifying information, and events never contain data values,
paths, names, or anything user-typed.

The collector re-validates every event before it is stored, and rejects the
whole event on the first thing that fails. How strict that is depends on the
field. Event name, platform, mode, the count/duration buckets and the export
target must match a frozen value set; `install_id` must parse as a UUID;
`error_class` and each entry of `node_types` must be a Python identifier
within the length cap. One field cannot be enumerated: `app_version` is a
version string, so it is capped in shape and length rather than checked
against a list of known releases. Treat it as a short constrained string, not
as a closed value set.

To rotate or archive, stop the collector (or just accept a seam), move
`events.jsonl` aside (e.g. `events-2026-08.jsonl`), and restart — the file
is recreated on the next batch, and the funnel script can be run over any
archived file.
