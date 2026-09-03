# Telemetry: what Flowfile sends, and how to turn it off

Flowfile can send anonymous usage events — opt-in, off by default, and limited to the closed schema on this page. Below: every event, every field, every value that can leave your machine, and the exact conditions under which anything is sent.

Product usage reporting only. Unrelated to the [Google Analytics reader](connect/apis.md#google-analytics), which pulls your own GA4 data into a flow, and to the [Explore Data](visual-editor/nodes/output.md) chart builder, which visualizes your data locally.

!!! info "Not in Flowfile Lite"
    The browser-only [Flowfile Lite](deployment/lite.md) edition has no backend, and the telemetry client is part of the backend — nothing is ever sent.

## Off unless you turn it on

Four gates, checked in this order. Each short-circuits before the next does any work; all four must pass before a single event is sent:

1. **The `FLOWFILE_TELEMETRY` kill switch.** Any falsy value (`0`/`false`/`no`/`off`, case-insensitive) disables telemetry outright — before any consent prompt, any file read, any send. A fleet deployed with `FLOWFILE_TELEMETRY=0` never phones home once. Unset changes nothing; a truthy value grants nothing.
2. **`TESTING=True`.** Test runs never send.
3. **A collector endpoint must resolve.** Flowfile ships with one built in — the project's own collector at `events.flowfile.app` ([details below](#where-it-goes)) — so there is nothing to configure. `FLOWFILE_TELEMETRY_ENDPOINT` redirects events to a collector you run instead; an empty value falls back to the built-in one. Blanking it is not a way to switch telemetry off — that is `FLOWFILE_TELEMETRY=0`, or simply never consenting.
4. **Your consent, stored locally.** Nothing is sent until you say yes in the app. The answer lives in a local file you can read and edit ([see below](#where-consent-lives)) — never in a remote service.

Declining the one-time dialog is silent and permanent — you are never asked again. Change your answer any time under **Settings → Preferences → Privacy**.

!!! info "Multi-user Docker deployments"
    Consent there is a single deployment-wide setting an administrator grants or revokes on behalf of everyone using that server. Other users never see the dialog and view the state read-only under **Settings → Preferences → Privacy**.

## What is sent

Every event is a flat JSON object with a fixed envelope. The canonical example — the same one the consent dialog shows:

```json
{
  "event": "flow_run_succeeded",
  "event_id": "b7a1d9c4-3e52-4f18-9a6b-0c5d2e7f8a13",
  "install_id": "3f6b1c2e-8a94-4c50-9d0e-2f7a61b8c4d1",
  "app_version": "0.12.7",
  "platform": "darwin",
  "mode": "electron",
  "ts": "2026-08-29T12:00:00Z",
  "props": {
    "node_count_bucket": "4-7",
    "node_types": ["filter", "output", "read"],
    "duration_bucket": "1-10s",
    "used_sample_data": false
  }
}
```

The envelope: event name, a random per-event id (so a re-sent event can be recognised as the same one, [see below](#where-it-goes)), random install id, app version, platform (`darwin`/`linux`/`windows`/`other`), run mode (`electron`/`docker`/`package`/`other`), UTC timestamp. Event-specific fields live in `props`. The schema is closed — twelve events, fixed props, fixed value sets — and the client drops anything outside it before sending.

| Event | When it fires |
|---|---|
| `app_started` | The backend starts, you switch telemetry on, or a flow runs headlessly (from the command line, a schedule, or Run now in the catalog). |
| `flow_created` | A flow is created. |
| `flow_run_started` | A flow run begins. |
| `flow_run_succeeded` | A flow run completes successfully. |
| `flow_run_failed` | A flow run fails. |
| `activation` | An app session first completes a successful run of a flow with 3 or more nodes that reads at least one real (non-sample) data source. |
| `ai_diff_accepted` | An AI-proposed flow edit is accepted. |
| `ai_diff_rejected` | An AI-proposed flow edit is rejected. |
| `catalog_used` | A flow run in an app session first includes a catalog reader or writer node. Browsing the catalog UI sends nothing. |
| `schedule_created` | A schedule is created. |
| `kernel_used` | A Python kernel is first used in an app session. |
| `export_code_used` | Generated code is exported. |

Only three events carry props at all:

| Prop | Event | Allowed values |
|---|---|---|
| `node_count_bucket` | `flow_run_succeeded` | `1-3` · `4-7` · `8-15` · `16-30` · `31+` |
| `node_types` | `flow_run_succeeded` | Built-in node type names only (as in the example above), sorted, capped at 60 entries; every custom node appears as `custom` |
| `duration_bucket` | `flow_run_succeeded` | `<1s` · `1-10s` · `10-60s` · `1-5m` · `5-30m` · `30m+` |
| `used_sample_data` | `flow_run_succeeded` | `true` · `false` |
| `error_class` | `flow_run_failed` | An exception class name from a fixed allowlist, or `OtherError` — never the error message |
| `target` | `export_code_used` | `polars` · `flowframe` · `project_zip` · `project_save` |

Fine print, so the tables can't mislead:

- `export_code_used` fires when you press Export or Download (or save a project to a folder), never when you open or switch the Code tab. The `polars` and `flowframe` downloads are built in the browser, so the app posts a small empty confirmation to `/editor/code_to_*/exported` to record it.
- A run you cancel sends no completion event — only the `flow_run_started` that fired when it began.
- `activation`, `catalog_used`, and `kernel_used` fire at most once per app session.
- Seeding the built-in Demo catalog runs its flows with telemetry suppressed — Flowfile's own demo runs never count. Later runs of the demo's daily schedule, while the scheduler is enabled, count like any other scheduled run.

## What is never sent

No payload ever contains:

- data values — not a cell, not a row, not a sample
- file paths or file names
- column names
- SQL
- formulas
- flow names or node names you typed
- error message text — a failure sends only the exception class name
- credentials, secrets, or tokens
- hostnames or IP addresses as identity

The only identifier is the install id: a random UUID created the moment you opt in, and at no other time. Turning telemetry off deletes it; opting back in creates a new one, unlinkable to the old. No account, no email, no fingerprint.

## Where it goes

Consented events are sent to `https://events.flowfile.app/events` — the Flowfile project's own collector. It is the same open-source service you can [run yourself](#self-hosting-the-collector): it validates each event against the schema above and appends it to a JSON-lines file. Nothing goes anywhere else — no analytics vendor, no ad network, no third-party SDK inside Flowfile. The one third party in the path is the CDN in front of the collector, described below.

Sending happens on a background thread and never blocks or slows anything you are doing. If the collector is unreachable, the batch is written to a local buffer file instead of being lost, and re-sent the next time Flowfile starts — which is why each event carries its own `event_id`. Undelivered events are buffered locally for at most 30 days and 16 MiB (oldest dropped first), and the buffer is deleted immediately if telemetry is turned off.

The stored event is exactly the fields above plus a server-side receive timestamp. As with any web request, the receiving server and the CDN in front of it see the connecting IP address. `events.flowfile.app` is served through Cloudflare, which terminates TLS and keeps its own request logs under Cloudflare's retention, not a Flowfile setting. The collector itself runs with its access log switched off, so no request line — and so no IP address — is written on the origin; the only thing that lands on disk there is the validated event, in `events.jsonl`. The collector is run by the Flowfile maintainers, and nothing in it rotates or expires that file, so an accepted event stays until a maintainer removes it. To have the events for your install id removed, ask in [GitHub Discussions](https://github.com/edwardvaneechoud/Flowfile/discussions/categories/q-a) with that id — it is the only thing the stored events can be matched on.

## Where consent lives

| Path | Contents |
|---|---|
| `<internal storage>/telemetry.yaml` | Your consent answer and — only while consented — the install id. |
| `<internal storage>/telemetry_spool.jsonl` | Events that could not be delivered yet, one per line, in exactly the shape shown above. Present only after a failed send; deleted as soon as they are delivered, when they age past 30 days, and immediately when you turn telemetry off. |

Internal storage is `~/.flowfile` locally, `$FLOWFILE_STORAGE_DIR` when set, and `/app/internal_storage` in Docker. The whole consent file:

```yaml
# Anonymous usage telemetry — managed from the Flowfile UI (Settings -> Preferences -> Privacy).
# FLOWFILE_TELEMETRY=0 disables telemetry regardless of this file.
# Docs: https://edwardvaneechoud.github.io/Flowfile/users/telemetry.html
consent: true
install_id: 3f6b1c2e-8a94-4c50-9d0e-2f7a61b8c4d1
```

Declined consent is the same file with `consent: false` and no `install_id` line. Hand-editable; there is no database record behind either file. Deleting the buffer file by hand simply discards whatever had not been delivered — turning telemetry off does exactly that for you.

## Self-hosting the collector

The collector is open source at [`tools/telemetry_collector/`](https://github.com/edwardvaneechoud/Flowfile/tree/main/tools/telemetry_collector). `POST /events` validates every event against the schema above (anything outside it is rejected and counted) and appends accepted events as JSON lines to a local `events.jsonl` — readable with any text editor.

```bash
cd tools/telemetry_collector
docker compose up -d
```

Point an install at it with `FLOWFILE_TELEMETRY_ENDPOINT=http://<host>:8300/events` and opt in. That replaces the project's collector for that install; nothing is sent to both.

## For operators

| Variable | Effect |
|---|---|
| `FLOWFILE_TELEMETRY` | Kill switch, not a consent switch. Falsy (`0`/`false`/`no`/`off`, case-insensitive) hard-disables before any prompt, file read, or send. Unset changes nothing; truthy grants nothing — consent always comes from the user. |
| `FLOWFILE_TELEMETRY_ENDPOINT` | Collector URL, used verbatim; overrides the built-in default `https://events.flowfile.app/events`. Unset — or set but empty — uses that default, so blanking it does not disable telemetry. |

Both are read per call. `TESTING=True` also hard-disables, so CI and test runs never send. The bundled `docker-compose.yml` ships `FLOWFILE_TELEMETRY=0`: multi-user deployments are hard-off until the operator lifts the kill switch, and consent there is admin-only. That compose file also passes `FLOWFILE_TELEMETRY_ENDPOINT` through as an empty value, which resolves to the built-in collector — an operator who lifts the kill switch and wants their own destination must set the variable to a real URL.

## Related

- [Settings](visual-editor/settings.md) — app settings; consent itself is managed under **Settings → Preferences → Privacy**.
- [Docker reference](deployment/docker.md) — operator configuration for multi-user deployments.
- [Users, Groups & Sharing](deployment/sharing.md) — how your actual data is protected in multi-user mode.
- For the system the events describe — core, worker, kernels — see [Architecture](../for-developers/architecture.md).
