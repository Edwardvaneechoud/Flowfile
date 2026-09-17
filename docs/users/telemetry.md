---
description: What usage data Flowfile sends, when, and how to turn it off.
---

# Privacy & Telemetry

Flowfile can send anonymous usage statistics. We use them to see how the platform is used and what to improve. It is off by default and needs your consent.

## Turning it on or off

- A one-time dialog after install asks for consent. Declining is permanent unless you change it.
- **Settings → Preferences → Privacy** changes the answer at any time.
- `FLOWFILE_TELEMETRY=0` disables it for a whole deployment, regardless of consent. `docker-compose.yml` sets this by default.
- In multi-user Docker deployments consent is one setting, controlled by an administrator.
- [Flowfile Lite](deployment/lite.md) has no telemetry.

## What is sent

Example event:

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

`install_id` is a random UUID created when you opt in. It is deleted when you opt out.

| Event | When |
|---|---|
| `app_started` | The backend starts, telemetry is switched on, or a flow runs headlessly. |
| `flow_created` | A flow is created. |
| `flow_run_started` | A flow run begins. |
| `flow_run_succeeded` | A flow run completes. |
| `flow_run_failed` | A flow run fails. |
| `activation` | First successful run in a session of a flow with 3+ nodes reading non-sample data. |
| `ai_diff_accepted` / `ai_diff_rejected` | An AI-proposed edit is accepted or rejected. |
| `catalog_used` | First flow run in a session with a catalog reader or writer node. |
| `schedule_created` | A schedule is created. |
| `kernel_used` | First use of a Python kernel in a session. |
| `export_code_used` | Code is exported or a project is saved to a folder. |
| `alteryx_imported` | An Alteryx workflow is imported. |
| `alteryx_import_failed` | An Alteryx import fails. |

Extra fields per event:

| Event | Field | Values |
|---|---|---|
| `flow_run_succeeded` | `node_count_bucket` | `1-3` · `4-7` · `8-15` · `16-30` · `31+` |
| | `node_types` | Built-in node type names, sorted, max 60. Custom nodes appear as `custom`. |
| | `duration_bucket` | `<1s` · `1-10s` · `10-60s` · `1-5m` · `5-30m` · `30m+` |
| | `used_sample_data` | `true` · `false` |
| `flow_run_failed`, `alteryx_import_failed` | `error_class` | Exception class name from a fixed list, or `OtherError`. |
| `export_code_used` | `target` | `polars` · `flowframe` · `project_zip` · `project_save` |
| `alteryx_imported` | `tool_count_bucket` | `1-3` · `4-7` · `8-15` · `16-30` · `31+` |
| | `converted_tools` | Names of Alteryx built-in tools that converted, sorted, max 60. |
| | `partial_tools` | Same, for tools that partially converted. |
| | `placeholder_tools` | Same, for tools with no Flowfile equivalent. |

Alteryx tool names are limited to tools Alteryx ships. Third-party plugins are reported as `custom_plugin`, your own macros as `user_macro`.

## What is not sent

- data values
- file names and paths
- column names, SQL, formulas
- flow and node names
- Alteryx workflow names and macro paths
- error messages
- credentials

## Where it goes

Events are sent to `https://events.flowfile.app/events`, a collector run by the Flowfile maintainers. Source: [`tools/telemetry_collector/`](https://github.com/edwardvaneechoud/Flowfile/tree/main/tools/telemetry_collector). It stores accepted events in a JSON-lines file with no expiry. It is behind Cloudflare, which keeps its own request logs. The collector's own access log is off.

To have your events removed, post your install id in [GitHub Discussions](https://github.com/edwardvaneechoud/Flowfile/discussions/categories/q-a).

Failed sends are retried on the next start, for up to 30 days.

## Files on disk

Internal storage is `~/.flowfile` locally, `$FLOWFILE_STORAGE_DIR` when set, or `/app/internal_storage` in Docker.

| Path | Contents |
|---|---|
| `<internal storage>/telemetry.yaml` | Consent answer and install id. |
| `<internal storage>/telemetry_spool.jsonl` | Events not yet delivered. Deleted on delivery, after 30 days, or on opt-out. |

```yaml
consent: true
install_id: 3f6b1c2e-8a94-4c50-9d0e-2f7a61b8c4d1
```

## Environment variables

| Variable | Effect |
|---|---|
| `FLOWFILE_TELEMETRY` | `0`/`false`/`no`/`off` disables telemetry. Other values have no effect. |
| `FLOWFILE_TELEMETRY_ENDPOINT` | Send to a different collector URL. Empty means the default. |
| `TESTING` | `True` disables telemetry. |

Self-host the collector with:

```bash
cd tools/telemetry_collector && docker compose up -d
```

Then set `FLOWFILE_TELEMETRY_ENDPOINT=http://<host>:8300/events`.

## Related

- [Settings](visual-editor/settings.md)
- [Docker reference](deployment/docker.md)
