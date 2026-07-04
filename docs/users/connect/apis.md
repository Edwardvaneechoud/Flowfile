# REST APIs and Google Analytics

Pull data from HTTP endpoints and Google Analytics 4 into a flow. This page covers the REST API Reader (node and `ff.read_api`) and the Google Analytics reader. Both are read-only sources: they fetch data in, they do not write back.

!!! info "Not in Flowfile Lite"
    Both readers require the full desktop or server build. The browser-only [Flowfile Lite](../deployment/lite.md) edition has no backend and cannot reach external APIs.

## REST APIs

The **REST API Reader** node fetches JSON from an HTTP endpoint, flattens the response into a table, and hands it downstream. It handles authentication and pagination so you don't script the request loop yourself. JSON is the only supported response format.

### Request settings

The node's request is built from these settings (the same fields `ff.read_api` accepts):

| Setting | Meaning |
|---|---|
| URL | The request URL. |
| Method | `GET` or `POST`. |
| Headers | Request headers as key/value pairs. |
| Query params | URL query parameters. |
| JSON body | Request body for `POST`. |
| Record path | Dot-path to the record array inside the response (e.g. `data.items`). Empty uses the top-level response. |
| Timeout (seconds) | Per-request timeout (default 30). |
| Max retries | Retries for transient failures (default 3). |

### Authentication

Authentication is configured by `auth_type`, one of:

- `none` — no authentication.
- `api_key` — an API key sent as a header (default name `X-API-Key`) or query parameter, per `api_key_location`.
- `bearer` — a bearer token in the `Authorization` header.
- `basic` — HTTP basic auth with a username and a secret password.

In the node, the credential references a stored secret by name ([Secrets](../visual-editor/catalog/secrets.md)) so it never lands in the flow file. In `ff.read_api` you may instead pass an inline `secret` for programmatic use; it is encrypted with the master key and never persisted.

### Pagination

`pagination_type` selects the strategy:

- `none` — a single request.
- `offset` — offset/limit paging (`offset_param`, `limit_param`, `page_size`).
- `page` — page-number paging (`page_param`, `start_page`, `page_size`).
- `cursor` — cursor / next-token paging (`cursor_param`, `cursor_response_path`, `initial_cursor`).

Safety caps `max_pages` (default 1000), `max_records`, and `page_delay_seconds` bound the paging loop.

### Read a REST API in Python

`ff.read_api` builds a REST API Reader node and returns a [FlowFrame](../python-api/index.md). The fragment below is **illustrative** — it points at a placeholder host, so it will not run as written; substitute your own endpoint and credentials.

```python
# Illustrative — not runnable as written.
import flowfile as ff

users = ff.read_api(
    "https://api.example.com/v1/users",
    auth={"auth_type": "bearer", "secret": "YOUR_TOKEN"},
    pagination={"pagination_type": "offset", "page_size": 100},
    record_path="data",
).collect()
```

The full signature is:

```python
ff.read_api(
    url,
    *,
    method="GET",
    headers=None,
    params=None,
    json_body=None,
    auth=None,
    pagination=None,
    record_path="",
    timeout_seconds=30.0,
    max_retries=3,
)
```

`auth` and `pagination` accept a plain dict (as above) or the typed `RestApiAuthSettings` / `RestApiPaginationSettings` objects.

## Google Analytics

The **Google Analytics** reader pulls reports from a **GA4 property** — dimensions and metrics over a date range — into a flow. It reads through a saved Google Analytics connection; there is no `ff.*` helper for GA, so set the connection up first, then use the reader node on the canvas.

### Connecting an account

Create a Google Analytics connection on the **Connections** page under the **Google Analytics** tab. Two credential types are supported:

- **OAuth 2.0** — sign in with Google and consent to the `analytics.readonly` scope. Flowfile stores the resulting refresh token encrypted; no raw credential is entered by hand. This requires a Google OAuth **web application client** (client id, client secret, redirect URI), which you paste into the Google OAuth card on the same page. Create the client at [Google Cloud Console → Credentials](https://console.cloud.google.com/apis/credentials).
- **Service account** — upload a service-account JSON key. Flowfile validates and encrypts it at rest. The service account's email must be granted **Viewer** on each GA4 property you want to read; a service-account connection sees whatever properties that email has access to.

Each connection can carry a **default property id** (the GA4 property, e.g. `123456789`). Use **Test** on the connection to confirm the credential is valid — for a service account with a default property set, the test also verifies Viewer access to that property.

!!! note "Credentials needed"
    OAuth needs a Google OAuth web-application client (id, secret, redirect URI) plus the interactive sign-in. A service account needs the JSON key and Viewer access on the target GA4 property. In either case you supply a GA4 property id to run a report.

No runnable example is provided for Google Analytics: it reads from a live Google account and property that can't be seeded in a test.
