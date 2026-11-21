# SharePoint Online files and lists

Use the **Read from SharePoint** node to read CSV (`.csv`), Excel (`.xls` and `.xlsx`), Parquet (`.parquet`), or JSON (`.json`) files into a flow. For JSON, use a single document containing an array of row objects, not newline-delimited JSON. The same reader also supports SharePoint Lists.

## Before you connect

SharePoint reads and writes require the versioned worker compute protocol. Keep `FLOWFILE_COMPUTE_PROTOCOL` enabled and update Core and worker together to compatible versions. The legacy worker API does not support these nodes.

Flowfile uses a saved app-only connection with a tenant ID, application (client) ID, and either a client secret or certificate. This connection is separate from your Flowfile login. For setup on the Microsoft side, follow Microsoft Learn:

- [Register an application in Microsoft Entra ID](https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app)
- [Add and manage app credentials](https://learn.microsoft.com/en-us/entra/identity-platform/how-to-add-credentials)
- [Configure `Sites.Selected` and grant access to the target site](https://learn.microsoft.com/en-us/graph/permissions-selected-overview)

## Connect and pick a file in Flowfile

1. Add a **Read from SharePoint** node and open its settings. Create a saved connection by entering a connection name, **Tenant ID**, **Application (client) ID**, and an authentication method, or select an existing connection. For **Certificate**, select a PEM file containing the certificate and its matching unencrypted RSA private key; register only the public certificate in Entra.
2. Under **Browse SharePoint**, enter the **Site URL** and click **Find libraries**. Choose a **Document library**, navigate to a folder, and select a file. The picker fills in **Drive ID** and **File path**. You can also enter them manually: open **Advanced connection settings** for the drive ID. The file path is relative to the library root.
3. Under **Advanced connection settings**, click **Test connection** to check access to the drive. This does not read the selected file.
4. For Excel, leave **Sheet name** blank to read the first sheet, or enter a sheet name directly. A successful run populates the worksheet choices. **Refresh File Metadata** checks the selected file without downloading it or discovering new worksheet names. If a saved worksheet is missing, enter an existing name or clear the field, then apply and run again. Flowfile never changes the selection for you.
5. Click **Apply**, then run the flow to read the file.

To read several files, enter a pattern such as `reports/sales-*.csv` in **File path or glob**. Use `*` or `?` in the filename only, with a fixed extension. Flowfile checks only the specified folder and appends matching files in filename order. The files must have the same columns in the same order and with the same types. A pattern can read up to 32 files. For Excel patterns, enter a sheet name that exists in every matching workbook, or leave it blank to read the first sheet from each. Worksheet choices after a successful run are limited to names common to all those workbooks.

For CSV files, Flowfile can preview column names and inferred types before a run using a range of up to 64 KiB from the worker. Types are provisional. If SharePoint ignores the range request, or the sample has too few complete records or cannot be parsed, schema is unavailable until execution. Preview does not fall back to a full download.

For JSON arrays of records, Flowfile infers provisional columns and types from complete records within the first 64 KiB. Preview does not support NDJSON or fall back to a full download.

For Parquet, Flowfile previews the stored column names and types from a suffix range of up to 256 KiB. If SharePoint ignores the range or the footer cannot be read within that limit, schema is unavailable until execution.

The optional personal [Excel column preview](#microsoft-sign-in-desktop-only) only works for `.xlsx` files. For `.xls`, columns are available after a run. Wildcard patterns do not preview a schema. Each reader run can download up to 64 MiB total, including all matches for a pattern. This download limit does not cap the memory needed to parse the data.

You can reuse a saved connection across multiple nodes. Flowfile encrypts the credential and does not include it in the saved flow. In a multi-user deployment, an owner can use **Share** to grant a group access to use the connection without giving members the credential. Choose **Manage** to also allow the group to edit or delete the connection.

Select a saved connection and choose **Edit** to rename it. To use a different saved default site or tenant, create a new connection and authenticate again. Existing flows keep their original connection until you explicitly change it. **Delete** removes the saved credential and group grants. Flows that use that connection will need another connection selected before they can run.

FlowFrame Python export references the saved connection by ID and does not include credentials. Run the exported script in a Flowfile environment that has access to that connection.

Switching between the visual flow and Notebook preserves the SharePoint connection reference and node settings. Editing the Notebook cells does not upload files or append list items. Run the flow to write to SharePoint.

When creating a connection, you can set a **Default site URL**. Selecting that connection prefills the picker with its site instead of the deployment default. You can still browse another site by editing the URL; this does not change the saved connection or any previously selected file.

Core also exposes site search through `GET /sharepoint_connections/{connection_id}/sites?search=...`. Microsoft Graph does not support site search with the `Sites.Selected` application permission. For a least-privilege connection, continue to browse by the known Site URL rather than broadening permissions just to search.

## Read a SharePoint list

In the **Read from SharePoint** node, set **Source** to **SharePoint list**. Enter a site URL, click **Find lists**, select a list, and select at least one column. Optionally enter a Graph OData filter expression such as `fields/Quantity gt 0` (without the `$filter=` prefix; use the internal column name). Graph applies the filter using its usual restrictions, including indexed-column requirements. Then click **Apply** and run the flow. List schema is available after execution, not before. The reader requests up to 200 items per page and fails rather than returning partial data if Graph requires more than 10 pages.

## Browse OneDrive for Business

Click **Find OneDrive for Business** instead of **Find libraries** in the file reader or writer. With an app-only connection, enter the target user's Microsoft Entra **object ID** (not their email or application ID). The app needs permission to discover and access that user's organizational drive; a site-specific grant for another site does not grant access to it. A desktop personal Microsoft sign-in can discover only the signed-in user's business drive, and only for reading. Consumer OneDrive is not supported. The node saves the selected drive ID and path; scheduled runs use an app-only connection and do not rediscover the drive.

## Read a file only when it changes

For an exact CSV, JSON, or Parquet path, enable **Incremental read**, click **Apply**, and save/register the flow. Full reads remain the default. The first incremental run downloads the entire file. After that, an update downloads the whole changed file, not just changed rows. An unchanged or deleted file returns zero rows with the last known schema; it does not skip downstream nodes. Use a connection that can query Microsoft Graph delta for the selected drive.

Flowfile stores the checkpoint separately from the flow. It advances after the reader and its downstream nodes complete without failure or error-driven skips. A canceled run does not advance it. Deliberately gated-off outputs count as complete, and failures in unrelated branches do not block this reader's checkpoint. If Graph expires the checkpoint, the run fails instead of silently downloading the file again. Use **Reset checkpoint on save** and **Apply** to make the next run read the full file. Design downstream writes to tolerate retries: a failed or canceled run may already have made some writes.

## Write a file to a SharePoint document library

Connect **Write to SharePoint** to an upstream node and select a saved app connection with write access to the destination site. If using `Sites.Selected`, grant the app the `write` role on that site. Personal Microsoft sign-in is not supported for writes.

1. Enter the **Site URL**, click **Find libraries**, and select your document library. Open an existing folder and click **Use this folder**, or enter its drive ID and a library-relative path manually.
2. Choose CSV, Excel (`.xlsx`), or Parquet and enter a filename with the matching extension, such as `Reports/output.csv`.
3. Choose **Create** (fails if the file exists), **Overwrite** (replaces the whole file), or **Create a new file with a suffix** (keeps the existing file). Excel also supports **Update sheet**, which replaces or adds the named sheet, and **Append rows**, which adds rows below existing data without repeating headers. Both create a missing workbook or sheet and preserve the other sheets. The sheet defaults to `Sheet1`. Append requires the incoming columns to match the worksheet's first-row headers in name and order. If the sheet contains an Excel table, it must be a single table covering the data with no totals row. Append extends that table but does not deduplicate rows. Both modes download and re-upload the workbook when editing an existing file; embedded images may be lost.
4. **Write output when there are no rows** is off by default. With it off, the writer leaves the destination unchanged. Turn it on to create or replace a zero-row file (with headers for Excel and CSV). **Create** still fails on a name conflict; the suffix option writes a separate file on conflict; **Overwrite** replaces the entire file; **Update sheet** replaces only the named sheet with headers or creates it if missing. **Append rows** always skips empty output.
5. Click **Apply**, then run the flow. Opening the settings or previewing the node does not upload a file. Larger files use a Microsoft Graph upload session.

In standalone Python, `frame.write_sharepoint(connection_id, drive_id="…", file_path="Reports/output.csv", format="csv", conflict_mode="error").collect()` runs the upload using a saved app connection. Python export references the connection ID, not its credential. Shared browser flows show the writer as an unsupported placeholder and cannot upload files.

Use `write_when_empty=True` to opt in from Python, `conflict_mode="update", sheet_name="Report"` to replace an Excel sheet, or `conflict_mode="append", sheet_name="Report"` to append rows. Microsoft Graph's [drive delta API](https://learn.microsoft.com/en-us/graph/api/driveitem-delta?view=graph-rest-1.0) reports file changes; it does not apply workbook patches. Graph can [append](https://learn.microsoft.com/en-us/graph/api/tablerowcollection-add?view=graph-rest-1.0) or [delete](https://learn.microsoft.com/en-us/graph/api/tablerow-delete?view=graph-rest-1.0) Excel table rows without a file upload, but those endpoints do not support the app-only connections used by this writer.

In a Flowfile notebook, `ff.read_sharepoint(...)` and `frame.write_sharepoint(...)` build nodes using saved connection IDs. Push the cells to the canvas and run the flow there to read or write SharePoint data. Running a cell does not contact SharePoint, even to infer columns.

For [headless or scheduled runs](../deployment/where-flows-run.md), use a saved app-only connection accessible to the account that runs the flow. The runtime needs the corresponding Flowfile credential store; a flow file or exported script alone does not carry credentials. In Docker and other multi-user server deployments, use app-only connections for both reads and writes. A write runs on each execution, so choose a conflict mode appropriate for repeated runs. If Graph denies or throttles an upload, the run fails rather than retrying it automatically.

To try the workflow against Microsoft Graph, use a dedicated non-production site, library, and test connection. Grant only the necessary access, read a known fixture file, then write to a separate test folder and inspect the uploaded contents before removing the test files. Keep connection credentials in the runtime's credential store or project secret configuration, not in fixtures, shell history, or logs.

## Append items to a SharePoint list

In **Write to SharePoint**, select **SharePoint list (append items)**, use a saved app connection with write access, and choose the site and list. Enter a JSON mapping from input column names to the list's internal column names, for example `{"sku": "Title", "quantity": "Quantity"}`. **Show writable columns** displays the internal names. The writer supports text, single-choice, boolean, number, currency, and date/time values. Nulls are omitted; each row needs at least one mapped non-null value.

Each run appends new items; there is no update, overwrite, or deduplication. If Graph rejects a request partway through, Flowfile reports how many items Graph confirmed. Check the list before retrying to avoid duplicates. Opening or previewing the writer does not append items.

## Import a SharePoint connection with a project

Projects that include the `sharepoint_connection` kind can export app-only connections you own. The YAML under `connections/sharepoint/` contains a `${secret:<name>}` reference instead of the credential. Before importing, supply the client secret or certificate PEM through Flowfile's [project secret configuration](../projects.md), then apply the project. Without a credential or a matching local connection, the reader or writer stays unbound. You can supply the credential and apply again; Flowfile restores the connection using its project identity without changing the selected file, worksheet, or list.

Personal Microsoft sign-in connections and connections shared to you are not portable project connections. Select an authorized local connection after importing those readers.

## Optional deployment default

Set `FLOWFILE_SHAREPOINT_DEFAULT_SITE_URL` on the Flowfile Core service to prefill the picker's **Site URL** field. Users can edit the prefilled URL. This setting does not grant access, share connections, or retarget saved flows.

The workflow has been verified locally with simulated Microsoft responses. Live tenant access has not yet been verified.

## Microsoft sign-in (desktop only)

For a personal connection, choose **Microsoft sign-in** and enter the tenant and
application IDs. Configure the Entra app as a public client and grant delegated
Microsoft Graph permissions for `Files.Read.All` and `Sites.Read.All`. See
[Microsoft's device code flow documentation](https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-device-code)
for the sign-in flow and tenant requirements.

Open the Microsoft link shown in Flowfile and enter the displayed code. Flowfile
asks you to save the connection after sign-in. It encrypts the refresh token and keeps the connection private. Access is limited to what your Microsoft account can access.

**Preview Excel column names before running** is off by default. If you enable it when signing in or reconnecting, configure and consent to the delegated Microsoft Graph permission `Files.ReadWrite`. [Microsoft requires that permission for Excel range reads](https://learn.microsoft.com/en-us/graph/api/worksheet-range?view=graph-rest-1.0), even though Flowfile only reads cells and never edits the workbook. Preview reads up to 256 headers from row 1 of the selected worksheet (or the first worksheet if no name is set). Types are not inferred. Without consent, or if the range cannot be read, columns are unavailable until a run. To turn off the feature for an existing connection, reconnect with the option unchecked. This stops Flowfile requesting the permission for that connection; it does not revoke existing Microsoft consent.

Flowfile does not yet persist replacement refresh tokens issued during token renewal. If the stored token stops working, select the saved connection and choose **Reconnect**. Sign in with the intended Microsoft account and confirm. Existing flows keep the same connection selection. App-only authentication is unaffected.

## Run the opt-in live smoke test

Use a dedicated **non-production** SharePoint Online site and document library. Create a test Entra app with Microsoft Graph application permission `Sites.Selected`, grant admin consent, and grant the app the `read` role on that site only. In the library root, create a synthetic file named `flowfile-live-input.csv`:

```csv
sku,quantity
A,2
B,5
```

Set the following environment variables in the test process:

| Variable | Value |
| --- | --- |
| `FLOWFILE_TEST_SHAREPOINT_ENABLED` | `1` to explicitly opt in |
| `FLOWFILE_TEST_SHAREPOINT_TENANT_ID` | Test Entra directory (tenant) UUID |
| `FLOWFILE_TEST_SHAREPOINT_CLIENT_ID` | Test app (client) UUID |
| `FLOWFILE_TEST_SHAREPOINT_CLIENT_SECRET` | Test app secret value, injected from a secure store |
| `FLOWFILE_TEST_SHAREPOINT_SITE_ID` | Microsoft Graph site ID (`hostname,site UUID,web UUID`) |
| `FLOWFILE_TEST_SHAREPOINT_DRIVE_ID` | Microsoft Graph drive ID of the dedicated test library on that site |

From the repository root, run:

```bash
poetry run pytest -q -rs --tb=short --show-capture=no -m sharepoint shared/tests/test_sharepoint_live.py
```

Without explicit opt-in or any required setting, the test skips without contacting Microsoft. With configuration, it checks that the library belongs to the specified site and reads the known CSV through the connector, asserting the exact rows and schema. The test is read-only and leaves the fixture untouched. Remove the fixture and revoke any temporary site grant or app credential when testing is finished. Do not put the secret in a committed file, shell history, or test report; do not run with `--showlocals`.

This smoke test covers only app-only client-secret authentication, library lookup, and CSV reading. It does not verify saved connections, writes, other auth methods, or desktop and Docker scheduling.
