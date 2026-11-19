# SharePoint Online files

Use the **SharePoint File** node to read CSV (`.csv`), Excel (`.xlsx`), Parquet (`.parquet`), or JSON (`.json`) files into a flow. For JSON, use a single document containing an array of row objects, not newline-delimited JSON. The node is read-only and does not support SharePoint Lists.

## Before you connect

Flowfile uses a saved app-only connection with a tenant ID, application (client) ID, and client secret. This connection is separate from your Flowfile login. For setup on the Microsoft side, follow Microsoft Learn:

- [Register an application in Microsoft Entra ID](https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app)
- [Add and manage app credentials](https://learn.microsoft.com/en-us/entra/identity-platform/how-to-add-credentials)
- [Configure `Sites.Selected` and grant access to the target site](https://learn.microsoft.com/en-us/graph/permissions-selected-overview)

## Connect and pick a file in Flowfile

1. Add a **SharePoint File** node and open its settings. Create a saved connection by entering a connection name, **Tenant ID**, **Application (client) ID**, and **Client secret**, or select an existing connection.
2. Under **Browse SharePoint**, enter the **Site URL** and click **Find libraries**. Choose a **Document library**, navigate to a folder, and select a file. The picker fills in **Drive ID** and **File path**. You can also enter them manually: open **Advanced connection settings** for the drive ID. The file path is relative to the library root.
3. Under **Advanced connection settings**, click **Test connection** to check access to the drive. This does not read the selected file.
4. For Excel, Flowfile reads the first sheet by default. Enter a sheet name directly, or run the node to populate the worksheet choices. **Refresh File Metadata** checks the selected file without downloading it. If worksheet names are unavailable, the saved sheet name stays unchanged.
5. Click **Apply**, then run the flow to read the file.

For CSV files, Flowfile can preview column names and inferred types before a run using up to 64 KiB from the worker. Types are provisional. If SharePoint ignores the range request, the worker reads only a capped prefix and closes the stream. If the sample has too few complete records or cannot be parsed, schema is unavailable until execution; preview does not fall back to a full download.

For JSON arrays of records, Flowfile infers provisional columns and types from complete records within the first 64 KiB. Preview does not support NDJSON or fall back to a full download.

For Parquet, Flowfile previews the stored column names and types from a suffix range of up to 256 KiB. If SharePoint ignores the range or the footer cannot be read within that limit, schema is unavailable until execution.

Flowfile can read SharePoint files up to 64 MiB per file. This download limit does not cap the memory needed to parse a file.

You can reuse a saved connection across multiple nodes. Flowfile encrypts the client secret and does not include it in the saved flow.

When creating a connection, you can set a **Default site URL**. Selecting that connection prefills the picker with its site instead of the deployment default. You can still browse another site by editing the URL; this does not change the saved connection or any previously selected file.

## Optional deployment default

Set `FLOWFILE_SHAREPOINT_DEFAULT_SITE_URL` on the Flowfile Core service to prefill the picker's **Site URL** field. Users can edit the prefilled URL. This setting does not grant access, share connections, or retarget saved flows.

The workflow has been verified locally with simulated Microsoft responses. Live tenant access has not yet been verified.
