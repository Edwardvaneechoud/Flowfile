Changes since [v0.24.0](https://github.com/edwardvaneechoud/Flowfile/releases/tag/v0.24.0).

A patch release: a rebuilt REST API reader that can page cursor APIs with a POST body, a fix for flows that failed after sitting open for a day, and a set of smaller fixes.

## Highlights

### Improvements
- **REST API reader: cursor paging in a POST body, and a new drawer (#886, closes #858).** The reader can now page keyset APIs that expect the cursor inside a POST body (set the cursor location to body; nested names like `variables.page` work). Dot-paths can index lists, so `data.-1._id` is the `_id` of the last record. Paging stops at an empty page or a repeated cursor. Thanks @KenlyYang for the request. The drawer now follows the order of the request: URL, parameters and headers, body, authentication, **Test request**, Records, More pages, Advanced, Columns. **Test request** shows the status and the request exactly as sent, with **Copy as cURL**. **Records** shows the response as a foldable tree where you click a list to use it as the rows, and **More pages** offers **Read all pages** when it finds a next-page token. A run logs one line per page and why it stopped, and a failure names the request, the status and the API's own answer.
- **Known cached results on canvas nodes (#862, toward #83).** A node whose result is still cached on its backend now shows a small cached icon and a thin border, next to the usual run, error and placement indicators. 
- **Cancelled runs show as Cancelled (#887).** A run you stop on purpose is now recorded and shown as **Cancelled** in run history, the flow and run detail panels and the designer, instead of **Failed**. Cancelled runs are counted separately from failures in run summaries, and are not treated as failed runs for alerts.
- **Catalog previews show the right row totals (#859, closes #566).** After appending to a catalog table, the reported row count is the whole table (200, then 210, then 220) rather than the rows of the last batch. The Rows card uses the stored count, and switching quickly between tables or versions no longer lets a late preview or history response replace the one you selected.

### Fixes
- **Flows no longer fail after being open for a day (#883, closes #878).** After 24 hours the worker cleaned up results that an open flow still used, and the next run failed at the nodes reading them (for example Explore Data nodes under a sort). Flowfile now recomputes only the inputs whose result is gone and retries the node. Previews, column statistics, CSV/TSV export and Explore Data charts over such a result no longer fail obscurely: the preview offers **Fetch data**, and the others tell you to run the flow again. Thanks @steven4320555 for the report.
- **Delete key removes selected canvas elements (#880, closes #354).** Delete now removes a selected node or connection, as Backspace already did, so you can drop a connection without deleting the nodes it joins. Thanks @csarrico for the report.
- **Hyphenated table names in catalog SQL (#889).** Qualified references to a table whose name continues another table's name after a hyphen (such as `city-sales` and `city-sales-unique`) are no longer partly rewritten.
- **OpenAI defaults move off gpt-4.1 (#884).** OpenAI is retiring `gpt-4.1` and `gpt-4.1-mini` on 14 January 2027. The OpenAI fallback and the default model for every AI surface are now `gpt-6-luna`, the replacement OpenAI lists. Models you pick yourself are unchanged.
- **Newer OpenAI models route correctly (#891).** OpenAI model names are now always sent with the `openai/` prefix, so a model too new for the bundled model list still reaches OpenAI.

### Docs and examples
- **Bill of materials template (#881).** A new **Bill of Materials Explosion** template flow explodes a multi-level bicycle bill of materials, rolls up material cost per product and traces the path of each component. It comes with sample data (`bom_lines.csv`, `part_costs.csv`).

## Security
- The REST API reader masks the Authentication secret as `REDACTED` in errors and logs. Before, an API key sent as a query parameter could appear in the error text (#886).

If you run into something that doesn't work as expected, please [open an issue](https://github.com/edwardvaneechoud/Flowfile/issues).

## What's Changed
* Fix catalog preview row totals and selection races by @LynxLeuck in https://github.com/edwardvaneechoud/Flowfile/pull/859
* Add example for bill of materials and unpack node by @Edwardvaneechoud in https://github.com/edwardvaneechoud/Flowfile/pull/881
* Show a cancelled run as Cancelled instead of Failed by @Edwardvaneechoud in https://github.com/edwardvaneechoud/Flowfile/pull/887
* Implement "-" in recognizing the table names by @Edwardvaneechoud in https://github.com/edwardvaneechoud/Flowfile/pull/889
* Fix Delete key for selected canvas elements by @LynxLeuck in https://github.com/edwardvaneechoud/Flowfile/pull/880
* Fix OpenAI defaults before gpt-4.1 shutdown by @Anandb71 in https://github.com/edwardvaneechoud/Flowfile/pull/884
* Bug/unresolved data after 24h by @Edwardvaneechoud in https://github.com/edwardvaneechoud/Flowfile/pull/883
* Feature/improve json handling by @Edwardvaneechoud in https://github.com/edwardvaneechoud/Flowfile/pull/886
* Show known cached results on canvas nodes by @LynxLeuck in https://github.com/edwardvaneechoud/Flowfile/pull/862
* Fix/OpenAI model handling by @Edwardvaneechoud in https://github.com/edwardvaneechoud/Flowfile/pull/891

## New contributor 🎉

A big thanks to **@Anandb71** for their first contribution! #884 moves the OpenAI defaults off `gpt-4.1` ahead of its shutdown.

**Full Changelog**: https://github.com/edwardvaneechoud/Flowfile/compare/v0.24.0...v0.24.1
