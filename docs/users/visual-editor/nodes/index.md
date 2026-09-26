# Data Actions

Everything Flowfile can do to your data is one of the actions on this page — read a file, filter rows, join two tables, summarize by group, train a model, write the result somewhere. You drag one onto the canvas, fill in its settings, and connect it to the next one. This page lists all of them, so you can see the full set without opening the app and find the one you need without knowing which category it lives in.

!!! info "On the canvas, an action is called a node"
    The palette, the canvas and the rest of this reference use the word **node**. One node is one action, and the two words mean the same thing here.

## Find an action by what you want to do

| You want to | Use | Where |
|---|---|---|
| Keep only the rows that meet a condition | **Filter data** | [Transformations](transform.md#filter-data) |
| Add a calculated column | **Formula** | [Transformations](transform.md#formula) |
| Remove duplicate rows | **Drop duplicates** | [Transformations](transform.md#drop-duplicates) |
| Fix blanks, stray spaces and inconsistent casing | **Data cleansing** | [Transformations](transform.md#data-cleansing) |
| Rename a lot of columns at once | **Rename columns** | [Transformations](transform.md#rename-columns) |
| Choose, reorder or drop columns | **Select data** | [Transformations](transform.md#select-data) |
| Number the rows | **Add record Id** | [Transformations](transform.md#add-record-id) |
| Split one cell's list into separate rows | **Text to rows** | [Transformations](transform.md#text-to-rows) |
| Look up values from another table (VLOOKUP) | **Join** | [Combine Operations](combine.md#join) |
| Stack two files that share the same columns | **Union data** | [Combine Operations](combine.md#union-data) |
| Match names that are spelled slightly differently | **Fuzzy match** | [Combine Operations](combine.md#fuzzy-match) |
| Total a bill of materials or roll up a chart of accounts | **Explode hierarchy** | [Combine Operations](combine.md#explode-hierarchy) |
| Run part of the flow only when a condition holds | **Gate** | [Combine Operations](combine.md#gate) |
| Reuse another flow as a single step | **Run Flow** | [Combine Operations](combine.md#run-flow) |
| Total or average per customer, month, region | **Group by** | [Aggregations](aggregate.md#group-by) |
| Turn row values into columns (crosstab) | **Pivot data** | [Aggregations](aggregate.md#pivot-data) |
| Turn columns back into rows | **Unpivot data** | [Aggregations](aggregate.md#unpivot-data) |
| Running total, rank or moving average | **Window functions** | [Aggregations](aggregate.md#window-functions) |
| Count the rows | **Count records** | [Aggregations](aggregate.md#count-records) |
| Read every file in a folder | **List files** | [Input Sources](input.md#list-files) |
| Look at the data before deciding what to do | **Explore data** | [Output Operations](output.md#explore-data) |
| Save a result colleagues can query and chart | **Write to Catalog** | [Output Operations](output.md#catalog-writer) |
| Predict a number or a category | **Train Model**, then **Apply Model** | [Machine Learning](ml.md) |
| Write SQL or Python instead of filling in a form | **SQL Query**, **Polars code**, **Python Script** | [Transformations](transform.md#sql-query) |

Nothing here fits? [Build your own node](../node-designer.md) in the Node Designer, or [install one someone else published](../community-nodes.md).

## The six categories

The palette groups actions the same way this reference does, under the same headings.

<div class="grid cards" markdown>

-   :material-tray-arrow-down: **[Input Sources](input.md)**

    ---

    10 actions. Get data in: files, folders, databases, cloud storage, REST APIs, Kafka, Google Analytics, the catalog.

-   :material-table-edit: **[Transformations](transform.md)**

    ---

    14 actions. Reshape one dataset: filter, sort, cleanse, calculate columns, or drop into SQL, Polars or Python.

-   :material-call-merge: **[Combine Operations](combine.md)**

    ---

    9 actions. Bring datasets together with a join, union or fuzzy match — group connected records, explode a hierarchy, branch the flow with a gate, or call another flow.

-   :material-sigma: **[Aggregations](aggregate.md)**

    ---

    5 actions. Summarize and restructure: group, pivot, unpivot, count, window calculations.

-   :material-brain: **[Machine Learning](ml.md)**

    ---

    4 actions. Split a dataset, fit a model, score new rows, and measure how well it did.

-   :material-tray-arrow-up: **[Output Operations](output.md)**

    ---

    7 actions. Send results out: files, databases, cloud storage, the catalog, an API response — or explore them on screen.

</div>

## Every action, A to Z

49 actions as of 2026-09. The palette is the live list; this table is generated from the same source (`flowfile_core/flowfile_core/configs/node_store/nodes.py`) and each name matches what the palette shows.

| Action | What it does | Category | Lite |
|---|---|---|:--:|
| [Add record Id](transform.md#add-record-id) | Generate unique identifiers for each row | Transformations | ● |
| [API response](output.md#api-response) | Return this dataset as the body of an HTTP API endpoint | Output | |
| [Apply Model](ml.md#apply-model) | Score data with a trained model | Machine Learning | |
| [Count records](aggregate.md#count-records) | Calculate the total number of rows | Aggregations | ● |
| [Cross join](combine.md#cross-join) | Create all possible combinations between two datasets | Combine | ● |
| [Data cleansing](transform.md#data-cleansing) | Fix nulls, whitespace, unwanted characters and casing in one step | Transformations | |
| [Drop duplicates](transform.md#drop-duplicates) | Remove duplicate rows based on selected columns | Transformations | ● |
| [Evaluate Model](ml.md#evaluate-model) | Compare actual vs predicted columns and compute quality metrics | Machine Learning | |
| [Explode hierarchy](combine.md#explode-hierarchy) | Explode a bill of materials or chart of accounts to every level; quantities multiply along each path | Combine | |
| [Explore data](output.md#explore-data) | Interactive data exploration and analysis | Output | ● |
| [Filter data](transform.md#filter-data) | Keep only rows that match your conditions | Transformations | ● |
| [Flow Input](input.md#flow-input) | Named entry point for data when this flow runs inside another flow | Input | |
| [Flow Output](output.md#flow-output) | Named exit point exposing this dataset when the flow runs inside another flow | Output | |
| [Formula](transform.md#formula) | Create or modify columns using custom expressions | Transformations | ● |
| [Fuzzy match](combine.md#fuzzy-match) | Join datasets based on similar values instead of exact matches | Combine | |
| [Gate](combine.md#gate) | Pass data through only when a condition holds; otherwise skip what follows | Combine | |
| [Google Analytics](input.md#google-analytics-reader) | Load reports from a Google Analytics 4 property | Input | |
| [Graph solver](combine.md#graph-solver) | Group related records in graph-structured data | Combine | |
| [Group by](aggregate.md#group-by) | Aggregate data by grouping and calculating statistics | Aggregations | ● |
| [Join](combine.md#join) | Merge two datasets based on matching column values | Combine | ● |
| [Kafka Source](input.md#kafka-source) | Read data from a Kafka or Redpanda topic | Input | |
| [List files](input.md#list-files) | List a folder's contents as a table | Input | |
| [Manual input](input.md#manual-input) | Create data directly | Input | ● |
| [Multi-field formula](transform.md#multi-field-formula) | Apply one expression to many columns at once | Transformations | |
| [Pivot data](aggregate.md#pivot-data) | Convert data from long format to wide format | Aggregations | ● |
| [Polars code](transform.md#polars-code) | Write custom Polars DataFrame transformations | Transformations | ● |
| [Python Script](transform.md#python-script) | Execute Python code on an isolated kernel container | Transformations | |
| [Random Split](ml.md#random-split) | Randomly partition rows into named groups (e.g. train/test) | Machine Learning | |
| [Read data](input.md#read-data) | Load data from CSV, Excel, Parquet and other files | Input | ● |
| [Read from Catalog](input.md#catalog-reader) | Read a table from the data catalog | Input | ● |
| [Read from cloud provider](input.md#cloud-storage-reader) | Read data from AWS S3 and other cloud storage | Input | |
| [Read from Database](input.md#database-reader) | Load data from database tables or queries | Input | |
| [Rename columns](transform.md#rename-columns) | Bulk-rename columns by prefix, suffix, or a formula | Transformations | ● |
| [REST API](input.md#rest-api-reader) | Read JSON data from a REST API with auth and pagination | Input | |
| [Run Flow](combine.md#run-flow) | Execute a flow from the catalog, mapping data and parameters into it | Combine | |
| [Select data](transform.md#select-data) | Choose, rename, and reorder columns to keep | Transformations | ● |
| [Sort data](transform.md#sort-data) | Order your data by one or more columns | Transformations | ● |
| [SQL Query](transform.md#sql-query) | Write SQL queries against connected data sources | Transformations | |
| [Take Sample](transform.md#take-sample) | Work with a subset of your data | Transformations | ● |
| [Text to rows](transform.md#text-to-rows) | Split text into multiple rows based on a delimiter | Transformations | |
| [Train Model](ml.md#train-model) | Fit a regression or classification model | Machine Learning | |
| [Union data](combine.md#union-data) | Stack multiple datasets by combining rows | Combine | ● |
| [Unpivot data](aggregate.md#unpivot-data) | Transform data from wide format to long format | Aggregations | ● |
| [Wait For](combine.md#wait-for) | Pass the left input through; the right input only enforces ordering | Combine | |
| [Window functions](aggregate.md#window-functions) | Rolling, cumulative, rank, tile and partition-aggregate calculations | Aggregations | |
| [Write data](output.md#write-data) | Save your data as CSV, Excel, Parquet and other files | Output | ● |
| [Write to Catalog](output.md#catalog-writer) | Save data as a table in the data catalog | Output | ● |
| [Write to cloud provider](output.md#cloud-storage-writer) | Save data to AWS S3 and other cloud storage | Output | |
| [Write to Database](output.md#database-writer) | Save data to database tables | Output | |

A ● marks the 22 actions that also run in [Flowfile Lite](../../deployment/lite.md), the browser-only edition. Lite adds two of its own — External Data and External Output, which fetch from and post to a URL — for 24 in total.

## How an action works

Every node on the canvas behaves the same way:

- **Inputs and outputs.** A node reads from whatever is connected to its input handles and passes its result on from its output handle. The count is fixed per action: Join takes two inputs, Random Split emits two outputs, Write data has no output at all.
- **Settings.** Click a node and its settings open in the right-hand panel. The fields differ per action; each section below lists them.
- **Schema preview.** Once configured, a node reports its output columns and types without running the flow. Run it to see actual rows in the preview panel.
- **Lazy by default.** Most actions build up a Polars query that only executes when you run the flow, so intermediate steps cost nothing until you ask for a result.

New to the canvas? [Building Flows](../building-flows.md) covers creating, connecting, configuring and running nodes. Every action here is also [available from Python](../../python-api/index.md).
