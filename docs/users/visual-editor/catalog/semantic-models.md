# Semantic Models

A semantic model records what the tables of a catalog schema mean: what each column describes, how tables join, and which metrics (such as total revenue) everyone should calculate the same way. This page covers where to find a model, how to fill it in, how its facts are reviewed, and how to ask for a number from an approved metric.

!!! info "Not in Flowfile Lite"
    Semantic models require the full desktop/server build and are not available in the browser-only [Flowfile Lite](../../deployment/lite.md) edition.

---

## What a semantic model holds

There is one model per **schema**. It holds three kinds of facts:

| Fact | Example |
|---|---|
| **Descriptions and roles** | "One row per order" on a table; on a column, a description, how it is used (Identifier, Date, Group by or Amount) and a confidentiality level (Public, Internal, Confidential or Restricted). |
| **Joins** | `orders.customer_id` matches `customers.customer_id`. |
| **Metrics** | `revenue` is `SUM(amount)`, counted on `orders`, without cancelled orders. |

Joins that cross schemas are held one level up, in the model of the **catalog** that contains the schemas.

Every fact is a proposal first. It becomes part of the model when it is **accepted**, and it keeps its author, its status and a history of who changed it and when. Only accepted facts are used to answer questions. A metric that is edited keeps its accepted definition until the new one is accepted, and carries a version number.

---

## Open a model

In the catalog tree, expand a schema (or a catalog) and click **Semantic model**, the first entry under it. A badge shows how many facts wait for review. The address of the page keeps the selection (`semanticNamespaceId` in the URL), so a reload returns to the same model.

A table that is part of the model shows a small model icon after its name in the catalog tree (blue when it has accepted facts, orange when its facts all wait for review or are stale), and its own page carries the line **In the semantic model**, with its fact count; a table with no facts shows **Describe this table in the semantic model** instead. Both open the model on the Tables tab at that table, with its card expanded and briefly highlighted. From the model, the **Open table** button on a table card (and on a table's heading in the Metrics tab) and the table names in the Joins tab lead back to that table's page.

A schema without facts shows a short explanation and a **Describe your tables** button. The header holds the model's name and description (click to edit), a progress bar of accepted, pending and stale facts, **Re-sync** and **Export YAML**.

A fact is **stale** when the catalog changed underneath it, for example a column it names was removed. **Re-sync** compares the model with the catalog again; the model is also compared when you open it.

---

## Suggest a model

A schema with no facts shows **Suggest a model** next to **Describe your tables**; once the model has facts, a **Suggest** button sits in the header. It needs no AI and nothing to configure. Flowfile reads one summary query per table (distinct counts, null counts and, for number and time columns, smallest and largest values) and proposes:

- **Column roles** (`key`, `time`, `category`, `amount`).
- **Joins**, where a column matches another table's unique key by name and type, each with its evidence, for example "`customer_id` is unique in `customers` (3 of 3 rows) and repeats in `orders`". Joins are suggested only between tables of the same schema.
- **Metrics**: a row count, the sum and average of amount columns and a distinct count of keys, with the time column and allowed dimensions filled in.

Everything lands in the **Review** tab, which opens afterwards, and nothing is live until a person accepts it. Running **Suggest** again adds nothing that the model already holds. If the statistics cannot be read, roles come from names and types only, and the page says so.

### With AI

When AI is usable (a provider is configured, or the local model is installed), the same button also has the model write words: a one-sentence description of each table and column, and a name and meaning for each metric candidate. It makes one small call per table (wider tables are split into several), and the model is only asked for text; the roles, joins and metrics themselves still come from the rules. A table the model cannot describe keeps its rule-based suggestions and the page names it. AI suggestions are marked as AI in Review and are decided like any other fact.

Metric names from the model apply to candidates that are not stored yet; if you ran **Suggest** before AI was available, a later run adds descriptions only.

### The local model

The default writer is the small model that runs on your own computer: nothing leaves it. When no AI is set up and the local model can be installed, the panel shows a card, **Let AI on this computer write the descriptions**, that downloads it in one click with progress; the Ask tab offers the same. Online providers are optional. To use one for this feature, choose a model for the **Semantic model** task in **Settings → AI → Assistant**; the choice is kept in your browser or desktop app. See [AI in semantic models](../../../ai/index.md#ai-in-semantic-models).

!!! note "Test results"
    On the 2B local model, a set of twenty test questions on one sample data set was answered correctly in three runs after the checks under [How a question is read](#how-a-question-is-read) were added. That is one data set, not a guarantee; Review shows each fact's source, and every answer shows how it was read.

---

## Describe columns

The **Tables** tab lists each table of the schema with its columns. Click a table's description, or a column's **Description**, **Used as** or **Confidentiality**, to edit it. **Used as** offers:

| Option | Meaning |
|---|---|
| **Identifier** | Tells rows apart. Counted, never added up. |
| **Date** | When it happened. Used for trends over time. |
| **Group by** | A label to split a number by. |
| **Amount** | A number to add up or average. |

A status chip appears next to a value only while it still needs a person: proposed and waiting for review, or stale.

A table or column description that is accepted is also written back to the catalog, so it shows wherever the catalog shows descriptions.

### Confidentiality

Marking a column **Confidential** or **Restricted** keeps it from AI on every surface that reads catalog tables: the semantic draft and question box, the chart prompt bar, and the dashboard builder and its story. The model gets the column's name and type only, never a value, description or synonym. It applies as soon as the level is proposed, before anyone accepts it, and a table marked at that level covers all its columns. If the level cannot be read, values are withheld for that table.

This does not hide anything from people who can open the schema; they still see every column and value.

---

## Add a join

Open the **Joins** tab and click **Add join**. Fill in:

- **Name**, for example `orders_to_customers`.
- **From table (many rows)** and **To table (one row)**.
- **Matching columns**, one or more pairs: a column of the from table and a column of the to table.
- **Description** (optional).

"Many rows on the from side per one row on the to side" means each row of the to table is matched by any number of rows of the from table, and each from row has one match. `orders` is the from side and `customers` the to side because many orders belong to one customer. Flowfile follows a join only in this direction when it answers a question, so a total is never counted twice. 

Before a join is stored, Flowfile measures it: for each side, the rows that have a key and the distinct keys. A correct join is stored with that measurement as its evidence, for example "`customer_id` is unique in `customers` (40 of 40 rows) and repeats in `orders` (40 distinct in 300 rows)". Two cases are refused with a message that says what to do:

- **Reversed**: the from side has the unique key and the to side repeats. Swap the two sides. The dialog has **Swap sides** for this.
- **Both sides repeat**: the join would multiply rows. The to side needs columns that identify one row.

An empty table, or a worker that cannot be reached, stores the join marked "Direction not checked". The **Joins** tab flags a join that is already wrong and offers **Fix direction** on an accepted reversed one, which proposes the same join with the sides swapped. A wrong join that was already accepted keeps running until it is fixed.

The to table can be in another schema of the same catalog. That join is stored in the catalog-level model and listed with a `catalog` label.

---

## Define a metric

Open the **Metrics** tab and click **Add metric**.

| Field | Meaning |
|---|---|
| **Table** | The table the metric counts rows of. A metric belongs to one table. |
| **Name** | The name people ask for, for example `total_revenue`. Unique within the schema. |
| **What it means** | A sentence in plain words. |
| **Expression** | The aggregate as SQL, for example `SUM(amount)`. A ratio of two aggregates works inside one expression. |
| **Filter (optional)** | A condition that applies to every use of the metric, for example `status = 'completed'`. |
| **Time column (optional)** | The column used to group by day, week, month, quarter or year. |
| **Number format (optional)** | For example `#,##0.00`. |
| **Allowed dimensions** | The columns of the table the metric can be broken down by. |

Before a metric is stored, Flowfile runs its expression against the table on a single row. An expression that does not run is refused with the engine's message. The SQL dialect is the one the [SQL Editor](sql-editor.md#writing-queries) uses.

Editing a metric proposes a new definition; the old one stays in force until the new one is accepted.

---

## Review and who can accept

The **Review** tab lists the facts that wait for a decision. Each shows its source (a person, or rules, or AI), the confidence and rationale when the source gave them, and **Accept** and **Reject** buttons. **Accept all** decides everything in the queue. A metric or join edit that consists of several facts is decided as one group.

Who can accept depends on where Flowfile runs. The chip in the header says which case applies:

- **Solo**, on your own desktop: your edits are accepted at once. Facts proposed by AI still queue, because only a person can accept.
- **Team review**, on a shared server: every change waits for someone who **manages** the schema, that is its owner or a user with a manage grant. Anyone who may write to the schema can propose. Joins that cross schemas are decided by whoever manages the catalog.

A fact proposed by AI can never be accepted by AI. Reading a model needs access to the schema; a schema that a person can see shows the facts about all its tables.

!!! note "Team review"
    The team rules follow the catalog's [sharing and grants](../../deployment/sharing.md) and have not been exercised on a live shared server as part of this feature.

The **History** tab shows the events of one table, column, metric or join: who proposed, accepted, rejected, retracted or reopened it, and when.

---

## Ask for a number

The **Ask** tab computes a metric from its definition. To do it by hand, choose a **Metric**, then optionally **Group by** one or more dimensions (columns of the metric's table, or of any table reached by following joins from the many side to the one side, including tables in other schemas), a **Time grain** (when the metric has a time column) and **Filters** (**Add filter**), and press **Run**. The result shows the rows and a line saying which definition answered: "Answered with *revenue* · approved by … · version 3". The generated SQL can be shown below the rows.

**Ask** (the question box at the top of the tab, placeholder "Ask a question about your data", for example "What is the revenue?" or "revenue by country") fills the same controls from a sentence and runs them. The model chooses among your **approved** metrics and their listed dimensions; it never writes SQL, and a name it gets slightly wrong is matched to the closest listed one. The answer says which metric answered and who approved it, and which model chose it. When no approved metric fits, the answer is "No approved metric answers this. Propose one in the Metrics tab." and the Metrics tab opens. When the model's reply cannot be used, the box says so and the controls below still work. Without AI the box is replaced by an offer to install the local model; the manual controls need no AI.

A metric that is not accepted yet can be run as a preview by anyone who may write to the schema; the result is marked "not approved yet".

The SQL is generated from the model. Dimensions and filters are checked against it and written as escaped values; free SQL comes only from a metric's expression. The query runs through the same path as the [SQL Editor](sql-editor.md), on the worker.

### How a question is read

The model suggests a query and Flowfile checks it against the model before anything runs:

- A grouping, filter, date range or time grain is used only when the question names it.
- A value the question names (a status, a country) becomes a filter even if the model missed it. Flowfile matches it against the distinct values of your category columns, which it reads itself and does not send to the model.
- Every answer starts with a **Read as: …** line saying how the question was understood, followed by what was added, corrected or left out.
- When part of the question cannot be applied, for example a breakdown with no matching dimension, that part is left out, the notes say so, and the response is marked partial. Nothing is substituted silently.

**Keep as a flow** names a flow (**Flow name**, then **Save**) and registers the query in the catalog, so it can be scheduled or opened on the canvas.

---

## Export

**Export YAML** in the header downloads the model as Apache Ossie YAML: descriptions, joins and metrics with their status and history, for tools that read that format.

---

## Limits

- A query asks for **one metric**.
- A metric belongs to **one table**.
- A join is followed only from the many side to the one side; Flowfile measures the direction when the join is stored but does not check types of the key columns.
- Confidentiality keeps column values from AI, not from people with access to the schema.
- A saved flow cannot read another schema's table, so a query that reaches across schemas can be run but not kept as a flow.
- Suggested joins stay within one schema.
- Tables whose name contains `::` are left out of the model.
- Actor names in the history are numeric user ids, stable on one installation only.

---

## See also

- [SQL Editor](sql-editor.md) — the query path metrics run on.
- [AI Assistant](../../../ai/index.md#ai-in-semantic-models) — what AI may and may not do in a model.
- [Projects & Git](../../projects.md) — tables are saved one file each.
