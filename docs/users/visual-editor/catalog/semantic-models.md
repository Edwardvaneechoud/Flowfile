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

Joins that cross schemas are held one level up, by the **catalog** that contains the schemas, and are managed on the catalog's own page.

Every fact is a proposal first. It becomes part of the model when it is **accepted**, and it keeps its author, its status and a history of who changed it and when. Only accepted facts are used to answer questions. A metric that is edited keeps its accepted definition until the new one is accepted, and carries a version number.

---

## Open a model

In the catalog tree, expand a schema and click **Semantic model**, the first entry under it. A badge shows how many facts wait for review. The address of the page keeps the selection (`semanticNamespaceId` in the URL), so a reload returns to the same model.

A catalog has no entry of its own. Click the catalog in the tree: its page lists, under **Semantic models**, each schema with its counts ("12 accepted · 3 waiting for review", or "Not modelled yet") and a button that opens that schema's model (**Start** for one without facts). Below that, **Joins across schemas** lists the joins between tables of two different schemas, with **Add join**, **Suggest joins**, and **Accept** and **Reject** on one that waits for review. An old address that named a catalog's model opens the catalog's page.

The page of a table that is part of the model carries the line **In the semantic model**, with its fact count; a table with no facts shows **Describe this table in the semantic model** instead. Either opens the model on the Tables tab at that table, with its card expanded and briefly highlighted. From the model, the **Open table** button on a table card (and on a table's heading in the Metrics tab) and the table names in the Joins tab lead back to that table's page.

A schema without facts shows a short explanation and a **Describe your tables** button. The header holds the model's name and description (click to edit), a progress bar of accepted, pending and stale facts, **Re-sync** and **Export YAML**.

A fact is **stale** when the catalog changed underneath it, for example a column it names was removed. **Re-sync** compares the model with the catalog again; the model is also compared when you open it.

---

## Suggest a model

A schema with no facts shows **Suggest a model** next to **Describe your tables**. Once the model has facts, the header has one **Suggest** button: it opens a box with one field, "What should be suggested? Leave empty for the whole model.", and a **Suggest** button (Enter, or Cmd/Ctrl+Enter, sends it). Left empty, it suggests the whole model as described here; with words in it, it is a [specific request](#ask-for-something-specific). The whole-model suggestion needs no AI and nothing to configure. Flowfile reads one summary query per table (distinct counts, null counts and, for number and time columns, smallest and largest values) and proposes, from types and those statistics only, never from what a column's name seems to mean:

- **Column roles** (`key`, `time`, `category`, `amount`): a unique integer or text column is a key, a date is a date, a number that repeats is an amount, a boolean or a text column with few distinct values is a group-by column, and a column that points to another table's unique column is a key.
- **Joins**, where a column has the same name and type as a column that is unique in another table and repeats in its own, each with its evidence, for example "`customer_id` is unique in `customers` (3 of 3 rows) and repeats in `orders`". A schema's **Suggest** proposes joins between its own tables; **Suggest joins** on the catalog's page looks across all its schemas and says how many of its proposals wait there and how many in a schema's Review.
- **Metrics**: a row count, the sum and average of amount columns and a distinct count of keys, with the time column and allowed dimensions filled in.

Everything lands in the **Review** tab, which opens afterwards. Running **Suggest** again adds nothing that the model already holds. If the statistics cannot be read, only what a column's type tells (dates, booleans) is proposed, and the page says so.

### While a suggestion runs

A suggestion on the local model can take minutes. While one runs, a status line under the header says so, for example "Suggesting a model… started 2 minutes ago · 3 of 7 tables", and every Suggest button of the model is disabled with a tooltip saying a suggestion is already running. The line is the same in every window and after a page reload. When it ends, the model, the Review queue and the tree's counts reload and the line becomes the result, for example "Proposed 12 column roles, 2 joins and 9 metrics.", with a link to **Review** and a button to dismiss it; a dismissed result is not shown again in that browser tab. A suggestion started from a tab's button or the request box shows its result there instead. Starting a second suggestion for the same model while one runs is refused.

### With AI

When AI is usable (a provider is configured, or the local model is installed), the same button also has the model write words: a one-sentence description of each table and column, and a name and meaning for each metric candidate. It makes one small call per table (wider tables are split into several), and the model is only asked for text; the roles, joins and metrics themselves still come from the rules. A table the model cannot describe keeps its rule-based suggestions and the page names it. AI suggestions are marked as AI in Review and are decided like any other fact.

Metric names from the model apply to candidates that are not stored yet; if you ran **Suggest** before AI was available, a later run adds descriptions only.

### Suggest more of one kind

The **Joins**, **Metrics** and **Tables** tabs each have one button beside their add button: **Suggest joins**, **Suggest metrics** and **Suggest descriptions and roles**. One click runs the rules for that kind only (no AI needed); when AI is usable, the model then looks for what the rules cannot find, such as a join whose column names differ (`orders.customer` to `customers.id`) or a metric beyond a count, sum or average. The button's tooltip names the model that will run. The tab says what was proposed in one line, for example "Proposed 2 joins. They wait in Review.", or that nothing new was found. New joins and metrics show on the tab as pending straight away. A request for one particular thing goes through the header's request box.

### Ask for something specific

Write what you want in the header's request box, in your own words: "a metric for average order value", "describe the orders table", "how are orders and products related?", "other words people use for revenue". This needs AI; there is no rules-only version of it. The box shows which model will run it, for example "Uses qwen3.5-2b on this computer · Change"; see [Which model does what](#which-model-does-what).

The model reads the request and Flowfile checks what can be checked against the model and the data. The model sees the tables' names, column types, roles and descriptions, the joins and the metric names, never a data row; a Confidential or Restricted column is sent by name and type only and is never described. Every suggestion is checked before it is stored:

- Table, column and metric names must exist, and a suggestion's kind must be one of metric, description, join, role or synonyms.
- A metric is compiled and tried against the data exactly like one you write: a formula that does not run is not stored, and neither is one whose formula and filter the model already has.
- A join is measured like one you add: the right way round it is proposed with the measurement, the wrong way round it is turned around and proposed with a note saying so, and a join that would multiply rows, or cannot be measured, is refused. Its two columns must hold the same type of value.
- A role must be one of the four and fit the column's type (a date for Date, a number for Amount); a description must not be empty; synonyms are a list of other words for a column or a metric.

What fails goes back to the model once with the reasons; what still fails is listed under "not stored" with its reason. Everything stored waits in **Review**, also on a desktop where your own edits count at once: AI can propose, never accept.

### The local model

The default writer is the small model that runs on your own computer: nothing leaves it. When no AI is set up and the local model can be installed, the panel shows a card, **Let AI on this computer write the descriptions**, that downloads it in one click with progress; the Ask tab offers the same. Online providers are optional. See [AI in semantic models](../../../ai/index.md#ai-in-semantic-models).

### Which model does what

Two choices in **Settings → AI → Assistant** decide which model runs: **Semantic model: questions** (the Ask tab's question box) and **Semantic model: suggestions** (Suggest a model, the tab buttons and the request box). Each can be **Same as main** or a provider and model of its own, and they are independent. The question box and the request box show the model they will use before you send, with a **Change** link that opens the same two selects in place and a link to all AI settings. With no choice of its own, a task uses the local model when it is installed, else your main provider and model. An earlier single **Semantic model** choice serves both until you change one. The choices are kept in your browser or desktop app.

!!! note "Test results"
    On the 2B local model, a set of twenty test questions on one sample data set was answered fully right in 10 and 8 of 20 (two runs), with 4 and 3 more answers that applied part of the question and said so. The questions are now checked only against the model and the data, not against wording rules, so the model's own reading decides more; earlier versions with wording rules scored higher on this one set. That is one data set, not a guarantee; Review shows each fact's source, and every answer shows how it was read.

---

## Describe columns

The **Tables** tab lists each table of the schema with its columns. Click a table's description, or a column's **Description**, **Used as** or **Confidentiality**, to edit it. **Used as** offers:

| Option | Meaning |
|---|---|
| **Identifier** | Tells rows apart. Counted, never added up. |
| **Date** | When it happened. Used for trends over time. |
| **Group by** | A label to split a number by. |
| **Amount** | A number to add up or average. |

Under a column's description, **Also called…** holds other words people use for it, separated by commas ("turnover, sales" for `revenue`). The question box shows them to the model beside the column. They start empty; nothing fills them but you, or an AI suggestion you accept.

A status chip appears next to a value only while it still needs a person: proposed and waiting for review, or stale.

A table's own page shows the same columns under **Schema**, and you edit them there in the same way. **Open in semantic model** jumps to the table's card in the **Tables** tab.

A table or column description that is accepted is also written back to the catalog, so it shows wherever the catalog shows descriptions.

### Confidentiality

Marking a column **Confidential** or **Restricted** keeps it from AI on every surface that reads catalog tables: the semantic draft and question box, the chart prompt bar, and the dashboard builder and its story. The model gets the column's name and type only, never a value, description or synonym. It applies as soon as the level is proposed, before anyone accepts it, and a table marked at that level covers all its columns. If the level cannot be read, values are withheld for that table.

This does not hide anything from people who can open the schema; they still see every column and value.

---

## Data rules

A data rule says what every row of a column must hold. In the **Tables** tab (and under **Schema** on a table's own page), click **Add rule** in a column's **Rules** cell (the pencil, once the column has rules), pick a kind and click **Add rule**. Remove a rule with the **✕** beside it in the same box.

| Kind | Passes when |
|---|---|
| **Not null** | The row has a value. |
| **Unique** | No other row has the same value. |
| **One of** | The value is one of the values listed, separated by commas. Not offered for date and time columns. |
| **Pattern** | The value matches a regular expression, such as `^[A-Z]{2}$`. |
| **Between** | A number is within a range. Either end may be left open. Offered for number columns. |
| **Length** | The number of characters is within a range. Either end may be left open. |
| **Formula** | One [formula](../../formulas/index.md) line is true for the row, such as `[amount] >= 0` or `[end_date] >= [start_date]`. |

A formula may name any column of the table, and its box completes column names (type `[`) and functions as in the Formula node.

A row without a value passes every rule except **Not null**. A formula that answers nothing for a row (it reads an empty value) fails that row.

A column's rules are one fact, reviewed like any other: a change waits in **Review** where review is on, and only accepted rules run. A rule that cannot work is refused when you add it, with the reason: a pattern that does not compile, a range on a text column, a formula that names a missing column or does not answer true or false. A rule is refused only when that check proves it wrong. When the check cannot be made, for instance because the catalog holds no usable type for the column, the rule is saved with a notice and judged when it runs.

### Run the rules

A table with rules shows **Run rules** in its header. A run checks the table's accepted rules against all its rows, on the compute backend, and marks each rule: green when every row passes, red with the number of failing rows when some do not (hover the rule for the count), amber when the rule could not be evaluated. The header sums it up ("3 of 4 rules pass"). Anyone who can read the model can run its rules.

Results are kept, so the marks are still there when you come back, and they belong to the rules as accepted at that moment: change a rule and it shows as not run yet. The header and each rule's hover say when the rules last ran; a result is not cleared when the table is written, so run the rules again after the data changes. Rules run only when you press **Run rules**; a write to the table does not run them.

A virtual table is skipped: it holds no stored rows, and checking it would run what produces it. A table on this server's disk is skipped too when the compute backend does not read this server's files, as with a [worker on another host](../../deployment/remote-worker.md): its catalog tables must be on object storage.

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

The to table can be in another schema of the same catalog. That join belongs to the catalog: it is listed on the catalog's page under **Joins across schemas**, and in the schema's Joins tab with a `catalog` label.

---

## Define a metric

Open the **Metrics** tab and click **Add metric**.

| Field | Meaning |
|---|---|
| **Table** | The table the metric counts rows of. A metric belongs to one table. |
| **Name** | The name people ask for, for example `total_revenue`. Unique within the schema. |
| **What it means** | A sentence in plain words. |
| **Also called (optional)** | Other words people use for the metric, separated by commas. The question box shows them to the model. |
| **Expression** | The aggregate as SQL, for example `SUM(amount)`. A ratio of two aggregates works inside one expression. |
| **Filter (optional)** | A condition that applies to every use of the metric, for example `status = 'completed'`. |
| **Time column (optional)** | The column used to group by day, week, month, quarter or year. |
| **Number format (optional)** | For example `#,##0.00`. |
| **Allowed dimensions** | The columns the metric can be broken down by: the table's own, or a joined table's as `table.column`. Leave it empty to allow any column the metric can reach; a question in the Ask tab then chooses among the ones marked **Group by**. |

The expression and the filter may also use columns of joined tables, written `table.column` (for example `SUM(quantity * products.price)`, or `schema.table.column` when two tables share a name). Only tables reached by following joins from the many side to the one side can be used, so each row of the metric's table meets at most one row of a joined table and the total is never multiplied. The editor completes the table's columns, the joined tables' columns after `table.` and SQL functions, and lists under the expression which tables can be used.

Before a metric is stored, Flowfile runs its expression against the table on a single row. An expression that does not run is refused with the engine's message. The SQL dialect is the one the [SQL Editor](sql-editor.md#writing-queries) uses.

Editing a metric proposes a new definition; the old one stays in force until the new one is accepted. Clearing an optional part (the filter, the description) in an edit that is accepted at once removes that part.

---

## Remove a metric or a join

- **A metric**: **Remove metric** at the bottom left of the metric's editor, or the bin icon on its card in the **Metrics** tab. After a confirmation, questions and queries can no longer use it.
- **A join**: **Remove** on its row in the **Joins** tab (or, for a join across schemas, on the catalog's page). The confirmation first lists the metrics that use the join and will stop working, for example a metric whose formula reads `products.price` or that is broken down by `customers.country`, or says that no metric uses it. A query that still asks for such a metric is refused with a sentence naming the table it can no longer reach.

Nothing is deleted. Removing withdraws the metric's or join's facts: an accepted fact, or your own proposal, is **retracted**; a proposal someone else made (a person, the rules or AI) is **rejected**, so the rules and AI do not propose it again. The **History** tab keeps every step, and the same name can be used for a new metric or join afterwards.

To remove a column's description, role or confidentiality, clear it: empty the description, or clear **Used as** or **Confidentiality** back to **Not set**.

On a shared server the same review rules decide who may remove: whoever manages the schema (or, for a join across schemas, the catalog), and anyone for a proposal of their own. The button is not shown to others.

---

## Review and who can accept

The **Review** tab lists the facts that wait for a decision. Each shows its source (a person, or rules, or AI), the confidence and rationale when the source gave them, and **Accept** and **Reject** buttons. **Accept all** decides everything in the queue. A metric or join edit that consists of several facts is decided as one group.

Who can accept depends on where Flowfile runs. The chip in the header says which case applies:

- **Solo**, on your own desktop: your edits are accepted at once. Facts proposed by AI still queue, because only a person can accept.
- **Team review**, on a shared server: every change waits for someone who **manages** the schema, that is its owner or a user with a manage grant. Anyone who may write to the schema can propose. Joins that cross schemas are decided by whoever manages the catalog.

A fact proposed by AI can never be accepted by AI.

Reading a model follows the catalog. A schema you own or were granted (`use` or `manage`, on it or on its catalog) shows the facts about all its tables. A public schema that you neither own nor were granted shows its own name, description and AI instructions, the tables in it that you own or were granted, and the joins between those; its other tables do not appear, in the model, in Review, in History, in the export or in answers to questions. Facts at the catalog level, such as joins across schemas, show only when you can read every schema of the catalog. In a public schema facts are accepted by its owner, a user with a manage grant or an admin; the built-in public schemas (`General`, `Unnamed Flows`, `Python Editor`) are accepted by an admin.

!!! note "Team review"
    The team rules follow the catalog's [sharing and grants](../../deployment/sharing.md). They are tested in process, not on a live shared server.

The **History** tab shows the events of one table, column, metric or join: who proposed, accepted, rejected, retracted or reopened it, and when.

---

## Ask for a number

The **Ask** tab computes a metric from its definition. To do it by hand, choose a **Metric**, then optionally **Group by** one or more dimensions (columns of the metric's table, or of any table reached by following joins from the many side to the one side, including tables in other schemas), a **Time grain** (when the metric has a time column) and **Filters** (**Add filter**), and press **Run**. The result shows the rows and a line saying which definition answered: "Answered with *revenue* · approved by … · version 3". The generated SQL can be shown below the rows.

**Ask** (the question box at the top of the tab, placeholder "Ask a question about your data", for example "What is the revenue?" or "revenue by country") fills the same controls from a sentence and runs them. The model chooses among your **approved** metrics and their listed dimensions; it never writes SQL, and a name it gets slightly wrong is matched to the closest listed one. The answer says which metric answered and who approved it, and which model chose it. When no approved metric fits, the answer is "No approved metric answers this. Propose one in the Metrics tab." and the Metrics tab opens. When the model's reply cannot be used, the box says so and the controls below still work. Without AI the box is replaced by an offer to install the local model; the manual controls need no AI.

A metric that is not accepted yet can be run as a preview by anyone who may write to the schema; the result is marked "not approved yet".

The SQL is generated from the model. Dimensions and filters are checked against it and written as escaped values; free SQL comes only from a metric's expression. The query runs through the same path as the [SQL Editor](sql-editor.md), on the worker.

### How a question is read

The model reads the question and writes a query; Flowfile checks it against the model and the data before anything runs, and never judges the question's wording:

- The model sees each approved metric's name, description, **Also called** words and time column, the columns it may be grouped by and filtered on, and the model's **AI instructions** when they are set (the line under the model's description in the panel header).
- A grouping must be one the metric may be grouped by, a filter must name one of its columns with a valid operator, and a grain needs a time column. A filter value on a group-by column must be one of that column's values; a value that differs only in case is corrected to the stored spelling. Flowfile reads those values itself and does not send them to the model.
- Every answer starts with a **Read as: …** line saying which query ran.
- When part of the model's query cannot be applied, for example a breakdown by a column the metric may not be grouped by, or a filter value the column does not hold, that part is left out and the answer says what is missing after the number: "The average num bikes available is 14.37. It is not split by station: station_id is not among the columns average_num_bikes_available can be grouped by." The answer is marked partial. Nothing is substituted silently.
- The question box shows the model it will use; Cmd/Ctrl+Enter sends it, as does Enter.

**Keep as a flow** names a flow (**Flow name**, then **Save**) and registers the query in the catalog, so it can be scheduled or opened on the canvas.

---

## Export

**Export YAML** in the header downloads the model as Apache Ossie YAML: descriptions, joins and metrics with their status and history, for tools that read that format.

---

## Limits

- A query asks for **one metric**.
- A metric belongs to **one table**. Its formula can read columns of tables joined many to one, never of a table with many rows per row of its own (a metric on customers cannot use a column of orders).
- A join is followed only from the many side to the one side; Flowfile measures the direction when the join is stored but does not check types of the key columns.
- Confidentiality keeps column values from AI, not from people with access to the schema.
- Data rules are per column and run on demand. There are no rules on a whole table, no failing-row samples, and no date range in **Between** or dates in **One of** (use a **Formula** rule). **Unique** counts every copy of a repeated value as failing. Where review is on, two people changing the same column's rules replace each other's list, and removing a column's last rule takes effect at once.
- A saved flow cannot read another schema's table, so a query that reaches across schemas can be run but not kept as a flow.
- Rule-suggested joins from a schema's model stay within that schema; **Suggest joins** on the catalog's page also proposes joins across its schemas. An AI suggestion may join to a table of another schema only when a join already reaches it.
- The checks on a specific suggestion catch a wrong table, column, type or direction, not a wrong meaning: a well-formed metric with a misleading name passes and is caught only in Review.
- The question box applies what the model wrote when it fits the model; it does not check the query against the question's words, so a well-formed query that misreads the question runs. The **Read as** line shows what ran.
- The running-suggestion status lives in the core process: a core restart forgets a running suggestion's status (what it had already proposed stays in Review), and two core processes on one catalog do not see each other's runs.
- Tables whose name contains `::` are left out of the model.
- Actor names in the history are numeric user ids, stable on one installation only.

---

## See also

- [SQL Editor](sql-editor.md) — the query path metrics run on.
- [AI Assistant](../../../ai/index.md#ai-in-semantic-models) — what AI may and may not do in a model.
- [Projects & Git](../../projects.md) — tables are saved one file each.
