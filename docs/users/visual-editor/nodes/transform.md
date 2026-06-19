# Transform Nodes

Transform nodes modify and shape your data. These nodes handle everything from basic operations like filtering and sorting to more complex transformations like custom formulas and text manipulation.

!!! info "Some transform nodes are not in Flowfile Lite"
    The browser-only [Flowfile Lite](../../deployment/lite.md) build includes **Select**, **Filter**, **Sort**, **Take Sample**, **Drop Duplicates**, and **Polars Code**. **Add Record ID**, **Formula**, **Text to Rows**, and **Python Script** are not available — use the **Polars Code** node for Formula-style column logic.

## Node Details


### ![Add Record ID](../../../assets/images/nodes/record_id.png){ width="50" height="50" } Add Record ID  

The **Add Record ID** transformation generates a unique identifier for each record in your dataset. You can create a simple sequential ID or generate grouped IDs based on one or more columns.

#### **Usage:**

1. Add the **Add Record ID** node to your flow.  
2. Configure the settings:
   - Define the output column name.
   - Set an optional offset for ID numbering.
   - (Optional) Enable grouping and specify grouping columns.  
3. Apply the transformation.  

---

#### Configuration Options  

| Parameter              | Description                                                                                                      |
|------------------------|------------------------------------------------------------------------------------------------------------------|
| **Output Column Name** | Name of the new column where the record ID will be stored. Default is `"record_id"`.                             |
| **Offset**             | Starting value for the record ID. Default is `1`.                                                                |
| **Group By**           | If `true`, record IDs are assigned within groups instead of sequentially across all records. Default is `false`. |
| **Group By Columns**   | List of columns to group by when assigning record IDs. Only applies when **Group By** is enabled.                |

---

#### Behavior  

- **Sequential Record ID** (Default)  
  - A new column is added with a simple incremental ID starting from the defined offset.  
- **Grouped Record ID**  
  - When grouping is enabled, the record ID resets within each group based on the specified columns.  

This transformation helps in creating unique keys, tracking row order, or structuring data for downstream processing.

---

### ![Formula](../../../assets/images/nodes/formula.png){ width="50" height="50" } Formula  

The **Formula** node creates a new column — or replaces an existing one — by evaluating a formula for every row. Formulas are written in the [Flowfile formula language](../../formulas/index.md): reference columns as `[column]`, call any of the [95 built-in functions](../../formulas/functions.md), and use `if ... then ... else ... endif` for conditional logic.

!!! tip "Try formulas in your browser"
    The [interactive formula playground](https://edwardvaneechoud.github.io/polars_expr_transformer/) lets you experiment with the full language against sample data — nothing to install.

---

#### **Key Features**

- Create new columns or replace existing ones  
- Math, string, date/time, and type-conversion [functions](../../formulas/functions.md)  
- Conditional logic with `if ... then ... elseif ... else ... endif`  
- Autocomplete for column names and functions, with inline function documentation, in the formula editor  
- Compiles to a native Polars expression — no row-by-row Python overhead

---

#### **Usage**

1. Drag the **Formula** node onto your canvas.  
2. Connect input data.  
3. Set the output column name.  
4. Write your formula, e.g. `round([price] * (1 - [discount]), 2)`.  
5. Preview the results and optionally set a data type.  

---

#### **Configuration Options**

| Parameter       | Description                                                                                                      |
|-----------------|------------------------------------------------------------------------------------------------------------------|
| **Column Name** | The name of the new or modified column.                                                                          |
| **Formula**     | The [formula](../../formulas/index.md) used to compute values for the column.                                    |
| **Data Type**   | The data type of the resulting column. Defaults to `Auto`, which infers the type from the formula; set it explicitly to force a cast. |

---

#### **Behavior**

- If the column name is **new**, the column is added to the dataset.  
- If the column name **already exists**, its values are replaced with the formula result.  

This transformation is useful for feature engineering, data cleaning, and enriching datasets with computed values.

---

### ![Select Data](../../../assets/images/nodes/select.png){ width="50" height="50" } Select Data  

The **Select Data** node allows you to choose which columns to keep, rename, and reorder. This transformation is useful for refining datasets, dropping unnecessary fields, and ensuring a structured column layout.

---

#### **Key Features**  
- **Select** specific columns to retain in the dataset  
- **Reorder** columns by dragging them into the desired order or by ordering them alphabetically
- **Rename** columns to provide meaningful names  
- **Automatically remove missing fields**  

---

#### **Configuration Options**  

| Parameter               | Description                                                                              |
|-------------------------|------------------------------------------------------------------------------------------|
| **Column Selection**    | Choose which columns to keep in the dataset.                                             |
| **Reordering**          | Drag and drop to change the column order.                                                |
| **Rename Column**       | Assign a new name to any selected column.                                                |
| **Keep Missing Fields** | If enabled, columns that are missing from input data are retained in the selection list. |

---

#### **Behavior**  

- If a selected column is missing from the input, it is marked as **unavailable**.  
- Columns can be **renamed** without affecting their original data.  
- Changing the order affects how the columns appear in downstream processing.  

This transformation ensures that datasets are structured efficiently before further analysis or processing.


---

### ![Filter Data](../../../assets/images/nodes/filter.png){ width="50" height="50" } Filter Data

The **Filter Data** node keeps only rows that match a specified condition.

---

#### **Key Features**  

- **Basic mode**: pick a column, an operator, and a value — no syntax needed  
- **Advanced mode**: write a [formula](../../formulas/index.md) that evaluates to `true` or `false`; only `true` rows remain  
- **Split mode**: route matching rows to one output and non-matching rows to a second output, instead of dropping them  
- Support for **string, numeric, and date filtering**  

---

#### **Usage**

1. Drag the **Filter Data** node onto your canvas.  
2. Connect input data.  
3. Choose **basic** mode and fill in the condition, or **advanced** mode and enter a filter formula (e.g., `[City] = 'Amsterdam'`).  
4. (Optional) Enable **split mode** to keep the non-matching rows on a second output.  

---

#### **Example Filters (advanced mode)**  

| Expression                              | Description                                                 |
|-----------------------------------------|-------------------------------------------------------------|
| `[City] = 'Amsterdam'`                  | Keep rows where `City` is "Amsterdam".                      |
| `[Age] > 30`                            | Keep rows where `Age` is greater than 30.                   |
| `[Country] = 'USA' and [Sales] > 100`   | Keep rows where `Country` is "USA" and `Sales` is over 100. |
| `is_not_empty([email])`                 | Keep rows that have an email address.                       |

Advanced filters use the [Flowfile formula language](../../formulas/index.md) — any formula that returns `true`/`false` works, including [functions](../../formulas/functions.md) like `contains()`, `between()`, or `is_empty()`.

---

### ![Sort Data](../../../assets/images/nodes/sort.png){ width="50" height="50" } Sort Data  

The **Sort Data** node orders your data based on one or more columns.

---

#### **Key Features**  
- Sort by multiple columns  
- Choose **ascending** or **descending** order

---

#### **Usage**  
1. Select columns to sort by.  
2. Choose sort direction (**ascending** or **descending**) for each column.  

---

#### **Configuration Options**  

| Parameter        | Description                                   |
|------------------|-----------------------------------------------|
| **Sort Columns** | Columns used to sort the dataset.             |
| **Sort Order**   | Set ascending (`asc`) or descending (`desc`). |

This node ensures structured and ordered data for better analysis.

---

### ![Take Sample](../../../assets/images/nodes/sample.png){ width="50" height="50" } Take Sample

The Take Sample node lets you work with a subset of your data.

---

### ![Drop Duplicates](../../../assets/images/nodes/unique.png){ width="50" height="50" } Drop Duplicates  

The **Drop Duplicates** node removes duplicate rows based on selected columns. Only the first occurrence is kept by default.

---

### **Key Features**  

- Remove duplicate rows  
- Select columns to check for duplicates  

---

#### **Usage**  

1. Select columns to check for duplicates.  
2. Choose whether to keep the **first** or **last** occurrence.  

---

#### **Configuration Options**  

| Parameter   | Description                           |
|-------------|---------------------------------------|
| **Columns** | Columns used to check for duplicates. |

This node ensures a clean dataset by eliminating redundant rows.

---

### ![Text to Rows](../../../assets/images/nodes/text_to_rows.png){ width="50" height="50" } Text to Rows  

The **Text to Rows** node splits text from a selected column into multiple rows based on a delimiter.

---

#### **Key Features**  
- Split a column into multiple rows  
- Use a **fixed delimiter** (e.g., `,`, `;`, `|`)  
- Split using values from another column  

---

#### **Usage**  
1. Select the **column to split**.  
2. Choose a **delimiter** or use another column for splitting.  
3. (Optional) Set an **output column name**. 

---

#### **Configuration Options**  

| Parameter                | Description                                                               |
|--------------------------|---------------------------------------------------------------------------|
| **Column to Split**      | The column containing text to be split.                                   |
| **Output Column Name**   | Name of the new column after splitting (defaults to the original column). |
| **Split by Fixed Value** | If `true`, use a fixed delimiter (default: `,`).                          |
| **Delimiter**            | The character used to split text (e.g., `,`, `                            |`, `;`).                 |
| **Split by Column**      | Instead of a fixed delimiter, use values from another column.             |

This transformation helps normalize datasets by converting **text lists into structured rows**.


---

### Window Functions

The **Window Functions** node adds rolling, cumulative, rank, or tile columns calculated over ordered — and optionally partitioned — rows. Each configured function produces one new column using Polars `over(...)` semantics, without collapsing rows the way a Group By does.

---

#### **Key Features**

- **Partition** calculations so they restart per group (like `.over(...)`)
- **Order** rows within each partition
- **Rolling**, **cumulative**, **ranking**, and **tile** functions
- Add multiple functions in one node — each writes its own output column

---

#### **Configuration Options**

| Parameter | Description |
|-----------|-------------|
| **Partition by** | *(Optional)* Columns that reset each calculation per group. Leave empty to compute over the whole table. |
| **Order by** | Column(s) and direction (ascending/descending) defining row order within each partition. **Required** for rolling and tile functions. |
| **Window functions** | One or more operations. Each takes a **function**, a **source column**, an **output column name**, and any function-specific parameters. |

---

#### **Available Functions**

| Function | Group | Parameters | Output |
|----------|-------|------------|--------|
| **Rolling sum / mean / min / max / std** | Rolling | **Window size** (rows) and how to handle **incomplete windows** | Aggregate over a sliding window of rows |
| **Cumulative sum / count / min / max** | Cumulative | — | Running total / count / min / max up to each row |
| **Rank** | Ranking | **Tie-breaking method**: `ordinal`, `dense`, `min`, `max`, or `average` | Rank of each row |
| **Tile** | Ranking | **Number of groups** | Splits the ordered rows into N equal-sized groups |

For rolling functions, **incomplete windows** (the first rows, before the window is full) can be left empty (`null`, the default), computed from the **partial** window, or **filled with 0**.

---

#### **Behavior**

- Rolling and tile functions require at least one **Order by** column; cumulative functions don't require ordering but usually want it.
- Each window function must have a **unique output column name**.
- Existing columns are preserved — every function adds a new column.

This node is useful for time-series features such as moving averages, running totals, ranking within groups, and bucketing rows into quantiles.

---
### ![Polars Code](../../../assets/images/nodes/polars_code.png){ width="50" height="50" } Polars Code  

The **Polars Code** node allows you to write custom **Polars DataFrame** transformations directly in your workflow.

---

#### **Key Features**  
- Write custom **Polars expressions**  
- Apply **advanced transformations** not covered by standard nodes  
- Filter, aggregate, or modify data using **Polars API**  

---

#### **Usage**  
1. Write a **single-line or multi-line** Polars expression.  
2. Use **`input_df`** as the DataFrame reference.  
3. Assign results to **`output_df`** for multi-line operations.  


#### **Example Code**  

##### **Single-line transformation**

```python
input_df.filter(pl.col('Age') > 30)
```

##### **Multi-line transformation**

```python
result = input_df.select(['Name', 'City'])
filtered = result.filter(pl.col('City') == 'Amsterdam')
output_df = filtered.with_columns(pl.col('Name').alias('Customer_Name')) # this will be the output of the node
```

---

### SQL Query

The **SQL Query** node runs a SQL `SELECT` query across one or more connected inputs using the **Polars SQL** dialect. Use it to filter, aggregate, join, and reshape data with familiar SQL.

---

#### **Key Features**

- Query connected inputs as tables named `input_1`, `input_2`, … (in connection order)
- Join, filter, group, and compute across **up to 10 inputs**
- Chain SQL Query nodes together like any other transform
- Read-only by design — only `SELECT` / `WITH` queries are allowed

---

#### **Usage**

1. Connect one or more inputs to the **SQL Query** node.
2. Write a query that references each input as `input_1`, `input_2`, etc.
3. Run the flow to materialize the query result.

---

#### **Configuration Options**

| Parameter | Description |
|-----------|-------------|
| **SQL Query** | The `SELECT` statement to execute. Connected inputs are available as `input_1`, `input_2`, … |

---

#### **Example Queries**

##### **Filter and select**

```sql
SELECT name, city FROM input_1 WHERE city = 'Amsterdam'
```

##### **Aggregate**

```sql
SELECT city, COUNT(*) AS cnt FROM input_1 GROUP BY city
```

##### **Join two inputs**

```sql
SELECT c.name, SUM(o.amount) AS total
FROM input_1 c
JOIN input_2 o ON c.id = o.customer_id
GROUP BY c.name
```

---

#### **Behavior**

- Queries must start with `SELECT` or `WITH` (for common table expressions). Statements that modify data or schema — such as `INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE`, `ALTER`, or `TRUNCATE` — are rejected.
- Invalid or unsafe SQL surfaces as a node error before the flow runs.

!!! tip "Querying catalog tables"
    To run SQL across registered **catalog** tables instead of connected nodes, use the [Catalog Reader's SQL mode](../catalog/sql-editor.md).

---

### Python Script

The **Python Script** node executes custom Python code in an isolated Docker [kernel](../kernels.md) container. It supports multiple named inputs and outputs, persistent namespaces, and the full `flowfile` API.

#### **Key Features:**
- Jupyter-style notebook editor with multiple code cells
- Named inputs and outputs for connecting to multiple data sources
- Access to the `flowfile` API for data I/O, artifacts, display, and logging
- Persistent variables across executions within the same flow

#### **Settings:**

| Parameter | Description |
|-----------|-------------|
| **Kernel** | Select a running kernel from the dropdown |
| **Code** | Python code written in the notebook editor |
| **Output Names** | Configure named outputs (default: `main`) |

For full documentation, see [Kernel Execution](../kernels.md).

---
[← Read data](input.md) | [Next: Combine data →](combine.md)
