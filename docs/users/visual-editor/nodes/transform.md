# Transform Nodes

Transform nodes modify and shape your data. These nodes handle everything from basic operations like filtering and sorting to more complex transformations like custom formulas and text manipulation.

## Node Details


### ![Add Record ID](../assets/images/nodes/record_id.png){ width="50" height="50" } Add Record ID  

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

### ![Formula](../assets/images/nodes/formula.png){ width="50" height="50" } Formula  

The **Formula** node allows you to create new columns or modify existing ones using custom expressions. It supports a wide range of operations, including mathematical calculations, string manipulations, and conditional logic.

---

#### **Key Features**

- Create new columns dynamically  
- Modify existing columns using expressions  
- Perform mathematical operations (`+`, `-`, `*`, `/`)  
- Apply string functions (`concat`, `uppercase`, `lowercase`)  
- Use conditional logic
- Use date time transformations

---

#### **Usage**

1. Drag the **Formula** node onto your canvas.  
2. Connect input data.  
3. Write your formula expression.  
4. Preview the results.  
5. Configure column names and data types.  

---

#### **Configuration Options**

| Parameter       | Description                                                |
|-----------------|------------------------------------------------------------|
| **Column Name** | The name of the new or modified column.                    |
| **Formula**     | The expression used to compute values for the column.      |
| **Data Type**   | The expected data type of the resulting column (optional). |

---

#### **Behavior**

- If a **new column** is created, it will be added to the dataset.  
- If an **existing column** is modified, its values will be updated based on the formula.  
- If no data type is specified, the result defaults to `String`.  

This transformation is useful for feature engineering, data cleaning, and enriching datasets with computed values.

---

### ![Select Data](../assets/images/nodes/select.png){ width="50" height="50" } Select Data  

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

### ![Filter Data](../assets/images/nodes/filter.png){ width="50" height="50" } Filter Data

The **Filter Data** node keeps only rows that match a specified condition. Enter a formula that evaluates to `true` or `false`, and only `true` rows remain.

---

#### **Key Features**  

- Apply **custom conditions** to filter data  
- Use **operators** (`or`, `and`, `<`,)  
- Support for **string, numeric, and date filtering**  

---

#### **Usage**

1. Drag the **Filter Data** node onto your canvas.  
2. Connect input data.  
3. Enter a filter formula (e.g., `[City] = 'Amsterdam'`).  
4. Apply the filter to keep matching rows.  

---

#### **Example Filters**  

| Expression                          | Description                                                 |
|-------------------------------------|-------------------------------------------------------------|
| `[City] = 'Amsterdam'`              | Keep rows where `City` is "Amsterdam".                      |
| `[Age] > 30`                        | Keep rows where `Age` is greater than 30.                   |
| `[Country] = 'USA' &&[Sales] > 100` | Keep rows where `Country` is "USA" and `Sales` is over 100. |

Use this node to refine datasets efficiently.

---

### ![Sort Data](../assets/images/nodes/sort.png){ width="50" height="50" } Sort Data  

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

### ![Take Sample](../assets/images/nodes/sample.png){ width="50" height="50" } Take Sample

The Take Sample node lets you work with a subset of your data.

---

### ![Drop Duplicates](../assets/images/nodes/unique.png){ width="50" height="50" } Drop Duplicates  

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

### ![Text to Rows](../assets/images/nodes/text_to_rows.png){ width="50" height="50" } Text to Rows  

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
### ![Polars Code](../assets/images/nodes/polars_code.png){ width="50" height="50" } Polars Code  

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
