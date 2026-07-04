# Aggregate Nodes

Aggregate nodes summarize and analyze your data by grouping and calculating statistics.

!!! info "Aggregate nodes in Flowfile Lite"
    **Group By**, **Pivot**, and **Unpivot** are included in the browser-only [Flowfile Lite](../../deployment/lite.md) build. **Count Records** is not — it requires the full desktop/server build.

## Node Details

### ![Group By](../../../assets/images/nodes/group_by.svg){ width="50" height="50" } Group By  

The **Group By** node aggregates data based on selected columns, allowing calculations such as sums, averages, counts, and more.

---

#### **Key Features**  
- Group by one or more columns  
- Apply aggregation functions to other columns  
- Rename output columns  

---

#### **Usage**  
1. Select one or more **columns to group by**.  
2. Choose aggregation functions for other columns.  
3. Set custom output column names if needed.  

---

#### **Configuration Options**  

| Parameter              | Description                                         |
|------------------------|-----------------------------------------------------|
| **Group By Columns**   | Columns used to define groups.                      |
| **Aggregations**       | One per output column. See the list below.          |
| **Output Column Name** | Custom name for the aggregated result (optional).   |

**Aggregation functions** (as shown in the drawer): **Sum**, **Count**, **Mean**, **Median**, **Min**, **Max**, **N_unique**, **First**, **Last**, and **Concat**. The average is **Mean** — there is no `avg` option.

### ![Pivot Data](../../../assets/images/nodes/pivot.svg){ width="50" height="50" } Pivot Data  

The **Pivot Data** node converts data from a long format to a wide format by creating new columns based on unique values in a pivot column.

---

#### **Key Features**  
- Transform long format into wide format  
- Select multiple index columns  
- Aggregate values during pivoting  

---

#### **Usage**  
1. Select **index columns** to retain in the final output.  
2. Choose a **pivot column** whose unique values will become new columns.  
3. Select a **value column** containing the data to fill the new columns.  
4. Apply aggregation functions (e.g., **Sum**, **Count**, **Mean**) if needed.  

---

#### **Configuration Options**  

| Parameter         | Description                                                          |
|-------------------|----------------------------------------------------------------------|
| **Index Columns** | Columns that define the groups in the final table.                   |
| **Pivot Column**  | Unique values from this column become new column names.              |
| **Value Column**  | The column containing values to be placed in the new columns.        |
| **Aggregations**  | Functions applied when multiple values exist per pivot column entry. |

### ![Unpivot Data](../../../assets/images/nodes/unpivot.svg){ width="50" height="50" } Unpivot Data  

The **Unpivot Data** node transforms data from wide format to long format, making it easier for analysis and reporting.

---

#### **Key Features**  
- Convert multiple columns into key-value pairs  
- Select index columns to retain  
- Use dynamic data type selection  

---

#### **Usage**  
1. Select **index columns** to keep unchanged.  
2. Choose **value columns** to transform into key-value pairs.  
3. (Optional) Enable **dynamic data type selection** to filter columns automatically.  

---

#### **Configuration Options**  

| Parameter              | Description                                                       |
|------------------------|-------------------------------------------------------------------|
| **Index Columns**      | Columns that remain unchanged in the final structure.             |
| **Value Columns**      | Columns that will be unpivoted into key-value pairs.              |
| **Data Type Selector** | Automatically select columns based on data type (e.g., `string`). |
| **Selection Mode**     | Choose between `column` or `data_type` for unpivot selection.     |

### ![Count Records](../../../assets/images/nodes/record_count.svg){ width="50" height="50" } Count Records

The **Count Records** node calculates the total number of rows in the dataset. It has no configuration and adds a new column `number_of_records`.

---
[← Combine data](combine.md) | [Next: Write data →](output.md)