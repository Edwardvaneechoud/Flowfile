# Combine Nodes  

**Combine nodes** merge multiple datasets by **matching values**, **stacking rows**, **finding similar records**, **generating all possible combinations**, or **grouping related elements in a network**.

!!! info "Some combine nodes are not in Flowfile Lite"
    The browser-only [Flowfile Lite](../../deployment/lite.md) build supports **Join**, **Cross Join**, and **Union Data**. **Fuzzy Match** and **Graph Solver** require the full desktop/server build.

## Node Details

### ![Join](../../../assets/images/nodes/join.svg){ width="50" height="50" } Join  

The **Join** node merges two datasets based on matching values in selected columns.

---

#### **Key Features**  
- Supports seven join types: **inner**, **left**, **right**, **full**, **semi**, **anti**, and **cross**  
- Join on **one or more columns**  
- Handles duplicate column names with automatic renaming  

---

#### **Usage**  
1. Connect two input datasets (**left** and **right**).  
2. Select a **join type** (see the table below).  
3. Choose **columns to join on** (not needed for `cross`).  
4. Select **which columns to keep** from each dataset.  

---

#### **Configuration Options**  

| Parameter         | Description                                              |
|------------------|----------------------------------------------------------|
| **Join Type**    | `inner`, `left`, `right`, `full`, `semi`, `anti`, or `cross`. |
| **Join Columns** | Columns used to match records between datasets. Not required for a `cross` join. |

**Join types:**

| Type | Result |
|------|--------|
| `inner` | Only rows with a match in both inputs. |
| `left` | Every left row; matched right columns, nulls where there is no match. |
| `right` | Every right row; matched left columns, nulls where there is no match. |
| `full` | Every row from both inputs, matched where possible. |
| `semi` | Left rows that have a match on the right — right columns are not added. |
| `anti` | Left rows that have no match on the right. |
| `cross` | Every combination of left and right rows (Cartesian product). |

!!! note "`full` vs `outer`"
    The Join node's dropdown labels the full outer join **`full`**. The programmatic API also accepts `outer` as an alias for the same strategy. For a Cartesian product with no keys, the dedicated [Cross Join](#cross-join) node is usually clearer than the `cross` type here.

---

### ![Fuzzy Match](../../../assets/images/nodes/fuzzy_match.svg){ width="50" height="50" } Fuzzy Match  

The **Fuzzy Match** node joins datasets based on **similar values** instead of exact matches, using various matching algorithms.

---

#### **Key Features**  
- Six string-similarity algorithms: `levenshtein`, `jaro`, `jaro_winkler`, `hamming`, `damerau_levenshtein`, and `indel`  
- Configurable **similarity threshold**  
- Calculates **match scores**  
- Joins datasets based on approximate values  

---

#### **Usage**  
1. Connect two datasets (**left** and **right**).  
2. Select **columns** to match on.  
3. Choose a **fuzzy matching algorithm**.  
4. Set a **similarity threshold** (0-100; defaults to `80`).  

---

#### **Configuration Options**  

| Parameter           | Description                                                                                                   |
|---------------------|--------------------------------------------------------------------------------------------------------------|
| **Join Columns**    | Columns used for fuzzy matching.                                                                             |
| **Fuzzy Algorithm** | One of `levenshtein`, `jaro`, `jaro_winkler`, `hamming`, `damerau_levenshtein`, or `indel`.                  |
| **Threshold Score** | Minimum similarity score for a match, on a scale of 0-100. Defaults to `80`.                                 |

---

### ![Union Data](../../../assets/images/nodes/union.svg){ width="50" height="50" } Union Data  

The **Union Data** node merges multiple datasets by stacking rows together.

---

#### **Key Features**  

- Combines multiple datasets into one  
- **Automatically aligns columns** based on names  
- Uses **diagonal relaxed mode**, allowing flexible column matching  

---

#### **Usage**  

1. Connect multiple input datasets.  
2. The node will automatically align and stack the data.  

---

### ![Cross Join](../../../assets/images/nodes/cross_join.svg){ width="50" height="50" } Cross Join

The **Cross Join** node creates all possible combinations between two datasets.

---

#### **Key Features**  

- Generates a **Cartesian product** of two datasets  
- Automatically aligns columns  
- Handles duplicate column names  

---

#### **Usage**  

1. Connect two datasets (**left** and **right**).
2. Select the columns that you would like to keep and their output names
3. The node will generate all possible row combinations.  

---

### ![Graph Solver](../../../assets/images/nodes/graph_solver.svg){ width="50" height="50" } Graph Solver

The **Graph Solver** node groups related records based on connections in a graph-structured dataset.

---

#### **Key Features**  
- Identifies **connected components** in graph-like data  
- Groups related nodes into the same category  
- Supports **custom output column names**  

---

#### **Usage**  
1. Select **From** and **To** columns to define relationships.  
2. The node assigns a **group identifier** to connected nodes.  

---

#### **Configuration Options**  

| Parameter           | Description                                      |
|--------------------|--------------------------------------------------|
| **From Column**    | Defines the starting point of each connection.  |
| **To Column**      | Defines the endpoint of each connection.        |
| **Output Column**  | Stores the assigned group identifier.           |

---

### Run Flow

The **Run Flow** node executes another, catalog-registered flow inside the current one — the calling side of a [subflow](../subflows.md). Its input and output handles are shaped by the child flow's Flow Input and Flow Output nodes, and its settings map values into the child's parameters, including running the child once per row of a driving table.

| Parameter | Description |
|-----------|-------------|
| **Flow** | The catalog-registered child flow to run |
| **Parameter bindings** | Default, constant, or column-mapped value per child parameter |
| **Iteration mode** | Run once with the first row's values, or once per row (capped at 1000) |

See [Subflows](../subflows.md) for registration, wiring, and error handling.

---
[← Transform data](transform.md) | [Next: Aggregate data →](aggregate.md)