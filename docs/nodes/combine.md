# Combine Nodes  

**Combine nodes** allow you to merge multiple datasets in different ways, enabling data integration and enrichment. These nodes help in aligning, linking, and structuring data from various sources to create a unified dataset.  

Depending on the method used, datasets can be merged by **matching values**, **stacking rows**, **finding similar records**, **generating all possible combinations**, or **grouping related elements in a network**.  

These transformations are essential for tasks like **data preparation, consolidation, and relationship mapping** across datasets.

## Node Details

### ![Join](../assets/images/nodes/join.png){ width="50" height="50" } Join  

The **Join** node merges two datasets based on matching values in selected columns.

---

#### **Key Features**  
- Supports multiple join types: **Inner, Left, Right, Outer**  
- Join on **one or more columns**  
- Handles duplicate column names with automatic renaming  

---

#### **Usage**  
1. Connect two input datasets (**left** and **right**).  
2. Select **join type** (`inner`, `left`, `right`, `anti` or `outer`).  
3. Choose **columns to join on**.  
4. Select **which columns to keep** from each dataset.  

---

#### **Configuration Options**  

| Parameter         | Description                                              |
|------------------|----------------------------------------------------------|
| **Join Type**    | Choose `inner`, `left`, `right`, `anti` or `outer` join. |
| **Join Columns** | Columns used to match records between datasets.          |

This node is useful for merging related datasets, such as **combining customer data with orders** or **linking product details with inventory**.


### ![Fuzzy Match](../assets/images/nodes/fuzzy_match.png){ width="50" height="50" } Fuzzy Match  

The **Fuzzy Match** node joins datasets based on **similar values** instead of exact matches, using various matching algorithms.

---

#### **Key Features**  
- Supports **fuzzy matching algorithms** (e.g., **Levenshtein**)  
- Configurable **similarity threshold**  
- Calculates **match scores**  
- Joins datasets based on approximate values  

---

#### **Usage**  
1. Connect two datasets (**left** and **right**).  
2. Select **columns** to match on.  
3. Choose a **fuzzy matching algorithm**.  
4. Set a **similarity threshold** (e.g., 75%).  

---

#### **Configuration Options**  

| Parameter           | Description                                   |
|---------------------|-----------------------------------------------|
| **Join Columns**    | Columns used for fuzzy matching.              |
| **Fuzzy Algorithm** | Choose an algorithm (e.g., `Levenshtein`).    |
| **Threshold Score** | Minimum similarity score for a match (0-100). |

This node is useful for handling **typos, name variations, and inconsistent formatting** when merging datasets.


### ![Union Data](../assets/images/nodes/union.png){ width="50" height="50" } Union Data  

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

This node is useful for **combining similar datasets**, such as **monthly reports or regional data**.


### ![Cross Join](../assets/images/nodes/cross_join.png){ width="50" height="50" } Cross Join

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

This node is useful for **creating test scenarios, generating all possible product combinations, or building comparison matrices**.

---

### ![Graph Solver](../assets/images/nodes/graph_solver.png){ width="50" height="50" } Graph Solver

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

This node is useful for **detecting dependencies, clustering related entities, and analyzing network connections**.
