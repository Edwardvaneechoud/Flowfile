# Combine Nodes

Combine nodes allow you to merge multiple datasets together in different ways. These nodes are essential for data integration and connecting related information from different sources.

## Node Details

### ![Join](../assets/images/nodes/join.png){ width="50" height="50" } Join

The Join node combines two datasets based on matching keys, similar to SQL joins.

**Key Features:**
- Multiple join types (Inner, Left, Right, Outer)
- Join on multiple columns
- Suffix handling for duplicate columns
- Preview of joined results

**Usage:**
1. Connect two input datasets (left and right)
2. Select join type
3. Choose columns to join on
4. Configure which columns you want to 
5. Preview joined data

**Common Use Cases:**
- Combining customer data with orders
- Linking product information with inventory
- Merging transaction data with reference tables

---

### ![Fuzzy Match](../assets/images/nodes/fuzzy_match.jpg){ width="50" height="50" } Fuzzy Match

The Fuzzy Match node joins datasets based on similar (but not exactly matching) values.

**Key Features:**
- Multiple matching algorithms
- Configurable similarity threshold
- Match score calculation
- Best match selection

**Usage:**
1. Connect two datasets
2. Select columns to match on
3. Choose matching algorithm
4. Set similarity threshold
5. Review and validate matches

**Common Use Cases:**
- Matching company names with variations
- Linking customer records with typos
- Connecting addresses with different formats

---

### ![Union Data](../assets/images/nodes/union.png){ width="50" height="50" } Union Data

The Union Data node combines multiple datasets vertically (stacking rows).

**Key Features:**
- Support for up to 10 inputs
- Automatic column alignment
- Default settings available
- Optional source tracking

**Usage:**
1. Connect multiple input datasets
2. Configure column matching
3. Set handling of missing values
4. Preview combined dataset

**Common Use Cases:**
- Combining monthly data files
- Merging regional datasets
- Consolidating similar reports

---

### ![Cross Join](../assets/images/nodes/cross_join.png){ width="50" height="50" } Cross Join

The Cross Join node creates all possible combinations between two datasets.

**Key Features:**
- Complete cartesian product
- Optional filtering after join
- Column name conflict handling
- Result size warning

**Usage:**
1. Connect two datasets
2. Configure output column names
3. Preview initial results
4. Optional: Add filters

**Common Use Cases:**
- Creating all possible product combinations
- Generating test scenarios
- Building comparison matrices

---

### ![Graph Solver](../assets/images/nodes/graph_solver.png){ width="50" height="50" } Graph Solver

The Graph Solver node helps to group nodes based on a graph structured dataset

**Usage:**
1. Define from and to columns
2. Observe which nodes belong in the same group

**Common Use Cases:**
- Finding connection paths
- Analyzing dependencies

## Technical Notes

### Input/Output Specifications
| Node | Inputs | Output |
|------|--------|---------|
| Join | 2 | 1 |
| Fuzzy Match | 2 | 1 |
| Union Data | Up to 10 | 1 |
| Cross Join | 2 | 1 |
| Graph Solver | 1 | 1 |

### Performance Considerations
- Cross Joins can generate very large datasets
- Fuzzy Matching is more computationally intensive than exact joins
- Union operations scale with the number of input datasets
- Graph operations complexity depends on relationship complexity

### Best Practices
- Always preview results after combining datasets
- Consider filtering data before joining large datasets
- Use appropriate join types to avoid data loss
- Monitor memory usage with large datasets