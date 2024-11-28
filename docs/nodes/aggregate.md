# Aggregate Nodes

Aggregate nodes help you summarize and analyze your data by grouping and calculating statistics. These nodes are essential for creating summaries and transforming data structure.

## Node Details

### ![Group By](../assets/images/nodes/group_by.png){ width="50" height="50" } Group By

The Group By node allows you to aggregate data based on specific columns and calculate summary statistics.

**Key Features:**
- Group by multiple columns
- Apply various aggregation functions
- Calculate summary statistics
- Handle null values
- Create complex groupings

**Usage:**
1. Select grouping columns
2. Choose aggregation functions
3. Configure aggregated column names
4. Preview grouped results

---

### ![Pivot Data](../assets/images/nodes/pivot.png){ width="50" height="50" } Pivot Data

The Pivot Data node transforms your data from long to wide format, creating a matrix-like view.

**Key Features:**
- Convert long format to wide
- Multiple value columns
- Custom column naming
- Aggregation for duplicates
- Handle missing values

**Usage:**
1. Select columns to pivot
2. Choose value column
3. Configure aggregation method
4. Preview pivoted structure

---

### ![Unpivot Data](../assets/images/nodes/unpivot.png){ width="50" height="50" } Unpivot Data

The Unpivot Data node transforms wide format data into a longer format, making it more suitable for analysis.

**Key Features:**
- Convert wide format to long
- Multiple value columns
- Custom naming for new columns
- Preserve identifier columns
- Handle null values

**Usage:**
1. Select columns to unpivot
2. Configure value column name
3. Set variable column name
4. Preview transformed data

---

### ![Count Records](../assets/images/nodes/record_count.png){ width="50" height="50" } Count Records

The Count Records node provides various ways to count records in your dataset.

**Key Features:**
- Count total records
- Count by group
- Count distinct values
- Handle null values
- Default settings available

**Usage:**
1. Choose counting method
2. Select grouping columns (optional)
3. Configure output column names
4. Preview count results

## Technical Notes

### Input/Output Specifications
| Node | Inputs | Output |
|------|--------|---------|
| Group By | 1 | 1 |
| Pivot Data | 1 | 1 |
| Unpivot Data | 1 | 1 |
| Count Records | 1 | 1 |

### Data Considerations
- Group By: Memory usage scales with number of unique groups
- Pivot: Wide output can create many columns
- Unpivot: Long output can create many rows
- Count Records: Efficient operation, minimal memory impact

### Best Practices
- Preview results after aggregation
- Consider memory usage with large datasets
- Use appropriate aggregation functions
- Validate output data structure
- Handle null values appropriately

### Default Settings
The Count Records node comes with default settings for quick analysis.