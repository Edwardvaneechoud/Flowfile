# Output Nodes

Output nodes represent the final steps in your data pipeline, allowing you to save your transformed data or explore it visually. These nodes help you deliver your results in the desired format or analyze them directly.

## Node Details

### ![Write Data](../assets/images/nodes/output.png){ width="50" height="50" } Write Data

The Write Data node allows you to save your processed data to various file formats.

**Key Features:**
- Multiple output formats support
- File path configuration
- Compression options
- Overwrite protection
- Partition writing support

**Supported Formats:**
- CSV (.csv)
- Excel (.xlsx)
- Parquet (.parquet)

**Usage:**
1. Configure output path
2. Select file format
3. Set writing options
4. Configure compression (if applicable)
5. Preview and save

**Configuration Options:**
- File path and name
- Output format selection
- CSV delimiter and encoding
- Excel sheet name
- Parquet compression options
- Overwrite settings

---

### ![Explore Data](../assets/images/nodes/explore_data.png){ width="50" height="50" } Explore Data

The Explore Data node provides interactive data exploration and analysis capabilities.

**Key Features:**
- Data preview
- Basic statistics
- Column profiling
- Data quality checks
- Value distribution analysis

**Analysis Options:**
- Column statistics
- Data type information
- Missing value analysis
- Value distributions
- Pattern detection

**Usage:**
1. Connect input data
2. Select analysis options
3. View interactive dashboard
4. Export analysis results (optional)

**Exploration Features:**
- Preview first/last rows
- Column statistics
- Data quality metrics
- Value frequencies
- Pattern detection
- Relationship analysis

## Technical Notes

### Input/Output Specifications
| Node | Inputs | Output |
|------|--------|---------|
| Write Data | 1 | 0 |
| Explore Data | 1 | 0 |

### Performance Considerations
- Write Data: 
  - Large files may require significant disk space
  - Compression can impact write speed
  - Consider partitioning for very large datasets
- Explore Data:
  - Memory usage depends on dataset size
  - Some analyses may take time on large datasets
  - Interactive features work best with smaller datasets

### Best Practices
- Write Data:
  - Use appropriate file formats for your data size
  - Consider compression for large files
  - Verify write permissions before saving
  - Use meaningful file names and organization
  - Validate written files

- Explore Data:
  - Review data quality before final export
  - Use sampling for large datasets
  - Export important findings
  - Document insights
  - Check for anomalies

### File Format Guidelines
| Format | Best For | Considerations |
|--------|----------|----------------|
| CSV | Simple data, universal compatibility | No data type preservation |
| Excel | Small datasets, human readability | Size limitations |
| Parquet | Large datasets, type preservation | Requires specific tools to read |