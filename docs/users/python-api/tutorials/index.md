# Python API Tutorials

Learn to build powerful data pipelines with code through practical, hands-on examples.

## Available Tutorials

### [Building Flows with Code](flowfile_frame_api.md)
The complete guide to creating data pipelines programmatically while maintaining visual compatibility.

**You'll learn:**
- Creating pipelines with the FlowFrame API
- Using Polars-compatible operations
- Automatically generating visual graphs
- Switching between code and visual editing

**Perfect for:**
- Python developers new to Flowfile
- Data scientists wanting reproducible pipelines
- Anyone preferring code over drag-and-drop

## Coming Soon

### Data Pipeline Patterns
Common patterns for ETL, data cleaning, and analysis.

### Performance Optimization
Advanced techniques for handling large datasets efficiently.

### Integration Examples
Connecting Flowfile with pandas, scikit-learn, and other tools.

## Tutorial Style

Our Python tutorials focus on:
- **Real-world examples** - Practical use cases you'll actually encounter
- **Code-first approach** - Everything done programmatically
- **Visual integration** - How to leverage the UI when helpful
- **Best practices** - Production-ready patterns

## Quick Examples

### ETL Pipeline
```python
import flowfile as ff

# Extract
raw_data = ff.read_csv("sales.csv")

# Transform
transformed = (
    raw_data
    .filter(ff.col("amount") > 0)
    .with_columns([
        ff.col("date").str.strptime(ff.Date, "%Y-%m-%d")
    ])
    .group_by("region")
    .agg(ff.col("amount").sum())
)

# Load
transformed.write_parquet("output.parquet")
```

### Data Validation
```python
# Check for data quality issues
df = ff.read_csv("input.csv")

# Find duplicates
duplicates = df.group_by("id").agg(
    ff.count().alias("count")
).filter(ff.col("count") > 1)

# Find nulls
null_counts = df.select([
    ff.col(c).is_null().sum().alias(f"{c}_nulls")
    for c in df.columns
])
```

## Resources

- [API Reference](../reference/index.md) - Complete method documentation
- [Core Concepts](../concepts/index.md) - Understand the architecture
- [Quick Start](../quickstart.md) - Get running in 5 minutes

---

*Want more tutorials? Let us know what you'd like to see in our [GitHub Discussions](https://github.com/edwardvaneechoud/Flowfile/discussions)!*