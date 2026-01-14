# Flowfile User Guides

Welcome to Flowfile! Whether you prefer visual drag-and-drop or writing code, we've got you covered.

## Choose Your Path

<div markdown="1" style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 1.5rem; margin: 2rem 0;">

<div markdown="1" style="border: 2px solid #00CED1; padding: 1.5rem; border-radius: 8px; background: linear-gradient(135deg, rgba(0,206,209,0.05) 0%, rgba(0,206,209,0.1) 100%);">

### 🌐 [Try in Browser](https://demo.flowfile.org)

**No installation required!** Perfect for trying Flowfile instantly.

**Features:**

- Zero setup - just open a URL
- 14 essential data transformation nodes
- 100% privacy - data stays in your browser
- Export results as CSV or Parquet

[**Open in Browser →**](https://demo.flowfile.org)

[Learn more about browser version](deployment/browser.md)

</div>

<div markdown="1" style="border: 1px solid #ddd; padding: 1.5rem; border-radius: 8px;">

### 🎨 [Visual Editor](visual-editor/index.md)

Perfect for analysts and business users who want to build data pipelines visually.

**You'll learn:**

- Drag and drop nodes to build flows
- Configure transformations with forms
- Connect to databases and cloud storage
- Export your flows as Python code

[**Get Started with Visual Editor →**](visual-editor/index.md)

</div>

<div markdown="1" style="border: 1px solid #ddd; padding: 1.5rem; border-radius: 8px;">

### 🐍 [Python API](python-api/index.md)

Perfect for developers and data scientists who prefer code.

**You'll learn:**

- Build pipelines with Polars-compatible API
- Seamlessly integrate with existing code
- Visualize your code as flow graphs
- Use advanced features and optimizations

[**Get Started with Python →**](python-api/index.md)

</div>

</div>

## The Best of Both Worlds

The beauty of Flowfile is that **you don't have to choose**. You can:

- Write code and visualize it instantly with `open_graph_in_editor()`
- Build visually and export as Python code
- Switch between visual and code at any time
- Collaborate across technical and non-technical teams

## Quick Examples

### Visual Approach
1. Drag a "Read Data" node onto canvas
2. Add a "Filter" node and connect them
3. Configure filter conditions in the form
4. Run and see results instantly

### Code Approach
```python
import flowfile as ff

df = ff.read_csv("data.csv")
result = df.filter(ff.col("amount") > 100)
ff.open_graph_in_editor(result.flow_graph)  # See it visually!
```

## Where to Start?

- **New to Flowfile?** Start with our [Quick Start Guide](../quickstart.md)
- **Coming from Excel/Tableau?** Try the [Visual Editor](visual-editor/index.md)
- **Know Python/Pandas/Polars?** Jump into the [Python API](python-api/index.md)
- **Want to see real examples?** Check out our tutorials in either section

---

*Remember: Every visual flow can become code, and every code pipeline can be visualized. Choose what feels natural and switch whenever you want!*