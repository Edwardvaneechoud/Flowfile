# Python Package

Install Flowfile as a Python package for programmatic use.

## Installation

```bash
pip install flowfile
```

## Usage

```python
import flowfile as ff

# Read data
df = ff.read_csv("data.csv")

# Transform
df = df.with_formula("total", "price * quantity")

# Write
df.write_csv("output.csv")
```

## Running the Visual Editor

Launch the visual editor from Python:

```python
import flowfile as ff

# Open the visual editor
ff.open_editor()
```

## Features

- Full Python API for building flows programmatically
- Export visual flows to Python code
- Master key auto-generated and stored securely
- Integrates with existing Python projects

## Documentation

See the [Python API Guide](../python-api/index.md) for detailed documentation.
