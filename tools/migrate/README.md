# Flowfile Migration Tool

Migrates `.flowfile` (pickle format, used in v0.4.1 and earlier) to YAML (v0.5+).

## Installation

```bash
pip install pyyaml  # Required for YAML output
```

## Usage

```bash
# Single file
python -m tools.migrate path/to/flow.flowfile

# Directory (recursive)
python -m tools.migrate ./flows/

# Specify output path
python -m tools.migrate flow.flowfile -o /output/path/flow.yaml

# Output as JSON
python -m tools.migrate flow.flowfile --format json

# Dry run
python -m tools.migrate ./flows/ --dry-run

# Verbose (show tracebacks)
python -m tools.migrate flow.flowfile -v
```

## Output Structure

```yaml
_version: '2.0'
_migrated_from: pickle
flow_id: 1
flow_name: my_analysis
flow_settings:
  name: my_analysis
  description: null
  execution_mode: Development
nodes:
  - id: 1
    type: read
    position: {x: 100, y: 200}
    settings:
      received_file:
        path: data/input.csv
        file_type: csv
connections:
  - [1, 2]
node_starts:
  - 1
```