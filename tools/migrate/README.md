
# Flowfile Migration Tool

Converts old pickle-based `.flowfile` format to the new YAML format.

## Why Migrate?

The old `.flowfile` format used Python pickle serialization, which has several issues:
- **Not human-readable** - binary format can't be inspected
- **Not versionable** - can't diff/merge in git
- **Fragile** - breaks when Python classes change
- **Security risk** - pickle can execute arbitrary code

The new YAML format is:
- ✅ Human-readable and editable
- ✅ Git-friendly (diff, merge, blame)
- ✅ Stable across code changes
- ✅ Safe to share

## Installation

No extra dependencies for JSON output. For YAML output:

```bash
pip install pyyaml
```

## Usage

### Migrate a single file

```bash
# From repository root
python -m tools.migrate path/to/flow.flowfile

# Output: path/to/flow.yaml
```

### Migrate a directory

```bash
python -m tools.migrate ./flows/

# Migrates all .flowfile files, preserving directory structure
```

### Options

```bash
# Specify output path
python -m tools.migrate flow.flowfile -o /output/path/flow.yaml

# Output as JSON instead of YAML
python -m tools.migrate flow.flowfile --format json

# Dry run (show what would be migrated)
python -m tools.migrate ./flows/ --dry-run

# Verbose output (show tracebacks on error)
python -m tools.migrate flow.flowfile -v
```

## Output Format

The migrated YAML has this structure:

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
    position:
      x: 100
      y: 200
    settings:
      received_file:
        path: data/input.csv
        file_type: csv
        table_settings:
          file_type: csv
          delimiter: ','
          encoding: utf-8
          
  - id: 2
    type: polars_code
    position:
      x: 300
      y: 200
    settings:
      polars_code: |
        output_df = input_df.with_columns(
            pl.col("name").str.to_uppercase()
        )

connections:
  - [1, 2]

node_starts:
  - 1
```

## Cleanup

After verifying migration works correctly, you can:

1. Delete old `.flowfile` files
2. Delete the `tools/migrate/` directory (it's not needed in production)

## Troubleshooting

### "ModuleNotFoundError: No module named 'tools'"

Make sure you're running from the repository root:

```bash
cd /path/to/Flowfile
python -m tools.migrate ...
```

### "PyYAML is required"

Install PyYAML:

```bash
pip install pyyaml
```

Or use JSON format:

```bash
python -m tools.migrate flow.flowfile --format json
```

### Migration fails for specific file

Run with verbose flag to see the full error:

```bash
python -m tools.migrate problem_file.flowfile -v
```

If the file uses very old schemas, you may need to add additional legacy class definitions to `tools/migrate/legacy_schemas.py`.