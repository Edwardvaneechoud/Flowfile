# Flowfile YAML Schema

This module generates and provides a JSON Schema for Flowfile YAML files, enabling IDE support for autocompletion and validation.

## Files

- `flowfile.schema.json` - JSON Schema for Flowfile YAML format
- `generate_yaml_schema.py` - Script to regenerate the schema from Pydantic models

## VS Code Setup

To enable YAML autocompletion and validation in VS Code:

### Prerequisites

Install the [YAML extension](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml) for VS Code.

### Option 1: Online URL (Recommended)

Add this comment at the top of your Flowfile YAML files:

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/Edwardvaneechoud/Flowfile/main/tools/yaml_schema/flowfile.schema.json
```

This works anywhere without needing a local copy of the schema.

### Option 2: Local file reference

If working within the Flowfile repository:

```yaml
# yaml-language-server: $schema=./path/to/tools/yaml_schema/flowfile.schema.json
```

### Option 3: Workspace settings

Create or update `.vscode/settings.json` in your project:

```json
{
  "yaml.schemas": {
    "https://raw.githubusercontent.com/Edwardvaneechoud/Flowfile/main/tools/yaml_schema/flowfile.schema.json": [
      "**/flows/**/*.yaml",
      "**/flows/**/*.yml",
      "**/*.flowfile.yaml"
    ]
  }
}
```

## Regenerating the Schema

If the Pydantic models change, regenerate the schema:

```bash
poetry run python -m tools.yaml_schema.generate_yaml_schema
```

## What You Get

With the schema enabled, VS Code will provide:

- **Autocompletion** for all Flowfile properties
- **Validation** of your YAML structure
- **Hover documentation** for fields
- **Error highlighting** for invalid configurations
- **Type hints** for node settings based on node type
