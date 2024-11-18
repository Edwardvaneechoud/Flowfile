# Input Nodes

Input nodes are your starting point for any data flow. FlowFile currently supports reading from local files, Airbyte sources, and manual input.

## Node Details

### ![Read Data](../assets/images/nodes/input_data.png){ width="50" height="50" } Read Data

The Read Data node is your primary way to load data from local files into FlowFile.

**Supported Formats:**
- CSV files (.csv)
- Excel files (.xlsx, .xls)
- Parquet files (.parquet)

**Key Features:**
- Auto-detection of data types
- Preview of data before loading
- Configuration of delimiter and encoding for CSV files
- Sheet selection for Excel files

**Usage:**
1. Drag the Read Data node onto your canvas
2. Click to configure the node
3. Select your input file
4. Configure any format-specific options
5. Preview and confirm your data

**Configuration Options:**
- File path selection
- Sheet name (for Excel files)
- Delimiter (for CSV files)
- Header row options
- Data type inference settings

---

### ![Read Airbyte](../assets/images/nodes/airbyte.png){ width="50" height="50" } Read Airbyte

The Read Airbyte node allows you to connect to any configured Airbyte source and import data directly.

**Key Features:**
- Connect to any Airbyte source
- Select specific streams
- Configure sync modes

**Usage:**
1. Drag the Read Airbyte node onto your canvas
2. Configure the Airbyte connection
3. Select the desired stream
4. Configure sync options
5. Preview the data schema

**Configuration Options:**
- Connection selection
- Stream selection
- Sync mode
- Cursor field (for incremental syncs)
- Primary key configuration

---

### ![Manual Input](../assets/images/nodes/manual_input.png){ width="50" height="50" } Manual Input

The Manual Input node allows you to create data directly within FlowFile or paste data from your clipboard.

**Key Features:**
- Create data from scratch
- Paste data from clipboard
- Define column types manually
- Preview data as you enter it

**Usage:**
1. Drag the Manual Input node onto your canvas
2. Click to open the data editor
3. Either:
   - Enter data manually using the grid interface
   - Paste data from clipboard
4. Define column types if needed
5. Preview and confirm your data

**Configuration Options:**
- Column names and types
- Data validation rules
- Copy/paste settings
- Preview options

**Example Use Cases:**
- Creating small lookup tables
- Testing data transformations
- Quick data prototyping
- Adding configuration data to your flow

## Technical Notes

### Input/Output Specifications
| Node | Inputs | Output |
|------|--------|---------|
| Read Data | 0 | 1 |
| Read Airbyte | 0 | 1 |
| Manual Input | 0 | 1 |

### Best Practices
- Preview data after loading to verify structure
- Check data types are correctly inferred
- Use appropriate delimiters for CSV files
- Consider file encoding for text files