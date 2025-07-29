# Input Nodes

Input nodes are the starting point for any data flow. Flowfile currently supports reading from **local files**, and **manual input**.

## Node Details

### ![Read Data](../assets/images/nodes/input_data.png){ width="50" height="50" } Read Data

The **Read Data** node allows you to load local data into your flow. It currently supports **CSV**, **Excel**, and **Parquet** file formats, each with specific configuration options.

#### **Supported Formats:**

- **CSV files** (`.csv`)
- **Excel files** (`.xlsx`, `.xls`)
- **Parquet files** (`.parquet`)

#### **Usage:**

1. Select your input file.  
2. Configure any format-specific options.  
3. Preview and confirm your data.  

---

#### CSV  
When a **CSV** file is selected, the following setup options are available:  

| Parameter               | Description                                                                                                                                                                          |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Has Headers**         | Determines whether the first row is used as headers. If `"yes"`, the first row is treated as column names. If `"no"`, default column names like `"Column 1, Column 2, ..."` are assigned. |
| **Delimiter**           | Specifies the character used to separate values (e.g., comma `,`, semicolon `;`, tab `\t`).                                                                                          |
| **Encoding**            | Defines the file encoding (e.g., `UTF-8`, `ISO-8859-1`).                                                                                                                             |
| **Quote Character**     | Character used to enclose text fields, preventing delimiter conflicts (e.g., `"`, `'`).                                                                                              |
| **New Line Delimiter**  | Specifies how new lines are detected (e.g., `\n`, `\r\n`).                                                                                                                          |
| **Schema Infer Length** | Determines how many rows are scanned to infer column types.                                                                                                                         |
| **Truncate Long Lines** | If enabled, long lines are truncated instead of causing errors.                                                                                                                     |
| **Ignore Errors**       | If enabled, the process continues even if some rows cause errors.                                                                                                                   |

---

#### Excel  
When an **Excel** file is selected, you can specify the sheet, select specific rows and columns, and configure headers and type inference options to tailor data loading to your needs.

| Parameter          | Description                                                                                                                                              |
|--------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Sheet Name**     | The name of the sheet to be read. If not specified, the first sheet is used.                                                                             |
| **Start Row**      | The row index (zero-based) from which reading starts. Default is `0` (beginning of the sheet).                                                           |
| **Start Column**   | The column index (zero-based) from which reading starts. Default is `0` (first column).                                                                  |
| **End Row**        | The row index (zero-based) at which reading stops. Default is `0` (read all rows).                                                                       |
| **End Column**     | The column index (zero-based) at which reading stops. Default is `0` (read all columns).                                                                 |
| **Has Headers**    | Determines whether the first row is treated as headers. If `true`, the first row is used as column names. If `false`, default column names are assigned. |
| **Type Inference** | If `true`, the engine attempts to infer data types. If `false`, data types are not automatically inferred.                                               |

---

#### Parquet  
When a **Parquet** file is selected, no additional setup options are required. Parquet is a columnar storage format optimized for efficiency and performance. It retains schema information and data types, enabling faster reads and writes without manual configuration.

---

### ![Manual Input](../assets/images/nodes/manual_input.png){ width="50" height="50" } Manual Input

The **Manual Input** node allows you to create data directly within Flowfile or paste data from your clipboard.

#### **Key Features:**
- Create data from scratch
- Paste data from clipboard in the raw data section

#### **Example Use Cases:**
- Creating small lookup tables
- Testing data transformations
- Quick data prototyping
- Adding configuration data to your flow
