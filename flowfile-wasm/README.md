# FlowFile WASM - Lightweight Polars Variant

A lightweight, separate implementation of FlowFile's FlowFrame pattern for WebAssembly environments. This package provides standard Polars operations with **operation graph tracking** in the same style as the Python FlowFile project.

## Philosophy

This is **NOT** a first-class citizen of the main FlowFile project. Instead, it's a separate directory that:

- ✅ Cherry-picks concepts from the existing Python codebase
- ✅ Maintains the same FlowFrame style (operation tracking, method chaining)
- ✅ Focuses on standard operations that work well in WASM/browser environments
- ❌ Does NOT aim for feature parity with Python FlowFile
- ❌ Does NOT include advanced plugins or complex I/O operations

## What's Included

### Core Operations
- **Selection**: `select()`, `drop()`
- **Filtering**: `filter()` with expression support
- **Grouping**: `groupBy()` with `agg()`
- **Joining**: `join()` (inner, left, outer, cross)
- **Transformation**: `withColumns()`, `rename()`
- **Sorting**: `sort()`, `unique()`
- **Limiting**: `head()`, `tail()`, `limit()`

### Expression System
- **Comparison**: `eq()`, `neq()`, `gt()`, `gte()`, `lt()`, `lte()`
- **Logical**: `and()`, `or()`, `not()`
- **Arithmetic**: `add()`, `sub()`, `mul()`, `div()`
- **Aggregations**: `sum()`, `mean()`, `min()`, `max()`, `count()`
- **String ops**: `str.contains()`, `str.startsWith()`, `str.toUppercase()`, etc.
- **Null handling**: `isNull()`, `isNotNull()`, `fillNull()`

### Operation Graph Tracking
Just like the Python version, every operation is tracked:
```typescript
const ff = FlowFrame.fromDataFrame(df);
const result = ff
  .filter(col('age').gt(25))
  .groupBy('city')
  .agg(col('salary').mean());

// Print the operation graph
result.printGraph();

// Serialize for sending to visual designer
const graph = result.serializeGraph();
```

## What's Excluded

To keep this lightweight and WASM-friendly:

- ❌ Fuzzy matching (`pl-fuzzy-frame-match`)
- ❌ Distance metrics (`polars-distance`)
- ❌ Advanced grouping (`polars-grouper`)
- ❌ Database I/O (SQL, S3, Delta Lake)
- ❌ Complex plugins and extensions
- ❌ Code generation features
- ❌ Worker/Core service integration

## Installation

```bash
cd flowfile-wasm
npm install
npm run build
```

## Usage

### Basic Example

```typescript
import { FlowFrame, col, lit } from 'flowfile-wasm';

// Create from DataFrame
const ff = FlowFrame.fromDataFrame(df);

// Chain operations (same style as Python!)
const result = ff
  .filter(col('age').gt(28))
  .select(col('name'), col('salary'))
  .sort('salary', true);

// Collect results
const output = result.collect();
console.log(output.toString());

// View operation graph
result.printGraph();
```

### Group By and Aggregate

```typescript
const result = ff
  .groupBy('department')
  .agg(
    col('salary').mean().alias('avg_salary'),
    col('id').count().alias('employee_count')
  );
```

### Joins

```typescript
const employees = FlowFrame.fromDataFrame(employeesDF);
const departments = FlowFrame.fromDataFrame(departmentsDF);

const result = employees
  .join(departments, {
    on: 'dept_id',
    how: 'left'
  })
  .select(col('name'), col('dept_name'), col('salary'));
```

### String Operations

```typescript
const result = ff
  .filter(col('email').str.endsWith('@company.com'))
  .withColumns(
    col('name').str.toUppercase().alias('name_upper')
  );
```

## Architecture

### Directory Structure

```
flowfile-wasm/
├── src/
│   ├── core/
│   │   ├── flowframe.ts    # Main FlowFrame class
│   │   ├── expr.ts          # Expression wrappers
│   │   └── index.ts
│   ├── types/
│   │   ├── operation.ts     # Operation graph types
│   │   └── index.ts
│   └── index.ts             # Main exports
├── examples/
│   └── basic-operations.js  # Usage examples
├── package.json
├── tsconfig.json
└── README.md
```

### Key Classes

**FlowFrame**
- Wraps `nodejs-polars` LazyFrame
- Tracks every operation in a graph
- Returns new FlowFrame instances for method chaining
- Serializes operation history

**FlowExpr**
- Wraps Polars expressions
- Provides fluent API for building expressions
- Supports method chaining (e.g., `col('x').gt(5).and(col('y').lt(10))`)

**FlowGroupBy**
- Represents a grouped FlowFrame
- Supports aggregation operations
- Maintains operation tracking

### Operation Graph

Each FlowFrame maintains an operation graph:

```typescript
interface Operation {
  id: string;                    // Unique operation ID
  type: string;                  // Operation type (filter, select, etc.)
  params: Record<string, any>;   // Operation parameters
  parents: string[];             // Parent operation IDs
  timestamp: number;             // When operation was created
}
```

This graph can be:
- Inspected with `getGraph()` or `getOperations()`
- Printed with `printGraph()`
- Serialized with `serializeGraph()` for JSON export
- Used to reconstruct the operation flow in a visual designer

## Running Examples

```bash
npm run build
npm run example
```

This will run through several examples showing:
1. Filter and select operations
2. Group by and aggregation
3. Complex operation chains
4. Join operations
5. String operations
6. Graph serialization

## Comparison with Python FlowFile

| Feature | Python FlowFile | WASM Variant |
|---------|----------------|--------------|
| Operation tracking | ✅ | ✅ |
| LazyFrame wrapper | ✅ | ✅ |
| Method chaining | ✅ | ✅ |
| Graph serialization | ✅ | ✅ |
| Core operations | ✅ | ✅ |
| Fuzzy matching | ✅ | ❌ |
| Database I/O | ✅ | ❌ |
| Cloud storage | ✅ | ❌ |
| Complex plugins | ✅ | ❌ |
| Code generation | ✅ | ❌ |
| Visual designer integration | ✅ | 🔄 (Partial) |

## Why a Separate Directory?

This approach offers several benefits:

1. **Independence**: No risk of breaking the main project
2. **Simplicity**: Focus on what works well in WASM
3. **Size**: Keep bundle size minimal for browsers
4. **Versioning**: Can evolve at its own pace
5. **Clarity**: Clear separation of concerns

The WASM variant **borrows the style** from the Python project but makes its own architectural decisions optimized for browser/WASM constraints.

## Future Roadmap

- [ ] True WASM support (currently using nodejs-polars)
- [ ] Browser File API integration
- [ ] IndexedDB for large datasets
- [ ] Streaming support for large files
- [ ] More comprehensive operation coverage
- [ ] Performance benchmarks
- [ ] Integration tests with visual designer

## Development

```bash
# Install dependencies
npm install

# Build TypeScript
npm run build

# Watch mode for development
npm run dev

# Run examples
npm run example
```

## License

Same as parent FlowFile project.

## Contributing

This is a lightweight, experimental variant. Contributions welcome, but keep in mind:
- Keep it simple and focused on standard operations
- No dependencies on complex Polars plugins
- Maintain the FlowFrame style from Python version
- Optimize for WASM/browser environments
