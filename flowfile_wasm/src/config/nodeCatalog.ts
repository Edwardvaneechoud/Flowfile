/**
 * The authoritative catalog of node types this in-browser build knows about.
 *
 * Extracted from Canvas.vue so there is exactly one palette definition. Three
 * consumers rely on this file staying the single source of truth:
 *  - Canvas.vue renders the palette from createNodeCategories() and types its
 *    settings-panel map with SupportedNodeType (compile-time exact-key check).
 *  - tests/unit/node-catalog.test.ts pins SUPPORTED_NODE_TYPES against the
 *    palette entries so the two cannot drift.
 *  - tools/generate_wasm_node_manifest.py (flowfile_core repo root) parses this
 *    file to build the share-link compatibility manifest core ships. Keep the
 *    category/node literals plain (no spreads, no computed keys).
 */

export interface NodeDefinition {
  type: string
  name: string
  icon: string
  inputs: number
  outputs: number
  // false → a full-app capability that can't run in this in-browser build; shown
  // greyed-out and locked (not draggable) so the breadth is still discoverable.
  available?: boolean
  // Extra search terms so the palette filter matches by concept, not just by name.
  keywords?: string[]
  // Heading slug appended to the category docsUrl for the locked-node "Learn more" link.
  docsAnchor?: string
}

export interface NodeCategory {
  name: string
  isOpen: boolean
  // Docs page for this category's nodes; locked (full-app) nodes link here.
  docsUrl?: string
  nodes: NodeDefinition[]
}

// Nodes flagged `available: false` run only in the full Flowfile app (they need a
// backend, network, or a heavier runtime than the in-browser Pyodide build). They
// render greyed-out and locked here so the full breadth stays discoverable.
// A fresh structure per call: isOpen is per-canvas-instance UI state.
export function createNodeCategories(): NodeCategory[] {
  return [
    {
      name: 'Input Sources',
      isOpen: true,
      docsUrl: 'https://edwardvaneechoud.github.io/Flowfile/users/visual-editor/nodes/input',
      nodes: [
        { type: 'read', name: 'Read File', icon: 'input_data.svg', inputs: 0, outputs: 1, keywords: ['csv', 'excel', 'parquet', 'json', 'file', 'import', 'load'] },
        { type: 'manual_input', name: 'Manual Input', icon: 'manual_input.svg', inputs: 0, outputs: 1, keywords: ['paste', 'type', 'create', 'test data'] },
        { type: 'external_data', name: 'External Data', icon: 'external_data.svg', inputs: 0, outputs: 1, keywords: ['url', 'http', 'fetch', 'remote', 'web', 'api'] },
        { type: 'read_from_catalog', name: 'Read from Catalog', icon: 'catalog_reader.svg', inputs: 0, outputs: 1, keywords: ['catalog', 'table', 'dataset', 'saved'] },
        { type: 'database_reader', name: 'Read from Database', icon: '', inputs: 0, outputs: 1, available: false, keywords: ['sql', 'postgres', 'postgresql', 'mysql', 'mssql', 'sqlserver', 'snowflake', 'oracle', 'redshift', 'bigquery', 'query', 'table', 'db'], docsAnchor: 'database-reader' },
        { type: 'cloud_storage_reader', name: 'Read from Cloud', icon: '', inputs: 0, outputs: 1, available: false, keywords: ['s3', 'aws', 'azure', 'adls', 'gcs', 'blob', 'bucket', 'cloud', 'object storage'], docsAnchor: 'cloud-storage-reader' },
        { type: 'rest_api_reader', name: 'REST API', icon: '', inputs: 0, outputs: 1, available: false, keywords: ['rest', 'api', 'http', 'json', 'endpoint', 'pagination', 'auth'], docsAnchor: 'rest-api-reader' },
        { type: 'kafka_source', name: 'Kafka Source', icon: '', inputs: 0, outputs: 1, available: false, keywords: ['kafka', 'redpanda', 'stream', 'streaming', 'topic', 'events'], docsAnchor: 'kafka-source' },
        { type: 'google_analytics_reader', name: 'Google Analytics', icon: '', inputs: 0, outputs: 1, available: false, keywords: ['google analytics', 'ga', 'ga4', 'analytics', 'web analytics'], docsAnchor: 'google-analytics-reader' }
      ]
    },
    {
      name: 'Transformations',
      isOpen: true,
      docsUrl: 'https://edwardvaneechoud.github.io/Flowfile/users/visual-editor/nodes/transform',
      nodes: [
        { type: 'filter', name: 'Filter', icon: 'filter.svg', inputs: 1, outputs: 1, keywords: ['where', 'subset', 'condition', 'rows'] },
        { type: 'select', name: 'Select', icon: 'select.svg', inputs: 1, outputs: 1, keywords: ['columns', 'rename', 'reorder', 'keep', 'drop'] },
        { type: 'formula', name: 'Formula', icon: 'formula.svg', inputs: 1, outputs: 1, keywords: ['expression', 'calculate', 'compute', 'sum', 'math', 'concat', 'new column'] },
        { type: 'sort', name: 'Sort', icon: 'sort.svg', inputs: 1, outputs: 1, keywords: ['order', 'arrange', 'rank', 'ascending', 'descending'] },
        { type: 'polars_code', name: 'Polars Code', icon: 'polars_code.svg', inputs: 1, outputs: 1, keywords: ['python', 'code', 'custom', 'script', 'dataframe'] },
        { type: 'unique', name: 'Unique', icon: 'unique.svg', inputs: 1, outputs: 1, keywords: ['dedupe', 'distinct', 'drop duplicates', 'deduplicate'] },
        { type: 'dynamic_rename', name: 'Rename', icon: 'dynamic_rename.svg', inputs: 1, outputs: 1, keywords: ['rename', 'columns', 'prefix', 'suffix'] },
        { type: 'record_id', name: 'Record ID', icon: 'record_id.svg', inputs: 1, outputs: 1, keywords: ['row number', 'index', 'id', 'sequence'] },
        { type: 'head', name: 'Take Sample', icon: 'sample.svg', inputs: 1, outputs: 1, keywords: ['sample', 'limit', 'top', 'head', 'subset'] },
        { type: 'window_functions', name: 'Window Functions', icon: '', inputs: 1, outputs: 1, available: false, keywords: ['window', 'rolling', 'cumulative', 'rank', 'partition', 'lag', 'lead', 'over'], docsAnchor: 'window-functions' },
        { type: 'sql_query', name: 'SQL Query', icon: '', inputs: 1, outputs: 1, available: false, keywords: ['sql', 'query', 'select', 'where'], docsAnchor: 'sql-query' },
        { type: 'python_script', name: 'Python Script', icon: '', inputs: 1, outputs: 1, available: false, keywords: ['python', 'code', 'script', 'kernel', 'pandas'], docsAnchor: 'python-script' }
      ]
    },
    {
      name: 'Combine Operations',
      isOpen: true,
      docsUrl: 'https://edwardvaneechoud.github.io/Flowfile/users/visual-editor/nodes/combine',
      nodes: [
        { type: 'join', name: 'Join', icon: 'join.svg', inputs: 2, outputs: 1, keywords: ['merge', 'lookup', 'vlookup', 'inner', 'left', 'right', 'outer'] },
        { type: 'cross_join', name: 'Cross Join', icon: 'cross_join.svg', inputs: 2, outputs: 1, keywords: ['cartesian', 'cross', 'combinations'] },
        // inputs: 1 — single handle accepts multiple connections (like polars_code).
        { type: 'union', name: 'Union', icon: 'union.svg', inputs: 1, outputs: 1, keywords: ['concat', 'append', 'stack', 'combine'] },
        { type: 'fuzzy_match', name: 'Fuzzy Match', icon: '', inputs: 2, outputs: 1, available: false, keywords: ['fuzzy', 'similarity', 'levenshtein', 'approximate', 'fuzzy join'], docsAnchor: 'fuzzy-match' },
        { type: 'graph_solver', name: 'Graph Solver', icon: '', inputs: 1, outputs: 1, available: false, keywords: ['graph', 'network', 'cluster', 'connected components'], docsAnchor: 'graph-solver' },
        { type: 'gate', name: 'Gate', icon: '', inputs: 2, outputs: 1, available: false, keywords: ['gate', 'condition', 'branch', 'skip', 'if'], docsAnchor: 'gate' }
      ]
    },
    {
      name: 'Aggregations',
      isOpen: true,
      docsUrl: 'https://edwardvaneechoud.github.io/Flowfile/users/visual-editor/nodes/aggregate',
      nodes: [
        { type: 'group_by', name: 'Group By', icon: 'group_by.svg', inputs: 1, outputs: 1, keywords: ['aggregate', 'sum', 'mean', 'average', 'count', 'min', 'max', 'median', 'summarize'] },
        { type: 'pivot', name: 'Pivot', icon: 'pivot.svg', inputs: 1, outputs: 1, keywords: ['crosstab', 'wide', 'reshape', 'spread'] },
        { type: 'unpivot', name: 'Unpivot', icon: 'unpivot.svg', inputs: 1, outputs: 1, keywords: ['melt', 'long', 'reshape', 'gather'] }
      ]
    },
    {
      name: 'Machine Learning',
      isOpen: true,
      docsUrl: 'https://edwardvaneechoud.github.io/Flowfile/users/visual-editor/nodes/ml',
      nodes: [
        { type: 'train_model', name: 'Train Model', icon: '', inputs: 1, outputs: 1, available: false, keywords: ['ml', 'machine learning', 'train', 'model', 'regression', 'classification', 'fit', 'sklearn'], docsAnchor: 'train-model' },
        { type: 'apply_model', name: 'Apply Model', icon: '', inputs: 1, outputs: 1, available: false, keywords: ['ml', 'machine learning', 'predict', 'score', 'inference', 'model'], docsAnchor: 'apply-model' },
        { type: 'evaluate_model', name: 'Evaluate Model', icon: '', inputs: 1, outputs: 1, available: false, keywords: ['ml', 'machine learning', 'evaluate', 'metrics', 'accuracy', 'model'], docsAnchor: 'evaluate-model' }
      ]
    },
    {
      name: 'Output Operations',
      isOpen: true,
      docsUrl: 'https://edwardvaneechoud.github.io/Flowfile/users/visual-editor/nodes/output',
      nodes: [
        { type: 'explore_data', name: 'Explore Data', icon: 'explore_data.svg', inputs: 1, outputs: 0, keywords: ['profile', 'describe', 'preview', 'eda', 'visualize', 'chart'] },
        { type: 'output', name: 'Write Data', icon: 'output.svg', inputs: 1, outputs: 0, keywords: ['csv', 'excel', 'parquet', 'write', 'save', 'export', 'file'] },
        { type: 'write_to_catalog', name: 'Write to Catalog', icon: 'catalog_writer.svg', inputs: 1, outputs: 0, keywords: ['catalog', 'table', 'save'] },
        { type: 'external_output', name: 'External Output', icon: 'external_output.svg', inputs: 1, outputs: 0, keywords: ['url', 'http', 'api', 'send', 'webhook'] },
        { type: 'database_writer', name: 'Write to Database', icon: '', inputs: 1, outputs: 0, available: false, keywords: ['sql', 'postgres', 'mysql', 'snowflake', 'redshift', 'bigquery', 'insert', 'table', 'db'], docsAnchor: 'database-writer' },
        { type: 'cloud_storage_writer', name: 'Write to Cloud', icon: '', inputs: 1, outputs: 0, available: false, keywords: ['s3', 'aws', 'azure', 'adls', 'gcs', 'blob', 'bucket', 'cloud'], docsAnchor: 'cloud-storage-writer' }
      ]
    }
  ]
}

/**
 * Every node type this build can actually run — the palette's `available !== false`
 * entries, which is also exactly the key set of Canvas.vue's settings-panel map
 * (enforced there via `satisfies Record<SupportedNodeType, ...>`) and of the
 * executeNode switch (pinned in tests/unit/node-catalog.test.ts).
 */
export const SUPPORTED_NODE_TYPES = [
  'read',
  'manual_input',
  'external_data',
  'read_from_catalog',
  'filter',
  'select',
  'formula',
  'sort',
  'polars_code',
  'unique',
  'dynamic_rename',
  'record_id',
  'head',
  'join',
  'cross_join',
  'union',
  'group_by',
  'pivot',
  'unpivot',
  'explore_data',
  'output',
  'write_to_catalog',
  'external_output'
] as const

export type SupportedNodeType = (typeof SUPPORTED_NODE_TYPES)[number]

const SUPPORTED_SET: ReadonlySet<string> = new Set(SUPPORTED_NODE_TYPES)

export function isSupportedNodeType(type: string): type is SupportedNodeType {
  return SUPPORTED_SET.has(type)
}

/** Palette entries flagged `available: false` — full-app-only teasers. */
export function lockedNodeTypes(): string[] {
  return createNodeCategories()
    .flatMap(cat => cat.nodes)
    .filter(n => n.available === false)
    .map(n => n.type)
}
