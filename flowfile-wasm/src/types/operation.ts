/**
 * Represents a single operation in the operation graph
 * Mirrors the Python FlowFrame operation tracking style
 */
export interface Operation {
  /** Unique identifier for this operation node */
  id: string;

  /** Type of operation (e.g., 'filter', 'select', 'group_by') */
  type: string;

  /** Parameters passed to the operation */
  params: Record<string, any>;

  /** Parent operation IDs (operations this depends on) */
  parents: string[];

  /** Timestamp when operation was created */
  timestamp: number;
}

/**
 * Represents the complete operation graph
 */
export interface OperationGraph {
  /** All operations in the graph, keyed by ID */
  operations: Map<string, Operation>;

  /** Root operation IDs (operations with no parents) */
  roots: string[];

  /** Current leaf operation ID (most recent) */
  current: string;
}

/**
 * Serializable version of the operation graph
 */
export interface SerializedGraph {
  operations: Array<[string, Operation]>;
  roots: string[];
  current: string;
}

/**
 * Configuration for FlowFrame behavior
 */
export interface FlowFrameConfig {
  /** Whether to track operations (can disable for performance) */
  trackOperations?: boolean;

  /** Custom ID generator function */
  generateId?: () => string;
}
