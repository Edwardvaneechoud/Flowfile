// Core classes
export { FlowFrame, FlowGroupBy } from './core/flowframe.js';
export { FlowExpr, col, lit } from './core/expr.js';

// Types
export type { Operation, OperationGraph, SerializedGraph, FlowFrameConfig } from './types/index.js';

// Re-export nodejs-polars for convenience
import pl from 'nodejs-polars';
export { pl };
