import pl from 'nodejs-polars';
import { FlowExpr } from './expr.js';
import type { Operation, OperationGraph, SerializedGraph, FlowFrameConfig } from '../types/index.js';

/**
 * FlowFrame - Lightweight wrapper around Polars LazyFrame with operation graph tracking
 * Mirrors the Python FlowFrame style while remaining a separate, simplified implementation
 */
export class FlowFrame {
  private _lf: any; // pl.LazyFrame
  private _graph: OperationGraph;
  private _config: FlowFrameConfig;
  private static _idCounter = 0;

  constructor(
    lazyFrame: any, // pl.LazyFrame
    graph?: OperationGraph,
    config?: FlowFrameConfig
  ) {
    this._lf = lazyFrame;
    this._config = config || { trackOperations: true };

    if (graph) {
      this._graph = graph;
    } else {
      // Initialize with a root operation
      const rootId = this._generateId();
      this._graph = {
        operations: new Map([
          [rootId, {
            id: rootId,
            type: 'init',
            params: {},
            parents: [],
            timestamp: Date.now()
          }]
        ]),
        roots: [rootId],
        current: rootId
      };
    }
  }

  /**
   * Generate a unique operation ID
   */
  private _generateId(): string {
    if (this._config.generateId) {
      return this._config.generateId();
    }
    return `op_${Date.now()}_${FlowFrame._idCounter++}`;
  }

  /**
   * Track a new operation in the graph
   */
  private _trackOperation(type: string, params: Record<string, any>): string {
    if (!this._config.trackOperations) {
      return this._graph.current;
    }

    const opId = this._generateId();
    const operation: Operation = {
      id: opId,
      type,
      params,
      parents: [this._graph.current],
      timestamp: Date.now()
    };

    this._graph.operations.set(opId, operation);
    this._graph.current = opId;

    return opId;
  }

  /**
   * Create a new FlowFrame with an updated LazyFrame and operation
   */
  private _withOperation(
    newLf: any, // pl.LazyFrame
    operationType: string,
    params: Record<string, any>
  ): FlowFrame {
    const newGraph: OperationGraph = {
      operations: new Map(this._graph.operations),
      roots: [...this._graph.roots],
      current: this._graph.current
    };

    const ff = new FlowFrame(newLf, newGraph, this._config);
    ff._trackOperation(operationType, params);
    return ff;
  }

  /**
   * Convert FlowExpr or FlowExpr[] to native Polars expressions
   */
  private _toPolarsExpr(expr: FlowExpr | FlowExpr[] | string | string[]): any {
    if (Array.isArray(expr)) {
      return expr.map(e => typeof e === 'string' ? e : e.expr);
    }
    if (typeof expr === 'string') {
      return expr;
    }
    return (expr as FlowExpr).expr;
  }

  // ========== Core Operations ==========

  /**
   * Select columns
   */
  select(...exprs: (FlowExpr | string)[]): FlowFrame {
    const polarsExprs = exprs.map(e => this._toPolarsExpr(e));
    const newLf = this._lf.select(...polarsExprs);
    return this._withOperation(newLf, 'select', { exprs: exprs.map(e => e.toString()) });
  }

  /**
   * Filter rows
   */
  filter(predicate: FlowExpr): FlowFrame {
    const newLf = this._lf.filter(predicate.expr);
    return this._withOperation(newLf, 'filter', { predicate: predicate.toString() });
  }

  /**
   * Add or replace columns
   */
  withColumns(...exprs: FlowExpr[]): FlowFrame {
    const polarsExprs = exprs.map(e => e.expr);
    const newLf = this._lf.withColumns(...polarsExprs);
    return this._withOperation(newLf, 'with_columns', {
      exprs: exprs.map(e => e.toString())
    });
  }

  /**
   * Group by columns
   */
  groupBy(...by: (string | FlowExpr)[]): FlowGroupBy {
    const byExprs = by.map(b => typeof b === 'string' ? b : this._toPolarsExpr(b));
    return new FlowGroupBy(this, byExprs, by.map(b => b.toString()));
  }

  /**
   * Join with another FlowFrame
   */
  join(
    other: FlowFrame,
    options: {
      on?: string | string[];
      leftOn?: string | string[];
      rightOn?: string | string[];
      how?: 'inner' | 'left' | 'outer' | 'cross';
      suffix?: string;
    }
  ): FlowFrame {
    const newLf = this._lf.join(other._lf, options as any);
    return this._withOperation(newLf, 'join', {
      how: options.how || 'inner',
      on: options.on,
      leftOn: options.leftOn,
      rightOn: options.rightOn
    });
  }

  /**
   * Sort by columns
   */
  sort(by: string | string[], descending?: boolean | boolean[]): FlowFrame {
    const newLf = this._lf.sort(by, descending);
    return this._withOperation(newLf, 'sort', { by, descending });
  }

  /**
   * Get unique rows
   */
  unique(subset?: string | string[], maintainOrder?: boolean): FlowFrame {
    const newLf = this._lf.unique({ subset, maintainOrder });
    return this._withOperation(newLf, 'unique', { subset, maintainOrder });
  }

  /**
   * Limit number of rows
   */
  limit(n: number): FlowFrame {
    const newLf = this._lf.limit(n);
    return this._withOperation(newLf, 'limit', { n });
  }

  /**
   * Get first n rows
   */
  head(n: number = 5): FlowFrame {
    const newLf = this._lf.head(n);
    return this._withOperation(newLf, 'head', { n });
  }

  /**
   * Get last n rows
   */
  tail(n: number = 5): FlowFrame {
    const newLf = this._lf.tail(n);
    return this._withOperation(newLf, 'tail', { n });
  }

  /**
   * Rename columns
   */
  rename(mapping: Record<string, string>): FlowFrame {
    const newLf = this._lf.rename(mapping);
    return this._withOperation(newLf, 'rename', { mapping });
  }

  /**
   * Drop columns
   */
  drop(...columns: string[]): FlowFrame {
    const newLf = this._lf.drop(...columns);
    return this._withOperation(newLf, 'drop', { columns });
  }

  // ========== Execution & I/O ==========

  /**
   * Collect the LazyFrame into a DataFrame
   */
  collect(): pl.DataFrame {
    return this._lf.collect();
  }

  /**
   * Get the underlying LazyFrame
   */
  get lazyFrame(): any {
    return this._lf;
  }

  /**
   * Get column names
   */
  get columns(): string[] {
    return this._lf.columns;
  }

  // ========== Graph Operations ==========

  /**
   * Get the operation graph
   */
  getGraph(): OperationGraph {
    return {
      operations: new Map(this._graph.operations),
      roots: [...this._graph.roots],
      current: this._graph.current
    };
  }

  /**
   * Serialize the operation graph
   */
  serializeGraph(): SerializedGraph {
    return {
      operations: Array.from(this._graph.operations.entries()),
      roots: [...this._graph.roots],
      current: this._graph.current
    };
  }

  /**
   * Get all operations as an array
   */
  getOperations(): Operation[] {
    return Array.from(this._graph.operations.values());
  }

  /**
   * Get the current operation
   */
  getCurrentOperation(): Operation | undefined {
    return this._graph.operations.get(this._graph.current);
  }

  /**
   * Print operation graph for debugging
   */
  printGraph(): void {
    console.log('\n=== Operation Graph ===');
    const ops = this.getOperations();
    ops.forEach(op => {
      const indent = op.parents.length > 0 ? '  ' : '';
      console.log(`${indent}[${op.id}] ${op.type}`);
      if (Object.keys(op.params).length > 0) {
        console.log(`${indent}  params:`, op.params);
      }
    });
    console.log('======================\n');
  }

  // ========== Static Constructors ==========

  /**
   * Create FlowFrame from a DataFrame
   */
  static fromDataFrame(df: pl.DataFrame, config?: FlowFrameConfig): FlowFrame {
    return new FlowFrame(df.lazy(), undefined, config);
  }

  /**
   * Create FlowFrame from CSV
   */
  static readCsv(path: string, config?: FlowFrameConfig): FlowFrame {
    const lf = pl.readCSV(path).lazy();
    return new FlowFrame(lf, undefined, config);
  }

  /**
   * Create FlowFrame from Parquet
   */
  static readParquet(path: string, config?: FlowFrameConfig): FlowFrame {
    const lf = pl.readParquet(path).lazy();
    return new FlowFrame(lf, undefined, config);
  }

  /**
   * Scan CSV lazily
   */
  static scanCsv(path: string, config?: FlowFrameConfig): FlowFrame {
    const lf = pl.scanCSV(path);
    return new FlowFrame(lf, undefined, config);
  }
}

/**
 * FlowGroupBy - Represents a grouped FlowFrame
 * Mirrors the Python GroupBy pattern
 */
export class FlowGroupBy {
  private _parent: FlowFrame;
  private _by: any[];
  private _byStr: string[];

  constructor(parent: FlowFrame, by: any[], byStr: string[]) {
    this._parent = parent;
    this._by = by;
    this._byStr = byStr;
  }

  /**
   * Aggregate with expressions
   */
  agg(...exprs: FlowExpr[]): FlowFrame {
    const polarsExprs = exprs.map(e => e.expr);
    const newLf = this._parent.lazyFrame.groupBy(this._by).agg(...polarsExprs);

    // Create new FlowFrame with groupby + agg tracked as single operation
    return new FlowFrame(
      newLf,
      this._parent.getGraph(),
      { trackOperations: true }
    )['_withOperation'](newLf, 'group_by_agg', {
      by: this._byStr,
      agg: exprs.map(e => e.toString())
    });
  }
}
