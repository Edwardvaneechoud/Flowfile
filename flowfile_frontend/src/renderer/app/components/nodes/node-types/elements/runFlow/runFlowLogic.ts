// Pure helpers for the RunFlow settings panel (unit-tested in node env —
// keep free of Vue / VueFlow imports).
import type { FlowParameter } from "../../../../../types/flow.types";
import type { RunFlowParameterBinding } from "../../../../../types/node.types";

export const MAX_RUN_FLOW_INPUTS = 9;
export const MAX_RUN_FLOW_OUTPUTS = 10;

export interface BindingRow {
  spec: FlowParameter;
  binding: RunFlowParameterBinding;
}

export function defaultBinding(parameterName: string): RunFlowParameterBinding {
  return {
    parameter_name: parameterName,
    source: "default",
    constant_value: null,
    column_name: null,
  };
}

// One row per parameter spec, reusing the existing binding object (same
// reference) when present so table edits mutate the saved bindings directly.
export function mergeBindingRows(
  specs: FlowParameter[],
  bindings: RunFlowParameterBinding[],
): BindingRow[] {
  return specs.map((spec) => ({
    spec,
    binding: bindings.find((b) => b.parameter_name === spec.name) ?? defaultBinding(spec.name),
  }));
}

// Rebuild bindings against a freshly fetched interface: surviving parameters
// keep their binding, removed ones are dropped, and new ones bind to a column
// on a case-insensitive name match (else the subflow default).
export function reconcileBindings(
  oldBindings: RunFlowParameterBinding[],
  parameters: FlowParameter[],
  availableColumns: string[],
): RunFlowParameterBinding[] {
  const byName = new Map(oldBindings.map((b) => [b.parameter_name, b]));
  const columnsByLower = new Map(availableColumns.map((c) => [c.toLowerCase(), c]));
  return parameters.map((param) => {
    const existing = byName.get(param.name);
    if (existing) return { ...existing };
    const columnMatch = columnsByLower.get(param.name.toLowerCase());
    if (columnMatch) {
      return {
        parameter_name: param.name,
        source: "column",
        constant_value: null,
        column_name: columnMatch,
      };
    }
    return defaultBinding(param.name);
  });
}

export interface EdgeLike {
  id: string;
  source: string;
  target: string;
  sourceHandle?: string | null;
  targetHandle?: string | null;
}

// Edges touching nodeId whose handle no longer exists after an interface
// refresh. Edges between other nodes are never returned.
export function findDanglingEdges<T extends EdgeLike>(
  edges: T[],
  nodeId: string,
  validInputIds: string[],
  validOutputIds: string[],
): T[] {
  const inputs = new Set(validInputIds);
  const outputs = new Set(validOutputIds);
  return edges.filter((edge) => {
    if (edge.target === nodeId) {
      return !inputs.has(edge.targetHandle ?? "input-0");
    }
    if (edge.source === nodeId) {
      return !outputs.has(edge.sourceHandle ?? "output-0");
    }
    return false;
  });
}
