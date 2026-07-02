// Handle derivation shared by drop / copy / import paths. Pure (no VueFlow
// instance access) so it stays unit-testable in the node environment.
import { Position } from "@vue-flow/core";
import type { NodeHandle } from "../types/flow.types";
import { outputHandle, outputLabel } from "./outputHandle";

// Fixed parameter-data handle on dynamic-input nodes (run_flow).
export const PARAM_INPUT_HANDLE = "input-0";

export function inputHandle(index: number): string {
  return `input-${index}`;
}

// Build the outputs array for a node. When a node declares more than one
// output, each handle gets a compact letter id (A, B, …) for the canvas and the
// user-defined name (when available) as a hover tooltip via the `title` attr.
// For nodes whose output count is user-configurable (e.g. random_split), the
// effective count is whichever is larger: the template's static count or the
// number of saved output names.
export function buildOutputHandles(outputCount: number, names?: string[]): NodeHandle[] {
  const count = Math.max(outputCount, names?.length ?? 0);
  const multi = count > 1;
  return Array.from({ length: count }, (_, i) => ({
    id: outputHandle(i),
    position: Position.Right,
    label: multi ? outputLabel(i) : undefined,
    title: multi ? names?.[i] : undefined,
  }));
}

export function buildInputHandles(count: number, names?: string[]): NodeHandle[] {
  return Array.from({ length: count }, (_, i) => ({
    id: inputHandle(i),
    position: Position.Left,
    label: names?.[i],
    title: names?.[i],
  }));
}

// Dynamic-input nodes: index 0 is the fixed parameter handle. An empty label
// at index 0 means the subflow has no parameters -> the handle is omitted
// (data handles keep their input-1..N ids). The parameter handle renders at
// the bottom of the node.
export function buildDynamicInputHandles(inputNames: string[]): NodeHandle[] {
  const handles: NodeHandle[] = [];
  inputNames.forEach((name, i) => {
    if (i === 0) {
      if (!name) return;
      handles.push({
        id: inputHandle(0),
        position: Position.Bottom,
        title: "Parameter data (optional)",
        kind: "parameter",
      });
      return;
    }
    handles.push({
      id: inputHandle(i),
      position: Position.Left,
      label: name,
      title: name,
      kind: "data",
    });
  });
  return handles;
}

// Dynamic outputs carry the full subflow output name as their label.
export function buildDynamicOutputHandles(outputNames: string[]): NodeHandle[] {
  return outputNames.map((name, i) => ({
    id: outputHandle(i),
    position: Position.Right,
    label: name,
    title: name,
  }));
}

export interface HandleSource {
  input: number;
  output: number;
  multi?: boolean;
  dynamic_inputs?: boolean;
  input_names?: string[] | null;
  output_names?: string[] | null;
}

export interface DerivedHandles {
  inputs: NodeHandle[];
  outputs: NodeHandle[];
}

// Single source of truth for a node's canvas handles. Dynamic nodes derive
// them from per-instance input_names/output_names (a fresh run_flow shows no
// handles until a flow is picked); static nodes keep today's behavior.
export function deriveHandles(node: HandleSource): DerivedHandles {
  if (node.dynamic_inputs || node.input_names != null) {
    return {
      inputs: buildDynamicInputHandles(node.input_names ?? [""]),
      outputs: buildDynamicOutputHandles(node.output_names ?? []),
    };
  }
  return {
    inputs: buildInputHandles(node.multi ? 1 : node.input),
    outputs: buildOutputHandles(node.output, node.output_names ?? undefined),
  };
}
