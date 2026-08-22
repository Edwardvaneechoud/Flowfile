// Handle derivation shared by drop / copy / import paths. Pure (no VueFlow
// instance access) so it stays unit-testable in the node environment.
import { Position } from "@vue-flow/core";
import type { NodeHandle } from "../types/flow.types";
import { handleLetters, outputHandle } from "./outputHandle";

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
  const letters = handleLetters(count, names);
  return Array.from({ length: count }, (_, i) => ({
    id: outputHandle(i),
    position: Position.Right,
    label: letters[i],
    title: count > 1 ? names?.[i] : undefined,
  }));
}

// Mirror of buildOutputHandles for the input side: more than one handle gets a
// compact letter on the canvas and the declared name as a tooltip. Indices in
// controlIndices render as bottom-center "parameter" pips (gate's control
// input) instead of left data handles; letters derive from the remaining side
// inputs only, so a lone data handle carries no letter chip.
export function buildInputHandles(
  count: number,
  names?: string[],
  controlIndices?: number[],
): NodeHandle[] {
  const control = new Set(controlIndices ?? []);
  const sideIndices = Array.from({ length: count }, (_, i) => i).filter((i) => !control.has(i));
  const sideLetters = handleLetters(
    sideIndices.length,
    names ? sideIndices.map((i) => names[i]) : undefined,
  );
  let sidePosition = 0;
  return Array.from({ length: count }, (_, i) => {
    if (control.has(i)) {
      return {
        id: inputHandle(i),
        position: Position.Bottom,
        title: names?.[i],
        kind: "parameter" as const,
      };
    }
    return {
      id: inputHandle(i),
      position: Position.Left,
      label: sideLetters[sidePosition++],
      title: names?.[i],
    };
  });
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
  // Static per-type handle names. Distinct from input_names, which is the
  // per-instance dynamic-handle list and selects the branch below.
  input_labels?: string[] | null;
  // Cosmetic per-type marker: input indices rendered as bottom "parameter"
  // pips (gate's control input).
  control_input_indices?: number[] | null;
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
    inputs: buildInputHandles(
      node.multi ? 1 : node.input,
      node.input_labels ?? undefined,
      node.control_input_indices ?? undefined,
    ),
    outputs: buildOutputHandles(node.output, node.output_names ?? undefined),
  };
}
