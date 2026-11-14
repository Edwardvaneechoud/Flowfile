import type { Connection } from "@vue-flow/core";
import type { NodeHandle, NodeTemplate } from "../types/flow.types";
import { DEFAULT_OUTPUT_HANDLE } from "./outputHandle";

type PortNode = {
  inputs?: NodeHandle[];
  outputs?: NodeHandle[];
  nodeTemplate?: Pick<NodeTemplate, "name" | "multi" | "dynamic_inputs" | "inputs">;
};

// Validate the live instance handles, not the palette's counts. Omitted arrays
// are supported for legacy snapshots; an explicit empty array means no ports.
export function connectionPortError(
  connection: Connection,
  source: PortNode | undefined,
  target: PortNode | undefined,
  edges: readonly Connection[],
): string | null {
  if (!source || !target) return "Connection node is unavailable";
  const sourceHandle = connection.sourceHandle ?? DEFAULT_OUTPUT_HANDLE;
  const targetHandle = connection.targetHandle ?? "input-0";
  if (source.outputs && !source.outputs.some((port) => port.id === sourceHandle)) {
    return "Output is not available on this node";
  }
  // The reserved dynamic parameter port is legal even when its canvas pip is hidden.
  const input =
    target.inputs?.find((port) => port.id === targetHandle) ??
    (target.nodeTemplate?.dynamic_inputs
      ? target.nodeTemplate.inputs?.find((port) => port.id === targetHandle && port.hidden)
      : undefined);
  if (target.inputs && !input) return "Input is not available on this node";
  if (
    !target.nodeTemplate?.dynamic_inputs &&
    edges.some(
      (edge) =>
        edge.source === connection.source &&
        edge.target === connection.target &&
        (edge.sourceHandle ?? DEFAULT_OUTPUT_HANDLE) !== sourceHandle,
    )
  ) {
    return "Target already reads a different output of this source";
  }
  const multiple =
    input?.multiple ?? Boolean(target.nodeTemplate?.multi && !target.nodeTemplate?.dynamic_inputs);
  if (
    !multiple &&
    edges.some(
      (edge) =>
        edge.target === connection.target &&
        (edge.targetHandle ?? "input-0") === targetHandle &&
        !(
          edge.source === connection.source &&
          (edge.sourceHandle ?? DEFAULT_OUTPUT_HANDLE) === sourceHandle
        ),
    )
  ) {
    return `Input on "${target.nodeTemplate?.name ?? connection.target}" already has a connection — remove it first`;
  }
  return null;
}
