// Whether a canvas edge is one the flow's last run placed between two compute backends.

import type { RunInformation } from "../types";

/** True when the run's `boundaries` list the edge from `source` to `target`, in that direction. */
export function crossesBackends(
  run: RunInformation | null | undefined,
  source: string | number,
  target: string | number,
): boolean {
  const from = Number(source);
  const to = Number(target);
  return (run?.boundaries ?? []).some(([s, t]) => s === from && t === to);
}

/** "Node 2: filter" from the run's result for the node, "Node 2" when the run reported none. */
export function runNodeLabel(
  run: RunInformation | null | undefined,
  nodeId: string | number,
): string {
  const id = Number(nodeId);
  const name = run?.node_step_result?.find((r) => r.node_id === id)?.node_name;
  return name ? `Node ${id}: ${name}` : `Node ${id}`;
}
