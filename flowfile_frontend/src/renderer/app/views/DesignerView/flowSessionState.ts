// Pure flow-session resolution logic — no Vue/axios imports so it stays unit-testable.

/** Flow id to activate at boot: persisted id if still open, else first open flow, else -1. */
export function resolveBootFlowId(openFlowIds: number[], persistedFlowId: number): number {
  if (openFlowIds.length === 0) return -1;
  if (persistedFlowId > 0 && openFlowIds.includes(persistedFlowId)) return persistedFlowId;
  return openFlowIds[0];
}

/**
 * Flow id to activate after closing tabs: null when the current flow survives
 * (no switch needed), otherwise the first remaining id, or -1 when none remain.
 */
export function resolveNextFlowAfterClose(
  remainingFlowIds: number[],
  closedFlowIds: number[],
  currentFlowId: number,
): number | null {
  if (!closedFlowIds.includes(currentFlowId)) return null;
  return remainingFlowIds.length > 0 ? remainingFlowIds[0] : -1;
}
