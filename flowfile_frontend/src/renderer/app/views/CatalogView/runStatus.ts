// How a run record and its node rows read: one place, so every view agrees on what a cancel is.

export type RunOutcome = "running" | "success" | "cancelled" | "failed";

/** A cancelled run keeps `success` false, so the mark decides between cancelled and failed. */
export function runOutcome(success: boolean | null, cancelled?: boolean | null): RunOutcome {
  if (success === null) return "running";
  if (success) return "success";
  return cancelled ? "cancelled" : "failed";
}

export const RUN_OUTCOME_LABEL: Record<RunOutcome, string> = {
  running: "Running",
  success: "Success",
  cancelled: "Cancelled",
  failed: "Failed",
};

// Classes of `.status-badge`, the status text and the status dots.
export const RUN_OUTCOME_BADGE: Record<RunOutcome, string> = {
  running: "running",
  success: "success",
  cancelled: "cancelled",
  failed: "failure",
};

export const RUN_OUTCOME_TEXT: Record<RunOutcome, string> = {
  running: "text-pending",
  success: "text-success",
  cancelled: "text-cancelled",
  failed: "text-danger",
};

export const RUN_OUTCOME_ICON: Record<RunOutcome, string> = {
  running: "fa-solid fa-spinner fa-spin",
  success: "fa-solid fa-circle-check",
  cancelled: "fa-solid fa-ban",
  failed: "fa-solid fa-circle-xmark",
};

export type NodeOutcome = "skipped" | "ok" | "failed" | "cancelled" | "running";

interface NodeRow {
  success: boolean | null;
  skipped?: boolean;
}

/**
 * A node row with no outcome belongs to a node a cancel stopped once its run has ended: the engine
 * leaves `success` null only then. Nodes that never started have no row at all.
 */
export function nodeOutcome(node: NodeRow, runEnded: boolean): NodeOutcome {
  if (node.skipped) return "skipped";
  if (node.success) return "ok";
  if (node.success === false) return "failed";
  return runEnded ? "cancelled" : "running";
}

export const NODE_OUTCOME_LABEL: Record<NodeOutcome, string> = {
  skipped: "Skipped",
  ok: "OK",
  failed: "Failed",
  cancelled: "Cancelled",
  running: "Running",
};

export const NODE_OUTCOME_BADGE: Record<NodeOutcome, string> = {
  skipped: "skipped",
  ok: "success",
  failed: "failure",
  cancelled: "cancelled",
  running: "pending",
};
