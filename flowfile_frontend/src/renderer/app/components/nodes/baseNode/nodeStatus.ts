// Canvas status-dot derivation for a node, kept pure so it is unit-testable
// without mounting nodeButton.vue.
import type { NodeResult, NodeValidation } from "../../../types/node.types";

export type NodeStatusIndicator =
  | "success"
  | "failure"
  | "unknown"
  | "warning"
  | "running"
  | "skipped";

export interface NodeStatusOutput {
  success?: boolean;
  statusIndicator: NodeStatusIndicator;
  error?: string;
  hasRun: boolean;
}

/**
 * Derive the canvas status dot from a node's run result and its validation.
 *
 * A deliberately skipped node (a branch behind a closed gate) reports
 * `skipped` with `success === true`, so it is checked before anything that
 * would paint it green. `success` is tri-state — null/undefined means
 * "no verdict yet" (cancelled, or a result written before the node finished)
 * and must never paint red.
 */
export function deriveNodeStatus(
  nodeResult: NodeResult | undefined,
  nodeValidation: NodeValidation | undefined,
): NodeStatusOutput | undefined {
  if (nodeResult && nodeResult.is_running) {
    return {
      success: undefined,
      statusIndicator: "running",
      hasRun: false,
      error: undefined,
    };
  }

  if (nodeResult && !nodeResult.is_running) {
    if (nodeResult.skipped) {
      return {
        success: nodeResult.success,
        statusIndicator: "skipped",
        error: nodeResult.error,
        hasRun: false,
      };
    }
    if (nodeValidation) {
      if (
        nodeResult.success === true &&
        !nodeValidation.isValid &&
        nodeValidation.validationTime > nodeResult.start_timestamp
      ) {
        return {
          success: true,
          statusIndicator: "warning",
          error: nodeValidation.error,
          hasRun: true,
        };
      }
      if (nodeResult.success === true && nodeValidation.isValid) {
        return {
          success: true,
          statusIndicator: "success",
          error: nodeResult.error || nodeValidation.error,
          hasRun: true,
        };
      }
      if (
        nodeResult.success === false &&
        nodeValidation.isValid &&
        nodeValidation.validationTime > nodeResult.start_timestamp
      ) {
        return {
          success: false,
          statusIndicator: "unknown",
          error: nodeResult.error || nodeValidation.error,
          hasRun: true,
        };
      }
      if (
        nodeResult.success === false &&
        (!nodeValidation.isValid || !nodeValidation.validationTime)
      ) {
        return {
          success: false,
          statusIndicator: "failure",
          error: nodeResult.error || nodeValidation.error,
          hasRun: true,
        };
      }
    }
    return {
      success: nodeResult.success,
      statusIndicator:
        nodeResult.success === true
          ? "success"
          : nodeResult.success === false
            ? "failure"
            : "unknown",
      error: nodeResult.error,
      hasRun: true,
    };
  }

  if (nodeValidation) {
    if (!nodeValidation.isValid) {
      return {
        success: false,
        statusIndicator: "warning",
        error: nodeValidation.error,
        hasRun: false,
      };
    }
    if (nodeValidation.isValid) {
      return {
        success: true,
        statusIndicator: "unknown",
        error: nodeValidation.error,
        hasRun: false,
      };
    }
  }

  return undefined;
}

export function nodeStatusTooltip(status: NodeStatusOutput | undefined): string {
  switch (status?.statusIndicator) {
    case "success":
      return "Operation successful";
    case "failure":
      return "Operation failed: \n" + (status?.error || "No error message available");
    case "warning":
      return "Operation warning: \n" + (status?.error || "No warning message available");
    case "running":
      return "Operation in progress...";
    case "skipped":
      return "Skipped (gated off) — condition not met";
    case "unknown":
    default:
      return "Status unknown";
  }
}
