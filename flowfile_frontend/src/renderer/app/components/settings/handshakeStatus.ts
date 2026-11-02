import type { ComputeBackend, HandshakeVerdict } from "../../api/computeBackends.api";

export type StatusTone = "neutral" | "success" | "warning" | "error";

/** A one-line status for a folded card or row header. */
export interface StatusSummary {
  tone: StatusTone;
  label: string;
}

export interface HandshakeStatus extends StatusSummary {
  icon: string;
}

/** A verdict's status pill: not checked, refused, accepted with limitations or differences, or a match. */
export function handshakeStatus(verdict: HandshakeVerdict | null | undefined): HandshakeStatus {
  if (!verdict) {
    return { tone: "neutral", icon: "fa-regular fa-circle", label: "Not checked" };
  }
  if (!verdict.ok || !verdict.backend) {
    return {
      tone: "error",
      icon: "fa-solid fa-circle-xmark",
      label: verdict.backend ? "Not accepted by this server" : "Handshake failed",
    };
  }
  const unavailable = Object.keys(verdict.absent ?? {}).length > 0;
  if (unavailable || verdict.differences.length) {
    return {
      tone: "warning",
      icon: "fa-solid fa-triangle-exclamation",
      label: unavailable ? "Accepted with limitations" : "Accepted with differences",
    };
  }
  return { tone: "success", icon: "fa-solid fa-circle-check", label: "Matches this server" };
}

/**
 * A hub node's reachability chip: whether this computer reached its worker on the last check, "Not checked" until
 * one answered. A row added here is never checked, so it has none (null).
 */
export function reachabilityStatus(
  backend: Pick<ComputeBackend, "source" | "reachable">,
): StatusSummary | null {
  if (backend.source !== "hub") return null;
  if (backend.reachable === true) return { tone: "success", label: "Reachable" };
  if (backend.reachable === false) return { tone: "error", label: "Unreachable" };
  return { tone: "neutral", label: "Not checked" };
}
