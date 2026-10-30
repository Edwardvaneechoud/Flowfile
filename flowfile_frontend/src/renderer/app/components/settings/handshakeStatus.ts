import type { HandshakeVerdict } from "../../api/computeBackends.api";

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
