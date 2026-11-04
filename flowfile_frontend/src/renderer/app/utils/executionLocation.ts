// Execution locations as the Flow Settings dialog and node drawer offer them, with backend verdicts.

import type { ComputeBackend, HandshakeVerdict } from "../api/computeBackends.api";
import type { ExecutionLocation } from "../types";

export type BackendRow = Pick<ComputeBackend, "id" | "verdict" | "source">;

const UNREACHABLE = "ComputeUnavailable";

export interface ExecutionLocationOption {
  key: ExecutionLocation;
  label: string;
}

const LEGACY_LOCATIONS: ExecutionLocation[] = ["local", "remote"];

export function executionLocationLabel(location: ExecutionLocation): string {
  if (location === "local") return "Local";
  if (location === "remote") return "Remote";
  if (location === "auto") return "Auto (default worker)";
  return location;
}

/** A name a verdict's `absent` holds, in words: a plugin as it is, "python" as core's Python functions. */
export function absentLabel(name: string): string {
  return name === "python" ? "Python functions" : name;
}

/** A bounded picker label; capability names and diagnostics belong in the selected backend's details. */
export function verdictSummary(verdict: HandshakeVerdict | null): string {
  if (!verdict) return "Not checked";
  if (!verdict.ok || !verdict.backend) {
    return verdict.backend ? "Not accepted" : "Handshake failed";
  }
  const unavailable = Object.keys(verdict.absent).length;
  if (unavailable) return `Limited · ${unavailable} unavailable`;
  return verdict.differences.length ? "Accepted with differences" : "Compatible";
}

/**
 * Why a hub node could not be reached from this server, as its verdict says it, for the line under a picker; "" for
 * any other row or verdict. A non-administrator's verdict says "unreachable", the address kept from them.
 */
export function unreachableNote(row: BackendRow | undefined): string {
  if (row?.source !== "hub" || row.verdict?.error_kind !== UNREACHABLE) return "";
  return row.verdict.error ?? "";
}

function withVerdicts(
  keys: ExecutionLocation[],
  rows: BackendRow[],
  label: (key: ExecutionLocation) => string,
): ExecutionLocationOption[] {
  const byId = new Map(rows.map((row) => [row.id, row]));
  return keys.map((key) => {
    const row = byId.get(key);
    return { key, label: row ? `${label(key)} · ${verdictSummary(row.verdict)}` : label(key) };
  });
}

/**
 * "auto" and the configured backends, then the flow's own value when neither; never "local". A flow core answers
 * as "local" or "remote" (the compute protocol off) gets that pair.
 */
export function executionLocationOptions(
  current: ExecutionLocation,
  rows: BackendRow[],
): ExecutionLocationOption[] {
  if (LEGACY_LOCATIONS.includes(current)) {
    return LEGACY_LOCATIONS.map((key) => ({ key, label: executionLocationLabel(key) }));
  }
  const keys = ["auto", ...rows.map((row) => row.id)];
  if (!keys.includes(current)) keys.push(current);
  return withVerdicts(keys, rows, executionLocationLabel);
}

export function nodeExecutionLocationLabel(location: ExecutionLocation): string {
  if (location === "auto") return "Auto (the flow's backend)";
  if (location === "worker") return "Default worker";
  if (location === "local") return "Local";
  return location;
}

/** A backend id as a badge names it: the built-in worker as "default worker", any other as it is. */
export function backendName(id: string): string {
  return id === "worker" ? "default worker" : id;
}

/** A backend id inside a sentence: "the default worker", or `backend "<id>"` for any other. */
export function backendInSentence(id: string): string {
  return id === "worker" ? "the default worker" : `backend "${id}"`;
}

/** "auto", "worker" and the configured backends, then the node's own value ("local" only as that). */
export function nodeExecutionLocationOptions(
  current: ExecutionLocation,
  rows: BackendRow[],
): ExecutionLocationOption[] {
  const keys = ["auto", "worker", ...rows.map((row) => row.id)];
  if (!keys.includes(current)) keys.push(current);
  return withVerdicts(keys, rows, nodeExecutionLocationLabel);
}
