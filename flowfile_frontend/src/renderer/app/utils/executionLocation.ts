// Execution locations as the Flow Settings dialog and the node drawer offer them, verdicts beside backend ids.

import type { ComputeBackend, HandshakeVerdict } from "../api/computeBackends.api";
import type { ExecutionLocation } from "../types";

export type BackendRow = Pick<ComputeBackend, "id" | "verdict">;

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

function withVerdicts(
  keys: ExecutionLocation[],
  rows: BackendRow[],
  label: (key: ExecutionLocation) => string,
): ExecutionLocationOption[] {
  const verdicts = new Map(rows.map((row) => [row.id, row.verdict]));
  return keys.map((key) => ({
    key,
    label: verdicts.has(key)
      ? `${label(key)} · ${verdictSummary(verdicts.get(key) ?? null)}`
      : label(key),
  }));
}

/**
 * "auto" and the configured backends, then the flow's own value when it is neither; never "local". Core answers
 * "local" or "remote" only with the compute protocol off, so such a flow keeps that pair.
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

/** "auto", "worker" and the configured backends, then the node's own value when it is none of them; never "local" otherwise. */
export function nodeExecutionLocationOptions(
  current: ExecutionLocation,
  rows: BackendRow[],
): ExecutionLocationOption[] {
  const keys = ["auto", "worker", ...rows.map((row) => row.id)];
  if (!keys.includes(current)) keys.push(current);
  return withVerdicts(keys, rows, nodeExecutionLocationLabel);
}
