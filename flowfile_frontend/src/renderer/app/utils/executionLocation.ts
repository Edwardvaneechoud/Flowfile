// The flow's execution location as the Flow Settings dialog offers it and the run notification names it.
// Core answers "local" or "remote" only with the compute protocol off, so such a flow keeps that pair.
// A node's own location, as its drawer offers it, is "auto" (the flow's backend) or a backend id.
// A configured backend is offered with its last handshake verdict beside its id.

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

/** What the last handshake said of a backend, in a few words: what the picker shows beside its id. */
export function verdictSummary(verdict: HandshakeVerdict | null): string {
  if (!verdict) return "not checked";
  if (!verdict.ok) return `refused: ${verdict.error ?? verdict.differences[0]}`;
  const without = Object.keys(verdict.absent).map(absentLabel);
  return without.length ? `ok, without ${without.join(", ")}` : "ok";
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

/** "auto" and the configured backends, then the flow's own value when it is neither; never "local". */
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
