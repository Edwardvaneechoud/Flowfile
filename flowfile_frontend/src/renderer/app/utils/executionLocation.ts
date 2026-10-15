// The flow's execution location as the Flow Settings dialog offers it and the run notification names it.
// Core answers "local" or "remote" only with the compute protocol off, so such a flow keeps that pair.
// A node's own location, as its drawer offers it, is "auto" (the flow's backend) or a backend id.

import type { ExecutionLocation } from "../types";

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

/** "auto" and the configured backends' ids, then the flow's own value when it is neither; never "local". */
export function executionLocationOptions(
  current: ExecutionLocation,
  backendIds: string[],
): ExecutionLocationOption[] {
  const keys = LEGACY_LOCATIONS.includes(current) ? [...LEGACY_LOCATIONS] : ["auto", ...backendIds];
  if (!keys.includes(current)) keys.push(current);
  return keys.map((key) => ({ key, label: executionLocationLabel(key) }));
}

export function nodeExecutionLocationLabel(location: ExecutionLocation): string {
  if (location === "auto") return "Auto (the flow's backend)";
  if (location === "worker") return "Default worker";
  if (location === "local") return "Local";
  return location;
}

/** "auto", "worker" and the configured backends' ids, then the node's own value when it is none of them; never "local" otherwise. */
export function nodeExecutionLocationOptions(
  current: ExecutionLocation,
  backendIds: string[],
): ExecutionLocationOption[] {
  const keys = ["auto", "worker", ...backendIds];
  if (!keys.includes(current)) keys.push(current);
  return keys.map((key) => ({ key, label: nodeExecutionLocationLabel(key) }));
}
