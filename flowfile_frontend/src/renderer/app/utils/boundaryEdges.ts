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
