// Pure request-layer helpers for the column-stats popover: cache keys and the
// axios-error → verdict mapping. No Vue, no axios import — unit-tested in node
// env (statsRequest.test.ts). The panel's presentation helpers live next door in
// columnQuality.ts.
import type { FileColumn } from "../../../types/node.types";

/**
 * What we know about one column's stats. A 409 is a stable verdict about node
 * state (not run, Performance mode, external source), so it caches alongside a
 * success; a transient failure does not — the panel offers Retry, and caching
 * that would make the button a permanent no-op.
 */
export type StatsVerdict =
  | { kind: "ok"; stats: FileColumn }
  | { kind: "not-run"; detail: string | null };

export interface StatsFailure {
  kind: "not-run" | "error";
  detail: string | null;
}

/** Stats are per node, per output handle, per column. */
export function statsCacheKey(nodeId: number, outputHandle: string, columnName: string): string {
  return `${nodeId}::${outputHandle}::${columnName}`;
}

/** 409 carries the reason (not run yet / Performance mode) — shown verbatim. */
export function classifyStatsError(error: unknown): StatsFailure {
  const response = (error as { response?: { status?: number; data?: { detail?: string } } })
    ?.response;
  if (response?.status !== 409) return { kind: "error", detail: null };
  const detail = response.data?.detail;
  return { kind: "not-run", detail: typeof detail === "string" ? detail : null };
}
