// The run card's banner for nodes that failed on their compute backend rather than on their own settings or data.

import type { NodeResult } from "../types";

// The run report's compute_error values the banner shows; any other compute error stays on its node's row.
const BANNER_TITLES: Record<string, string> = {
  Refusal: "This flow cannot run on its compute backend",
  ComputeUnavailable: "The compute backend could not be reached",
  VersionMismatch: "The compute backend does not match this server",
};

export interface ComputeErrorBanner {
  title: string;
  messages: string[];
}

/** The title by the first such row's kind and each distinct message once, in row order; null when no row has one. */
export function computeErrorBanner(rows: NodeResult[] | undefined): ComputeErrorBanner | null {
  const failed = (rows ?? []).filter(
    (row) => row.success === false && !!row.compute_error && row.compute_error in BANNER_TITLES,
  );
  if (!failed.length) return null;
  return {
    title: BANNER_TITLES[failed[0].compute_error as string],
    messages: [...new Set(failed.map((row) => row.error))],
  };
}
