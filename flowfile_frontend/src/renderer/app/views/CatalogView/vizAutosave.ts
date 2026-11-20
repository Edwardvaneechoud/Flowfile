import type { VisualizationUpdatePayload, VizSourceDescriptor } from "../../types";
import type { IChart } from "@kanaries/graphic-walker/interfaces";
import type { SaveReason } from "../../composables/autosaveEngine";
import { drawnMark } from "./chartRequest";

/** djb2 — stable short hash so sql-source draft keys stay bounded. */
export function hashString(input: string): string {
  let hash = 5381;
  for (let i = 0; i < input.length; i++) {
    hash = ((hash << 5) + hash + input.charCodeAt(i)) >>> 0;
  }
  return hash.toString(36);
}

/** The rasterize+downscale pipeline is too heavy per tick; capture on flush only. */
export function shouldCaptureThumbnail(reason: SaveReason): boolean {
  return reason === "flush";
}

/**
 * The mark a saved chart draws (its first tab, the one the builder shows), stored as the
 * catalog's chart type; null when it names none, such as an empty chart.
 */
export function chartTypeOf(spec: Record<string, any>[] | null | undefined): string | null {
  const first = spec?.[0];
  if (!first || !Array.isArray(first.config?.geoms) || !first.encodings || !first.layout) {
    return null;
  }
  try {
    return drawnMark(first as unknown as IChart);
  } catch {
    return null;
  }
}

/** Assemble the autosave PUT body; the thumbnail rides along only on flush saves. */
export function buildVizSaveBody(
  base: { name: string; spec: Record<string, any>[] },
  opts: { reason: SaveReason; thumbnail: string | null; expectedUpdatedAt: string | null },
): VisualizationUpdatePayload {
  const body: VisualizationUpdatePayload = {
    name: base.name,
    spec: base.spec,
    chart_type: chartTypeOf(base.spec),
  };
  if (shouldCaptureThumbnail(opts.reason) && opts.thumbnail) {
    body.thumbnail_data_url = opts.thumbnail;
  }
  if (opts.expectedUpdatedAt) body.expected_updated_at = opts.expectedUpdatedAt;
  return body;
}

/**
 * localStorage key for an unsaved chart draft, scoped per user (shared browser
 * profiles must not leak drafts across accounts) and per source.
 */
export function vizDraftKey(source: VizSourceDescriptor, userKey: string | number): string {
  const scope =
    source.source_type === "table"
      ? `table:${source.table_id ?? "unknown"}`
      : `sql:${hashString(source.sql_query ?? "")}`;
  return `catalog.vizDraft.${userKey}.${scope}`;
}
