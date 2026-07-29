import type { VisualizationUpdatePayload, VizSourceDescriptor } from "../../types";
import type { SaveReason } from "../../composables/autosaveEngine";

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

/** Assemble the autosave PUT body; the thumbnail rides along only on flush saves. */
export function buildVizSaveBody(
  base: { name: string; spec: Record<string, any>[] },
  opts: { reason: SaveReason; thumbnail: string | null; expectedUpdatedAt: string | null },
): VisualizationUpdatePayload {
  const body: VisualizationUpdatePayload = { name: base.name, spec: base.spec };
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
