// Where a node runs when its backend cannot run it: core's placement preview (GET /flow/placement) and a run's rows.

import type { NodeResult, PlacementFallback, PlacementPreview, PlacementRefusal } from "../types";

export interface NodePlacementNote {
  kind: "fallback" | "refused";
  // The canvas badge's text.
  label: string;
  // The sentence the badge's tooltip and the drawer show.
  text: string;
}

export interface FallbackNotice {
  title: string;
  lines: { nodeId: number; text: string }[];
}

/** Why a node runs on the default backend; `ownBackend` when its own Compute backend names the one that cannot. */
export function fallbackSentence(
  fallback: Pick<PlacementFallback, "backend_id" | "fallback_id" | "reason">,
  ownBackend = false,
): string {
  const subject = ownBackend ? "Its Compute backend" : "Backend";
  return (
    `Runs on the default backend "${fallback.fallback_id}": ${subject} "${fallback.backend_id}" ` +
    `cannot run it (${fallback.reason}).`
  );
}

/** The refusal as core words it on the node's row. */
export function refusalSentence(refusal: PlacementRefusal): string {
  const sentence = `Cannot run on backend "${refusal.backend_id}": ${refusal.reason}`;
  return refusal.remedy ? `${sentence}. ${refusal.remedy}` : `${sentence}.`;
}

/** What the preview says about `nodeId`: refused, moved to the default backend, or nothing (null). */
export function nodePlacementNote(
  preview: PlacementPreview | undefined,
  nodeId: number,
  ownBackend = false,
): NodePlacementNote | null {
  if (!preview?.available) return null;
  const refusal = preview.refusals.find((item) => item.node_id === nodeId);
  if (refusal) return { kind: "refused", label: "can't run", text: refusalSentence(refusal) };
  const fallback = preview.fallbacks.find((item) => item.node_id === nodeId);
  if (!fallback) return null;
  const crosses = preview.boundaries.some(
    ([source, target]) => source === nodeId || target === nodeId,
  );
  const text = fallbackSentence(fallback, ownBackend);
  return {
    kind: "fallback",
    label: `→ ${fallback.fallback_id}`,
    text: crosses
      ? `${text} Its data crosses to the other nodes through the exchange store.`
      : text,
  };
}

/** The run card's notice for the rows placement ran on the default backend; null when none moved. */
export function fallbackNotice(rows: NodeResult[] | undefined): FallbackNotice | null {
  const moved = (rows ?? []).filter(
    (row) => row.fallback_from && row.backend_id && row.fallback_reason,
  );
  if (!moved.length) return null;
  return {
    title: `${moved.length} ${moved.length === 1 ? "node" : "nodes"} ran on the default backend`,
    lines: moved.map((row) => ({
      nodeId: row.node_id,
      text:
        `${row.node_name || `Node ${row.node_id}`} (#${row.node_id}) ran on "${row.backend_id}": ` +
        `"${row.fallback_from}" cannot run it (${row.fallback_reason}).`,
    })),
  };
}
