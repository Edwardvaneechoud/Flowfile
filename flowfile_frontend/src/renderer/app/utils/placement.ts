// Wording for nodes their backend cannot run, from the placement preview and a run's rows.

import type { NodeResult, PlacementFallback, PlacementPreview, PlacementRefusal } from "../types";
import { backendInSentence, backendName } from "./executionLocation";

export interface NodePlacementNote {
  kind: "fallback" | "refused";
  // The canvas badge's text.
  label: string;
  // The badge's tooltip: where the node runs and why, in one sentence.
  text: string;
  // The drawer's note: the tooltip, then what the backend lacks.
  detail: string;
}

export interface FallbackNotice {
  title: string;
  lines: { nodeId: number; label: string; reason: string }[];
}

/** Where a node runs instead and why; `ownBackend` when the one that cannot is the node's own pick. */
export function fallbackSentence(
  fallback: Pick<PlacementFallback, "backend_id" | "fallback_id">,
  ownBackend = false,
): string {
  const from =
    ownBackend && fallback.backend_id !== "worker"
      ? `its Compute backend "${fallback.backend_id}"`
      : backendInSentence(fallback.backend_id);
  return `Runs on ${backendInSentence(fallback.fallback_id)}, because ${from} doesn't support this node.`;
}

function capitalize(text: string): string {
  return text.charAt(0).toUpperCase() + text.slice(1);
}

/** The refusal as core words it on the node's row. */
export function refusalSentence(refusal: PlacementRefusal): string {
  const sentence = `Cannot run on ${backendInSentence(refusal.backend_id)}: ${refusal.reason}`;
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
  if (refusal) {
    const text = refusalSentence(refusal);
    return { kind: "refused", label: "can't run", text, detail: text };
  }
  const fallback = preview.fallbacks.find((item) => item.node_id === nodeId);
  if (!fallback) return null;
  const text = fallbackSentence(fallback, ownBackend);
  return {
    kind: "fallback",
    label: `→ ${backendName(fallback.fallback_id)}`,
    text,
    detail: `${text}\n${capitalize(fallback.reason)}.`,
  };
}

/** The run card's notice for the rows placement moved to the default backend; null when none moved.
 *
 * When every moved node went the same way the title names both backends and each line only the node and why.
 */
export function fallbackNotice(rows: NodeResult[] | undefined): FallbackNotice | null {
  const moved = (rows ?? []).filter(
    (row) => row.fallback_from && row.backend_id && row.fallback_reason,
  );
  if (!moved.length) return null;
  const count = `${moved.length} ${moved.length === 1 ? "node" : "nodes"}`;
  const them = moved.length === 1 ? "it" : "them";
  const routes = new Set(moved.map((row) => `${row.fallback_from}\u0000${row.backend_id}`));
  const [first] = moved;
  const sameRoute = routes.size === 1;
  return {
    title: sameRoute
      ? `${count} ran on ${backendInSentence(first.backend_id as string)} because ` +
        `${backendInSentence(first.fallback_from as string)} doesn't support ${them}`
      : `${count} ran on the default worker because their backend doesn't support ${them}`,
    lines: moved.map((row) => ({
      nodeId: row.node_id,
      label: `${row.node_name || "node"} #${row.node_id}`,
      reason: sameRoute
        ? (row.fallback_reason as string)
        : `${backendName(row.fallback_from as string)} → ${backendName(row.backend_id as string)}: ` +
          `${row.fallback_reason}`,
    })),
  };
}
