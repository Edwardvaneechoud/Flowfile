// Pure presentation logic for the share-link report. No Vue/axios imports so it
// unit-tests as a plain module.
import type { ShareLinkNodeReport, ShareLinkResponse } from "../../api/shareLink.api";

export type ShareLinkSeverity = "success" | "warning" | "error";

export interface ShareLinkSummary {
  okCount: number;
  placeholderCount: number;
  total: number;
  headline: string;
  severity: ShareLinkSeverity;
}

/** Beyond this the URL gets long enough that chat apps start truncating it. */
export const LONG_LINK_THRESHOLD = 8000;

export const PRIVACY_NOTE = "The flow travels inside the link itself — your data files do not.";

export const LONG_LINK_HINT =
  "This link is long. Some chat apps truncate long URLs — send it as a file or an email link if the recipient reports a broken link.";

export const TOO_LARGE_HEADLINE = "This flow is too large to share as a link";

export const TOO_LARGE_MESSAGE =
  "The whole flow has to fit inside the link itself. Remove or simplify some nodes, or share the flow file instead.";

export const DEFAULT_PLACEHOLDER_REASON = "Not supported in the browser version.";

export function summarizeReport(response: ShareLinkResponse): ShareLinkSummary {
  const total = response.nodes_report.length;
  const placeholderCount = Math.max(0, response.placeholder_count);
  const okCount = Math.max(0, total - placeholderCount);

  if (response.url === null) {
    return { okCount, placeholderCount, total, headline: TOO_LARGE_HEADLINE, severity: "error" };
  }
  if (response.compatible) {
    return { okCount, placeholderCount, total, headline: "Ready to share", severity: "success" };
  }
  return {
    okCount,
    placeholderCount,
    total,
    headline: `${placeholderCount} of ${total} nodes won't run in the browser`,
    severity: "warning",
  };
}

export function placeholderRows(response: ShareLinkResponse): ShareLinkNodeReport[] {
  return response.nodes_report.filter((node) => node.status === "placeholder");
}

export function nodeReason(node: ShareLinkNodeReport): string {
  return node.reason?.trim() || DEFAULT_PLACEHOLDER_REASON;
}

export function needsLongLinkHint(hashChars: number): boolean {
  return hashChars > LONG_LINK_THRESHOLD;
}

export function localFileNote(nodeIds: number[]): string | null {
  if (nodeIds.length === 0) return null;
  const plural = nodeIds.length === 1 ? "node" : "nodes";
  return `Recipients will be asked to supply the file for ${nodeIds.length} read ${plural}.`;
}

/** Every note shown under the link, in display order. */
export function shareNotes(response: ShareLinkResponse): string[] {
  const notes: string[] = [];
  const files = localFileNote(response.local_file_nodes);
  if (files) notes.push(files);
  notes.push(PRIVACY_NOTE);
  if (needsLongLinkHint(response.hash_chars)) notes.push(LONG_LINK_HINT);
  notes.push(...response.warnings);
  return notes;
}
