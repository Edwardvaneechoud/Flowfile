// No Vue/axios imports here, so these helpers unit-test as a plain module.
import type {
  AlteryxConversionReport,
  AlteryxToolRow,
  AlteryxToolStatus,
} from "../../../api/alteryx.api";
import { NEW_ISSUE_URL } from "../../../lib/docsLinks";

export interface StatusChip {
  label: string;
  className: string;
}

export interface NodeRequestLink {
  label: string;
  url: string;
  existing: boolean;
}

export const NEW_NODE_REQUEST_URL = NEW_ISSUE_URL;
export const NODE_REQUEST_TEMPLATE = "alteryx_node_request.yml";

// Modifiers of the shared .status-badge system (styles/components/_status-badges.css).
// A tool Flowfile has decided not to convert is muted, not red: nothing is wrong with it.
export const CHIPS: Record<AlteryxToolStatus, StatusChip> = {
  placeholder: { label: "Placeholder", className: "status-badge--danger" },
  commented: { label: "Needs review", className: "status-badge--warning" },
  partial: { label: "Partial", className: "status-badge--warning" },
  out_of_scope: { label: "Out of scope", className: "status-badge--muted" },
  no_op: { label: "No-op", className: "status-badge--muted" },
  skipped: { label: "Skipped", className: "status-badge--info" },
  converted: { label: "Converted", className: "status-badge--success" },
};

// Rows needing manual work sort first — that's the only part the user must act on.
const STATUS_RANK: Record<AlteryxToolStatus, number> = {
  placeholder: 0,
  commented: 1,
  partial: 2,
  out_of_scope: 3,
  no_op: 4,
  skipped: 5,
  converted: 6,
};

export const SUMMARY_ORDER: AlteryxToolStatus[] = [
  "converted",
  "partial",
  "commented",
  "placeholder",
  "out_of_scope",
  "no_op",
  "skipped",
];

export function statusChip(status: AlteryxToolStatus): StatusChip {
  return CHIPS[status] ?? { label: String(status), className: "status-badge--info" };
}

export function sortReportRows(rows: AlteryxToolRow[]): AlteryxToolRow[] {
  return rows
    .map((row, index) => ({ row, index }))
    .sort((a, b) => {
      const rank = (STATUS_RANK[a.row.status] ?? 9) - (STATUS_RANK[b.row.status] ?? 9);
      return rank !== 0 ? rank : a.index - b.index;
    })
    .map((entry) => entry.row);
}

export function needsAttentionCount(report: AlteryxConversionReport): number {
  return (report.placeholder ?? 0) + (report.commented ?? 0) + (report.partial ?? 0);
}

export function outOfScopeCount(report: AlteryxConversionReport): number {
  return (report.out_of_scope ?? 0) + (report.no_op ?? 0);
}

// The headline is the first thing read, so it must not call a flow ready when tools were skipped.
export function headline(report: AlteryxConversionReport): string {
  const attention = needsAttentionCount(report);
  if (attention > 0) {
    const subject = attention === 1 ? "1 tool needs" : `${attention} tools need`;
    return `${subject} manual work — the flow opens with notes on those nodes.`;
  }
  const unconvertible = outOfScopeCount(report);
  if (unconvertible > 0) {
    const one = unconvertible === 1;
    const subject = one ? "1 tool is out of scope" : `${unconvertible} tools are out of scope`;
    const verb = one ? "passes" : "pass";
    return `Everything else converted — ${subject} for Flowfile and ${verb} data straight through.`;
  }
  if ((report.total_tools ?? 0) === 0) return "No tools found — the canvas had nothing to convert.";
  return "Every tool converted — the flow is ready to open.";
}

export function summaryLine(report: AlteryxConversionReport): string {
  const total = report.total_tools ?? 0;
  const parts = [`${total} tool${total === 1 ? "" : "s"}`];
  for (const status of SUMMARY_ORDER) {
    const count = report[status] ?? 0;
    if (count > 0) parts.push(`${count} ${statusChip(status).label.toLowerCase()}`);
  }
  const annotations = report.total_annotations ?? 0;
  if (annotations > 0) parts.push(`${annotations} comment${annotations === 1 ? "" : "s"}`);
  return parts.join(" · ");
}

// The backend writes the sentence so both percentages are always shown together.
export function coverageLine(report: AlteryxConversionReport): string {
  return report.coverage?.definition ?? "";
}

export function entityLabel(row: AlteryxToolRow): string {
  return row.flowfile_node_type || (row.entity === "annotation" ? "comment" : "—");
}

// Placeholder rows for an official Alteryx tool link to the open request for it, else to a prefilled new one.
// Out-of-scope and no-op rows are never placeholders, so they never offer a node that is not coming.
export function nodeRequestLink(
  row: AlteryxToolRow,
  issues: Record<string, string>,
): NodeRequestLink | null {
  const key = row.alteryx_tool_key;
  if (row.status !== "placeholder" || !key || !row.requestable) return null;
  const existing = issues[key];
  if (existing) return { label: "Upvote request on GitHub", url: existing, existing: true };
  const params = new URLSearchParams({
    template: NODE_REQUEST_TEMPLATE,
    title: `[Alteryx node] ${key}`,
    tool: key,
  });
  return {
    label: "Request node on GitHub",
    url: `${NEW_NODE_REQUEST_URL}?${params}`,
    existing: false,
  };
}
