// Pure presentation helpers for the Alteryx conversion report. No Vue/axios
// imports so they unit-test as a plain module.
import type {
  AlteryxConversionReport,
  AlteryxToolRow,
  AlteryxToolStatus,
} from "../../../api/alteryx.api";

export interface StatusChip {
  label: string;
  className: string;
}

// Modifiers of the shared .status-badge system (styles/components/_status-badges.css).
const CHIPS: Record<AlteryxToolStatus, StatusChip> = {
  placeholder: { label: "Placeholder", className: "status-badge--danger" },
  commented: { label: "Needs review", className: "status-badge--warning" },
  partial: { label: "Partial", className: "status-badge--warning" },
  skipped: { label: "Skipped", className: "status-badge--info" },
  converted: { label: "Converted", className: "status-badge--success" },
};

// Rows needing manual work first — that is the only part of the report the user
// has to act on.
const STATUS_RANK: Record<AlteryxToolStatus, number> = {
  placeholder: 0,
  commented: 1,
  partial: 2,
  skipped: 3,
  converted: 4,
};

const SUMMARY_ORDER: AlteryxToolStatus[] = [
  "converted",
  "partial",
  "commented",
  "placeholder",
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

export function summaryLine(report: AlteryxConversionReport): string {
  const total = report.total_tools ?? 0;
  const parts = [`${total} tool${total === 1 ? "" : "s"}`];
  for (const status of SUMMARY_ORDER) {
    const count = report[status] ?? 0;
    if (count > 0) parts.push(`${count} ${statusChip(status).label.toLowerCase()}`);
  }
  return parts.join(" · ");
}
