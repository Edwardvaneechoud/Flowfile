import { describe, it, expect } from "vitest";

import { needsAttentionCount, sortReportRows, statusChip, summaryLine } from "./alteryxReport";
import type {
  AlteryxConversionReport,
  AlteryxToolRow,
  AlteryxToolStatus,
} from "../../../api/alteryx.api";

function makeRow(
  alteryx_tool: string,
  status: AlteryxToolStatus,
  overrides: Partial<AlteryxToolRow> = {},
): AlteryxToolRow {
  return {
    alteryx_tool_id: "1",
    alteryx_tool,
    flowfile_node_ids: [1],
    flowfile_node_type: "select",
    status,
    messages: [],
    ...overrides,
  };
}

function makeReport(overrides: Partial<AlteryxConversionReport> = {}): AlteryxConversionReport {
  return {
    workflow_name: "orders",
    total_tools: 0,
    converted: 0,
    partial: 0,
    commented: 0,
    placeholder: 0,
    skipped: 0,
    rows: [],
    ...overrides,
  };
}

describe("statusChip", () => {
  it("labels every known status", () => {
    expect(statusChip("converted")).toEqual({
      label: "Converted",
      className: "status-badge--success",
    });
    expect(statusChip("commented").label).toBe("Needs review");
    expect(statusChip("placeholder").className).toBe("status-badge--danger");
  });

  it("falls back to a neutral chip for an unknown status", () => {
    const chip = statusChip("exploded" as AlteryxToolStatus);
    expect(chip.label).toBe("exploded");
    expect(chip.className).toBe("status-badge--info");
  });
});

describe("sortReportRows", () => {
  it("puts placeholder and commented rows first", () => {
    const rows = [
      makeRow("Select", "converted"),
      makeRow("Sample", "skipped"),
      makeRow("Formula", "commented"),
      makeRow("Union", "partial"),
      makeRow("Transpose", "placeholder"),
    ];
    expect(sortReportRows(rows).map((r) => r.alteryx_tool)).toEqual([
      "Transpose",
      "Formula",
      "Union",
      "Sample",
      "Select",
    ]);
  });

  it("keeps document order within a status and does not mutate the input", () => {
    const rows = [
      makeRow("Formula A", "commented"),
      makeRow("Select", "converted"),
      makeRow("Formula B", "commented"),
    ];
    const sorted = sortReportRows(rows);
    expect(sorted.map((r) => r.alteryx_tool)).toEqual(["Formula A", "Formula B", "Select"]);
    expect(rows.map((r) => r.alteryx_tool)).toEqual(["Formula A", "Select", "Formula B"]);
  });
});

describe("needsAttentionCount", () => {
  it("counts placeholder, commented and partial rows", () => {
    expect(
      needsAttentionCount(
        makeReport({ placeholder: 2, commented: 1, partial: 3, converted: 9, skipped: 4 }),
      ),
    ).toBe(6);
  });

  it("is zero for a clean conversion", () => {
    expect(needsAttentionCount(makeReport({ total_tools: 5, converted: 5 }))).toBe(0);
  });
});

describe("summaryLine", () => {
  it("lists only the statuses that occurred", () => {
    const report = makeReport({ total_tools: 6, converted: 4, placeholder: 2 });
    expect(summaryLine(report)).toBe("6 tools · 4 converted · 2 placeholder");
  });

  it("keeps the status order stable and singularises one tool", () => {
    const report = makeReport({
      total_tools: 1,
      converted: 0,
      partial: 0,
      commented: 1,
      skipped: 0,
    });
    expect(summaryLine(report)).toBe("1 tool · 1 needs review");
  });

  it("reports a workflow with nothing converted", () => {
    expect(summaryLine(makeReport({ total_tools: 0 }))).toBe("0 tools");
  });
});
