import { describe, it, expect } from "vitest";

import {
  coverageLine,
  entityLabel,
  needsAttentionCount,
  sortReportRows,
  statusChip,
  summaryLine,
} from "./alteryxReport";
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
    entity: "tool",
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
    total_annotations: 0,
    converted: 0,
    partial: 0,
    commented: 0,
    placeholder: 0,
    skipped: 0,
    coverage: {
      tools: 0,
      mapped: 0,
      converted: 0,
      mapped_percent: 0,
      converted_percent: 0,
      definition: "",
    },
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

  it("counts comments apart from the tools", () => {
    const report = makeReport({ total_tools: 2, total_annotations: 3, converted: 2 });
    expect(summaryLine(report)).toBe("2 tools · 2 converted · 3 comments");
  });

  it("says one comment in the singular", () => {
    expect(summaryLine(makeReport({ total_tools: 1, total_annotations: 1, converted: 1 }))).toBe(
      "1 tool · 1 converted · 1 comment",
    );
  });
});

describe("coverageLine", () => {
  it("shows the definition the backend wrote", () => {
    const report = makeReport({
      total_tools: 4,
      coverage: {
        tools: 4,
        mapped: 2,
        converted: 2,
        mapped_percent: 50,
        converted_percent: 50,
        definition: "2 of 4 Alteryx tools reached a Flowfile node (50%).",
      },
    });
    expect(coverageLine(report)).toBe("2 of 4 Alteryx tools reached a Flowfile node (50%).");
  });

  it("is empty when an older payload carries no coverage", () => {
    const report = makeReport();
    // @ts-expect-error — simulating a report from a core that predates the coverage field.
    report.coverage = undefined;
    expect(coverageLine(report)).toBe("");
  });
});

describe("entityLabel", () => {
  it("names the emitted node type", () => {
    expect(entityLabel(makeRow("Select", "converted"))).toBe("select");
  });

  it("calls an annotation without a node a comment", () => {
    const row = makeRow("Comment", "skipped", { entity: "annotation", flowfile_node_type: null });
    expect(entityLabel(row)).toBe("comment");
  });

  it("dashes a tool that emitted no node", () => {
    expect(entityLabel(makeRow("Sample", "skipped", { flowfile_node_type: null }))).toBe("—");
  });
});
