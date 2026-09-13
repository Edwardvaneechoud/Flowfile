import { describe, it, expect } from "vitest";

import {
  CHIPS,
  NEW_NODE_REQUEST_URL,
  SUMMARY_ORDER,
  coverageLine,
  entityLabel,
  headline,
  needsAttentionCount,
  nodeRequestLink,
  outOfScopeCount,
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
    out_of_scope: 0,
    no_op: 0,
    skipped: 0,
    coverage: {
      tools: 0,
      in_scope: 0,
      mapped: 0,
      converted: 0,
      mapped_percent: 0,
      converted_percent: 0,
      in_scope_percent: 0,
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

  it("mutes the statuses that are nobody's fault", () => {
    expect(statusChip("out_of_scope")).toEqual({
      label: "Out of scope",
      className: "status-badge--muted",
    });
    expect(statusChip("no_op")).toEqual({ label: "No-op", className: "status-badge--muted" });
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

  it("sorts the muted statuses after the work and before the clean rows", () => {
    const rows = [
      makeRow("Select", "converted"),
      makeRow("Message", "no_op"),
      makeRow("Sample", "skipped"),
      makeRow("Buffer", "out_of_scope"),
      makeRow("Union", "partial"),
    ];
    expect(sortReportRows(rows).map((r) => r.alteryx_tool)).toEqual([
      "Union",
      "Buffer",
      "Message",
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

describe("SUMMARY_ORDER", () => {
  // CHIPS is a Record over the status union, so vue-tsc forces it complete: a status added
  // to the union but not to SUMMARY_ORDER fails here rather than silently vanishing.
  it("names every status, so no count can drop out of the summary line", () => {
    expect(new Set(SUMMARY_ORDER)).toEqual(new Set(Object.keys(CHIPS)));
  });
});

describe("outOfScopeCount", () => {
  it("adds the two statuses Flowfile will never convert", () => {
    expect(outOfScopeCount(makeReport({ out_of_scope: 4, no_op: 2 }))).toBe(6);
    expect(outOfScopeCount(makeReport())).toBe(0);
  });
});

describe("headline", () => {
  it("leads with the work when there is any", () => {
    expect(headline(makeReport({ total_tools: 3, placeholder: 2, converted: 1 }))).toBe(
      "2 tools need manual work — the flow opens with notes on those nodes.",
    );
    expect(headline(makeReport({ total_tools: 2, partial: 1, converted: 1 }))).toBe(
      "1 tool needs manual work — the flow opens with notes on those nodes.",
    );
  });

  it("does not call a flow ready when its unconverted tools are out of scope", () => {
    expect(headline(makeReport({ total_tools: 8, converted: 2, out_of_scope: 4, no_op: 2 }))).toBe(
      "Everything else converted — 6 tools are out of scope for Flowfile and pass data straight through.",
    );
    expect(headline(makeReport({ total_tools: 2, converted: 1, out_of_scope: 1 }))).toBe(
      "Everything else converted — 1 tool is out of scope for Flowfile and passes data straight through.",
    );
  });

  it("says so when the canvas held no tools at all", () => {
    expect(headline(makeReport({ total_tools: 0, total_annotations: 3 }))).toBe(
      "No tools found — the canvas had nothing to convert.",
    );
  });

  it("celebrates only a genuinely complete conversion", () => {
    expect(headline(makeReport({ total_tools: 5, converted: 5 }))).toBe(
      "Every tool converted — the flow is ready to open.",
    );
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

  it("lists the out-of-scope counts the backend now sends", () => {
    const report = makeReport({ total_tools: 8, converted: 2, out_of_scope: 4, no_op: 2 });
    expect(summaryLine(report)).toBe("8 tools · 2 converted · 4 out of scope · 2 no-op");
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
        in_scope: 3,
        mapped: 2,
        converted: 2,
        mapped_percent: 50,
        converted_percent: 50,
        in_scope_percent: 67,
        definition:
          "2 of 4 Alteryx tools reached a Flowfile node (50% of all tools); of the 3 tools Flowfile aims to convert, 67%.",
      },
    });
    expect(coverageLine(report)).toBe(
      "2 of 4 Alteryx tools reached a Flowfile node (50% of all tools); of the 3 tools Flowfile aims to convert, 67%.",
    );
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

describe("nodeRequestLink", () => {
  const issues = { DateTime: "https://github.com/edwardvaneechoud/Flowfile/issues/42" };

  it("links a placeholder for an official tool to its open request", () => {
    const row = makeRow("DateTime", "placeholder", {
      alteryx_tool_key: "DateTime",
      requestable: true,
    });
    expect(nodeRequestLink(row, issues)).toEqual({
      label: "Upvote request on GitHub",
      url: issues.DateTime,
      existing: true,
    });
  });

  it("prefills a new issue when no request is open yet", () => {
    const row = makeRow("Tile", "placeholder", { alteryx_tool_key: "Tile", requestable: true });
    const link = nodeRequestLink(row, issues);
    expect(link?.existing).toBe(false);
    expect(link?.label).toBe("Request node on GitHub");
    const url = new URL(link!.url);
    expect(`${url.origin}${url.pathname}`).toBe(NEW_NODE_REQUEST_URL);
    expect(url.searchParams.get("template")).toBe("alteryx_node_request.yml");
    expect(url.searchParams.get("title")).toBe("[Alteryx node] Tile");
    expect(url.searchParams.get("tool")).toBe("Tile");
  });

  it("offers nothing for converted rows, user macros, vendor plugins or rows without a key", () => {
    expect(
      nodeRequestLink(
        makeRow("Filter", "converted", { alteryx_tool_key: "Filter", requestable: true }),
        issues,
      ),
    ).toBeNull();
    expect(
      nodeRequestLink(
        makeRow("Filter", "partial", { alteryx_tool_key: "Filter", requestable: true }),
        issues,
      ),
    ).toBeNull();
    expect(
      nodeRequestLink(
        makeRow("Something.yxmc", "placeholder", {
          alteryx_tool_key: "user_macro",
          requestable: false,
        }),
        issues,
      ),
    ).toBeNull();
    expect(
      nodeRequestLink(
        makeRow("Uploader", "placeholder", {
          alteryx_tool_key: "custom_plugin",
          requestable: false,
        }),
        issues,
      ),
    ).toBeNull();
    expect(nodeRequestLink(makeRow("DateTime", "placeholder"), issues)).toBeNull();
  });

  it("never offers a node for a tool Flowfile has decided not to convert", () => {
    for (const status of ["out_of_scope", "no_op"] as AlteryxToolStatus[]) {
      const row = makeRow("ReportMap", status, {
        alteryx_tool_key: "ReportMap",
        requestable: true,
      });
      expect(nodeRequestLink(row, issues)).toBeNull();
    }
  });
});
