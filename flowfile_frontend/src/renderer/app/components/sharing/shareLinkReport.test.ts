import { describe, expect, it } from "vitest";

import type { ShareLinkNodeReport, ShareLinkResponse } from "../../api/shareLink.api";
import {
  DEFAULT_PLACEHOLDER_REASON,
  LONG_LINK_HINT,
  LONG_LINK_THRESHOLD,
  PRIVACY_NOTE,
  TOO_LARGE_HEADLINE,
  localFileNote,
  needsLongLinkHint,
  nodeReason,
  placeholderRows,
  shareNotes,
  summarizeReport,
} from "./shareLinkReport";

const supported = (id: number, type = "filter"): ShareLinkNodeReport => ({
  node_id: id,
  node_type: type,
  status: "supported",
});

const placeholder = (
  id: number,
  type = "database_reader",
  reason = "Reads a database",
): ShareLinkNodeReport => ({
  node_id: id,
  node_type: type,
  status: "placeholder",
  reason,
});

function makeResponse(overrides: Partial<ShareLinkResponse> = {}): ShareLinkResponse {
  return {
    url: "https://demo.flowfile.org/designer#flow=abc",
    hash_chars: 120,
    compatible: true,
    nodes_report: [supported(1), supported(2)],
    warnings: [],
    placeholder_count: 0,
    local_file_nodes: [],
    ...overrides,
  };
}

describe("summarizeReport", () => {
  it("reports a fully compatible flow as ready", () => {
    const summary = summarizeReport(makeResponse());
    expect(summary).toEqual({
      okCount: 2,
      placeholderCount: 0,
      total: 2,
      headline: "Ready to share",
      severity: "success",
    });
  });

  it("counts placeholders in the headline of a degraded flow", () => {
    const summary = summarizeReport(
      makeResponse({
        compatible: false,
        placeholder_count: 1,
        nodes_report: [supported(1), placeholder(2), supported(3)],
      }),
    );
    expect(summary.severity).toBe("warning");
    expect(summary.headline).toBe("1 of 3 nodes won't run in the browser");
    expect(summary.okCount).toBe(2);
    expect(summary.placeholderCount).toBe(1);
  });

  it("treats a null url as the too-large refusal regardless of compatibility", () => {
    const summary = summarizeReport(makeResponse({ url: null }));
    expect(summary.severity).toBe("error");
    expect(summary.headline).toBe(TOO_LARGE_HEADLINE);
  });

  it("never returns a negative ok count when the backend counts disagree", () => {
    const summary = summarizeReport(
      makeResponse({ compatible: false, placeholder_count: 5, nodes_report: [placeholder(1)] }),
    );
    expect(summary.okCount).toBe(0);
  });
});

describe("placeholderRows / nodeReason", () => {
  it("keeps only placeholder entries", () => {
    const rows = placeholderRows(
      makeResponse({ nodes_report: [supported(1), placeholder(2), supported(3), placeholder(4)] }),
    );
    expect(rows.map((r) => r.node_id)).toEqual([2, 4]);
  });

  it("falls back to a generic reason when the backend sends none", () => {
    expect(nodeReason({ node_id: 1, node_type: "polars_code", status: "placeholder" })).toBe(
      DEFAULT_PLACEHOLDER_REASON,
    );
    expect(
      nodeReason({ node_id: 1, node_type: "polars_code", status: "placeholder", reason: "  " }),
    ).toBe(DEFAULT_PLACEHOLDER_REASON);
    expect(nodeReason(placeholder(1))).toBe("Reads a database");
  });
});

describe("needsLongLinkHint", () => {
  it.each([
    [0, false],
    [LONG_LINK_THRESHOLD, false],
    [LONG_LINK_THRESHOLD + 1, true],
  ])("hash of %i chars -> %s", (chars, expected) => {
    expect(needsLongLinkHint(chars)).toBe(expected);
  });
});

describe("localFileNote", () => {
  it("is absent when no node reads a local file", () => {
    expect(localFileNote([])).toBeNull();
  });

  it("singularizes for one node", () => {
    expect(localFileNote([3])).toBe("Recipients will be asked to supply the file for 1 read node.");
  });

  it("pluralizes for several nodes", () => {
    expect(localFileNote([3, 7])).toBe(
      "Recipients will be asked to supply the file for 2 read nodes.",
    );
  });
});

describe("shareNotes", () => {
  it("always includes the privacy line and appends backend warnings", () => {
    const notes = shareNotes(makeResponse({ warnings: ["Flow parameters are not shared."] }));
    expect(notes).toEqual([PRIVACY_NOTE, "Flow parameters are not shared."]);
  });

  it("leads with the local-file note and adds the long-link hint", () => {
    const notes = shareNotes(
      makeResponse({ local_file_nodes: [4], hash_chars: LONG_LINK_THRESHOLD + 1 }),
    );
    expect(notes[0]).toContain("1 read node");
    expect(notes).toContain(PRIVACY_NOTE);
    expect(notes).toContain(LONG_LINK_HINT);
  });
});
