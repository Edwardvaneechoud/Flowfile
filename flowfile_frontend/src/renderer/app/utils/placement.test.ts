import { describe, expect, it } from "vitest";
import type { NodeResult, PlacementPreview } from "../types";
import { fallbackNotice, nodePlacementNote } from "./placement";

const preview = (overrides: Partial<PlacementPreview> = {}): PlacementPreview => ({
  available: true,
  assignment: { 1: "worker", 2: "worker:polars" },
  fallbacks: [
    {
      node_id: 1,
      backend_id: "worker:polars",
      fallback_id: "worker",
      reason: "it reads storage this backend does not read: https",
    },
  ],
  refusals: [],
  boundaries: [[1, 2]],
  ...overrides,
});

const row = (overrides: Partial<NodeResult>): NodeResult => ({
  node_id: 1,
  start_timestamp: 0,
  end_timestamp: 0,
  error: "",
  run_time_ms: 1,
  is_running: false,
  ...overrides,
});

describe("nodePlacementNote", () => {
  it("says a moved node runs on the default backend and that its data crosses", () => {
    expect(nodePlacementNote(preview(), 1)).toEqual({
      kind: "fallback",
      label: "→ worker",
      text:
        'Runs on the default backend "worker": Backend "worker:polars" cannot run it (it reads storage this ' +
        "backend does not read: https). Its data crosses to the other nodes through the exchange store.",
    });
  });

  it("names the node's own Compute backend and leaves out the crossing when nothing crosses", () => {
    const note = nodePlacementNote(preview({ boundaries: [] }), 1, true);

    expect(note?.text).toBe(
      'Runs on the default backend "worker": Its Compute backend "worker:polars" cannot run it (it reads ' +
        "storage this backend does not read: https).",
    );
  });

  it("words a refusal as the run does, its remedy after it", () => {
    const refused = preview({
      fallbacks: [],
      refusals: [
        {
          node_id: 2,
          backend_id: "worker:polars",
          reason: "it needs polars_ds",
          remedy: "Set it.",
        },
      ],
    });

    expect(nodePlacementNote(refused, 2)).toEqual({
      kind: "refused",
      label: "can't run",
      text: 'Cannot run on backend "worker:polars": it needs polars_ds. Set it.',
    });
  });

  it("says nothing for a node that runs where its settings put it, nor without a preview", () => {
    expect(nodePlacementNote(preview(), 2)).toBeNull();
    expect(nodePlacementNote(undefined, 1)).toBeNull();
    expect(nodePlacementNote(preview({ available: false }), 1)).toBeNull();
  });
});

describe("fallbackNotice", () => {
  const moved = (nodeId: number, from = "worker:polars", reason = "it reads https") =>
    row({
      node_id: nodeId,
      node_name: "read",
      backend_id: "worker",
      fallback_from: from,
      fallback_reason: reason,
    });

  it("names both backends once when every moved node went the same way", () => {
    expect(
      fallbackNotice([
        moved(1),
        moved(2, "worker:polars", "it needs polars_ds"),
        row({ node_id: 3 }),
      ]),
    ).toEqual({
      title: '2 nodes ran on "worker" because "worker:polars" cannot run them',
      lines: [
        { nodeId: 1, label: "read #1", reason: "it reads https" },
        { nodeId: 2, label: "read #2", reason: "it needs polars_ds" },
      ],
    });
  });

  it("puts the backends on each line when the moves differ", () => {
    const notice = fallbackNotice([moved(1), moved(2, "worker:team")]);

    expect(notice?.title).toBe(
      "2 nodes ran on the default backend because their backend cannot run them",
    );
    expect(notice?.lines[1].reason).toBe('"worker:team" → "worker": it reads https');
  });

  it("is singular for one node and null when none moved", () => {
    expect(fallbackNotice([moved(1)])?.title).toBe(
      '1 node ran on "worker" because "worker:polars" cannot run it',
    );
    expect(fallbackNotice([row({ backend_id: "worker" })])).toBeNull();
    expect(fallbackNotice(undefined)).toBeNull();
  });
});
