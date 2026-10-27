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
      reason: "it reads https URLs",
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
  it("says in one sentence where a moved node runs, and adds what its backend lacks for the drawer", () => {
    const text =
      'Runs on the default worker, because backend "worker:polars" doesn\'t support this node.';

    expect(nodePlacementNote(preview(), 1)).toEqual({
      kind: "fallback",
      label: "→ default worker",
      text,
      detail: `${text}\nIt reads https URLs.`,
    });
  });

  it("names the node's own Compute backend", () => {
    const note = nodePlacementNote(preview(), 1, true);

    expect(note?.text).toBe(
      'Runs on the default worker, because its Compute backend "worker:polars" doesn\'t support ' +
        "this node.",
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
      detail: 'Cannot run on backend "worker:polars": it needs polars_ds. Set it.',
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
      title:
        '2 nodes ran on the default worker because backend "worker:polars" doesn\'t support them',
      lines: [
        { nodeId: 1, label: "read #1", reason: "it reads https" },
        { nodeId: 2, label: "read #2", reason: "it needs polars_ds" },
      ],
    });
  });

  it("puts the backends on each line when the moves differ", () => {
    const notice = fallbackNotice([moved(1), moved(2, "worker:team")]);

    expect(notice?.title).toBe(
      "2 nodes ran on the default worker because their backend doesn't support them",
    );
    expect(notice?.lines[1].reason).toBe("worker:team → default worker: it reads https");
  });

  it("is singular for one node and null when none moved", () => {
    expect(fallbackNotice([moved(1)])?.title).toBe(
      '1 node ran on the default worker because backend "worker:polars" doesn\'t support it',
    );
    expect(fallbackNotice([row({ backend_id: "worker" })])).toBeNull();
    expect(fallbackNotice(undefined)).toBeNull();
  });
});
