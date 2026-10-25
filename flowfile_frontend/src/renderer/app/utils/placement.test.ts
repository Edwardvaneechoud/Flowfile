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
  it("lists each row placement ran on the default backend", () => {
    const rows = [
      row({
        node_name: "read",
        backend_id: "worker",
        fallback_from: "worker:polars",
        fallback_reason: "it reads storage this backend does not read: https",
      }),
      row({ node_id: 2, backend_id: "worker:polars" }),
    ];

    expect(fallbackNotice(rows)).toEqual({
      title: "1 node ran on the default backend",
      lines: [
        {
          nodeId: 1,
          text: 'read (#1) ran on "worker": "worker:polars" cannot run it (it reads storage this backend does not read: https).',
        },
      ],
    });
  });

  it("is null when no row moved", () => {
    expect(fallbackNotice([row({ backend_id: "worker" })])).toBeNull();
    expect(fallbackNotice(undefined)).toBeNull();
  });
});
