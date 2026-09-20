import { describe, it, expect, vi } from "vitest";
import {
  batchProgress,
  beginExecution,
  bumpSessionEpoch,
  bumpSourceRevision,
  cellRuntime,
  clearResults,
  disposeOwner,
  endBatch,
  ensureOwner,
  getOwner,
  invalidateFrom,
  isBatchActive,
  markDownstreamStale,
  onExecutionSettled,
  onSessionEpoch,
  runExecutionBatch,
  settleExecution,
  snapshotRevisions,
  staleLabel,
  staleTitle,
  startBatch,
} from "./notebookRuntimeState";
import type { CellRuntime, RuntimeCellRef, SettleVerdict } from "./notebookRuntimeState";

let ownerSeq = 0;
function newOwner(): string {
  ownerSeq += 1;
  return `owner-${ownerSeq}`;
}

const py = (id: string): RuntimeCellRef => ({ id, isPython: true });
const md = (id: string): RuntimeCellRef => ({ id, isPython: false });

function snapshot(ownerId: string, cellId: string): CellRuntime {
  return { ...cellRuntime(ownerId, cellId)! };
}

/** Gives a cell a result, so staleness marks have something to be about. */
function withResult(ownerId: string, cellId: string): void {
  settleExecution(beginExecution(ownerId, cellId));
}

describe("notebookRuntimeState: owners and cells", () => {
  it("creates an owner on demand and returns the same object", () => {
    const owner = newOwner();
    const rt = ensureOwner(owner);
    expect(rt.sessionEpoch).toBe(0);
    expect(rt.batch).toBeNull();
    expect(ensureOwner(owner)).toBe(rt);
    expect(getOwner(owner)).toBe(rt);
  });

  it("cellRuntime reads without creating anything", () => {
    const owner = newOwner();
    expect(cellRuntime(owner, "a")).toBeUndefined();
    expect(getOwner(owner)).toBeUndefined();
  });

  it("forgets everything on dispose, and a ticket issued before it discards", () => {
    const owner = newOwner();
    const ticket = beginExecution(owner, "a");
    disposeOwner(owner);
    expect(getOwner(owner)).toBeUndefined();
    expect(settleExecution(ticket)).toBe("discard");
    expect(getOwner(owner)).toBeUndefined();
  });
});

describe("notebookRuntimeState: source revisions", () => {
  it("bumps without marking a cell that has no result", () => {
    const owner = newOwner();
    bumpSourceRevision(owner, "a");
    expect(cellRuntime(owner, "a")).toMatchObject({
      sourceRevision: 1,
      submittedRevision: null,
      staleReason: null,
    });
  });

  it("marks a cell that has a result as code-changed", () => {
    const owner = newOwner();
    withResult(owner, "a");
    bumpSourceRevision(owner, "a");
    expect(cellRuntime(owner, "a")).toMatchObject({
      sourceRevision: 1,
      submittedRevision: 0,
      staleReason: "code-changed",
    });
  });
});

describe("notebookRuntimeState: invalidation", () => {
  const cells = [py("a"), md("b"), py("c")];

  it("marks only python cells from the index onwards", () => {
    const owner = newOwner();
    cells.filter((c) => c.isPython).forEach((c) => withResult(owner, c.id));
    invalidateFrom(owner, cells, 1, "upstream-changed");
    expect(cellRuntime(owner, "a")!.staleReason).toBeNull();
    expect(cellRuntime(owner, "b")!.staleReason).toBeNull();
    expect(cellRuntime(owner, "c")!.staleReason).toBe("upstream-changed");
  });

  it("never touches source revisions", () => {
    const owner = newOwner();
    bumpSourceRevision(owner, "a");
    invalidateFrom(owner, cells, 0, "upstream-changed");
    expect(cellRuntime(owner, "a")!.sourceRevision).toBe(1);
    expect(cellRuntime(owner, "c")!.sourceRevision).toBe(0);
  });

  it("syncs membership on insert and delete", () => {
    const owner = newOwner();
    invalidateFrom(owner, cells, cells.length, "upstream-changed");
    expect(Object.keys(getOwner(owner)!.cells).sort()).toEqual(["a", "b", "c"]);

    invalidateFrom(owner, [py("a"), py("x"), md("b"), py("c")], 4, "upstream-changed");
    expect(Object.keys(getOwner(owner)!.cells).sort()).toEqual(["a", "b", "c", "x"]);

    invalidateFrom(owner, [py("a"), py("c")], 2, "upstream-changed");
    expect(Object.keys(getOwner(owner)!.cells).sort()).toEqual(["a", "c"]);
  });

  it("marks from min(oldIndex, newIndex) after a move", () => {
    const owner = newOwner();
    const before = [py("a"), py("b"), py("c")];
    before.forEach((c) => withResult(owner, c.id));
    // "c" moved from index 2 to index 0.
    const after = [py("c"), py("a"), py("b")];
    invalidateFrom(owner, after, Math.min(2, 0), "upstream-changed");
    expect(after.map((c) => cellRuntime(owner, c.id)!.staleReason)).toEqual([
      "upstream-changed",
      "upstream-changed",
      "upstream-changed",
    ]);
  });

  it("marks everything after the given cell via markDownstreamStale", () => {
    const owner = newOwner();
    const list = [py("a"), md("b"), py("c")];
    list.forEach((c) => withResult(owner, c.id));
    markDownstreamStale(owner, list, "a");
    expect(cellRuntime(owner, "a")!.staleReason).toBeNull();
    expect(cellRuntime(owner, "b")!.staleReason).toBeNull();
    expect(cellRuntime(owner, "c")!.staleReason).toBe("upstream-changed");
  });

  it("marks nothing when the cell is not in the list", () => {
    const owner = newOwner();
    const list = [py("a"), py("c")];
    list.forEach((c) => withResult(owner, c.id));
    markDownstreamStale(owner, list, "gone");
    expect(list.every((c) => cellRuntime(owner, c.id)!.staleReason === null)).toBe(true);
  });
});

describe("notebookRuntimeState: stale precedence", () => {
  it("upgrades but never downgrades a marker", () => {
    const owner = newOwner();
    const list = [py("a")];
    withResult(owner, "a");

    invalidateFrom(owner, list, 0, "upstream-changed");
    expect(cellRuntime(owner, "a")!.staleReason).toBe("upstream-changed");

    invalidateFrom(owner, list, 0, "code-changed");
    expect(cellRuntime(owner, "a")!.staleReason).toBe("code-changed");

    invalidateFrom(owner, list, 0, "upstream-changed");
    expect(cellRuntime(owner, "a")!.staleReason).toBe("code-changed");

    invalidateFrom(owner, list, 0, "previous-session");
    expect(cellRuntime(owner, "a")!.staleReason).toBe("previous-session");

    invalidateFrom(owner, list, 0, "code-changed");
    expect(cellRuntime(owner, "a")!.staleReason).toBe("previous-session");
  });
});

describe("notebookRuntimeState: execution tickets", () => {
  it("applies a clean settle and clears any earlier marker", () => {
    const owner = newOwner();
    withResult(owner, "a");
    invalidateFrom(owner, [py("a")], 0, "upstream-changed");

    const ticket = beginExecution(owner, "a");
    expect(cellRuntime(owner, "a")!.status).toBe("running");
    expect(settleExecution(ticket)).toBe("apply");
    expect(cellRuntime(owner, "a")).toMatchObject({
      status: "idle",
      staleReason: null,
      submittedRevision: 0,
      epoch: 0,
    });
  });

  it("keeps the result but labels it code-changed when the source moved mid-run", () => {
    const owner = newOwner();
    const ticket = beginExecution(owner, "a");
    bumpSourceRevision(owner, "a");
    expect(settleExecution(ticket)).toBe("apply-stale");
    expect(cellRuntime(owner, "a")).toMatchObject({
      status: "idle",
      staleReason: "code-changed",
      submittedRevision: ticket.revision,
    });
  });

  it("keeps upstream-changed when an earlier cell ran during the flight", () => {
    const owner = newOwner();
    const list = [py("a"), py("b")];
    const ticket = beginExecution(owner, "b");
    markDownstreamStale(owner, list, "a");
    expect(settleExecution(ticket)).toBe("apply-stale");
    expect(cellRuntime(owner, "b")).toMatchObject({
      staleReason: "upstream-changed",
      sourceRevision: 0,
      submittedRevision: 0,
    });
  });

  it("discards a superseded request without mutating anything", () => {
    const owner = newOwner();
    const first = beginExecution(owner, "a");
    beginExecution(owner, "a");
    const before = snapshot(owner, "a");
    expect(settleExecution(first)).toBe("discard");
    expect(snapshot(owner, "a")).toEqual(before);
    expect(cellRuntime(owner, "a")!.status).toBe("running");
  });

  it("discards a response from a previous session epoch", () => {
    const owner = newOwner();
    const ticket = beginExecution(owner, "a");
    bumpSessionEpoch(owner);
    const before = snapshot(owner, "a");
    expect(settleExecution(ticket)).toBe("discard");
    expect(snapshot(owner, "a")).toEqual(before);
  });

  it("discards a response for a cell that was removed", () => {
    const owner = newOwner();
    const ticket = beginExecution(owner, "a");
    invalidateFrom(owner, [py("b")], 1, "upstream-changed");
    expect(cellRuntime(owner, "a")).toBeUndefined();
    expect(settleExecution(ticket)).toBe("discard");
    expect(cellRuntime(owner, "a")).toBeUndefined();
  });
});

describe("notebookRuntimeState: session epoch", () => {
  it("marks only cells with results, clears the batch and fires the hook", () => {
    const owner = newOwner();
    withResult(owner, "a");
    bumpSourceRevision(owner, "b");
    startBatch(owner, ["a", "b"]);
    const seen: number[] = [];
    const off = onSessionEpoch(owner, (epoch) => seen.push(epoch));

    bumpSessionEpoch(owner);

    expect(seen).toEqual([1]);
    expect(getOwner(owner)!.sessionEpoch).toBe(1);
    expect(getOwner(owner)!.batch).toBeNull();
    expect(cellRuntime(owner, "a")!.staleReason).toBe("previous-session");
    expect(cellRuntime(owner, "b")!.staleReason).toBeNull();
    expect(cellRuntime(owner, "a")!.status).toBe("idle");
    expect(cellRuntime(owner, "b")!.status).toBe("idle");

    off();
    bumpSessionEpoch(owner);
    expect(seen).toEqual([1]);
  });

  it("clearResults drops results and their markers", () => {
    const owner = newOwner();
    withResult(owner, "a");
    invalidateFrom(owner, [py("a")], 0, "code-changed");
    clearResults(owner);
    expect(cellRuntime(owner, "a")).toMatchObject({ submittedRevision: null, staleReason: null });
  });
});

describe("notebookRuntimeState: batches", () => {
  it("refuses a second batch, queues cells and releases them on end", () => {
    const owner = newOwner();
    const batchId = startBatch(owner, ["a", "b"]);
    expect(batchId).not.toBeNull();
    expect(startBatch(owner, ["a"])).toBeNull();
    expect(isBatchActive(owner)).toBe(true);
    expect(batchProgress(owner)).toMatchObject({ total: 2, done: 0, currentCellId: null });
    expect(cellRuntime(owner, "a")!.status).toBe("queued");

    endBatch(owner, batchId! + 1000);
    expect(isBatchActive(owner)).toBe(true);

    endBatch(owner, batchId!);
    expect(isBatchActive(owner)).toBe(false);
    expect(batchProgress(owner)).toBeNull();
    expect(cellRuntime(owner, "a")!.status).toBe("idle");
    expect(cellRuntime(owner, "b")!.status).toBe("idle");
  });

  it("snapshots the revisions it knows about", () => {
    const owner = newOwner();
    bumpSourceRevision(owner, "a");
    bumpSourceRevision(owner, "a");
    expect(snapshotRevisions(owner, ["a", "missing"])).toEqual({ a: 2 });
    expect(snapshotRevisions(newOwner(), ["a"])).toEqual({});
  });
});

describe("notebookRuntimeState: settled listeners", () => {
  it("reports the verdict and the response metadata", () => {
    const owner = newOwner();
    const seen: Array<[string, SettleVerdict, unknown]> = [];
    const off = onExecutionSettled(owner, (cellId, verdict, meta) =>
      seen.push([cellId, verdict, meta]),
    );

    const ticket = beginExecution(owner, "a");
    settleExecution(ticket, { namespace_generation: "gen-1", revision: 4 });
    expect(seen).toEqual([["a", "apply", { namespace_generation: "gen-1", revision: 4 }]]);

    off();
    settleExecution(beginExecution(owner, "a"));
    expect(seen).toHaveLength(1);
  });
});

describe("runExecutionBatch", () => {
  const present = () => true;

  it("runs python cells in order, renders markdown and reports progress", async () => {
    const owner = newOwner();
    const cells = [py("a"), md("m"), py("b")];
    const ran: string[] = [];
    const rendered: string[] = [];
    const runOne = vi.fn(async (cellId: string) => {
      ran.push(cellId);
      expect(batchProgress(owner)!.currentCellId).toBe(cellId);
      expect(batchProgress(owner)!.done).toBe(ran.length - 1);
      return { ok: true };
    });

    const started = await runExecutionBatch({
      ownerId: owner,
      cells,
      runOne,
      onMarkdown: (id) => rendered.push(id),
      stillPresent: present,
    });

    expect(started).toBe(true);
    expect(ran).toEqual(["a", "b"]);
    expect(rendered).toEqual(["m"]);
    expect(isBatchActive(owner)).toBe(false);
    expect(cellRuntime(owner, "a")!.status).toBe("idle");
  });

  it("refuses while a batch is active, without calling runOne", async () => {
    const owner = newOwner();
    startBatch(owner, ["a"]);
    const runOne = vi.fn(async () => ({ ok: true }));

    const pending = runExecutionBatch({
      ownerId: owner,
      cells: [py("a")],
      runOne,
      stillPresent: present,
    });
    expect(runOne).not.toHaveBeenCalled();
    await expect(pending).resolves.toBe(false);
    expect(runOne).not.toHaveBeenCalled();
    expect(isBatchActive(owner)).toBe(true);
  });

  it("stops at a cell that is gone", async () => {
    const owner = newOwner();
    const ran: string[] = [];
    await runExecutionBatch({
      ownerId: owner,
      cells: [py("a"), py("b")],
      runOne: async (cellId) => {
        ran.push(cellId);
        return { ok: true };
      },
      stillPresent: (id) => id !== "b",
    });
    expect(ran).toEqual(["a"]);
  });

  it("stops at a cell whose source moved after the snapshot", async () => {
    const owner = newOwner();
    const runOne = vi.fn(async (cellId: string) => {
      if (cellId === "a") bumpSourceRevision(owner, "b");
      return { ok: true };
    });
    await runExecutionBatch({
      ownerId: owner,
      cells: [py("a"), py("b")],
      runOne,
      stillPresent: present,
    });
    expect(runOne.mock.calls.map((c) => c[0])).toEqual(["a"]);
  });

  it("stops on the first failure and still counts it", async () => {
    const owner = newOwner();
    const runOne = vi.fn(async () => ({ ok: false }));
    await runExecutionBatch({
      ownerId: owner,
      cells: [py("a"), py("b")],
      runOne,
      stillPresent: present,
    });
    expect(runOne).toHaveBeenCalledTimes(1);
    expect(isBatchActive(owner)).toBe(false);
  });

  it("stops when the session epoch changes mid-batch", async () => {
    const owner = newOwner();
    const runOne = vi.fn(async (cellId: string) => {
      if (cellId === "a") bumpSessionEpoch(owner);
      return { ok: true };
    });
    await runExecutionBatch({
      ownerId: owner,
      cells: [py("a"), py("b")],
      runOne,
      stillPresent: present,
    });
    expect(runOne.mock.calls.map((c) => c[0])).toEqual(["a"]);
    expect(isBatchActive(owner)).toBe(false);
  });

  it("clears the batch even when runOne throws", async () => {
    const owner = newOwner();
    const failing = runExecutionBatch({
      ownerId: owner,
      cells: [py("a")],
      runOne: async () => {
        throw new Error("boom");
      },
      stillPresent: present,
    });
    await expect(failing).rejects.toThrow("boom");
    expect(isBatchActive(owner)).toBe(false);
    expect(cellRuntime(owner, "a")!.status).toBe("idle");
  });

  it("iterates its own snapshot, not the live list", async () => {
    const owner = newOwner();
    const cells = [py("a"), py("b")];
    const runOne = vi.fn(async (cellId: string) => {
      if (cellId === "a") cells.length = 1;
      return { ok: true };
    });
    await runExecutionBatch({ ownerId: owner, cells, runOne, stillPresent: present });
    expect(runOne.mock.calls.map((c) => c[0])).toEqual(["a", "b"]);
  });
});

describe("stale copy", () => {
  it("labels every reason", () => {
    expect(staleLabel("code-changed")).toBe("Code changed — rerun");
    expect(staleLabel("upstream-changed")).toBe("Earlier cells changed — rerun");
    expect(staleLabel("previous-session")).toBe("Previous session");
  });

  it("explains every reason in one sentence", () => {
    const titles = [
      staleTitle("code-changed"),
      staleTitle("upstream-changed"),
      staleTitle("previous-session"),
    ];
    expect(new Set(titles).size).toBe(3);
    for (const title of titles) {
      expect(title.endsWith(".")).toBe(true);
      expect(title.length).toBeGreaterThan(20);
    }
  });
});
