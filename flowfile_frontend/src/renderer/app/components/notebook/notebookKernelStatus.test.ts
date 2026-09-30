import { describe, it, expect } from "vitest";
import type { KernelInfo, KernelState } from "@/types/kernel.types";
import { kernelStatusNeedsAttention, resolveNotebookKernelStatus } from "./notebookKernelStatus";

function kernel(id: string, state: KernelState): KernelInfo {
  return { id, name: id, state } as KernelInfo;
}

const base = { kernels: [kernel("k1", "idle")], kernelsLoaded: true, dockerAvailable: true };

describe("resolveNotebookKernelStatus", () => {
  it("reports docker-off before anything else", () => {
    expect(
      resolveNotebookKernelStatus({ ...base, kernelId: "k1", dockerAvailable: false }).kind,
    ).toBe("docker-off");
  });

  it("reports none when no kernel is selected", () => {
    expect(resolveNotebookKernelStatus({ ...base, kernelId: null }).kind).toBe("none");
  });

  it("holds off on judging a selection until the kernel list has loaded", () => {
    const s = resolveNotebookKernelStatus({ ...base, kernelId: "gone", kernelsLoaded: false });
    expect(s).toEqual({ kind: "loading", kernelId: "gone" });
  });

  it("flags a selection that no longer exists", () => {
    const s = resolveNotebookKernelStatus({ ...base, kernelId: "gone" });
    expect(s).toEqual({ kind: "missing", kernelId: "gone" });
    expect(kernelStatusNeedsAttention(s)).toBe(true);
  });

  it("treats idle and executing as ready", () => {
    for (const state of ["idle", "executing"] as const) {
      const s = resolveNotebookKernelStatus({
        ...base,
        kernels: [kernel("k1", state)],
        kernelId: "k1",
      });
      expect(s.kind).toBe("ready");
      expect(kernelStatusNeedsAttention(s)).toBe(false);
    }
  });

  it("maps starting/creating to starting, stopped to stopped, error to error", () => {
    const cases: Array<[KernelState, string]> = [
      ["starting", "starting"],
      ["creating", "starting"],
      ["stopped", "stopped"],
      ["error", "error"],
    ];
    for (const [state, kind] of cases) {
      const s = resolveNotebookKernelStatus({
        ...base,
        kernels: [kernel("k1", state)],
        kernelId: "k1",
      });
      expect(s.kind).toBe(kind);
    }
    expect(
      kernelStatusNeedsAttention(
        resolveNotebookKernelStatus({
          ...base,
          kernels: [kernel("k1", "stopped")],
          kernelId: "k1",
        }),
      ),
    ).toBe(true);
  });
});
