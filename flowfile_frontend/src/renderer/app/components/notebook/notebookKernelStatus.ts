// Pure resolution of "what should the notebook say about its kernel?" — keeps the
// banner/picker branching testable without mounting the panel.
import type { KernelInfo } from "@/types/kernel.types";

export type NotebookKernelStatus =
  | { kind: "docker-off" }
  | { kind: "none" }
  | { kind: "loading"; kernelId: string }
  | { kind: "missing"; kernelId: string }
  | { kind: "ready"; kernel: KernelInfo }
  | { kind: "stopped" | "starting" | "error"; kernel: KernelInfo };

export interface KernelStatusInput {
  kernelId: string | null;
  kernels: KernelInfo[];
  kernelsLoaded: boolean;
  dockerAvailable: boolean;
}

export function resolveNotebookKernelStatus(input: KernelStatusInput): NotebookKernelStatus {
  if (!input.dockerAvailable) return { kind: "docker-off" };
  if (!input.kernelId) return { kind: "none" };
  if (!input.kernelsLoaded) return { kind: "loading", kernelId: input.kernelId };
  const kernel = input.kernels.find((k) => k.id === input.kernelId);
  if (!kernel) return { kind: "missing", kernelId: input.kernelId };
  switch (kernel.state) {
    case "idle":
    case "executing":
      return { kind: "ready", kernel };
    case "starting":
    case "creating":
      return { kind: "starting", kernel };
    case "error":
      return { kind: "error", kernel };
    default:
      return { kind: "stopped", kernel };
  }
}

/** True when the header should flag the selection (amber border, warning label). */
export function kernelStatusNeedsAttention(status: NotebookKernelStatus): boolean {
  return status.kind === "missing" || status.kind === "stopped" || status.kind === "error";
}
