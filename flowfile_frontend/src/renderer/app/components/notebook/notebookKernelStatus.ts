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

/** True when a poll brought back the same kernels: the panel keeps its list and does not re-render. */
export function sameKernelList(current: KernelInfo[], next: KernelInfo[]): boolean {
  return (
    current.length === next.length &&
    current.every((k, i) => {
      const o = next[i];
      return (
        k.id === o.id &&
        k.name === o.name &&
        k.state === o.state &&
        k.error_message === o.error_message &&
        k.custom_image === o.custom_image &&
        k.packages.join() === o.packages.join()
      );
    })
  );
}

/** True when the header should flag the selection (amber border, warning label). */
export function kernelStatusNeedsAttention(status: NotebookKernelStatus): boolean {
  return status.kind === "missing" || status.kind === "stopped" || status.kind === "error";
}
