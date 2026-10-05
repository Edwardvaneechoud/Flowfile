// Pure helpers behind the notebook toolbar's one-click kernel setup — no Vue/axios
// imports so the id, label and update logic stays unit-testable in the node-env vitest setup.
import type { KernelConfig, KernelInfo } from "@/types/kernel.types";

import { imageUpdateAvailable } from "../kernel/imageVersion";

export type NotebookKernelPhase = "idle" | "creating" | "starting" | "restarting";

export interface NotebookKernelActionInput {
  imageInstalled: boolean | null;
  phase: NotebookKernelPhase;
  pulling: boolean;
}

/** The default notebook kernel: the Notebook image (this app's flowfile baked in); the id dodges taken ones. */
export function notebookKernelConfig(existingIds: string[]): KernelConfig {
  const taken = new Set(existingIds);
  let id = "notebook";
  for (let n = 2; taken.has(id); n++) id = `notebook-${n}`;
  return {
    id,
    name: "Notebook",
    packages: [],
    cpu_cores: 2,
    memory_gb: 4,
    gpu: false,
    image_flavour: "notebook",
    custom_image: null,
  };
}

const DEFAULT_PICK_RANK: Partial<Record<KernelInfo["state"], number>> = {
  idle: 0,
  executing: 0,
  starting: 1,
  stopped: 2,
};

/** The notebook kernel a flow picks by itself when the user never picked one: a running one first, else
 * one that can start; a creating or errored kernel is never picked. Ties keep the list order. */
export function defaultNotebookKernel(kernels: KernelInfo[]): KernelInfo | null {
  let best: KernelInfo | null = null;
  for (const kernel of kernels) {
    if (kernel.image_flavour !== "notebook") continue;
    const rank = DEFAULT_PICK_RANK[kernel.state];
    if (rank === undefined) continue;
    if (best === null || rank < DEFAULT_PICK_RANK[best.state]!) best = kernel;
  }
  return best;
}

/** Whether a notebook kernel's image is an older release than `currentImage`, the flavour's tag today. */
export function notebookKernelOutdated(
  kernel: KernelInfo,
  currentImage: string | null | undefined,
): boolean {
  if (kernel.image_flavour !== "notebook") return false;
  return imageUpdateAvailable(kernel.image, currentImage)?.available ?? false;
}

export function notebookKernelActionLabel(input: NotebookKernelActionInput): string {
  switch (input.phase) {
    case "creating":
      return input.pulling ? "Downloading image…" : "Creating…";
    case "starting":
      return "Starting…";
    case "restarting":
      return input.pulling ? "Downloading image…" : "Restarting…";
    default:
      return input.imageInstalled === false
        ? "Download image and create notebook kernel"
        : "Create notebook kernel";
  }
}
