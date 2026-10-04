// Pure helpers behind the notebook toolbar's one-click kernel setup — no Vue/axios
// imports so the id, version and label logic stays unit-testable in the node-env vitest setup.
import type { KernelConfig, KernelInfo } from "@/types/kernel.types";

export type NotebookKernelPhase = "idle" | "creating" | "starting" | "updating";

export interface NotebookKernelActionInput {
  imageInstalled: boolean | null;
  phase: NotebookKernelPhase;
  pulling: boolean;
}

/** The default notebook kernel: Lite image with this app's flowfile; the id dodges taken ones. */
export function notebookKernelConfig(appVersion: string, existingIds: string[]): KernelConfig {
  const taken = new Set(existingIds);
  let id = "notebook";
  for (let n = 2; taken.has(id); n++) id = `notebook-${n}`;
  return {
    id,
    name: "Notebook",
    packages: [appVersion ? `flowfile==${appVersion}` : "flowfile"],
    cpu_cores: 2,
    memory_gb: 4,
    gpu: false,
    image_flavour: "lite",
    custom_image: null,
  };
}

const FLOWFILE_PIN = /^flowfile(?:\[[^\]]*\])?\s*==\s*([^\s;,=]+)/i;

/** The version a `flowfile==X` spec pins in the kernel's packages; null when unpinned or absent. */
export function flowfileVersionOf(kernel: KernelInfo): string | null {
  for (const spec of kernel.packages) {
    const match = spec.trim().match(FLOWFILE_PIN);
    if (match) return match[1];
  }
  return null;
}

/** True only when the kernel's pinned flowfile and the app version are both known and differ. */
export function notebookKernelOutdated(kernel: KernelInfo, appVersion: string): boolean {
  if (!appVersion) return false;
  const pinned = flowfileVersionOf(kernel);
  return pinned !== null && pinned !== appVersion;
}

export function notebookKernelActionLabel(input: NotebookKernelActionInput): string {
  switch (input.phase) {
    case "creating":
      return input.pulling ? "Downloading image…" : "Installing flowfile… (about 2 minutes)";
    case "starting":
      return "Starting…";
    case "updating":
      return "Updating…";
    default:
      return input.imageInstalled === false
        ? "Download image and create notebook kernel"
        : "Create notebook kernel";
  }
}
