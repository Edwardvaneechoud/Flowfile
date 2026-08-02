// Orchestration for "add the missing packages to an existing kernel":
// PATCH /kernels/{id} requires a stopped kernel, so stop -> rebuild -> start.
import { KernelApi } from "@/api/kernel.api";
import type { KernelInfo } from "@/types";

import { mergePackages } from "./kernelMatch";

export type UpgradePhase = "stopping" | "rebuilding" | "starting";

// Errors propagate: a failed rebuild after the stop leaves the kernel stopped,
// and callers surface that explicitly rather than guessing.
export async function addPackagesToKernel(
  kernel: KernelInfo,
  packagesToAdd: string[],
  onPhase?: (phase: UpgradePhase) => void,
): Promise<KernelInfo> {
  if (kernel.state !== "stopped") {
    onPhase?.("stopping");
    await KernelApi.stop(kernel.id);
  }
  onPhase?.("rebuilding");
  const updated = await KernelApi.update(kernel.id, {
    packages: mergePackages(kernel.packages, packagesToAdd),
  });
  onPhase?.("starting");
  return KernelApi.start(updated.id);
}
