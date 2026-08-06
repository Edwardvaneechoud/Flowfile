// Module-singleton tracker for in-flight kernel creations. Owns the
// create(+optional start) call so the operation outlives any dialog or
// component: pending entries render in the Kernel Manager sidebar and
// settle-time outcomes surface via ElNotification, making the tracker the
// sole reporter of creation results. Known tradeoff: the backend only
// registers a kernel after its image build finishes, so a page reload
// during the build loses this pending state.
import { ref, type Ref } from "vue";
import { ElNotification } from "element-plus";

import { KernelApi } from "@/api/kernel.api";
import type { ImageFlavour, KernelConfig, KernelInfo } from "@/types";

export interface PendingKernelCreation {
  id: string;
  name: string;
  flavour: ImageFlavour;
  packages: string[];
  startedAt: number;
  phase: "creating" | "starting";
}

const pendingCreations: Ref<PendingKernelCreation[]> = ref([]);

function isPending(kernelId: string): boolean {
  return pendingCreations.value.some((p) => p.id === kernelId);
}

function removePending(kernelId: string): void {
  pendingCreations.value = pendingCreations.value.filter((p) => p.id !== kernelId);
}

function notify(type: "success" | "warning" | "error", title: string, message: string): void {
  ElNotification({ title, message, type, position: "top-left" });
}

async function createKernel(
  config: KernelConfig,
  opts: { autoStart?: boolean } = {},
): Promise<KernelInfo> {
  if (isPending(config.id)) {
    notify(
      "warning",
      "Creation already in progress",
      `Kernel "${config.name}" is already being created — this submission was not applied.`,
    );
    throw new Error(`Kernel "${config.name}" is already being created`);
  }

  pendingCreations.value = [
    ...pendingCreations.value,
    {
      id: config.id,
      name: config.name,
      flavour: config.image_flavour,
      packages: [...config.packages],
      startedAt: Date.now(),
      phase: "creating",
    },
  ];

  try {
    let created: KernelInfo;
    try {
      created = await KernelApi.create(config);
    } catch (error) {
      notify(
        "error",
        "Kernel creation failed",
        `"${config.name}": ${(error as Error).message || "unknown error"}`,
      );
      throw error;
    }

    if (opts.autoStart) {
      pendingCreations.value = pendingCreations.value.map((p) =>
        p.id === config.id ? { ...p, phase: "starting" } : p,
      );
      try {
        created = await KernelApi.start(created.id);
      } catch (error) {
        // The kernel exists and is usable regardless of a start failure.
        notify(
          "warning",
          "Kernel created but failed to start",
          `"${config.name}": ${(error as Error).message}. Start it from the Kernel Manager.`,
        );
        return created;
      }
    }

    notify("success", "Kernel ready", `Kernel "${config.name}" was created.`);
    return created;
  } finally {
    removePending(config.id);
  }
}

export function _resetKernelCreationTracker(): void {
  pendingCreations.value = [];
}

export function useKernelCreationTracker() {
  return { pendingCreations, createKernel, isPending };
}
