import { describe, it, expect } from "vitest";
import type { ImageFlavour, KernelInfo } from "@/types/kernel.types";
import {
  defaultNotebookKernel,
  notebookKernelActionLabel,
  notebookKernelConfig,
  notebookKernelOutdated,
} from "./notebookKernelSetup";

const REPO = "edwardvaneechoud/flowfile-kernel-notebook";

function kernel(
  image: string | null,
  flavour: ImageFlavour = "notebook",
  state: KernelInfo["state"] = "idle",
  id = "k1",
): KernelInfo {
  return { id, name: id, state, packages: [], image_flavour: flavour, image } as unknown as KernelInfo;
}

describe("defaultNotebookKernel", () => {
  it("prefers a running notebook kernel, then one that can start, in list order", () => {
    const stopped = kernel(null, "notebook", "stopped", "a");
    const starting = kernel(null, "notebook", "starting", "b");
    const idle = kernel(null, "notebook", "idle", "c");
    const idle2 = kernel(null, "notebook", "idle", "d");
    expect(defaultNotebookKernel([stopped, starting, idle, idle2])).toBe(idle);
    expect(defaultNotebookKernel([stopped, starting])).toBe(starting);
    expect(defaultNotebookKernel([stopped])).toBe(stopped);
  });

  it("never picks another flavour, an errored or a creating kernel", () => {
    expect(defaultNotebookKernel([kernel(null, "lite", "idle")])).toBeNull();
    expect(defaultNotebookKernel([kernel(null, "notebook", "error")])).toBeNull();
    expect(defaultNotebookKernel([kernel(null, "notebook", "creating")])).toBeNull();
    expect(defaultNotebookKernel([])).toBeNull();
  });
});

describe("notebookKernelConfig", () => {
  it("is the Notebook image with no extra packages", () => {
    expect(notebookKernelConfig([])).toEqual({
      id: "notebook",
      name: "Notebook",
      packages: [],
      cpu_cores: 2,
      memory_gb: 4,
      gpu: false,
      image_flavour: "notebook",
      custom_image: null,
    });
  });

  it("dodges ids that are already taken", () => {
    expect(notebookKernelConfig(["notebook"]).id).toBe("notebook-2");
    expect(notebookKernelConfig(["notebook", "notebook-2"]).id).toBe("notebook-3");
    expect(notebookKernelConfig(["notebook-2", "other"]).id).toBe("notebook");
  });
});

describe("notebookKernelOutdated", () => {
  it("is true only for a notebook kernel on an older release of the current image", () => {
    expect(notebookKernelOutdated(kernel(`${REPO}:0.22.0`), `${REPO}:0.22.1`)).toBe(true);
    expect(notebookKernelOutdated(kernel(`${REPO}:0.22.1`), `${REPO}:0.22.1`)).toBe(false);
    expect(notebookKernelOutdated(kernel("flowfile-kernel-notebook:local"), `${REPO}:0.22.1`)).toBe(false);
    expect(notebookKernelOutdated(kernel(null), `${REPO}:0.22.1`)).toBe(false);
    expect(notebookKernelOutdated(kernel(`${REPO}:0.22.0`), null)).toBe(false);
  });

  it("never flags another flavour", () => {
    expect(notebookKernelOutdated(kernel(`${REPO}:0.22.0`, "lite"), `${REPO}:0.22.1`)).toBe(false);
  });
});

describe("notebookKernelActionLabel", () => {
  const idle = (imageInstalled: boolean | null) =>
    notebookKernelActionLabel({ imageInstalled, phase: "idle", pulling: false });

  it("offers the download only when the Notebook image is known to be missing", () => {
    expect(idle(false)).toBe("Download image and create notebook kernel");
    expect(idle(true)).toBe("Create notebook kernel");
    expect(idle(null)).toBe("Create notebook kernel");
  });

  it("narrates the in-flight phases", () => {
    expect(
      notebookKernelActionLabel({ imageInstalled: false, phase: "creating", pulling: true }),
    ).toBe("Downloading image…");
    expect(
      notebookKernelActionLabel({ imageInstalled: true, phase: "creating", pulling: false }),
    ).toBe("Creating…");
    expect(
      notebookKernelActionLabel({ imageInstalled: true, phase: "starting", pulling: false }),
    ).toBe("Starting…");
    expect(
      notebookKernelActionLabel({ imageInstalled: true, phase: "restarting", pulling: true }),
    ).toBe("Downloading image…");
    expect(
      notebookKernelActionLabel({ imageInstalled: true, phase: "restarting", pulling: false }),
    ).toBe("Restarting…");
  });
});
