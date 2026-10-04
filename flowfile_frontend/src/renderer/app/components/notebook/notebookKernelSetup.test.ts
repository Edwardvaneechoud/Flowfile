import { describe, it, expect } from "vitest";
import type { KernelInfo } from "@/types/kernel.types";
import {
  flowfileVersionOf,
  notebookKernelActionLabel,
  notebookKernelConfig,
  notebookKernelOutdated,
} from "./notebookKernelSetup";

function kernel(packages: string[]): KernelInfo {
  return { id: "k1", name: "k1", state: "idle", packages } as KernelInfo;
}

describe("notebookKernelConfig", () => {
  it("pins flowfile to the app version on the Lite image", () => {
    expect(notebookKernelConfig("0.22.0", [])).toEqual({
      id: "notebook",
      name: "Notebook",
      packages: ["flowfile==0.22.0"],
      cpu_cores: 2,
      memory_gb: 4,
      gpu: false,
      image_flavour: "lite",
      custom_image: null,
    });
  });

  it("leaves flowfile unpinned when the app version is unknown", () => {
    expect(notebookKernelConfig("", []).packages).toEqual(["flowfile"]);
  });

  it("dodges ids that are already taken", () => {
    expect(notebookKernelConfig("0.22.0", ["notebook"]).id).toBe("notebook-2");
    expect(notebookKernelConfig("0.22.0", ["notebook", "notebook-2"]).id).toBe("notebook-3");
    expect(notebookKernelConfig("0.22.0", ["notebook-2", "other"]).id).toBe("notebook");
  });
});

describe("flowfileVersionOf", () => {
  it("reads a flowfile== pin, tolerating spaces", () => {
    expect(flowfileVersionOf(kernel(["polars", "flowfile==0.21.0"]))).toBe("0.21.0");
    expect(flowfileVersionOf(kernel([" flowfile == 0.21.0 "]))).toBe("0.21.0");
    expect(flowfileVersionOf(kernel(["FlowFile==0.21.0"]))).toBe("0.21.0");
  });

  it("is null when flowfile is unpinned or absent", () => {
    expect(flowfileVersionOf(kernel(["flowfile"]))).toBeNull();
    expect(flowfileVersionOf(kernel(["flowfile>=0.21"]))).toBeNull();
    expect(flowfileVersionOf(kernel(["polars"]))).toBeNull();
    expect(flowfileVersionOf(kernel(["flowfile-extras==1.0"]))).toBeNull();
    expect(flowfileVersionOf(kernel([]))).toBeNull();
  });
});

describe("notebookKernelOutdated", () => {
  it("is true only when both versions are known and differ", () => {
    expect(notebookKernelOutdated(kernel(["flowfile==0.21.0"]), "0.22.0")).toBe(true);
    expect(notebookKernelOutdated(kernel(["flowfile==0.22.0"]), "0.22.0")).toBe(false);
    expect(notebookKernelOutdated(kernel(["flowfile"]), "0.22.0")).toBe(false);
    expect(notebookKernelOutdated(kernel(["polars"]), "0.22.0")).toBe(false);
    expect(notebookKernelOutdated(kernel(["flowfile==0.21.0"]), "")).toBe(false);
  });
});

describe("notebookKernelActionLabel", () => {
  const idle = (imageInstalled: boolean | null) =>
    notebookKernelActionLabel({ imageInstalled, phase: "idle", pulling: false });

  it("offers the download only when the Lite image is known to be missing", () => {
    expect(idle(false)).toBe("Download image and create notebook kernel");
    expect(idle(true)).toBe("Create notebook kernel");
    expect(idle(null)).toBe("Create notebook kernel");
  });

  it("narrates the in-flight phases", () => {
    expect(
      notebookKernelActionLabel({ imageInstalled: false, phase: "creating", pulling: true }),
    ).toBe("Downloading image…");
    expect(
      notebookKernelActionLabel({ imageInstalled: false, phase: "creating", pulling: false }),
    ).toBe("Installing flowfile… (about 2 minutes)");
    expect(
      notebookKernelActionLabel({ imageInstalled: true, phase: "starting", pulling: false }),
    ).toBe("Starting…");
    expect(
      notebookKernelActionLabel({ imageInstalled: true, phase: "updating", pulling: false }),
    ).toBe("Updating…");
  });
});
