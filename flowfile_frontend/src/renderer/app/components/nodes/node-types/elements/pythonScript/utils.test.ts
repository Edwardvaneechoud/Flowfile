import { afterEach, describe, expect, it, vi } from "vitest";

import type { ImageFlavour, KernelInfo, KernelState } from "../../../../../types/kernel.types";
import { createPythonScriptNode, pickDefaultKernel } from "./utils";

describe("createPythonScriptNode", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("makes its first cell on a page that is not a secure context", () => {
    // Plain http to a LAN address: the browser offers crypto without randomUUID.
    vi.stubGlobal("crypto", { getRandomValues: globalThis.crypto.getRandomValues });

    const node = createPythonScriptNode(1, 2);

    const cells = node.python_script_input.cells ?? [];
    expect(cells).toHaveLength(1);
    expect(cells[0].id).toMatch(/\S/);
    expect(cells[0].code).toBe(node.python_script_input.code);
  });
});

const kernel = (id: string, image_flavour: ImageFlavour, state: KernelState): KernelInfo => ({
  id,
  name: id,
  state,
  container_id: null,
  port: null,
  packages: [],
  resolved_packages: [],
  memory_gb: 4,
  cpu_cores: 2,
  gpu: false,
  image_flavour,
  custom_image: null,
  image: null,
  created_at: "2026-01-01T00:00:00Z",
  error_message: null,
  kernel_version: null,
});

describe("pickDefaultKernel", () => {
  it("prefers a base kernel over a lite one", () => {
    const picked = pickDefaultKernel([kernel("l", "lite", "idle"), kernel("b", "base", "stopped")]);
    expect(picked?.id).toBe("b");
  });

  it("falls back to a lite kernel when there is no base one", () => {
    const picked = pickDefaultKernel([kernel("m", "ml", "idle"), kernel("l", "lite", "stopped")]);
    expect(picked?.id).toBe("l");
  });

  it("prefers a running kernel within a flavour", () => {
    const picked = pickDefaultKernel([
      kernel("stopped", "base", "stopped"),
      kernel("idle", "base", "idle"),
    ]);
    expect(picked?.id).toBe("idle");
  });

  it("never picks other flavours or kernels in error or being created", () => {
    const picked = pickDefaultKernel([
      kernel("m", "ml", "idle"),
      kernel("n", "notebook", "idle"),
      kernel("c", "custom", "idle"),
      kernel("e", "base", "error"),
      kernel("new", "lite", "creating"),
    ]);
    expect(picked).toBeNull();
  });
});
