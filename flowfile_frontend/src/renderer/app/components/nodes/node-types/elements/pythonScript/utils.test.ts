import { afterEach, describe, expect, it, vi } from "vitest";

import { createPythonScriptNode } from "./utils";

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
