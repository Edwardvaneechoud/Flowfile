// @vue/test-utils is not a dependency, so the consent dialog's meaningful logic
// is extracted into communityCapabilities.ts and unit-tested here directly
// (the .vue shell is a thin binding over these pure functions).
import { describe, expect, it } from "vitest";

import {
  NO_CAPABILITIES_NOTE,
  capabilityLines,
  environmentNote,
  shortSha,
} from "./communityCapabilities";

describe("capabilityLines", () => {
  it("maps known risk flags to plain-language label + description", () => {
    const lines = capabilityLines(["network", "fs_write", "secrets"]);
    expect(lines.map((l) => l.flag)).toEqual(["network", "fs_write", "secrets"]);
    expect(lines[0].label).toMatch(/network/i);
    expect(lines[0].description.length).toBeGreaterThan(0);
    expect(lines[1].label).toMatch(/writes files/i);
    expect(lines[2].label).toMatch(/secrets/i);
  });

  it("surfaces unknown flags with a generic line rather than dropping them", () => {
    const lines = capabilityLines(["quantum_tunnelling"]);
    expect(lines).toHaveLength(1);
    expect(lines[0].flag).toBe("quantum_tunnelling");
    expect(lines[0].label).toBe("quantum_tunnelling");
    expect(lines[0].description).toMatch(/elevated capability/i);
  });

  it("returns an empty list for no flags (dialog then shows the no-caps note)", () => {
    expect(capabilityLines([])).toEqual([]);
    expect(NO_CAPABILITIES_NOTE).toMatch(/not a safety guarantee/i);
  });
});

describe("environmentNote", () => {
  it("warns about secrets on the local worker", () => {
    const note = environmentNote("local");
    expect(note).toMatch(/worker/i);
    expect(note).toMatch(/secrets/i);
  });

  it("notes kernel isolation and absent secrets", () => {
    const note = environmentNote("kernel");
    expect(note).toMatch(/isolated/i);
    expect(note).toMatch(/not available/i);
  });
});

describe("shortSha", () => {
  it("returns the first 12 hex chars", () => {
    expect(shortSha("0123456789abcdef0123")).toBe("0123456789ab");
  });

  it("is safe on empty input", () => {
    expect(shortSha("")).toBe("");
  });
});
