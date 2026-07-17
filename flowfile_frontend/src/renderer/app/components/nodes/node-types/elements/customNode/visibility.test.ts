import { describe, expect, it } from "vitest";
import { isSectionVisible } from "./visibility";

const data = {
  main: { show_advanced: true, mode: false },
};

describe("isSectionVisible", () => {
  it("shows a section with no visible_when rule", () => {
    expect(isSectionVisible({ visible_when: undefined }, data)).toBe(true);
  });

  it("shows when the referenced toggle equals the default target (true)", () => {
    expect(isSectionVisible({ visible_when: { field: "main.show_advanced" } }, data)).toBe(true);
    expect(isSectionVisible({ visible_when: { field: "main.mode" } }, data)).toBe(false);
  });

  it("inverts with equals=false (show when the toggle is off)", () => {
    expect(isSectionVisible({ visible_when: { field: "main.mode", equals: false } }, data)).toBe(
      true,
    );
    expect(
      isSectionVisible({ visible_when: { field: "main.show_advanced", equals: false } }, data),
    ).toBe(false);
  });

  it("fails open on an unresolvable reference (missing section/field)", () => {
    expect(isSectionVisible({ visible_when: { field: "nope.missing" } }, data)).toBe(true);
    expect(isSectionVisible({ visible_when: { field: "main.absent" } }, data)).toBe(true);
  });

  it("fails open on a malformed reference (empty or no dot)", () => {
    expect(isSectionVisible({ visible_when: { field: "" } }, data)).toBe(true);
    expect(isSectionVisible({ visible_when: { field: "no_dot" } }, data)).toBe(true);
  });
});
