import { describe, expect, it } from "vitest";
import { isMultiSelectClick, multiSelectionKeys } from "./multiSelect";

describe("multiSelectionKeys", () => {
  it("adds Alt to the platform's own multi-select key", () => {
    expect(multiSelectionKeys(true)).toEqual(["Alt", "Meta"]);
    expect(multiSelectionKeys(false)).toEqual(["Alt", "Control"]);
  });
});

describe("isMultiSelectClick", () => {
  it("treats an Alt click as multi-select on every platform", () => {
    expect(isMultiSelectClick({ altKey: true }, true)).toBe(true);
    expect(isMultiSelectClick({ altKey: true }, false)).toBe(true);
  });

  it("honours only the platform's own command modifier", () => {
    expect(isMultiSelectClick({ metaKey: true }, true)).toBe(true);
    expect(isMultiSelectClick({ ctrlKey: true }, true)).toBe(false);
    expect(isMultiSelectClick({ ctrlKey: true }, false)).toBe(true);
    expect(isMultiSelectClick({ metaKey: true }, false)).toBe(false);
  });

  it("leaves a plain click or a missing event alone", () => {
    expect(isMultiSelectClick({}, true)).toBe(false);
    expect(isMultiSelectClick({ altKey: false, metaKey: false, ctrlKey: false }, false)).toBe(
      false,
    );
    expect(isMultiSelectClick(undefined)).toBe(false);
  });
});
