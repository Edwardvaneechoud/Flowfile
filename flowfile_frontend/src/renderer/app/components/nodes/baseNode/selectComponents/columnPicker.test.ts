import { describe, expect, it } from "vitest";

import {
  COLUMNS_MIN_PX,
  SETTINGS_STRIP_PX,
  capUsageChips,
  clampSettingsHeight,
  dropZoneAt,
  pluralize,
  shiftAfterRemoval,
  withoutRows,
} from "./columnPicker";

describe("clampSettingsHeight", () => {
  it("never goes below the bare strip", () => {
    expect(clampSettingsHeight(-50, 400)).toBe(SETTINGS_STRIP_PX);
    expect(clampSettingsHeight(10, 400)).toBe(SETTINGS_STRIP_PX);
  });

  it("leaves the column list its floor", () => {
    expect(clampSettingsHeight(1000, 400)).toBe(400 - COLUMNS_MIN_PX);
    expect(clampSettingsHeight(200, 400)).toBe(200);
  });

  it("keeps the strip even when the card is shorter than both floors", () => {
    expect(clampSettingsHeight(80, 100)).toBe(SETTINGS_STRIP_PX);
  });

  it("rounds to whole pixels so the flex basis never carries subpixel drift", () => {
    expect(clampSettingsHeight(150.6, 400)).toBe(151);
  });
});

describe("dropZoneAt", () => {
  const rect = { left: 100, width: 300 };
  const halves = [
    { value: "a", label: "A" },
    { value: "b", label: "B" },
  ];

  it("splits the pane into equal bands", () => {
    expect(dropZoneAt(rect, 100, halves)?.value).toBe("a");
    expect(dropZoneAt(rect, 249, halves)?.value).toBe("a");
    expect(dropZoneAt(rect, 250, halves)?.value).toBe("b");
    const thirds = [...halves, { value: "c", label: "C" }];
    expect(dropZoneAt(rect, 199, thirds)?.value).toBe("a");
    expect(dropZoneAt(rect, 200, thirds)?.value).toBe("b");
    expect(dropZoneAt(rect, 399, thirds)?.value).toBe("c");
  });

  it("clamps pointers just outside the pane onto the edge bands", () => {
    expect(dropZoneAt(rect, 90, halves)?.value).toBe("a");
    expect(dropZoneAt(rect, 410, halves)?.value).toBe("b");
  });

  it("returns nothing for a disabled band or an empty zone list", () => {
    expect(dropZoneAt(rect, 300, [halves[0], { ...halves[1], disabled: true }])).toBeNull();
    expect(dropZoneAt(rect, 300, [])).toBeNull();
  });
});

describe("withoutRows", () => {
  it("drops the given indices and keeps the rest in order", () => {
    expect(withoutRows(["a", "b", "c", "d"], [1, 3])).toEqual(["a", "c"]);
    expect(withoutRows(["a", "b"], [])).toEqual(["a", "b"]);
  });
});

describe("shiftAfterRemoval", () => {
  it("drops removed indices and shifts the survivors down", () => {
    expect(shiftAfterRemoval([0, 2, 3], [1])).toEqual([0, 1, 2]);
    expect(shiftAfterRemoval([4], [0, 2])).toEqual([2]);
  });

  it("is empty when the selection itself was removed", () => {
    expect(shiftAfterRemoval([1, 2], [1, 2])).toEqual([]);
  });
});

describe("capUsageChips", () => {
  const chip = (label: string, row: number) => ({ label, title: label, isKey: false, rows: [row] });

  it("passes short lists through and folds the rest into a +N chip", () => {
    expect(capUsageChips([chip("a", 0), chip("b", 1)])).toHaveLength(2);
    const capped = capUsageChips([chip("a", 0), chip("b", 1), chip("c", 2), chip("d", 3)]);
    expect(capped.map((c) => c.label)).toEqual(["a", "b", "+2"]);
    expect(capped[2]).toMatchObject({ title: "c, d", rows: [2, 3] });
  });
});

describe("pluralize", () => {
  it("handles the singular and an irregular plural", () => {
    expect(pluralize(1, "key")).toBe("1 key");
    expect(pluralize(2, "key")).toBe("2 keys");
    expect(pluralize(0, "entry", "entries")).toBe("0 entries");
  });
});
