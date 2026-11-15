import { describe, expect, it } from "vitest";
import { incompleteLines } from "./importNotice";

describe("incompleteLines", () => {
  it("says nothing when every step finished, or for an older core without the field", () => {
    expect(incompleteLines({ incomplete: [] })).toEqual([]);
    expect(incompleteLines({})).toEqual([]);
  });

  it("names each unfinished step and how to finish it", () => {
    const lines = incompleteLines({
      incomplete: ["Could not write the flow file of 'a': disk full"],
    });
    expect(lines[0]).toBe("The database was updated, but 1 step(s) did not finish:");
    expect(lines).toContain("Could not write the flow file of 'a': disk full");
    expect(lines.at(-1)).toBe("Reload the project from its folder to finish them.");
  });
});
