import { describe, it, expect } from "vitest";
import { applySelectPositions, createSelectInput } from "./nodeSelectLogic";
import type { FileColumn, SelectInput } from "../../../../types/node.types";

const column = (name: string, data_type: string): FileColumn => ({ name, data_type }) as FileColumn;

const input = (old_name: string, data_type: string, position: number): SelectInput => {
  const selectInput = createSelectInput(old_name, data_type, position, position);
  return selectInput;
};

describe("applySelectPositions", () => {
  it("orders by position and re-stamps position as the array index", () => {
    const inputs = [input("c", "String", 2), input("a", "String", 0), input("b", "String", 1)];

    const result = applySelectPositions(inputs, [
      column("a", "String"),
      column("b", "String"),
      column("c", "String"),
    ]);

    expect(result.map((i) => i.old_name)).toEqual(["a", "b", "c"]);
    expect(result.map((i) => i.position)).toEqual([0, 1, 2]);
  });

  it("keeps original_position 0 for the first upstream column after a reorder", () => {
    // Regression: `original_index || index` treated a legitimate 0 as falsy and
    // fell back to the loop index, so moving column "a" to the bottom of the list
    // rewrote its original_position from 0 to 2.
    const inputs = [input("b", "String", 0), input("c", "String", 1), input("a", "String", 2)];

    applySelectPositions(inputs, [
      column("a", "String"),
      column("b", "String"),
      column("c", "String"),
    ]);

    const movedColumn = inputs.find((i) => i.old_name === "a");
    expect(movedColumn?.original_position).toBe(0);
    expect(movedColumn?.position).toBe(2);
  });

  it("compares data types against the schema entry matching by name, not by loop index", () => {
    // Regression: the lookup used the loop index, so after a reorder every row was
    // compared against whichever column happened to sit at the same position.
    const inputs = [input("b", "Int64", 0), input("a", "String", 1)];

    applySelectPositions(inputs, [column("a", "String"), column("b", "Int64")]);

    expect(inputs.map((i) => [i.old_name, i.data_type_change])).toEqual([
      ["b", false],
      ["a", false],
    ]);
  });

  it("flags a genuine data type change on both flags", () => {
    const inputs = [input("a", "String", 0)];
    inputs[0].data_type = "Int64";

    applySelectPositions(inputs, [column("a", "String")]);

    expect(inputs[0].data_type_change).toBe(true);
    expect(inputs[0].is_altered).toBe(true);
  });

  it("treats a rename as altered but not as a data type change", () => {
    // Matches the backend contract: is_altered covers renames too, while
    // data_type_change is what drives the cast and the changed-cell marker.
    const inputs = [input("a", "String", 0)];
    inputs[0].new_name = "renamed";

    applySelectPositions(inputs, [column("a", "String")]);

    expect(inputs[0].data_type_change).toBe(false);
    expect(inputs[0].is_altered).toBe(true);
  });

  it("still reindexes position for columns missing from the upstream schema", () => {
    const inputs = [input("gone", "String", 0), input("a", "String", 1)];

    applySelectPositions(inputs, [column("a", "String")]);

    expect(inputs.map((i) => i.position)).toEqual([0, 1]);
    // The vanished column keeps whatever original_position it was loaded with.
    expect(inputs[0].original_position).toBe(0);
  });

  it("reindexes position when there is no upstream schema at all", () => {
    const inputs = [input("b", "String", 5), input("a", "String", 1)];

    applySelectPositions(inputs, undefined);

    expect(inputs.map((i) => [i.old_name, i.position])).toEqual([
      ["a", 0],
      ["b", 1],
    ]);
  });

  it("preserves object identity so the in-place position write reaches the parent", () => {
    const first = input("b", "String", 1);
    const second = input("a", "String", 0);
    const inputs = [first, second];

    const result = applySelectPositions(inputs, [column("a", "String"), column("b", "String")]);

    expect(result).toBe(inputs);
    expect(result[0]).toBe(second);
    expect(result[1]).toBe(first);
  });
});
