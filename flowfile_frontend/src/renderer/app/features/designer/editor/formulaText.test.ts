import { describe, it, expect } from "vitest";
import { isInsideStringOrComment } from "./formulaText";

describe("isInsideStringOrComment", () => {
  it("is false in plain code", () => {
    expect(isInsideStringOrComment("to_date([order_date])", 21)).toBe(false);
    expect(isInsideStringOrComment("t", 1)).toBe(false);
    expect(isInsideStringOrComment("", 0)).toBe(false);
  });

  it("is true inside an open double- or single-quoted string", () => {
    expect(isInsideStringOrComment('"t', 2)).toBe(true);
    expect(isInsideStringOrComment("concat([a], 'ta", 15)).toBe(true);
    expect(isInsideStringOrComment('"[col', 5)).toBe(true);
  });

  it("is false once the string is closed", () => {
    expect(isInsideStringOrComment('"text"', 6)).toBe(false);
    expect(isInsideStringOrComment('"a" + t', 7)).toBe(false);
  });

  it("ignores the other quote kind inside a string", () => {
    expect(isInsideStringOrComment(`"it's"`, 6)).toBe(false);
    expect(isInsideStringOrComment(`"it's`, 5)).toBe(true);
    expect(isInsideStringOrComment(`'say "hi"'`, 10)).toBe(false);
  });

  it("is true after a // comment marker, but not for // inside a string", () => {
    expect(isInsideStringOrComment("[a] // ta", 9)).toBe(true);
    expect(isInsideStringOrComment('"http://x" + t', 14)).toBe(false);
  });

  it("only looks at the text before the cursor", () => {
    expect(isInsideStringOrComment('t + "x"', 1)).toBe(false);
    expect(isInsideStringOrComment('"x" + t', 1)).toBe(true);
  });
});
