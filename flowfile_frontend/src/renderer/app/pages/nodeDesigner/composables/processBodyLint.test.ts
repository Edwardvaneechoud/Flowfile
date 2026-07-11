import { describe, it, expect } from "vitest";
import { lintProcessBody } from "./processBodyLint";

// The editor holds the BODY of process(), indented one level. A def/class at
// column 0 escapes the method and becomes a class member — that's what we flag.

describe("lintProcessBody", () => {
  it("passes a normal indented body", () => {
    const body = "    lf = inputs[0]\n    return lf\n";
    expect(lintProcessBody(body)).toEqual([]);
  });

  it("passes a nested helper function inside process", () => {
    const body = "    def helper():\n        return 12\n    return helper()\n";
    expect(lintProcessBody(body)).toEqual([]);
  });

  it("warns on a column-0 def with no self and shows a concrete example", () => {
    const body =
      "    do_something()\n    return inputs[0]\n\n\ndef do_something():\n    return 12\n";
    const issues = lintProcessBody(body);
    expect(issues).toHaveLength(1);
    expect(issues[0].severity).toBe("warning");
    expect(issues[0].message).toContain("def my_function(self):");
    // Anchored at the escaped def, not the in-body call.
    expect(body.slice(issues[0].from, issues[0].to)).toContain("def do_something");
  });

  it("only informs (not warns) on a column-0 def that already has self", () => {
    const body = "    return inputs[0]\n\n\ndef helper(self):\n    return 1\n";
    const issues = lintProcessBody(body);
    expect(issues).toHaveLength(1);
    expect(issues[0].severity).toBe("info");
    expect(issues[0].message).toContain("self.<name>");
  });

  it("flags a column-0 class definition", () => {
    const body = "    return inputs[0]\n\n\nclass Thing:\n    pass\n";
    const issues = lintProcessBody(body);
    expect(issues).toHaveLength(1);
    expect(issues[0].message.toLowerCase()).toContain("class");
  });

  it("flags multiple escaped definitions", () => {
    const body = "    return inputs[0]\n\n\ndef a():\n    return 1\n\n\ndef b():\n    return 2\n";
    expect(lintProcessBody(body)).toHaveLength(2);
  });

  it("flags a decorated column-0 def (@staticmethod helper)", () => {
    const body = "    x = inputs[0]\n@staticmethod\ndef helper():\n    return 1\n    return x\n";
    const issues = lintProcessBody(body);
    expect(issues).toHaveLength(1);
    expect(issues[0].message).toContain("self");
    // Anchored at the decorator line that starts the escaping statement.
    expect(body.slice(issues[0].from, issues[0].to)).toContain("@staticmethod");
  });

  it("informs on a decorated column-0 def that already has self", () => {
    const body = "    return inputs[0]\n@property\ndef p(self):\n    return 1\n";
    const issues = lintProcessBody(body);
    expect(issues).toHaveLength(1);
    expect(issues[0].severity).toBe("info");
    expect(issues[0].message).toContain("self.<name>");
  });

  it("flags a decorated column-0 class", () => {
    const body = "    return inputs[0]\n@register\nclass Thing:\n    pass\n";
    const issues = lintProcessBody(body);
    expect(issues).toHaveLength(1);
    expect(issues[0].message.toLowerCase()).toContain("class");
  });

  it("flags both a bare and a decorated escaped def", () => {
    const body =
      "    return inputs[0]\ndef bare():\n    return 1\n@staticmethod\ndef dec():\n    return 2\n";
    expect(lintProcessBody(body)).toHaveLength(2);
  });

  it("does not false-positive on a def inside a triple-quoted string", () => {
    const body = '    doc = """\ndef fake():\n    pass\n"""\n    return inputs[0]\n';
    expect(lintProcessBody(body)).toEqual([]);
  });

  it("does not false-positive on a def-process mention inside a string", () => {
    // The `def process(` here is string content; only the real escaped def counts.
    const body = '    sql = """\ndef process(x):\n    pass\n"""\n    return inputs[0]\n';
    expect(lintProcessBody(body)).toEqual([]);
  });

  it("returns nothing for an empty body", () => {
    expect(lintProcessBody("")).toEqual([]);
    expect(lintProcessBody("   \n  \n")).toEqual([]);
  });
});
