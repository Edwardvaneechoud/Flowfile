import { describe, it, expect } from "vitest";

import { docSummary, paramDoc, paramName } from "./lspDocRender";

// Shapes as the kernel's _clean_doc hands them back (dedented, numpydoc underlines removed).
const GOOGLE = `Read data from a database using a stored connection.

Creates a database reader node in the flow graph and returns a FlowFrame.

Args:
    connection_name: Name of the stored database connection to use.
    table_name: Name of the table to read from.
    schema_name (str): Database schema name
        (e.g., \`public\` for PostgreSQL).
    **kwargs: Passed on.

Returns:
    FlowFrame: A FlowFrame backed by a database reader node.`;

const NUMPY = `Select columns from this DataFrame.
Returns a new frame.

Parameters
*exprs
    Column(s) to select, specified as positional arguments.
    Accepts expression input.
named_exprs : Expr
    Additional columns to select.

Returns
DataFrame`;

describe("paramName", () => {
  it("strips the annotation, default and star prefix", () => {
    expect(paramName("connection_name: 'str'")).toBe("connection_name");
    expect(paramName("table_name: 'str | None'=None")).toBe("table_name");
    expect(paramName("*exprs")).toBe("exprs");
    expect(paramName("**kwargs")).toBe("kwargs");
  });
});

describe("docSummary", () => {
  it("is the opening paragraph only", () => {
    expect(docSummary(GOOGLE)).toBe("Read data from a database using a stored connection.");
    expect(docSummary(NUMPY)).toBe("Select columns from this DataFrame. Returns a new frame.");
  });

  it("stops at a section title with no blank line before it", () => {
    expect(docSummary("Do a thing.\nArgs:\n    x: y")).toBe("Do a thing.");
  });
});

describe("paramDoc", () => {
  it("reads a Google-style entry", () => {
    expect(paramDoc(GOOGLE, "connection_name")).toBe(
      "Name of the stored database connection to use.",
    );
  });

  it("joins continuation lines and skips a (type) annotation", () => {
    expect(paramDoc(GOOGLE, "schema_name")).toBe(
      "Database schema name (e.g., `public` for PostgreSQL).",
    );
  });

  it("matches starred names", () => {
    expect(paramDoc(GOOGLE, "kwargs")).toBe("Passed on.");
    expect(paramDoc(NUMPY, "exprs")).toBe(
      "Column(s) to select, specified as positional arguments. Accepts expression input.",
    );
  });

  it("reads a numpydoc `name : type` entry", () => {
    expect(paramDoc(NUMPY, "named_exprs")).toBe("Additional columns to select.");
  });

  it("ignores names outside a parameter section and unknown names", () => {
    expect(paramDoc(GOOGLE, "FlowFrame")).toBe("");
    expect(paramDoc(GOOGLE, "query")).toBe("");
    expect(paramDoc(GOOGLE, "")).toBe("");
  });

  it("does not match a longer name sharing the prefix", () => {
    expect(paramDoc(GOOGLE, "table")).toBe("");
  });
});
