// `|` in a source marks the caret; it is stripped before the document is built.
import { describe, expect, it } from "vitest";
import { EditorState } from "@codemirror/state";
import { python } from "@codemirror/lang-python";
import {
  COLUMN_POSITION_METHODS,
  COLUMN_REF_FUNCTIONS,
  resolveColumnContext,
  type ColumnContext,
} from "./dataframeColumnContext";

function resolveAt(source: string): { ctx: ColumnContext | null; doc: string } {
  const pos = source.indexOf("|");
  if (pos < 0) throw new Error(`no caret marker in ${JSON.stringify(source)}`);
  const doc = source.slice(0, pos) + source.slice(pos + 1);
  const state = EditorState.create({ doc, extensions: [python()] });
  return { ctx: resolveColumnContext(state, pos), doc };
}

function hit(source: string): { method: string; receiver: string; prefix: string } {
  const { ctx, doc } = resolveAt(source);
  if (!ctx) throw new Error(`expected a column context for ${JSON.stringify(source)}`);
  if (ctx.receiver.kind !== "expr") {
    throw new Error(`expected an expression receiver for ${JSON.stringify(source)}`);
  }
  return {
    method: ctx.method,
    receiver: doc.slice(ctx.receiver.node.from, ctx.receiver.node.to),
    prefix: ctx.prefix,
  };
}

function isNull(source: string): boolean {
  return resolveAt(source).ctx === null;
}

describe("resolveColumnContext — column positions", () => {
  it("fires for every method in the column-position table", () => {
    for (const method of COLUMN_POSITION_METHODS) {
      expect(hit(`orders.${method}("|")`), method).toEqual({
        method,
        receiver: "orders",
        prefix: "",
      });
    }
  });

  it("fires on rename dict keys but not values", () => {
    expect(hit('orders.rename({"|": "b"})')).toEqual({
      method: "rename",
      receiver: "orders",
      prefix: "",
    });
    expect(hit('orders.rename({"a": "b", "c|"})')).toMatchObject({ method: "rename" });
    expect(isNull('orders.rename({"a": "|"})')).toBe(true);
    expect(isNull('orders.rename({"a": "b|"})')).toBe(true);
  });

  it("fires on an unfinished rename dict, which lezer parses as a set", () => {
    expect(hit('orders.rename({"|')).toEqual({ method: "rename", receiver: "orders", prefix: "" });
    expect(hit('orders.rename({"ab|')).toMatchObject({ method: "rename", prefix: "ab" });
  });

  it("fires on the auto-closed shapes closeBrackets produces", () => {
    expect(hit('orders.rename({"|"})')).toEqual({
      method: "rename",
      receiver: "orders",
      prefix: "",
    });
    expect(hit('orders.rename({"a": "b", "|"})')).toEqual({
      method: "rename",
      receiver: "orders",
      prefix: "",
    });
    expect(isNull('orders.rename({"a": "|"})')).toBe(true);
  });

  it("keeps a set argument to another method on the plain wrapper path", () => {
    expect(hit('orders.select({"|"})')).toEqual({
      method: "select",
      receiver: "orders",
      prefix: "",
    });
    expect(isNull('orders.head({"|"})')).toBe(true);
  });

  it("fires on a subscript with method __getitem__", () => {
    expect(hit('df["|"]')).toEqual({ method: "__getitem__", receiver: "df", prefix: "" });
    expect(hit('df["|')).toEqual({ method: "__getitem__", receiver: "df", prefix: "" });
    expect(hit('orders.select("a")["|"]')).toMatchObject({
      method: "__getitem__",
      receiver: 'orders.select("a")',
    });
  });

  it("resolves the enclosing frame call for every column-reference function", () => {
    for (const fn of COLUMN_REF_FUNCTIONS) {
      expect(hit(`orders.filter(pl.${fn}("|"))`), fn).toEqual({
        method: "filter",
        receiver: "orders",
        prefix: "",
      });
      expect(hit(`orders.filter(${fn}("|"))`), fn).toMatchObject({ receiver: "orders" });
    }
  });

  it("falls back to bare-col with no enclosing frame call", () => {
    for (const source of ['pl.col("|")', 'col("|")', 'x = pl.col("|")', 'pl.exclude("|")']) {
      const { ctx } = resolveAt(source);
      expect(ctx?.receiver.kind, source).toBe("bare-col");
    }
    expect(resolveAt('pl.col("|")').ctx?.method).toBe("col");
  });

  it("keeps the receiver of a chained call", () => {
    expect(hit('orders.group_by("k").agg(pl.col("|"))')).toEqual({
      method: "agg",
      receiver: 'orders.group_by("k")',
      prefix: "",
    });
    expect(hit('orders.select("a").filter("|")')).toMatchObject({
      receiver: 'orders.select("a")',
    });
  });

  it("keeps an aliased variable as the receiver and rejects the alias string itself", () => {
    expect(hit('df2 = orders.select("a")\ndf2.select("|")')).toMatchObject({ receiver: "df2" });
    expect(isNull('orders.select(pl.col("a").alias("|"))')).toBe(true);
  });

  it("handles both quote styles", () => {
    expect(hit("orders.group_by('|')")).toEqual({
      method: "group_by",
      receiver: "orders",
      prefix: "",
    });
    expect(resolveAt("orders.group_by('ab|')").ctx?.quote).toBe("'");
    expect(resolveAt('orders.group_by("ab|")').ctx?.quote).toBe('"');
  });

  it("handles multiline calls and string lists", () => {
    expect(hit('orders.select(\n  "a",\n  "b|",\n)')).toMatchObject({
      method: "select",
      receiver: "orders",
      prefix: "b",
    });
    expect(hit('orders.select(["a", "b|"])')).toMatchObject({ receiver: "orders", prefix: "b" });
    expect(hit('orders.select(("a", "b|"))')).toMatchObject({ receiver: "orders", prefix: "b" });
    expect(hit('orders.select([pl.col("|")])')).toMatchObject({ receiver: "orders" });
  });

  it("handles unfinished strings at the end of the document and of a line", () => {
    const open = resolveAt('orders.select("|');
    expect(open.ctx).toMatchObject({
      method: "select",
      prefix: "",
      contentFrom: 15,
      contentTo: 15,
    });
    const typed = resolveAt('orders.select("ab|');
    expect(typed.ctx).toMatchObject({ prefix: "ab", contentFrom: 15, contentTo: 17 });
    expect(hit('orders.select("ab|\nx = 1')).toMatchObject({ method: "select", prefix: "ab" });
  });

  it("replaces the whole content of a closed string when the caret is mid-content", () => {
    const { ctx } = resolveAt('orders.select("a|bc")');
    expect(ctx).toMatchObject({
      stringFrom: 14,
      stringTo: 19,
      contentFrom: 15,
      contentTo: 18,
      prefix: "a",
    });
  });

  it("returns null outside a column position", () => {
    const sources = [
      "orders.select(|)",
      'orde|rs.select("a")',
      'orders.select("a")|',
      'f"ab|"',
      'orders.select(f"ab|")',
      'orders.select("""ab|""")',
      'orders.select(r"ab|")',
      'flowfile_ctx.read_input("ma|")',
      'read_input("ma|")',
      'print("ab|")',
      'orders.join(other, on="a|")',
      'orders.with_columns(pl.col("a").alias("x|"))',
    ];
    for (const source of sources) expect(isNull(source), source).toBe(true);
  });
});
