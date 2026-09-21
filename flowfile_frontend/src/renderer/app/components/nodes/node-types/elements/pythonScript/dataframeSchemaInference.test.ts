// `|` in a source marks the caret; the receiver is taken from resolveColumnContext,
// exactly as the completion source will do it.
import { describe, expect, it } from "vitest";
import { EditorState } from "@codemirror/state";
import { python } from "@codemirror/lang-python";
import { resolveColumnContext } from "./dataframeColumnContext";
import {
  inferSchema,
  scanCatalogRefs,
  type CellSource,
  type InferResult,
  type InferredSchema,
  type SchemaSources,
} from "./dataframeSchemaInference";
import { catalogRefKey, type FrameKind, type SchemaColumn } from "./dataframeSchemaTypes";

const ORDERS: SchemaColumn[] = [
  { name: "id", dtype: "Int64" },
  { name: "amount", dtype: "Float64" },
  { name: "region", dtype: "String" },
];

const ORDERS_CELL: CellSource = {
  id: "c1",
  code: 'orders = flowfile_ctx.read_catalog_table("orders")',
};

interface SourceOverrides {
  runtime?: Record<string, { columns: SchemaColumn[]; kind: FrameKind }>;
  catalogRefs?: Record<string, SchemaColumn[] | null>;
  inputs?: Record<string, SchemaColumn[]>;
  outdated?: string[];
}

function makeSources(overrides: SourceOverrides = {}): SchemaSources {
  const runtime = overrides.runtime ?? {};
  const catalogRefs = overrides.catalogRefs ?? { orders: ORDERS };
  const inputs = overrides.inputs ?? {};
  const outdated = new Set(overrides.outdated ?? []);
  return {
    runtime: (name) => runtime[name] ?? null,
    catalogRef: (key) => (key in catalogRefs ? catalogRefs[key] : undefined),
    input: (name) => inputs[name] ?? null,
    isCellOutdated: (cellId) => cellId !== null && outdated.has(cellId),
  };
}

function run(
  source: string,
  opts: { cells?: CellSource[]; cellId?: string | null; sources?: SourceOverrides } = {},
): InferResult {
  const pos = source.indexOf("|");
  if (pos < 0) throw new Error(`no caret marker in ${JSON.stringify(source)}`);
  const doc = source.slice(0, pos) + source.slice(pos + 1);
  const state = EditorState.create({ doc, extensions: [python()] });
  const ctx = resolveColumnContext(state, pos);
  if (!ctx || ctx.receiver.kind !== "expr") {
    throw new Error(`no expression receiver for ${JSON.stringify(source)}`);
  }
  return inferSchema(
    ctx.receiver.node,
    doc,
    pos,
    opts.cells ?? [ORDERS_CELL],
    opts.cellId ?? "c2",
    makeSources(opts.sources),
  );
}

function schemaOf(source: string, opts: Parameters<typeof run>[1] = {}): InferredSchema {
  const { schema } = run(source, opts);
  if (!schema) throw new Error(`expected a schema for ${JSON.stringify(source)}`);
  return schema;
}

function names(source: string, opts: Parameters<typeof run>[1] = {}): string[] {
  return schemaOf(source, opts).columns.map((column) => column.name);
}

describe("inferSchema — roots", () => {
  it("resolves a catalog root and labels it", () => {
    const schema = schemaOf('flowfile_ctx.read_catalog_table("orders").select("|")');
    expect(schema.columns).toEqual(ORDERS);
    expect(schema.kind).toBe("LazyFrame");
    expect(schema.provenance).toBe("source");
    expect(schema.sourceLabel).toBe("orders (catalog)");
  });

  it("resolves the schema= and namespace_id= catalog forms by ref key", () => {
    const sources = { catalogRefs: { "sales.orders": ORDERS, "ns:7:orders": ORDERS } };
    expect(
      names('flowfile_ctx.read_catalog_table("orders", schema="sales").select("|")', { sources }),
    ).toEqual(["id", "amount", "region"]);
    expect(
      names('flowfile_ctx.read_catalog_table("orders", namespace_id=7).select("|")', { sources }),
    ).toEqual(["id", "amount", "region"]);
  });

  it("resolves the get_catalog chain forms", () => {
    const sources = { catalogRefs: { "cat.sales.orders": ORDERS } };
    const chain = 'flowfile_ctx.get_catalog("cat").get_schema("sales")';
    expect(names(`${chain}.read_table("orders").select("|")`, { sources })).toHaveLength(3);
    expect(names(`${chain}.get_table_ref("orders").read().select("|")`, { sources })).toHaveLength(
      3,
    );
  });

  it("reports an unresolved catalog ref as pending and yields no schema", () => {
    const result = run('flowfile_ctx.read_catalog_table("unseen").select("|")');
    expect(result.schema).toBeNull();
    expect(result.pendingRefs).toEqual([{ table: "unseen" }]);
  });

  it("treats a catalog ref resolved to null as unresolved without re-queueing it", () => {
    const result = run('flowfile_ctx.read_catalog_table("gone").select("|")', {
      sources: { catalogRefs: { gone: null } },
    });
    expect(result.schema).toBeNull();
    expect(result.pendingRefs).toEqual([]);
  });

  it("resolves node input roots", () => {
    const inputs = { main: ORDERS, lookup: [{ name: "id", dtype: "String" }] };
    expect(
      schemaOf('flowfile_ctx.read_input("main").select("|")', { sources: { inputs } }),
    ).toMatchObject({ sourceLabel: "main (input)", kind: "LazyFrame" });
    expect(names('flowfile_ctx.read_input().select("|")', { sources: { inputs } })).toHaveLength(3);
    expect(names('read_first("lookup").select("|")', { sources: { inputs } })).toEqual(["id"]);
    expect(
      run('flowfile_ctx.read_input(name).select("|")', { sources: { inputs } }).schema,
    ).toBeNull();
  });

  it("reads column names off a literal frame constructor", () => {
    for (const ctor of ["DataFrame", "LazyFrame", "from_dict"]) {
      const schema = schemaOf(`pl.${ctor}({"a": [1], "b": [2]}).select("|")`);
      expect(schema.columns, ctor).toEqual([
        { name: "a", dtype: "" },
        { name: "b", dtype: "" },
      ]);
      expect(schema.kind, ctor).toBe(ctor === "LazyFrame" ? "LazyFrame" : "DataFrame");
    }
    expect(run('pl.DataFrame(data).select("|")').schema).toBeNull();
  });
});

describe("inferSchema — method rules", () => {
  it("passes row-only operations through unchanged", () => {
    const rowOnly = [
      "filter",
      "sort",
      "head",
      "tail",
      "limit",
      "slice",
      "unique",
      "drop_nulls",
      "reverse",
      "clone",
      "cache",
      "rechunk",
    ];
    for (const method of rowOnly) {
      expect(schemaOf(`orders.${method}().select("|")`).columns, method).toEqual(ORDERS);
    }
  });

  it("changes only the kind on collect and lazy", () => {
    expect(schemaOf('orders.collect().select("|")')).toMatchObject({
      kind: "DataFrame",
      columns: ORDERS,
    });
    expect(schemaOf('orders.collect().lazy().select("|")').kind).toBe("LazyFrame");
  });

  it("subsets on select in argument order, for every accepted argument form", () => {
    expect(names('orders.select("region", "id").select("|")')).toEqual(["region", "id"]);
    expect(names('orders.select(["id", "amount"]).select("|")')).toEqual(["id", "amount"]);
    expect(names('orders.select(pl.col("amount")).select("|")')).toEqual(["amount"]);
    expect(names('orders.select(col("amount")).select("|")')).toEqual(["amount"]);
    expect(schemaOf('orders.select(pl.col("amount").alias("total")).select("|")').columns).toEqual([
      { name: "total", dtype: "Float64" },
    ]);
    expect(schemaOf('orders.select(total=pl.col("amount")).select("|")').columns).toEqual([
      { name: "total", dtype: "Float64" },
    ]);
  });

  it("stops on a select argument that is not a literal column", () => {
    expect(run('orders.select("nope").select("|")').schema).toBeNull();
    expect(run('orders.select(pl.col("amount") * 2).select("|")').schema).toBeNull();
    expect(run('orders.select(cols).select("|")').schema).toBeNull();
  });

  it("renames by literal mapping and keeps the dtype", () => {
    expect(schemaOf('orders.rename({"amount": "total"}).select("|")').columns).toEqual([
      { name: "id", dtype: "Int64" },
      { name: "total", dtype: "Float64" },
      { name: "region", dtype: "String" },
    ]);
    expect(run('orders.rename({"nope": "total"}).select("|")').schema).toBeNull();
    expect(run('orders.rename(mapping).select("|")').schema).toBeNull();
  });

  it("drops literal names", () => {
    expect(names('orders.drop("region").select("|")')).toEqual(["id", "amount"]);
    expect(names('orders.drop("id", ["region"]).select("|")')).toEqual(["amount"]);
    expect(run('orders.drop("nope").select("|")').schema).toBeNull();
  });

  it("passes group_by through flagged grouped and stops at agg", () => {
    const grouped = schemaOf('orders.group_by("region").agg(pl.col("|"))');
    expect(grouped.columns).toEqual(ORDERS);
    expect(grouped.grouped).toBe(true);
    expect(run('orders.group_by("region").agg(pl.sum("amount")).select("|")').schema).toBeNull();
  });

  it("stops on transforms that can change the schema unpredictably", () => {
    expect(run('orders.with_columns(pl.col("amount") * 2).select("|")').schema).toBeNull();
    expect(run('orders.join(other, on="id").select("|")').schema).toBeNull();
    expect(run('orders.pivot(on="region").select("|")').schema).toBeNull();
  });
});

describe("inferSchema — assignments", () => {
  it("resolves an alias chain across cells in notebook order", () => {
    const cells = [ORDERS_CELL, { id: "c2", code: 'trimmed = orders.drop("region")' }];
    expect(names('trimmed.select("|")', { cells, cellId: "c3" })).toEqual(["id", "amount"]);
  });

  it("uses the previous binding when a variable is reassigned from itself", () => {
    const source = 'df = orders.select("id", "amount")\ndf = df.drop("amount")\ndf.select("|")';
    expect(names(source)).toEqual(["id"]);
  });

  it("uses the last assignment before the caret, not a later one", () => {
    const source = 'df = orders.drop("region")\ndf.select("|")\ndf = orders.drop("id")';
    expect(names(source)).toEqual(["id", "amount"]);
  });

  it("labels a source-resolved variable with its own name", () => {
    expect(schemaOf('df = orders.drop("id")\ndf.select("|")')).toMatchObject({
      sourceLabel: "df",
      provenance: "source",
      outdated: false,
    });
  });

  it("ignores tuple, augmented and nested assignments", () => {
    expect(run('a, df = 1, orders\ndf.select("|")').schema).toBeNull();
    expect(run('df += orders\ndf.select("|")').schema).toBeNull();
    expect(run('if True:\n    df = orders\ndf.select("|")').schema).toBeNull();
  });
});

describe("inferSchema — runtime fallback", () => {
  const runtime = { df: { columns: ORDERS, kind: "DataFrame" as FrameKind } };

  it("falls back to the last run when the source chain does not resolve", () => {
    const cells = [{ id: "c1", code: "df = load_from_somewhere()" }];
    const schema = schemaOf('df.select("|")', { cells, sources: { runtime } });
    expect(schema).toMatchObject({
      provenance: "runtime",
      kind: "DataFrame",
      sourceLabel: "df (last run)",
      outdated: false,
    });
  });

  it("marks the label outdated when the defining cell is outdated", () => {
    const cells = [{ id: "c1", code: "df = load_from_somewhere()" }];
    const schema = schemaOf('df.select("|")', {
      cells,
      sources: { runtime, outdated: ["c1"] },
    });
    expect(schema.sourceLabel).toBe("df (last run, outdated)");
    expect(schema.outdated).toBe(true);
  });

  it("stays plain last run when the name is assigned nowhere in the visible source", () => {
    const schema = schemaOf('df.select("|")', {
      cells: [],
      sources: { runtime, outdated: ["c1", "c2"] },
    });
    expect(schema.sourceLabel).toBe("df (last run)");
    expect(schema.outdated).toBe(false);
  });

  it("prefers a resolved source chain over the runtime entry", () => {
    const schema = schemaOf('orders.drop("region").select("|")', { sources: { runtime } });
    expect(schema.provenance).toBe("source");
    expect(schema.columns).toEqual([
      { name: "id", dtype: "Int64" },
      { name: "amount", dtype: "Float64" },
    ]);
  });

  it("takes the runtime dtypes for a literal frame with the same columns", () => {
    const cells = [{ id: "c1", code: 'df = pl.DataFrame({"a": [1], "b": [2]})' }];
    const literalRuntime = {
      df: {
        columns: [
          { name: "a", dtype: "Int64" },
          { name: "b", dtype: "String" },
        ],
        kind: "DataFrame" as FrameKind,
      },
    };
    const schema = schemaOf('df.select("|")', { cells, sources: { runtime: literalRuntime } });
    expect(schema.provenance).toBe("runtime");
    expect(schema.sourceLabel).toBe("df (last run)");
    expect(schema.columns).toEqual(literalRuntime.df.columns);

    const chained = schemaOf('df.select("|")', {
      cells: [{ id: "c1", code: 'df = pl.DataFrame({"a": [1], "b": [2]}).select("a", "b")' }],
      sources: { runtime: literalRuntime, outdated: ["c1"] },
    });
    expect(chained.sourceLabel).toBe("df (last run, outdated)");
    expect(chained.columns).toEqual(literalRuntime.df.columns);
  });

  it("keeps the literal names when the runtime frame has a different column set", () => {
    const cells = [{ id: "c1", code: 'df = pl.DataFrame({"a": [1], "b": [2]})' }];
    const schema = schemaOf('df.select("|")', {
      cells,
      sources: { runtime: { df: { columns: ORDERS, kind: "DataFrame" as FrameKind } } },
    });
    expect(schema.provenance).toBe("source");
    expect(schema.sourceLabel).toBe("df");
    expect(schema.columns).toEqual([
      { name: "a", dtype: "" },
      { name: "b", dtype: "" },
    ]);
  });

  it("keeps a source chain that already knows its dtypes", () => {
    const cells = [ORDERS_CELL];
    const schema = schemaOf('orders.select("|")', {
      cells,
      sources: { runtime: { orders: { columns: ORDERS, kind: "DataFrame" as FrameKind } } },
    });
    expect(schema.provenance).toBe("source");
    expect(schema.sourceLabel).toBe("orders");
  });

  it("carries the runtime label through a supported transform", () => {
    const cells = [{ id: "c1", code: "df = load_from_somewhere()" }];
    const schema = schemaOf('df.drop("region").select("|")', {
      cells,
      sources: { runtime, outdated: ["c1"] },
    });
    expect(schema.sourceLabel).toBe("df (last run, outdated)");
    expect(schema.columns.map((column) => column.name)).toEqual(["id", "amount"]);
  });
});

describe("scanCatalogRefs", () => {
  it("finds every literal catalog root form in a cell", () => {
    const code = [
      'a = flowfile_ctx.read_catalog_table("t1")',
      'b = flowfile_ctx.read_catalog_table("t2", schema="s")',
      'c = flowfile_ctx.read_catalog_table("t3", namespace_id=7)',
      'd = flowfile_ctx.get_catalog("c1").get_schema("s1").read_table("t4")',
      'e = flowfile_ctx.get_catalog("c1").get_schema("s1").get_table_ref("t5").read()',
      "f = flowfile_ctx.read_catalog_table(name)",
      'g = flowfile_ctx.read_input("main")',
    ].join("\n");
    expect(scanCatalogRefs(code).map(catalogRefKey)).toEqual([
      "t1",
      "s.t2",
      "ns:7:t3",
      "c1.s1.t4",
      "c1.s1.t5",
    ]);
  });

  it("deduplicates repeated references", () => {
    const code =
      'a = flowfile_ctx.read_catalog_table("t")\nb = flowfile_ctx.read_catalog_table("t")';
    expect(scanCatalogRefs(code)).toEqual([{ table: "t" }]);
  });
});
