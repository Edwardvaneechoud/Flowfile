import { describe, it, expect } from "vitest";
import { EditorState } from "@codemirror/state";
import { CompletionContext } from "@codemirror/autocomplete";
import {
  computeNewlineIndent,
  inferLocalTypes,
  makeLocalVariableSource,
  makeSettingsFieldSnippetSource,
  memberCompletionsFor,
  normalizePyType,
  precededByDot,
  pyType,
  scanAssignments,
  scanLocalSymbols,
  scanSelfMethods,
  settingsFieldAccessor,
  usePolarsAutocompletion,
} from "./usePolarsAutocompletion";
import type { SectionState } from "../designerState";

const localVariableSource = makeLocalVariableSource(() => []);

const oneSliderField = (): SectionState[] => [
  {
    name: "settings",
    title: "Settings",
    description: null,
    hidden: false,
    layout: "vertical",
    components: [
      {
        component_type: "SliderInput",
        name: "slider_input_1",
        label: "Slider input 1",
        min_value: 0,
        max_value: 100,
        step: 1,
      },
    ],
  },
];

const oneColumnActionField = (): SectionState[] => [
  {
    name: "agg",
    title: "Agg",
    description: null,
    hidden: false,
    layout: "vertical",
    components: [
      {
        component_type: "ColumnActionInput",
        name: "column_actions",
        label: "Column actions",
        actions: [{ value: "sum", label: "sum" }],
        output_name_template: "{column}_{action}",
        show_group_by: true,
        show_order_by: false,
        data_types: "ALL",
      },
    ],
  },
];

function ctxFor(code: string, pos: number = code.length, explicit = false): CompletionContext {
  const state = EditorState.create({ doc: code });
  return new CompletionContext(state, pos, explicit);
}

const names = (doc: string) =>
  scanLocalSymbols(doc)
    .map((s) => s.name)
    .sort();

describe("scanLocalSymbols", () => {
  it("finds simple and annotated assignments", () => {
    expect(names("a = 1\ncount: int = 5")).toEqual(["a", "count"]);
  });

  it("finds tuple-unpacking targets", () => {
    expect(names("x, y = foo()")).toEqual(["x", "y"]);
    expect(names("(first, second) = pair")).toEqual(["first", "second"]);
  });

  it("finds for-loop and with-as bindings", () => {
    expect(names("for i in range(3):")).toEqual(["i"]);
    expect(names("for k, v in d.items():")).toEqual(["k", "v"]);
    expect(names("with open('f') as fh:")).toEqual(["fh"]);
  });

  it("tags def and class symbols with their kind", () => {
    const syms = scanLocalSymbols("def helper():\n    pass\nclass Foo:\n    pass");
    expect(syms.find((s) => s.name === "helper")?.type).toBe("function");
    expect(syms.find((s) => s.name === "Foo")?.type).toBe("class");
  });

  it("does not treat comparisons as assignments", () => {
    expect(names("if a == b:\n    c = 1")).toEqual(["c"]);
    expect(names("x >= y")).toEqual([]);
  });

  it("skips Python keywords (return, self, ...)", () => {
    expect(names("return lf")).toEqual([]);
    expect(names("self = 1")).toEqual([]);
  });

  it("de-duplicates repeated bindings", () => {
    expect(names("lf = inputs[0]\nlf = lf.filter(pl.col('x') > 0)")).toEqual(["lf"]);
  });
});

describe("precededByDot", () => {
  it("is true right after a member-access dot", () => {
    expect(precededByDot(ctxFor("foo.bar"))).toBe(true);
    expect(precededByDot(ctxFor("self.settings_schema."))).toBe(true);
  });

  it("is false for a bare word", () => {
    expect(precededByDot(ctxFor("    sli"))).toBe(false);
    expect(precededByDot(ctxFor("foo bar"))).toBe(false);
  });
});

describe("localVariableSource", () => {
  it("suggests an earlier-defined local when typing its prefix", () => {
    const result = localVariableSource(ctxFor("slider_input_1 = 5\nsli"));
    expect(result).not.toBeNull();
    expect(result!.options.map((o) => o.label)).toContain("slider_input_1");
  });

  it("excludes the symbol currently being typed", () => {
    const result = localVariableSource(ctxFor("total = 1\ntotal"));
    // `total` is the current word; nothing else defined -> no suggestions.
    expect(result).toBeNull();
  });

  it("defers to the dotted-path source after a dot", () => {
    expect(localVariableSource(ctxFor("x = 1\nfoo.bar"))).toBeNull();
  });
});

describe("settingsFieldAccessor / pyType", () => {
  it("builds the same accessor string FormFieldsOverview uses", () => {
    expect(settingsFieldAccessor("settings", "slider_input_1", "SliderInput")).toBe(
      "self.settings_schema.settings.slider_input_1.value",
    );
    expect(settingsFieldAccessor("secrets", "api_key", "SecretSelector")).toBe(
      "self.settings_schema.secrets.api_key.secret_value",
    );
  });

  it("maps component types to Python types", () => {
    expect(pyType("SliderInput")).toBe("float");
    expect(pyType("TextInput")).toBe("str");
    expect(pyType("ToggleSwitch")).toBe("bool");
    expect(pyType("SecretSelector")).toBe("SecretStr");
    expect(pyType("ColumnActionInput")).toBe("ColumnActionValue");
    expect(pyType("Unknown")).toBe("Any");
  });
});

describe("makeSettingsFieldSnippetSource", () => {
  it("offers an unbound field as a full-accessor snippet with its type", () => {
    const src = makeSettingsFieldSnippetSource(oneSliderField);
    const result = src(ctxFor("sl"));
    expect(result).not.toBeNull();
    const opt = result!.options.find((o) => o.label === "slider_input_1");
    expect(opt?.apply).toBe("self.settings_schema.settings.slider_input_1.value");
    expect(opt?.detail).toBe("float");
  });

  it("suppresses the snippet once the field is bound to a same-named local (no double)", () => {
    const src = makeSettingsFieldSnippetSource(oneSliderField);
    const doc = "slider_input_1: float = self.settings_schema.settings.slider_input_1.value\nsl";
    expect(src(ctxFor(doc))).toBeNull();
  });

  it("defers after a member-access dot", () => {
    const src = makeSettingsFieldSnippetSource(oneSliderField);
    expect(src(ctxFor("foo.sl"))).toBeNull();
  });
});

describe("normalizePyType", () => {
  it("canonicalizes annotations", () => {
    expect(normalizePyType("float")).toBe("float");
    expect(normalizePyType("list[str]")).toBe("list");
    expect(normalizePyType("pl.LazyFrame")).toBe("LazyFrame");
    expect(normalizePyType("Optional[int]")).toBe("int");
    expect(normalizePyType("str | None")).toBe("str");
    expect(normalizePyType("SecretStr")).toBe("SecretStr");
  });
});

describe("scanAssignments", () => {
  it("captures name, annotation and rhs", () => {
    expect(scanAssignments("test: float = self.x\nlf = inputs[0]")).toEqual([
      { name: "test", annotation: "float", rhs: "self.x" },
      { name: "lf", annotation: null, rhs: "inputs[0]" },
    ]);
  });

  it("ignores comparisons and keyword lines", () => {
    expect(scanAssignments("if a == b:\n    return x")).toEqual([]);
  });
});

describe("inferLocalTypes", () => {
  it("lets annotations win", () => {
    expect(inferLocalTypes("test: float = whatever()", []).get("test")).toBe("float");
  });

  it("infers inputs[0] as LazyFrame and propagates through a chain", () => {
    const t = inferLocalTypes("lf = inputs[0]\nout = lf.filter(pl.col('x') > 0)", []);
    expect(t.get("lf")).toBe("LazyFrame");
    expect(t.get("out")).toBe("LazyFrame");
  });

  it("infers common literals", () => {
    const t = inferLocalTypes('s = "hi"\nn = 3\nf = 1.5\nb = True\nxs = [1]\nd = {"a": 1}', []);
    expect([t.get("s"), t.get("n"), t.get("f"), t.get("b"), t.get("xs"), t.get("d")]).toEqual([
      "str",
      "int",
      "float",
      "bool",
      "list",
      "dict",
    ]);
  });

  it("infers a settings-value assignment from the component type", () => {
    const t = inferLocalTypes(
      "v = self.settings_schema.settings.slider_input_1.value",
      oneSliderField(),
    );
    expect(t.get("v")).toBe("float");
  });

  it("infers .secret_value as SecretStr", () => {
    const t = inferLocalTypes("k = self.settings_schema.settings.api_key.secret_value", []);
    expect(t.get("k")).toBe("SecretStr");
  });

  it("infers a ColumnActionInput value as ColumnActionValue (accessor and annotation)", () => {
    const t = inferLocalTypes(
      "cfg = self.settings_schema.agg.column_actions.value",
      oneColumnActionField(),
    );
    expect(t.get("cfg")).toBe("ColumnActionValue");

    // The ControlInspector "insert variable" annotation normalizes the same way.
    const t2 = inferLocalTypes("cfg: nd.ColumnActionValue = something()", []);
    expect(t2.get("cfg")).toBe("ColumnActionValue");
  });
});

describe("memberCompletionsFor", () => {
  it("offers ColumnActionValue attributes for a ColumnActionValue variable", () => {
    const members = memberCompletionsFor("cfg", new Map([["cfg", "ColumnActionValue"]]));
    expect(members?.map((m) => m.label)).toEqual(["rows", "group_by_columns", "order_by_column"]);
  });

  it("returns null for an unknown/untyped variable", () => {
    expect(memberCompletionsFor("cfg", new Map())).toBeNull();
  });
});

describe("scanSelfMethods", () => {
  const doc =
    "def my_function(self):\n" +
    '    return "hi"\n' +
    "\n" +
    "def helper():\n" +
    "    return 1\n" +
    "def process(self, *inputs):\n" +
    "    return inputs[0]";

  it("finds self-methods, excluding process and plain functions", () => {
    expect(scanSelfMethods(doc).map((m) => m.name)).toEqual(["my_function"]);
  });

  it("infers the method's return type", () => {
    expect(scanSelfMethods(doc)[0].returnType).toBe("str");
  });
});

describe("inferLocalTypes with self-method calls", () => {
  it("types a variable from the method's inferred return", () => {
    const doc = "x = self.my_function()\n\ndef my_function(self):\n    return 12";
    expect(inferLocalTypes(doc, []).get("x")).toBe("int");
  });
});

describe("memberCompletionsFor", () => {
  const types = new Map<string, string>([
    ["test", "float"],
    ["lf", "LazyFrame"],
    ["name", "str"],
  ]);

  it("gives float methods (not LazyFrame) for a float local", () => {
    const labels = memberCompletionsFor("test", types)!.map((o) => o.label);
    expect(labels).toContain("is_integer");
    expect(labels).not.toContain("filter");
  });

  it("gives LazyFrame methods for a LazyFrame local", () => {
    expect(memberCompletionsFor("lf", types)!.map((o) => o.label)).toContain("filter");
  });

  it("gives str methods for a str local", () => {
    expect(memberCompletionsFor("name", types)!.map((o) => o.label)).toContain("upper");
  });

  it("gives polars module functions for `pl`", () => {
    expect(memberCompletionsFor("pl", types)!.map((o) => o.label)).toContain("col");
  });

  it("returns null for an unknown identifier", () => {
    expect(memberCompletionsFor("mystery", types)).toBeNull();
  });
});

describe("local-variable suggestions carry the inferred type", () => {
  it("shows `float` next to an annotated float local", () => {
    const src = makeLocalVariableSource(() => []);
    const result = src(ctxFor("test: float = self.settings_schema.settings.x.value\ntes"));
    expect(result!.options.find((o) => o.label === "test")?.detail).toBe("float");
  });
});

describe("logging completions", () => {
  const { schemaCompletions } = usePolarsAutocompletion(() => []);

  it("offers `logging` as a top-level suggestion", () => {
    const result = schemaCompletions(ctxFor("log", 3));
    expect(result!.options.map((o) => o.label)).toContain("logging");
  });

  it("suggests logging methods after `logging.`", () => {
    const result = schemaCompletions(ctxFor("logging."));
    expect(result).not.toBeNull();
    const labels = result!.options.map((o) => o.label);
    expect(labels).toEqual(
      expect.arrayContaining(["info", "debug", "warning", "error", "getLogger"]),
    );
  });

  it("infers logging.getLogger(...) as a Logger", () => {
    expect(inferLocalTypes("lg = logging.getLogger(__name__)", []).get("lg")).toBe("Logger");
  });

  it("gives logger methods (not getLogger) for a Logger local", () => {
    const labels = memberCompletionsFor("lg", new Map([["lg", "Logger"]]))!.map((o) => o.label);
    expect(labels).toContain("info");
    expect(labels).not.toContain("getLogger");
  });
});

describe("computeNewlineIndent", () => {
  it("copies the current line's indentation", () => {
    expect(computeNewlineIndent("    lf = inputs[0]", 18)).toBe("    ");
  });

  it("adds one level after a colon", () => {
    expect(computeNewlineIndent("    if x:", 9)).toBe("        ");
  });

  it("ignores a colon inside a trailing comment", () => {
    const line = "    x = 1  # note:";
    expect(computeNewlineIndent(line, line.length)).toBe("    ");
  });

  it("dedents one level after a block-terminating statement", () => {
    expect(computeNewlineIndent("        return lf", 17)).toBe("    ");
    expect(computeNewlineIndent("    pass", 8)).toBe("");
  });

  it("never produces negative indent at column 0", () => {
    expect(computeNewlineIndent("return x", 8)).toBe("");
  });
});
