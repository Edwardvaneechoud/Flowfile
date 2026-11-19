import { readFileSync } from "fs";
import { describe, expect, it } from "vitest";
import { coreRefusals, type ChartRequest, type ColumnKind } from "./chartRequest";

// The cases core's suite reads too (`flowfile_core/tests/ai/test_chart_request_cases.py`): the
// browser's mirror of core's rules must give each request core's verdict.
const CASES_FILE = new URL(
  "../../../../../../flowfile_core/tests/ai/chart_request_cases.json",
  import.meta.url,
);

interface CasesFile {
  columns: { name: string; type: string }[];
  cases: { name: string; verdict: "accepted" | "rejected"; request: ChartRequest }[];
}

const data = JSON.parse(readFileSync(CASES_FILE, "utf-8")) as CasesFile;

const KIND_OF_TYPE: Record<string, ColumnKind> = {
  integer: "number",
  float: "number",
  date: "date",
  datetime: "date",
  string: "text",
  boolean: "text",
};

const kinds = new Map(data.columns.map((c) => [c.name, KIND_OF_TYPE[c.type]]));

describe("the shared chart request cases", () => {
  it("are read and type every column", () => {
    expect(data.cases.length).toBeGreaterThan(20);
    expect([...kinds.values()].every(Boolean)).toBe(true);
  });

  it.each(data.cases.map((c) => [c.name, c] as const))("%s", (_name, c) => {
    const refusals = coreRefusals(c.request, kinds);
    expect(refusals.length === 0 ? "accepted" : "rejected", refusals.join("; ")).toBe(c.verdict);
  });
});
