import { describe, expect, it } from "vitest";

import { verdict } from "../api/__fixtures__/computeBackends";
import type { HandshakeVerdict } from "../api/computeBackends.api";
import {
  backendInSentence,
  backendName,
  executionLocationLabel,
  executionLocationOptions,
  nodeExecutionLocationLabel,
  nodeExecutionLocationOptions,
  unreachableNote,
  verdictSummary,
  type BackendRow,
} from "./executionLocation";

const row = (id: string, overrides: Partial<BackendRow> = {}): BackendRow => ({
  id,
  verdict: null,
  source: "local",
  reachable: null,
  reason: null,
  ...overrides,
});

const rows = (backendIds: string[]) => backendIds.map((id) => row(id));

const hubRow = (overrides: Partial<BackendRow> = {}) =>
  row("worker:team", { source: "hub", ...overrides });

const keys = (current: string, backendIds: string[]) =>
  executionLocationOptions(current, rows(backendIds)).map((option) => option.key);

const VERDICTS: [string, HandshakeVerdict | null, string][] = [
  ["unchecked", null, "Not checked"],
  ["passing", verdict({}), "Compatible"],
  [
    "narrowed",
    verdict({
      absent: {
        polars_ds: "version 0.0.1, core has 0.10.0",
        python:
          "core's Python functions do not run on this backend (Python: core 3.12.15, backend 3.12.8): …",
      },
      differences: ["python_version: core 3.12.15, backend 3.12.8"],
    }),
    "Limited · 2 unavailable",
  ],
  [
    "refused",
    verdict({
      ok: false,
      offered: null,
      error:
        'Backend "worker:team" does not match core (polars_version: core 1.43.2, backend 1.39.3)',
      differences: ["polars_version: core 1.43.2, backend 1.39.3"],
    }),
    "Not accepted",
  ],
  [
    "refused-without-an-error",
    verdict({ ok: false, offered: null, differences: ["polars_runtime: core 64, backend 32"] }),
    "Not accepted",
  ],
];

describe("verdictSummary", () => {
  it.each(VERDICTS)("says what the %s verdict says", (_, given, summary) => {
    expect(verdictSummary(given)).toBe(summary);
  });
});

describe("executionLocationOptions", () => {
  it("offers auto and the configured backends, never local", () => {
    expect(keys("auto", ["worker:team", "worker:gpu"])).toEqual([
      "auto",
      "worker:team",
      "worker:gpu",
    ]);
  });

  it("keeps a flow's backend id that is no longer configured, last", () => {
    expect(keys("worker:gone", ["worker:team"])).toEqual(["auto", "worker:team", "worker:gone"]);
  });

  it("offers auto and the flow's own value when the backends cannot be listed", () => {
    expect(keys("auto", [])).toEqual(["auto"]);
    expect(keys("worker:team", [])).toEqual(["auto", "worker:team"]);
  });

  it("offers exactly local and remote when core answers one of them", () => {
    expect(keys("local", ["worker:team"])).toEqual(["local", "remote"]);
    expect(keys("remote", ["worker:team"])).toEqual(["local", "remote"]);
  });
});

describe("the verdict beside a configured backend's id", () => {
  it.each(VERDICTS)("labels a %s backend in the dialog and the picker", (_, given, summary) => {
    const listed = [row("worker:team", { verdict: given })];

    expect(executionLocationOptions("auto", listed)).toEqual([
      { key: "auto", label: "Auto (default worker)" },
      { key: "worker:team", label: `worker:team · ${summary}` },
    ]);
    expect(nodeExecutionLocationOptions("auto", listed)).toEqual([
      { key: "auto", label: "Auto (the flow's backend)" },
      { key: "worker", label: "Default worker" },
      { key: "worker:team", label: `worker:team · ${summary}` },
    ]);
  });

  it("labels a saved id that is no longer configured by its id alone", () => {
    const listed = [row("worker:team", { verdict: verdict({}) })];

    for (const options of [
      executionLocationOptions("worker:gone", listed),
      nodeExecutionLocationOptions("worker:gone", listed),
    ]) {
      expect(options[options.length - 1]).toEqual({ key: "worker:gone", label: "worker:gone" });
    }
  });
});

describe("a hub node's reachability in the picker", () => {
  const label = (listed: BackendRow[]) =>
    executionLocationOptions("auto", listed)[1].label.replace("worker:team · ", "");

  it("uses the reachability word while the node has no verdict yet", () => {
    expect(label([hubRow()])).toBe("Not checked");
    expect(label([hubRow({ reachable: true })])).toBe("Reachable");
    expect(label([hubRow({ reachable: false, reason: "can't reach it" })])).toBe("Unreachable");
    expect(nodeExecutionLocationOptions("auto", [hubRow({ reachable: true })])[2].label).toBe(
      "worker:team · Reachable",
    );
  });

  it("gives way to the verdict's word once the node has one", () => {
    expect(label([hubRow({ reachable: true, verdict: verdict({}) })])).toBe("Compatible");
    expect(label([hubRow({ reachable: false, verdict: verdict({ ok: false }) })])).toBe(
      "Not accepted",
    );
  });

  it("keeps Not checked for a worker added here, whatever its fields say", () => {
    expect(label([row("worker:team", { reachable: true })])).toBe("Not checked");
  });

  it("notes why a hub node is unreachable, and nothing otherwise", () => {
    const reason =
      "can't reach https://team-worker.example.test:63579 from this computer: no route";
    expect(unreachableNote(hubRow({ reachable: false, reason }))).toBe(reason);
    expect(unreachableNote(hubRow({ reachable: false, reason: null }))).toBe("");
    expect(unreachableNote(hubRow({ reachable: true }))).toBe("");
    expect(unreachableNote(hubRow())).toBe("");
    expect(unreachableNote(row("worker:own", { reachable: false, reason: "x" }))).toBe("");
    expect(unreachableNote(undefined)).toBe("");
  });
});

describe("executionLocationLabel", () => {
  it("names each value", () => {
    expect(executionLocationLabel("local")).toBe("Local");
    expect(executionLocationLabel("remote")).toBe("Remote");
    expect(executionLocationLabel("auto")).toBe("Auto (default worker)");
    expect(executionLocationLabel("worker:team")).toBe("worker:team");
  });
});

const nodeKeys = (current: string, backendIds: string[]) =>
  nodeExecutionLocationOptions(current, rows(backendIds)).map((option) => option.key);

describe("nodeExecutionLocationOptions", () => {
  it("offers auto, the default worker and the configured backends, never local", () => {
    expect(nodeKeys("auto", ["worker:team", "worker:gpu"])).toEqual([
      "auto",
      "worker",
      "worker:team",
      "worker:gpu",
    ]);
  });

  it("keeps a node's backend id that is no longer configured, last", () => {
    expect(nodeKeys("worker:gone", ["worker:team"])).toEqual([
      "auto",
      "worker",
      "worker:team",
      "worker:gone",
    ]);
  });

  it("keeps a node saved as local, last, and offers local to no other node", () => {
    expect(nodeKeys("local", ["worker:team"])).toEqual(["auto", "worker", "worker:team", "local"]);
    expect(nodeKeys("worker", ["worker:team"])).not.toContain("local");
  });
});

describe("nodeExecutionLocationLabel", () => {
  it("names each value", () => {
    expect(nodeExecutionLocationLabel("auto")).toBe("Auto (the flow's backend)");
    expect(nodeExecutionLocationLabel("worker")).toBe("Default worker");
    expect(nodeExecutionLocationLabel("local")).toBe("Local");
    expect(nodeExecutionLocationLabel("worker:team")).toBe("worker:team");
  });
});

describe("backend names in placement text", () => {
  it("calls the built-in worker the default worker and quotes any other id", () => {
    expect(backendName("worker")).toBe("default worker");
    expect(backendName("worker:polars")).toBe("worker:polars");
    expect(backendInSentence("worker")).toBe("the default worker");
    expect(backendInSentence("local")).toBe('backend "local"');
  });
});
