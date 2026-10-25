import { describe, expect, it } from "vitest";

import type { HandshakeVerdict, WorkerCapabilities } from "../api/computeBackends.api";
import {
  executionLocationLabel,
  executionLocationOptions,
  nodeExecutionLocationLabel,
  nodeExecutionLocationOptions,
  verdictSummary,
} from "./executionLocation";

const rows = (backendIds: string[]) => backendIds.map((id) => ({ id, verdict: null }));

const keys = (current: string, backendIds: string[]) =>
  executionLocationOptions(current, rows(backendIds)).map((option) => option.key);

const capabilities: WorkerCapabilities = {
  app_version: "0.23.0",
  polars_version: "1.43.2",
  python_version: "3.12.15",
  protocol_version: 1,
  platform: "linux-x86_64",
  polars_runtime: "64",
  plugins: { polars_ds: "0.10.0", polars_grouper: "0.6.0" },
  plugin_libs: {},
  operations: [],
  read_schemes: ["s3"],
  write_schemes: ["s3"],
  credential_resolution: "master_key",
};

const verdict = (overrides: Partial<HandshakeVerdict>): HandshakeVerdict => ({
  id: "worker:team",
  ok: true,
  core: capabilities,
  backend: capabilities,
  offered: capabilities,
  differences: [],
  absent: {},
  error: null,
  ...overrides,
});

const VERDICTS: [string, HandshakeVerdict | null, string][] = [
  ["unchecked", null, "Not checked"],
  ["passing", verdict({}), "Compatible"],
  [
    "narrowed",
    verdict({
      absent: {
        polars_ds: "this backend has in version 0.0.1 where core has 0.10.0",
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
    const listed = [{ id: "worker:team", verdict: given }];

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
    const listed = [{ id: "worker:team", verdict: verdict({}) }];

    for (const options of [
      executionLocationOptions("worker:gone", listed),
      nodeExecutionLocationOptions("worker:gone", listed),
    ]) {
      expect(options[options.length - 1]).toEqual({ key: "worker:gone", label: "worker:gone" });
    }
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
