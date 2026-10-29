import type { HandshakeVerdict, WorkerCapabilities } from "../computeBackends.api";

/** A worker's capabilities on the current pins, with any field replaced. */
export const capabilities = (overrides: Partial<WorkerCapabilities> = {}): WorkerCapabilities => ({
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
  ...overrides,
});

/** A passing handshake of `worker:team` against a core with the same capabilities, with any field replaced. */
export const verdict = (overrides: Partial<HandshakeVerdict> = {}): HandshakeVerdict => ({
  id: "worker:team",
  ok: true,
  core: capabilities(),
  backend: capabilities(),
  offered: capabilities(),
  differences: [],
  absent: {},
  error: null,
  ...overrides,
});
