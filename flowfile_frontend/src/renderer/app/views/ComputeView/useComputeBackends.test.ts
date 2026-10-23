import { beforeEach, describe, expect, it, vi } from "vitest";
import type {
  ComputeBackend,
  ComputeBackendInput,
  HandshakeVerdict,
  WorkerCapabilities,
} from "../../api/computeBackends.api";

const mocks = vi.hoisted(() => ({
  list: vi.fn(),
  add: vi.fn(),
  remove: vi.fn(),
  handshake: vi.fn(),
}));

vi.mock("../../api/computeBackends.api", () => ({
  listComputeBackends: mocks.list,
  addComputeBackend: mocks.add,
  removeComputeBackend: mocks.remove,
  handshakeComputeBackend: mocks.handshake,
}));

import { plainHttpOffLoopback, useComputeBackends } from "./useComputeBackends";

const TEAM_URL = "https://team.example:63579";

const core: WorkerCapabilities = {
  app_version: "0.23.0",
  polars_version: "1.39.3",
  python_version: "3.12.8",
  protocol_version: 1,
  platform: "darwin-arm64",
  polars_runtime: "32",
  plugins: {},
  plugin_libs: {},
  operations: [],
  read_schemes: ["s3"],
  write_schemes: ["s3"],
  credential_resolution: "master_key",
};

let rows: ComputeBackend[] = [];

const httpError = (status: number, detail: unknown) =>
  Object.assign(new Error(`Request failed with status code ${status}`), {
    response: { status, data: { detail } },
  });

const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
};

beforeEach(() => {
  rows = [];
  mocks.list.mockReset().mockImplementation(async () => rows.map((row) => ({ ...row })));
  mocks.add.mockReset().mockImplementation(async (input: ComputeBackendInput) => {
    const kind = input.kind ?? "worker";
    const url = input.kind === "polars-cloud" ? input.scheduler_url : input.url;
    const row: ComputeBackend = { id: `${kind}:${input.name}`, kind, url, verdict: null };
    rows.push(row);
    return { ...row };
  });
  mocks.remove.mockReset().mockImplementation(async (id: string) => {
    rows = rows.filter((row) => row.id !== id);
  });
  mocks.handshake.mockReset();
});

const checked: HandshakeVerdict = {
  id: "worker:team",
  ok: true,
  core,
  backend: core,
  offered: core,
  differences: [],
  absent: {},
  error: null,
};

describe("load", () => {
  it("fills the rows and clears an earlier error", async () => {
    rows = [{ id: "worker:team", kind: "worker", url: TEAM_URL, verdict: null }];
    const page = useComputeBackends();
    page.loadError.value = "stale";

    await page.load();

    expect(page.backends.value).toEqual([
      { id: "worker:team", kind: "worker", url: TEAM_URL, verdict: null },
    ]);
    expect(page.loadError.value).toBe("");
    expect(page.loading.value).toBe(false);
  });

  it("seeds each worker's verdict from the listing, so one a run produced shows without a handshake", async () => {
    rows = [
      { id: "worker:team", kind: "worker", url: TEAM_URL, verdict: checked },
      { id: "worker:gpu", kind: "worker", url: "https://gpu.example:63579", verdict: null },
    ];
    const page = useComputeBackends();

    await page.load();

    expect(page.verdicts.value).toEqual({ "worker:team": checked });
    expect(mocks.handshake).not.toHaveBeenCalled();
  });

  it("keeps the rows it had when the list is refused, and says why", async () => {
    rows = [{ id: "worker:team", kind: "worker", url: TEAM_URL, verdict: null }];
    const page = useComputeBackends();
    await page.load();
    mocks.list.mockRejectedValueOnce(new Error("Network Error"));

    await page.load();

    expect(page.loadError.value).toBe("Couldn't read the configured backends.");
    expect(page.backends.value).toEqual([
      { id: "worker:team", kind: "worker", url: TEAM_URL, verdict: null },
    ]);
  });
});

describe("add", () => {
  it("posts the input as typed and reloads the list, which then holds the worker", async () => {
    const page = useComputeBackends();
    const input = { name: "team", url: TEAM_URL, token: "row-token" };

    const pending = page.add(input);
    expect(page.adding.value).toBe(true);
    const row = await pending;

    expect(mocks.add).toHaveBeenCalledWith(input);
    expect(row).toEqual({ id: "worker:team", kind: "worker", url: TEAM_URL, verdict: null });
    expect(page.backends.value.map((b) => b.id)).toEqual(["worker:team"]);
    expect(page.adding.value).toBe(false);
  });

  it("posts a CA certificate when one is given", async () => {
    const page = useComputeBackends();
    const pem = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";

    await page.add({ name: "team", url: TEAM_URL, token: "row-token", ca_cert: pem });

    expect(mocks.add).toHaveBeenCalledWith({
      name: "team",
      url: TEAM_URL,
      token: "row-token",
      ca_cert: pem,
    });
  });

  it("leaves an empty CA certificate out of the body", async () => {
    const page = useComputeBackends();

    await page.add({ name: "team", url: TEAM_URL, token: "row-token", ca_cert: "" });

    expect(mocks.add).toHaveBeenCalledWith({ name: "team", url: TEAM_URL, token: "row-token" });
    expect(mocks.add.mock.calls[0][0]).not.toHaveProperty("ca_cert");
  });

  it("rejects with the route's detail when the id exists, and reloads nothing", async () => {
    mocks.add.mockRejectedValueOnce(httpError(409, "Compute backend 'worker:team' already exists"));
    const page = useComputeBackends();

    await expect(page.add({ name: "team", url: TEAM_URL, token: "t" })).rejects.toThrow(
      "Compute backend 'worker:team' already exists",
    );
    expect(mocks.list).not.toHaveBeenCalled();
    expect(page.adding.value).toBe(false);
  });

  it("rejects with the token rule as the route words it", async () => {
    const detail =
      "The token was not stored: a worker token is sent as an HTTP header value, so it cannot hold a CR, " +
      "a LF, any other ASCII control character but a tab, or a character beyond Latin-1";
    mocks.add.mockRejectedValueOnce(httpError(422, detail));
    const page = useComputeBackends();

    await expect(page.add({ name: "team", url: TEAM_URL, token: "row\ntoken" })).rejects.toThrow(
      detail,
    );
  });

  it("rejects with the messages of FastAPI's validation list", async () => {
    mocks.add.mockRejectedValueOnce(
      httpError(422, [
        {
          type: "string_pattern_mismatch",
          loc: ["body", "name"],
          msg: "String should match pattern '^[A-Za-z0-9][A-Za-z0-9._-]*$'",
          input: "my team",
        },
        {
          type: "value_error",
          loc: ["body", "url"],
          msg: "Value error, the worker URL must start with http:// or https://",
          input: "ftp://team",
        },
      ]),
    );
    const page = useComputeBackends();

    await expect(page.add({ name: "my team", url: "ftp://team", token: "t" })).rejects.toThrow(
      "String should match pattern '^[A-Za-z0-9][A-Za-z0-9._-]*$'; " +
        "Value error, the worker URL must start with http:// or https://",
    );
  });

  it("never repeats the body a validation entry echoes, token included", async () => {
    mocks.add.mockRejectedValueOnce(
      httpError(422, [
        {
          type: "missing",
          loc: ["body", "url"],
          msg: "Field required",
          input: { name: "team", token: "row-token" },
        },
      ]),
    );
    const page = useComputeBackends();

    const failure = page.add({ name: "team", url: "", token: "row-token" });

    await expect(failure).rejects.toThrow("Field required");
    await expect(failure).rejects.not.toThrow("row-token");
  });
});

describe("remove", () => {
  it("deletes by id, and the row and its verdict are gone", async () => {
    rows = [{ id: "worker:team", kind: "worker", url: TEAM_URL, verdict: null }];
    mocks.handshake.mockResolvedValueOnce(checked);
    const page = useComputeBackends();
    await page.load();
    await page.handshake("worker:team");

    await page.remove("worker:team");

    expect(mocks.remove).toHaveBeenCalledWith("worker:team");
    expect(page.backends.value).toEqual([]);
    expect(page.verdicts.value["worker:team"]).toBeUndefined();
    expect(page.busy.value["worker:team"]).toBe(false);
  });

  it("rejects with the route's 404 detail", async () => {
    mocks.remove.mockRejectedValueOnce(
      httpError(404, "No compute backend 'worker:gone' is configured"),
    );
    const page = useComputeBackends();

    await expect(page.remove("worker:gone")).rejects.toThrow(
      "No compute backend 'worker:gone' is configured",
    );
  });
});

describe("handshake", () => {
  const verdicts: Record<string, HandshakeVerdict> = {
    match: checked,
    narrowed: {
      ...checked,
      offered: { ...core, plugins: {} },
      absent: { polars_ds: "this backend does not have" },
    },
    mismatch: {
      ...checked,
      ok: false,
      backend: { ...core, polars_version: "0.20.31" },
      offered: null,
      differences: ["polars_version: core 1.39.3, backend 0.20.31"],
      error:
        'Backend "worker:team" does not match core (polars_version: core 1.39.3, backend 0.20.31)',
    },
    unreachable: {
      ...checked,
      ok: false,
      backend: null,
      offered: null,
      error: `Could not reach the worker at ${TEAM_URL}: Connection refused`,
    },
  };

  it.each(Object.entries(verdicts))(
    "stores the %s verdict for its id, busy only while it is out",
    async (_, verdict) => {
      const answer = deferred<HandshakeVerdict>();
      mocks.handshake.mockReturnValueOnce(answer.promise);
      const page = useComputeBackends();

      const pending = page.handshake("worker:team");
      expect(page.busy.value["worker:team"]).toBe(true);
      answer.resolve(verdict);
      await pending;

      expect(mocks.handshake).toHaveBeenCalledWith("worker:team");
      expect(page.busy.value["worker:team"]).toBe(false);
      expect(page.verdicts.value["worker:team"]).toEqual(verdict);
    },
  );
});

describe("plainHttpOffLoopback", () => {
  it.each([
    ["http://10.0.0.5:63579", true],
    ["http://flowfile-worker:63579", true],
    ["http://localhost:63579", false],
    ["http://127.0.0.1:63579", false],
    ["http://[::1]:63579", false],
    ["https://10.0.0.5:63579", false],
    ["http://", false],
    ["", false],
  ])("warns for %s: %s", (url, warns) => {
    expect(plainHttpOffLoopback(url)).toBe(warns);
  });
});
