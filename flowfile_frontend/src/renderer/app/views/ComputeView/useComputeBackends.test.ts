import { beforeEach, describe, expect, it, vi } from "vitest";
import { deferred, httpError } from "../../__fixtures__/promises";
import { capabilities, verdict } from "../../api/__fixtures__/computeBackends";
import type {
  ComputeBackend,
  ComputeBackendInput,
  HandshakeVerdict,
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

import {
  backendInputFrom,
  emptyBackendForm,
  plainHttpOffLoopback,
  serverFilesLabel,
  useComputeBackends,
} from "./useComputeBackends";

const TEAM_URL = "https://team.example:63579";

const teamRow = (
  shares_core_disk = false,
  seen: HandshakeVerdict | null = null,
): ComputeBackend => ({
  id: "worker:team",
  url: TEAM_URL,
  shares_core_disk,
  verdict: seen,
});

let rows: ComputeBackend[] = [];

beforeEach(() => {
  rows = [];
  mocks.list.mockReset().mockImplementation(async () => rows.map((row) => ({ ...row })));
  mocks.add.mockReset().mockImplementation(async (input: ComputeBackendInput) => {
    const row = {
      id: `worker:${input.name}`,
      url: input.url,
      shares_core_disk: input.shares_core_disk ?? false,
      verdict: null,
    };
    rows.push(row);
    return { ...row };
  });
  mocks.remove.mockReset().mockImplementation(async (id: string) => {
    rows = rows.filter((row) => row.id !== id);
  });
  mocks.handshake.mockReset();
});

const checked = verdict();

describe("load", () => {
  it("fills the rows and clears an earlier error", async () => {
    rows = [teamRow()];
    const page = useComputeBackends();
    page.loadError.value = "stale";

    await page.load();

    expect(page.backends.value).toEqual([teamRow()]);
    expect(page.loadError.value).toBe("");
    expect(page.loading.value).toBe(false);
  });

  it("seeds each worker's verdict from the listing, so one a run produced shows without a handshake", async () => {
    rows = [
      teamRow(false, checked),
      {
        id: "worker:gpu",
        url: "https://gpu.example:63579",
        shares_core_disk: false,
        verdict: null,
      },
    ];
    const page = useComputeBackends();

    await page.load();

    expect(page.verdicts.value).toEqual({ "worker:team": checked });
    expect(mocks.handshake).not.toHaveBeenCalled();
  });

  it("keeps the rows it had when the list is refused, and says why", async () => {
    rows = [teamRow()];
    const page = useComputeBackends();
    await page.load();
    mocks.list.mockRejectedValueOnce(new Error("Network Error"));

    await page.load();

    expect(page.loadError.value).toBe("Couldn't read the configured workers.");
    expect(page.backends.value).toEqual([teamRow()]);
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
    expect(row).toEqual(teamRow());
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

  it("posts whether the worker sees this server's files", async () => {
    const page = useComputeBackends();

    await page.add({ name: "team", url: TEAM_URL, token: "row-token", shares_core_disk: true });

    expect(mocks.add).toHaveBeenCalledWith({
      name: "team",
      url: TEAM_URL,
      token: "row-token",
      shares_core_disk: true,
    });
  });

  it.each(["", "  \n\t "])(
    "leaves an empty or blank CA certificate out of the body: %j",
    async (ca_cert) => {
      const page = useComputeBackends();

      await page.add({ name: "team", url: "http://10.0.0.5:63579", token: "row-token", ca_cert });

      expect(mocks.add).toHaveBeenCalledWith({
        name: "team",
        url: "http://10.0.0.5:63579",
        token: "row-token",
      });
      expect(mocks.add.mock.calls[0][0]).not.toHaveProperty("ca_cert");
    },
  );

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
    rows = [teamRow()];
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
    narrowed: verdict({
      offered: capabilities({ plugins: {} }),
      absent: { polars_ds: "not installed" },
    }),
    mismatch: verdict({
      ok: false,
      backend: capabilities({ polars_version: "0.20.31" }),
      offered: null,
      differences: ["polars_version: core 1.43.2, backend 0.20.31"],
      error:
        'Backend "worker:team" does not match core (polars_version: core 1.43.2, backend 0.20.31)',
    }),
    unreachable: verdict({
      ok: false,
      backend: null,
      offered: null,
      error: `Could not reach the worker at ${TEAM_URL}: Connection refused`,
    }),
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

describe("the add form", () => {
  it("opens and resets unticked", () => {
    expect(emptyBackendForm()).toEqual({
      name: "",
      url: "",
      token: "",
      ca_cert: "",
      shares_core_disk: false,
    });
  });

  it.each([true, false])("sends the server's files tick as %s", (ticked) => {
    const form = { ...emptyBackendForm(), name: "team", url: TEAM_URL, token: "t" };
    form.shares_core_disk = ticked;
    expect(backendInputFrom(form)).toEqual({
      name: "team",
      url: TEAM_URL,
      token: "t",
      ca_cert: "",
      shares_core_disk: ticked,
    });
  });
});

describe("serverFilesLabel", () => {
  const declaring = (schemes: string[] | null): HandshakeVerdict => {
    const declared = schemes
      ? capabilities({ read_schemes: schemes, write_schemes: schemes })
      : null;
    return verdict({
      ok: schemes !== null,
      backend: declared,
      offered: declared,
      error: schemes ? null : "unreachable",
    });
  };

  it("says a marked row sees the server's files whatever the verdict", () => {
    expect(serverFilesLabel(teamRow(true), null)).toBe("Yes, marked here");
    expect(serverFilesLabel(teamRow(true), declaring(["s3"]))).toBe("Yes, marked here");
  });

  it("says yes for an unmarked worker that declares file itself", () => {
    expect(serverFilesLabel(teamRow(false), declaring(["s3", "file"]))).toBe(
      "Yes, declared by the worker",
    );
  });

  it("says no for an unmarked worker that does not declare file", () => {
    expect(serverFilesLabel(teamRow(false), declaring(["s3"]))).toBe("No");
  });

  it("reads a refused worker's own declaration", () => {
    const refused = (schemes: string[]): HandshakeVerdict => ({
      ...declaring(null),
      backend: capabilities({ read_schemes: schemes, write_schemes: schemes }),
      differences: ["polars_version: core 1.43.2, backend 0.20.31"],
    });
    expect(serverFilesLabel(teamRow(false), refused(["s3", "file"]))).toBe(
      "Yes, declared by the worker",
    );
    expect(serverFilesLabel(teamRow(false), refused(["s3"]))).toBe("No");
  });

  it("says unknown for an unmarked worker the handshake did not reach", () => {
    expect(serverFilesLabel(teamRow(false), declaring(null))).toBe("Unknown, the handshake failed");
  });

  it("says not checked for an unmarked worker with no verdict", () => {
    expect(serverFilesLabel(teamRow(false), undefined)).toBe("Not checked");
  });
});
