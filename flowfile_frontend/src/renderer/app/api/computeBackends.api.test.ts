import { beforeEach, describe, expect, it, vi } from "vitest";
import { httpError } from "../__fixtures__/promises";
import { capabilities, verdict } from "./__fixtures__/computeBackends";

const mocks = vi.hoisted(() => ({
  get: vi.fn(),
  post: vi.fn(),
  delete: vi.fn(),
}));

vi.mock("../services/axios.config", () => ({
  default: { get: mocks.get, post: mocks.post, delete: mocks.delete },
}));

import {
  addComputeBackend,
  handshakeComputeBackend,
  listComputeBackends,
  removeComputeBackend,
  type ComputeBackend,
} from "./computeBackends.api";

beforeEach(() => {
  mocks.get.mockReset();
  mocks.post.mockReset();
  mocks.delete.mockReset();
});

describe("listComputeBackends", () => {
  it("gets exactly /compute_backends (no trailing slash) and returns the rows verbatim", async () => {
    const rows: ComputeBackend[] = [
      {
        id: "worker:team",
        url: "https://team.example:63579",
        shares_core_disk: false,
        verdict: null,
      },
      { id: "worker:gpu", url: null, shares_core_disk: false, verdict: null },
    ];
    mocks.get.mockResolvedValue({ data: rows });

    const result = await listComputeBackends();

    expect(mocks.get).toHaveBeenCalledWith("/compute_backends");
    expect(result).toEqual(rows);
  });
});

describe("addComputeBackend", () => {
  it("posts the name, URL and token as given and returns the row", async () => {
    const row = {
      id: "worker:team",
      url: "https://team.example:63579",
      shares_core_disk: false,
      verdict: null,
    };
    mocks.post.mockResolvedValue({ status: 201, data: row });

    const input = { name: "team", url: "https://team.example:63579/", token: "row-token" };
    const result = await addComputeBackend(input);

    expect(mocks.post).toHaveBeenCalledWith("/compute_backends", {
      name: "team",
      url: "https://team.example:63579/",
      token: "row-token",
    });
    expect(result).toEqual(row);
  });

  it("posts a CA certificate as given", async () => {
    mocks.post.mockResolvedValue({ status: 201, data: { id: "worker:team", url: "https://t" } });
    const pem = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";

    await addComputeBackend({ name: "team", url: "https://t", token: "x", ca_cert: pem });

    expect(mocks.post).toHaveBeenCalledWith("/compute_backends", {
      name: "team",
      url: "https://t",
      token: "x",
      ca_cert: pem,
    });
  });

  it("propagates a rejection raw, so the caller reads the route's detail", async () => {
    const failure = httpError(409, "Compute backend 'worker:team' already exists");
    mocks.post.mockRejectedValue(failure);

    await expect(addComputeBackend({ name: "team", url: "https://t", token: "x" })).rejects.toBe(
      failure,
    );
  });
});

describe("removeComputeBackend", () => {
  it("deletes the encoded id", async () => {
    mocks.delete.mockResolvedValue({ status: 204, data: "" });

    await removeComputeBackend("worker:team");

    expect(mocks.delete).toHaveBeenCalledWith("/compute_backends/worker%3Ateam");
  });
});

describe("handshakeComputeBackend", () => {
  it("posts the encoded id's handshake and returns the verdict verbatim", async () => {
    const answer = verdict({
      core: capabilities({
        plugin_libs: { polars_ds: "lib/python3.12/site-packages/polars_ds/_polars_ds.abi3.so" },
        operations: ["database_read"],
      }),
      backend: capabilities({ python_version: "3.12.7" }),
      offered: capabilities({ python_version: "3.12.7", plugins: {} }),
      differences: ["python_version: core 3.12.15, backend 3.12.7"],
      absent: { polars_ds: "not installed" },
    });
    mocks.post.mockResolvedValue({ data: answer });

    const result = await handshakeComputeBackend("worker:team");

    expect(mocks.post).toHaveBeenCalledWith("/compute_backends/worker%3Ateam/handshake");
    expect(result).toEqual(answer);
  });

  it("propagates a rejection raw", async () => {
    const failure = new Error("404");
    mocks.post.mockRejectedValue(failure);

    await expect(handshakeComputeBackend("worker:gone")).rejects.toBe(failure);
  });
});
