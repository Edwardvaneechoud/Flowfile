import { describe, expect, it } from "vitest";

import { deltaProbeHint } from "./cloudDeltaProbe";

const httpError = (status: number, detail: unknown) => ({
  message: `Request failed with status code ${status}`,
  response: { status, data: { detail } },
});

describe("deltaProbeHint", () => {
  it("stays quiet for a table that does not exist yet", () => {
    const err = httpError(404, {
      error_code: "NOT_A_DELTA_TABLE",
      message: "No Delta table exists at this path.",
    });
    expect(deltaProbeHint(err)).toBeNull();
  });

  it("surfaces missing local AWS credentials", () => {
    const err = httpError(400, {
      error_code: "DELTA_ERROR",
      message:
        "No AWS credentials found in the local AWS profile or environment. Select a cloud storage connection on the node, or configure AWS credentials.",
    });
    expect(deltaProbeHint(err)).toBe(
      "Could not check the table at this path: No AWS credentials found in the local AWS profile or environment. Select a cloud storage connection on the node, or configure AWS credentials.",
    );
  });

  it("surfaces a refused or missing connection", () => {
    expect(
      deltaProbeHint(
        httpError(400, {
          error_code: "CONNECTION_REQUIRED",
          message: "Select a cloud connection to browse object storage.",
        }),
      ),
    ).toMatch(/Select a cloud connection/);
    expect(
      deltaProbeHint(
        httpError(404, {
          error_code: "CONNECTION_NOT_FOUND",
          message: "Cloud connection not found.",
        }),
      ),
    ).toMatch(/Cloud connection not found/);
  });

  it("reduces a nested delta-rs S3 error to the provider's message", () => {
    const esc = String.fromCharCode(27);
    const raw =
      `Kernel error -> Generic S3 error\n  ${esc}[31m↳${esc}[0m Error performing list request\n` +
      `   ${esc}[31m↳${esc}[0m 404 Not Found\n<?xml version="1.0" encoding="UTF-8"?>\n` +
      "<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message>" +
      "<BucketName>b</BucketName></Error>";
    expect(deltaProbeHint(httpError(400, { error_code: "DELTA_ERROR", message: raw }))).toBe(
      "Could not check the table at this path: The specified bucket does not exist (NoSuchBucket)",
    );
  });

  it("flattens and caps a long multi-line message", () => {
    const esc = String.fromCharCode(27);
    const raw = `${esc}[31mfirst${esc}[0m\n   second ${"x".repeat(400)}`;
    const hint = deltaProbeHint(httpError(400, { error_code: "DELTA_ERROR", message: raw }))!;
    expect(hint.startsWith("Could not check the table at this path: first second x")).toBe(true);
    expect(hint).not.toContain(esc);
    expect(hint.endsWith("…")).toBe(true);
    expect(hint.length).toBeLessThan(300);
  });

  it("falls back to a string detail, then the error message", () => {
    expect(deltaProbeHint(httpError(500, "Internal Server Error"))).toMatch(
      /Internal Server Error$/,
    );
    expect(deltaProbeHint(new Error("Network Error"))).toBe(
      "Could not check the table at this path: Network Error",
    );
    expect(deltaProbeHint(undefined)).toBe("Could not check the table at this path: unknown error");
  });
});
