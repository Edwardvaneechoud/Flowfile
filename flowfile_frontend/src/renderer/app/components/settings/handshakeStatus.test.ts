import { describe, expect, it } from "vitest";
import { verdict } from "../../api/__fixtures__/computeBackends";
import { handshakeStatus } from "./handshakeStatus";

describe("handshakeStatus", () => {
  it("is not checked without a verdict", () => {
    expect(handshakeStatus(null)).toMatchObject({ tone: "neutral", label: "Not checked" });
    expect(handshakeStatus(undefined)).toMatchObject({ tone: "neutral", label: "Not checked" });
  });

  it("matches when the worker is accepted with no differences or absences", () => {
    expect(handshakeStatus(verdict())).toMatchObject({
      tone: "success",
      label: "Matches this server",
    });
  });

  it("names limitations before differences", () => {
    expect(
      handshakeStatus(verdict({ absent: { python: "no" }, differences: ["Polars 1.40"] })),
    ).toMatchObject({ tone: "warning", label: "Accepted with limitations" });
    expect(handshakeStatus(verdict({ differences: ["Polars 1.40"] }))).toMatchObject({
      tone: "warning",
      label: "Accepted with differences",
    });
  });

  it("tells a refused worker from one the handshake never reached", () => {
    expect(handshakeStatus(verdict({ ok: false }))).toMatchObject({
      tone: "error",
      label: "Not accepted by this server",
    });
    expect(handshakeStatus(verdict({ ok: false, backend: null }))).toMatchObject({
      tone: "error",
      label: "Handshake failed",
    });
  });

  it("counts an ok verdict without capabilities as failed", () => {
    expect(handshakeStatus(verdict({ backend: null }))).toMatchObject({
      tone: "error",
      label: "Handshake failed",
    });
  });
});
