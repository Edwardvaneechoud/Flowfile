import { describe, expect, it } from "vitest";

import { decideChunkRecovery, isChunkLoadError } from "./chunkReload";

const CHROME_ERROR = new Error(
  "Failed to fetch dynamically imported module: http://localhost:8080/app/views/CatalogView/CatalogView.vue",
);
const FIREFOX_ERROR = new Error(
  "error loading dynamically imported module: http://localhost:8080/app/views/CatalogView/CatalogView.vue",
);
const SAFARI_ERROR = new Error("Importing a module script failed.");
const CSS_ERROR = new Error("Unable to preload CSS for /assets/CatalogView-CuwYzynd.css");

describe("isChunkLoadError", () => {
  it("matches the chunk-load failure of every browser engine", () => {
    expect(isChunkLoadError(CHROME_ERROR)).toBe(true);
    expect(isChunkLoadError(FIREFOX_ERROR)).toBe(true);
    expect(isChunkLoadError(SAFARI_ERROR)).toBe(true);
    expect(isChunkLoadError(CSS_ERROR)).toBe(true);
  });

  it("ignores ordinary navigation errors", () => {
    expect(isChunkLoadError(new Error("Navigation aborted"))).toBe(false);
    expect(isChunkLoadError(new Error("Request failed with status code 401"))).toBe(false);
  });

  it("tolerates a non-Error rejection value", () => {
    expect(isChunkLoadError("Failed to fetch dynamically imported module: x")).toBe(true);
    expect(isChunkLoadError(undefined)).toBe(false);
    expect(isChunkLoadError(null)).toBe(false);
  });
});

describe("decideChunkRecovery", () => {
  it("leaves non-chunk errors to the caller", () => {
    expect(decideChunkRecovery(new Error("Navigation aborted"), "/main/catalog", null)).toBe(
      "not-a-chunk-error",
    );
  });

  it("reloads on the first failure for a target", () => {
    expect(decideChunkRecovery(CHROME_ERROR, "/main/catalog", null)).toBe("reload");
  });

  it("gives up instead of looping when the same target fails again", () => {
    expect(decideChunkRecovery(CHROME_ERROR, "/main/catalog", "/main/catalog")).toBe("give-up");
  });

  it("still retries a different target after an earlier failure", () => {
    expect(decideChunkRecovery(CHROME_ERROR, "/main/connections", "/main/catalog")).toBe("reload");
  });

  it("treats query strings as part of the target", () => {
    expect(decideChunkRecovery(CHROME_ERROR, "/main/catalog?tab=community", "/main/catalog")).toBe(
      "reload",
    );
  });
});
