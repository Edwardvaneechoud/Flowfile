import { describe, it, expect } from "vitest";

import { CLOUD_CAPABILITIES, LOCAL_CAPABILITIES } from "./provider";

describe("storage capabilities", () => {
  it("keeps every local affordance available", () => {
    expect(LOCAL_CAPABILITIES).toEqual({
      hiddenFiles: true,
      createdDate: true,
      createFile: true,
    });
  });

  it("hides the affordances an object store cannot answer", () => {
    // No hidden-file flag and no creation time exist in S3/ADLS/GCS metadata.
    expect(CLOUD_CAPABILITIES.hiddenFiles).toBe(false);
    expect(CLOUD_CAPABILITIES.createdDate).toBe(false);
  });

  it("still allows naming a new object", () => {
    // Turning this off leaves the cloud writer's browse dialog with no way to
    // target a path that does not exist yet.
    expect(CLOUD_CAPABILITIES.createFile).toBe(true);
  });
});
