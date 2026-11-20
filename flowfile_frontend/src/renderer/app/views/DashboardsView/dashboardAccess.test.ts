import { describe, expect, it } from "vitest";
import { mayEdit } from "./dashboardAccess";

describe("mayEdit", () => {
  it("offers editing to an owner and a manager, not to a use grant", () => {
    expect(mayEdit({ is_owner: true, access_level: "owner" })).toBe(true);
    expect(mayEdit({ is_owner: false, access_level: "manage", shared_by: "team" })).toBe(true);
    expect(mayEdit({ is_owner: false, access_level: "use", shared_by: "team" })).toBe(false);
  });

  it("keeps editing offered where the rights are unknown", () => {
    expect(mayEdit(null)).toBe(true);
    expect(mayEdit(undefined)).toBe(true);
  });
});
