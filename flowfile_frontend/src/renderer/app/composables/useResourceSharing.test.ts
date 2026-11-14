import { ref } from "vue";
import { describe, expect, it, vi } from "vitest";

const isAdmin = ref(true);
const isMultiUser = ref(false);
vi.mock("../stores/auth-store", () => ({
  useAuthStore: () => ({
    get isAdmin() {
      return isAdmin.value;
    },
  }),
}));
vi.mock("./useMultiUser", () => ({ useMultiUser: () => ({ isMultiUser }) }));

import { useResourceSharing } from "./useResourceSharing";

const owner = { is_owner: true, access_level: "owner" } as const;
const manages = { is_owner: false, access_level: "manage" } as const;
const uses = { is_owner: false, access_level: "use" } as const;

describe("a row another catalog keeps, on a desktop whose one user is its admin", () => {
  const { canManage, canShare, canManageGrants } = useResourceSharing();
  const remote = (access: typeof owner | typeof manages | typeof uses | null) => ({
    source: "hub",
    access,
  });

  it("is shared by its owner and by who manages it, never by who only uses it", () => {
    expect(canShare(remote(owner))).toBe(true);
    expect(canShare(remote(manages))).toBe(true);
    expect(canShare(remote(uses))).toBe(false);
    expect(canShare(remote(null))).toBe(true);
  });

  it("is managed as its own access says, whatever the person is on this machine", () => {
    expect(canManage(remote(uses))).toBe(false);
    expect(canManage(remote(manages))).toBe(true);
  });

  it("lets only its owner hand out manage", () => {
    expect(canManageGrants(remote(owner))).toBe(true);
    expect(canManageGrants(remote(manages))).toBe(false);
  });
});

describe("a row of this install", () => {
  const { canManage, canShare, canManageGrants } = useResourceSharing();

  it("is managed by an admin and shared only in multi-user mode, as before", () => {
    for (const row of [{ access: uses }, { source: "local", access: uses }]) {
      expect(canManage(row)).toBe(true);
      expect(canManageGrants(row)).toBe(true);
      expect(canShare(row)).toBe(false);
    }
  });
});
