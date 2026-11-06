// @vitest-environment happy-dom
// The catalog panel's Hub badge follows the tree's rule: none on a server whose every catalog is the hub's, one on a
// hub catalog shown beside this machine's, never on this machine's own.
import { h, type FunctionalComponent } from "vue";
import { createPinia } from "pinia";
import { describe, expect, it, vi } from "vitest";
import { ns } from "../../__fixtures__/catalogTree";
import { element, mountIn } from "../../__fixtures__/mount";
import type { NamespaceTree } from "../../types/catalog.types";
import type { HubPresence } from "./hubRows";

vi.mock("element-plus", () => ({
  ElMessage: { success: vi.fn(), error: vi.fn() },
  ElMessageBox: { confirm: vi.fn() },
}));
vi.mock("../../services/axios.config", () => ({ default: {} }));
vi.mock("../../api/catalog.api", () => ({ CatalogApi: { deleteNamespace: vi.fn() } }));
vi.mock("../CloudConnectionView/api", () => ({
  fetchCloudStorageConnectionsInterfaces: vi.fn().mockResolvedValue([]),
}));
vi.mock("../../composables/useResourceSharing", () => ({
  useResourceSharing: () => ({
    canManage: () => false,
    canManageGrants: () => false,
    isShared: () => false,
    sharedLabel: () => "",
    sharedTitle: () => "",
  }),
}));
vi.mock("./EditCatalogStorageModal.vue", () => ({ default: { render: () => null } }));

import NamespaceDetailPanel from "./NamespaceDetailPanel.vue";

const stubs: Record<string, FunctionalComponent> = {
  "el-tooltip": (_, { slots }) => h("span", slots.default?.()),
  "el-button": element("button"),
};

const storage_uri = "s3://acme/data";
const catalog = ns({ storage_uri });
const hubCatalog = ns({ storage_uri, id: -10, name: "main", source: "hub", hub_id: 10 });

let host: HTMLElement;

const mount = async (namespace: NamespaceTree, hubPresence: HubPresence) => {
  host = await mountIn(NamespaceDetailPanel, {
    props: { namespace, hubPresence },
    pinia: createPinia(),
    stubs,
  });
};

const badge = () => host.querySelector('[data-testid="hub-badge"]');

describe("NamespaceDetailPanel", () => {
  it("badges a hub catalog shown beside this machine's", async () => {
    await mount(hubCatalog, "beside");

    expect(badge()).not.toBeNull();
    expect(host.querySelector('[data-testid="hub-managed-note"]')).not.toBeNull();
  });

  it("badges nothing on a server whose every catalog is the hub's", async () => {
    await mount(hubCatalog, "only");

    expect(badge()).toBeNull();
    expect(host.querySelector('[data-testid="hub-managed-note"]')).not.toBeNull();
  });

  it("badges none of this machine's own, with or without a hub", async () => {
    await mount(catalog, "beside");
    expect(badge()).toBeNull();

    await mount(catalog, "none");
    expect(badge()).toBeNull();
  });
});
