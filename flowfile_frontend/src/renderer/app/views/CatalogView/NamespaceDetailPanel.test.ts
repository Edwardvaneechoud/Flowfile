// @vitest-environment happy-dom
// The catalog panel says a hub catalog is managed on the hub, and says nothing of the kind for this machine's own.
import { h, type FunctionalComponent } from "vue";
import { createPinia } from "pinia";
import { describe, expect, it, vi } from "vitest";
import { ns } from "../../__fixtures__/catalogTree";
import { element, mountIn } from "../../__fixtures__/mount";
import type { NamespaceTree } from "../../types/catalog.types";

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

const mount = async (namespace: NamespaceTree) => {
  host = await mountIn(NamespaceDetailPanel, {
    props: { namespace },
    pinia: createPinia(),
    stubs,
  });
};

const note = () => host.querySelector('[data-testid="hub-managed-note"]');

describe("NamespaceDetailPanel", () => {
  it("notes that a hub catalog is managed on the hub, and not this machine's own", async () => {
    await mount(hubCatalog);
    expect(note()).not.toBeNull();

    await mount(catalog);
    expect(note()).toBeNull();
  });
});
