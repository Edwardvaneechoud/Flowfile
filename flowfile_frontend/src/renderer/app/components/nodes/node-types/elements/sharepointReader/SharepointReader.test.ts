// @vitest-environment happy-dom
import { createApp, nextTick, type ComponentPublicInstance } from "vue";
import { expect, it, onTestFinished, vi } from "vitest";
import SharepointReader from "./SharepointReader.vue";

const post = vi.hoisted(() => vi.fn(async () => ({ data: { id: 1 } as Record<string, unknown> })));
const get = vi.hoisted(() =>
  vi.fn<(url: string) => Promise<{ data: unknown[] }>>(async () => ({ data: [] as unknown[] })),
);
const updateSettings = vi.hoisted(() => vi.fn());

vi.mock("../../../baseNode/genericNodeSettings.vue", () => ({
  default: { template: `<div><slot /><button @click="$emit('request-save')">Apply</button></div>` },
}));
vi.mock("../../../baseNode/page_objects/dropDown.vue", () => ({
  default: { template: "<div />" },
}));
vi.mock("../../../../sharing/ShareDialog.vue", () => ({ default: { template: "<div />" } }));
vi.mock("../../../../../services/auth.service", () => ({
  default: { isInDesktopMode: () => true },
}));
vi.mock("../../../../../services/axios.config", () => ({ default: { post, get } }));
vi.mock("../../../../../stores/node-store", () => ({
  useNodeStore: () => ({ getNodeData: async () => null, updateSettings }),
}));
vi.mock("../../../../../stores/editor-store", () => ({
  useEditorStore: () => ({ disarmRefusedSave: vi.fn() }),
}));
vi.mock("element-plus", () => ({
  ElMessage: { success: vi.fn(), error: vi.fn() },
  ElMessageBox: {},
}));

// Vue exposes unwrapped setup bindings internally, but omits them from its public types.
type FormState = {
  settings: { connection_id?: number; cache_results?: boolean };
  showCreate: boolean;
  authMethod: string;
  clientSecret: string;
  connectionName: string;
  tenantId: string;
  clientId: string;
  createConnection: () => Promise<void>;
  selectedConnection: unknown;
  selectedConnectionId: number;
};

function formState(view: ComponentPublicInstance): FormState {
  return (view.$ as typeof view.$ & { setupState: FormState }).setupState;
}

// These inline components stand in for Element Plus controls in the test.
/* eslint-disable vue/one-component-per-file */
function mountReader(manageOnly = false) {
  const app = createApp(SharepointReader, { manageOnly });
  for (const name of ["dialog", "form", "select", "option", "input", "alert", "switch"]) {
    app.component(`el-${name}`, { template: "<div><slot /><slot name='footer' /></div>" });
  }
  app.component("ElCheckbox", {
    template: `<label><input type="checkbox" @change="$emit('change', $event.target.checked)" /><slot /></label>`,
  });
  app.component("ElButton", { template: "<button><slot /></button>" });
  app.component("ElFormItem", {
    props: { label: { type: String, default: "" }, error: { type: String, default: "" } },
    template: "<div :data-label='label'><slot /><span v-if='error'>{{ error }}</span></div>",
  });
  const host = document.createElement("div");
  const view = app.mount(host) as ComponentPublicInstance;
  onTestFinished(() => app.unmount());
  return { host, view };
}
/* eslint-enable vue/one-component-per-file */

it("shows an error on the invalid field and submits GUIDs without braces", async () => {
  const { host, view } = mountReader();
  const form = formState(view);
  const id = "a01b7819-6a0a-47c9-a6ff-e1563529ac33";
  form.settings = {};
  form.showCreate = true;
  form.authMethod = "client_secret";
  form.clientSecret = "fixture";
  form.connectionName = "Fixture";
  form.tenantId = `{${id}`;
  form.clientId = `{${id}}`;
  await nextTick();
  expect(host.querySelector('[data-label="Tenant ID"]')?.textContent).toContain(
    "Enter a valid GUID.",
  );
  expect(host.querySelector('[data-label="Application (client) ID"]')?.textContent).not.toContain(
    "Enter a valid GUID.",
  );
  await form.createConnection();
  expect(post).not.toHaveBeenCalled();
  form.tenantId = ` {${id}} `;
  await nextTick();
  expect(host.querySelector('[data-label="Tenant ID"]')?.textContent).not.toContain(
    "Enter a valid GUID.",
  );
  await form.createConnection();
  expect(post).toHaveBeenCalledWith(
    "/sharepoint_connections",
    expect.objectContaining({ tenant_id: id, client_id: id }),
  );
});

it("creates a connection without node settings and loads it in the node drawer", async () => {
  const saved = { id: 1, connection_name: "Page connection", can_manage: true };
  get.mockResolvedValueOnce({ data: [] });
  post.mockResolvedValueOnce({ data: saved });
  const { host, view } = mountReader(true);
  const form = formState(view);
  await nextTick();
  expect(host.textContent).toContain("New SharePoint connection");
  expect(host.textContent).not.toContain("Browse SharePoint");
  form.showCreate = true;
  form.authMethod = "client_secret";
  form.clientSecret = "fixture";
  form.connectionName = saved.connection_name;
  form.tenantId = form.clientId = "a01b7819-6a0a-47c9-a6ff-e1563529ac33";
  await form.createConnection();
  expect(form.selectedConnection).toEqual(saved);

  get.mockImplementation(async (url) => ({
    data: url === "/sharepoint_connections" ? [saved] : [],
  }));
  const drawer = mountReader().view;
  await drawer.$.exposed?.loadNodeData(42);
  const drawerForm = formState(drawer);
  drawerForm.selectedConnectionId = saved.id;
  expect(drawerForm.settings.connection_id).toBe(saved.id);
  expect(drawerForm.selectedConnection).toEqual(saved);
});

it("saves incremental reads with Cache Results off when applied to a cached reader", async () => {
  updateSettings.mockClear();
  get.mockImplementation(async (url) => ({
    data: url === "/sharepoint_connections" ? [{ id: 1, connection_name: "Sales" }] : [],
  }));
  const { host, view } = mountReader();
  await (
    view as ComponentPublicInstance & { loadNodeData: (id: number) => Promise<void> }
  ).loadNodeData(1);
  const form = formState(view);
  form.selectedConnectionId = 1;
  Object.assign(form.settings, { drive_id: "drive", file_path: "sales.csv", cache_results: true });
  await nextTick();

  const incremental = host.querySelector<HTMLInputElement>(
    '[data-label="Incremental read"] input',
  )!;
  incremental.checked = true;
  incremental.dispatchEvent(new Event("change"));
  expect(form.settings.cache_results).toBe(true);
  const apply = Array.from(host.querySelectorAll("button")).find(
    (button) => button.textContent === "Apply",
  )!;
  apply.click();

  await vi.waitFor(() => expect(updateSettings).toHaveBeenCalledTimes(1));
  expect(updateSettings.mock.calls[0][0].value).toMatchObject({
    cache_results: false,
    delta_generation: expect.any(Number),
  });
});
