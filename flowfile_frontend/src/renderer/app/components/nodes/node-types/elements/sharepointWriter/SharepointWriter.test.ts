// @vitest-environment happy-dom
import { createApp, nextTick, type ComponentPublicInstance } from "vue";
import { expect, it, onTestFinished, vi } from "vitest";
import SharepointWriter from "./SharepointWriter.vue";

const send = vi.hoisted(() => vi.fn());
vi.mock("../../../baseNode/genericNodeSettings.vue", () => ({
  default: { template: "<div><slot /></div>" },
}));
vi.mock("../../../../../stores/node-store", () => ({
  useNodeStore: () => ({ getNodeData: async () => ({ setting_input: {
    is_setup: true, connection_id: 1, drive_id: "drive", file_path: "old/report.xlsx",
    format: "xlsx", sheet_name: "Sales", conflict_mode: "overwrite",
  } }) }),
}));
vi.mock("../../../../../services/axios.config", () => ({
  default: { get: async (url: string) => ({ data: url.endsWith("browse_defaults")
    ? { site_url: null } : [{ id: 1, auth_method: "client_secret" }] }) },
}));
vi.mock("../../../../../composables/useNodeSettings", () => ({
  useNodeSettings: (options: any) => ({ saveSettings: () => {
    if (!options.onBeforeSave()) return false;
    send(options.toSavePayload?.(options.nodeRef.value) ?? { ...options.nodeRef.value });
    return true;
  } }),
}));

// Vue exposes unwrapped setup bindings internally, but omits them from its public types.
type FormState = {
  listMode: boolean;
  listTarget: { site_id: string;
  list_id: string };
  mapping: string;
  folders: { name: string }[];
  useFolder: () => void;
};

function formState(view: ComponentPublicInstance): FormState {
  return (view.$ as typeof view.$ & { setupState: FormState }).setupState;
}

it("keeps the file format and filename after applying a list target", async () => {
  const app = createApp(SharepointWriter);
  for (const name of ["form-item", "select", "option", "input", "button", "checkbox", "alert"]) {
    app.component(`el-${name}`, { template: "<div><slot /></div>" });
  }
  const view = app.mount(document.createElement("div")) as ComponentPublicInstance & {
    loadNodeData: (id: number) => Promise<void>;
    saveSettings: () => boolean;
  };
  onTestFinished(() => app.unmount());
  await view.loadNodeData(1);
  const form = formState(view);
  form.listMode = true;
  form.listTarget = { site_id: "site", list_id: "list" };
  form.mapping = '{"name":"Title"}';
  expect(view.saveSettings()).toBe(true);
  expect(send.mock.lastCall?.[0]).not.toHaveProperty("file_path");
  expect(send.mock.lastCall?.[0]).not.toHaveProperty("format");

  form.listMode = false;
  form.folders = [{ name: "new" }];
  form.useFolder();
  await nextTick();
  expect(view.saveSettings()).toBe(true);
  expect(send.mock.lastCall?.[0]).toMatchObject({
    file_path: "new/report.xlsx", format: "xlsx", sheet_name: "Sales",
    list_target: null, conflict_mode: "overwrite",
  });
});
