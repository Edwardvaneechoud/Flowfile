// @vitest-environment happy-dom
// The Workspace page: the file browser on its own page, with the server's roots, flows opened in the designer and delete.
import { createPinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { element, mountIn, settle, text } from "../../__fixtures__/mount";

const mocks = vi.hoisted(() => {
  const stored = new Map<string, string>();
  vi.stubGlobal("localStorage", {
    getItem: (key: string) => stored.get(key) ?? null,
    setItem: (key: string, value: string) => void stored.set(key, value),
    removeItem: (key: string) => void stored.delete(key),
    clear: () => stored.clear(),
  });
  return {
    roots: vi.fn(),
    list: vi.fn(),
    deletePath: vi.fn(),
    openFlow: vi.fn(),
    push: vi.fn(),
    confirm: vi.fn(),
    upload: vi.fn(),
    error: vi.fn(),
  };
});

vi.mock("../../api/file.api", () => {
  const FileApi = {
    getBrowseRoots: mocks.roots,
    getDirectoryContents: mocks.list,
    getDefaultPath: async () => "/data/user/workspaces/7",
    deletePath: mocks.deletePath,
    downloadFile: vi.fn(),
    createDirectory: vi.fn(),
  };
  return {
    FileApi,
    getDirectoryContents: mocks.list,
    getDefaultPath: FileApi.getDefaultPath,
    getCatalogFlowsDirectory: vi.fn(),
    createDirectory: FileApi.createDirectory,
    getLocalFiles: vi.fn(),
    getParentPath: vi.fn(),
    joinPath: vi.fn(),
    isRootPath: vi.fn(),
  };
});
vi.mock("../../services/axios.config", () => ({ default: { get: vi.fn(), post: vi.fn() } }));
vi.mock("../../api/fileManager.api", () => ({ FileManagerApi: { uploadFile: mocks.upload } }));
vi.mock("../../composables/useFlowOpener", () => ({
  useFlowOpener: () => ({ openFlow: mocks.openFlow }),
}));
vi.mock("vue-router", () => ({ useRouter: () => ({ push: mocks.push }) }));
vi.mock("element-plus", () => ({
  ElMessage: { success: vi.fn(), error: mocks.error, warning: vi.fn() },
  ElMessageBox: { confirm: mocks.confirm, prompt: vi.fn() },
}));

import WorkspaceView from "./WorkspaceView.vue";

const entry = (path: string, isDirectory = false) => ({
  name: path.split("/").pop(),
  path,
  is_directory: isDirectory,
  size: 10,
  file_type: isDirectory ? "directory" : (path.split(".").pop() ?? ""),
  last_modified: "2026-11-20T10:00:00",
  created_date: "2026-11-20T10:00:00",
  is_hidden: false,
  exists: true,
});

const FOLDERS: Record<string, ReturnType<typeof entry>[]> = {
  "/data/user/workspaces/7": [
    entry("/data/user/workspaces/7/churn", true),
    entry("/data/user/workspaces/7/churn.yaml"),
    entry("/data/user/workspaces/7/notes.txt"),
  ],
  "/data/user/workspaces/7/churn": [entry("/data/user/workspaces/7/churn/customers.csv")],
  "/data/user": [entry("/data/user/uploads", true), entry("/data/user/team.csv")],
};

const stubs = Object.fromEntries(
  ["el-input", "el-select", "el-option", "el-checkbox", "el-dialog", "el-form", "el-form-item"].map(
    (tag) => [tag, element("div")],
  ),
);
stubs["el-button"] = element("button");

let host: HTMLElement;
const names = () => Array.from(host.querySelectorAll(".file-name")).map((el) => text(el));
const item = (name: string) =>
  Array.from(host.querySelectorAll(".file-item")).find(
    (el) => text(el.querySelector(".file-name")) === name,
  )!;
const byTestId = (id: string) => host.querySelector<HTMLElement>(`[data-testid="${id}"]`);
const dblclick = async (el: Element) => {
  el.dispatchEvent(new MouseEvent("dblclick", { bubbles: true }));
  await settle();
};
const click = async (el: Element) => {
  (el as HTMLElement).click();
  await settle();
};

beforeEach(async () => {
  localStorage.clear();
  mocks.roots.mockReset().mockResolvedValue([
    { name: "Workspace", path: "/data/user/workspaces/7" },
    { name: "Shared", path: "/data/user" },
  ]);
  mocks.list.mockReset().mockImplementation(async (path: string) => FOLDERS[path] ?? []);
  mocks.deletePath.mockReset().mockResolvedValue({ path: "", items: 2 });
  mocks.openFlow.mockReset().mockResolvedValue(12);
  mocks.push.mockReset();
  mocks.confirm.mockReset().mockResolvedValue("confirm");
  mocks.upload.mockReset().mockResolvedValue({ filename: "", filepath: "", size: 1 });
  mocks.error.mockReset();
  host = await mountIn(WorkspaceView, { pinia: createPinia(), stubs });
});

describe("the Workspace page", () => {
  it("opens on the person's workspace, offers the server's roots and goes into folders", async () => {
    expect(text(host.querySelector(".page-title"))).toBe("Workspace");
    expect(byTestId("file-browser-root-workspace")).not.toBeNull();
    expect(byTestId("file-browser-root-shared")).not.toBeNull();
    expect(names()).toEqual(["churn", "churn.yaml", "notes.txt"]);
    expect(byTestId("file-browser-new-folder")).not.toBeNull();

    await dblclick(item("churn"));
    expect(names()).toEqual(["customers.csv"]);

    await click(byTestId("file-browser-root-shared")!);
    expect(names()).toEqual(["team.csv", "uploads"]);
  });

  it("opens a flow file in the designer and leaves other files to select", async () => {
    await dblclick(item("notes.txt"));
    expect(mocks.openFlow).not.toHaveBeenCalled();
    await click(item("notes.txt"));
    expect(byTestId("file-browser-open")).toBeNull();

    await click(item("churn.yaml"));
    expect(text(byTestId("file-browser-open"))).toContain("Open in designer");
    await dblclick(item("churn.yaml"));
    expect(mocks.openFlow).toHaveBeenCalledWith("/data/user/workspaces/7/churn.yaml");
    expect(mocks.push).toHaveBeenCalledWith({ name: "designer" });
  });

  it("asks before deleting a folder, naming how many items go with it, then deletes it", async () => {
    await click(item("churn"));
    await click(byTestId("file-browser-delete")!);

    expect(mocks.deletePath).toHaveBeenNthCalledWith(1, "/data/user/workspaces/7/churn", {
      dryRun: true,
    });
    expect(mocks.confirm.mock.calls[0][0]).toBe(
      'Delete the folder "churn" and the 2 items in it? This cannot be undone.',
    );
    expect(mocks.deletePath).toHaveBeenNthCalledWith(2, "/data/user/workspaces/7/churn");
    expect(mocks.list).toHaveBeenLastCalledWith("/data/user/workspaces/7", expect.anything());
  });

  it("deletes nothing when the person cancels", async () => {
    mocks.confirm.mockRejectedValue("cancel");
    await click(item("notes.txt"));
    await click(byTestId("file-browser-delete")!);

    expect(mocks.confirm.mock.calls[0][0]).toBe('Delete "notes.txt"? This cannot be undone.');
    expect(mocks.deletePath).not.toHaveBeenCalled();
  });
});

describe("an upload on the Workspace page", () => {
  const folder = "/data/user/workspaces/7";
  const drop = async (...files: string[]) => {
    const event = new Event("drop", { bubbles: true, cancelable: true });
    const picked = files.map((name) => new File(["a"], name));
    Object.assign(event, { dataTransfer: { files: picked, types: ["Files"] } });
    host.querySelector(".grid-container")!.dispatchEvent(event);
    await settle();
    await settle();
  };
  const sent = () => mocks.upload.mock.calls.map(([file, , opts]) => [file.name, opts]);

  it("sends a file whose name is free without asking", async () => {
    await drop("new.csv");
    expect(mocks.confirm).not.toHaveBeenCalled();
    expect(sent()).toEqual([["new.csv", { directory: folder }]]);
  });

  it("asks before it replaces a file, and replaces it on Replace", async () => {
    await drop("notes.txt");
    expect(mocks.confirm.mock.calls[0][0]).toBe('"notes.txt" is already in this folder.');
    expect(mocks.confirm.mock.calls[0][2]).toMatchObject({
      confirmButtonText: "Replace",
      cancelButtonText: "Keep both",
      distinguishCancelAndClose: true,
    });
    expect(sent()).toEqual([["notes.txt", { directory: folder, replace: true }]]);
  });

  it("keeps both on Keep both, and sends nothing when the question is closed", async () => {
    mocks.confirm.mockRejectedValueOnce("cancel");
    await drop("notes.txt");
    expect(sent()).toEqual([["notes.txt", { directory: folder, unique: true }]]);

    mocks.upload.mockClear();
    mocks.confirm.mockRejectedValueOnce("close");
    await drop("notes.txt", "other.csv");
    expect(sent()).toEqual([["other.csv", { directory: folder }]]);
    expect(mocks.error).not.toHaveBeenCalled();
  });

  it("asks when the server finds the name taken by a file the page does not list", async () => {
    mocks.upload.mockRejectedValueOnce({
      response: { status: 409, data: { detail: '".env" is already in this folder.' } },
    });
    await drop(".env");
    expect(mocks.confirm).toHaveBeenCalledTimes(1);
    expect(sent()).toEqual([
      [".env", { directory: folder }],
      [".env", { directory: folder, replace: true }],
    ]);
    expect(mocks.error).not.toHaveBeenCalled();
  });

  it("says why when the server refuses it", async () => {
    mocks.upload.mockRejectedValueOnce({
      response: {
        status: 400,
        data: { detail: '"churn" is a folder here, so no file takes its name.' },
      },
    });
    await drop("churn");
    expect(mocks.confirm).not.toHaveBeenCalled();
    expect(mocks.error.mock.calls[0][0]).toContain('"churn" is a folder here');
  });
});
