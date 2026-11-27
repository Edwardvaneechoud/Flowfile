import { describe, expect, it } from "vitest";
import {
  asksForWorkspace,
  deleteQuestion,
  fileTabLabel,
  fileTabRegisters,
  isBrowseRoot,
  listsFile,
  matchesFileTypes,
  offersBrowseRoots,
  pickableTypesNote,
  requestWorkspaceOpen,
  rootContaining,
  takeWorkspaceRequest,
} from "./workspaceRoots";

const roots = [
  { name: "Workspace", path: "/data/user/workspaces/7" },
  { name: "Shared", path: "/data/user" },
];

describe("the browser's roots on a server", () => {
  it("names the workspace for a path inside it, though the shared folder holds it too", () => {
    expect(rootContaining("/data/user/workspaces/7/churn/a.csv", roots)?.name).toBe("Workspace");
    expect(rootContaining("/data/user/workspaces/7/", roots)?.name).toBe("Workspace");
  });

  it("names the shared folder for a path outside the workspace", () => {
    expect(rootContaining("/data/user/team.csv", roots)?.name).toBe("Shared");
  });

  it("names no root for a path outside both, or a sibling sharing a prefix", () => {
    expect(rootContaining("/etc/passwd", roots)).toBeNull();
    expect(rootContaining("/data/user2/x", roots)).toBeNull();
  });

  it("stops Up at each root and nowhere else", () => {
    expect(isBrowseRoot("/data/user/workspaces/7", roots)).toBe(true);
    expect(isBrowseRoot("/data/user/", roots)).toBe(true);
    expect(isBrowseRoot("/data/user/workspaces", roots)).toBe(false);
    expect(isBrowseRoot("/data/user/workspaces/7/churn", roots)).toBe(false);
  });

  it("has no roots on a desktop", () => {
    expect(rootContaining("/Users/me/a.csv", [])).toBeNull();
    expect(isBrowseRoot("/", [])).toBe(false);
  });
});

describe("the dialogs' file tab", () => {
  it("is called Workspace on a server and File System on a desktop", () => {
    expect(fileTabLabel(true)).toBe("Workspace");
    expect(fileTabLabel(false)).toBe("File System");
  });
});

describe("registering a flow from the file tab", () => {
  it("is offered on a desktop that keeps its own catalog", () => {
    expect(fileTabRegisters(false, true)).toBe(true);
  });

  it("is not offered on a server, where a file of the Workspace tab is in no catalog", () => {
    expect(fileTabRegisters(true, true)).toBe(false);
  });

  it("is not offered where this Flowfile's own catalog takes no flows", () => {
    expect(fileTabRegisters(false, false)).toBe(false);
    expect(fileTabRegisters(true, false)).toBe(false);
  });
});

describe("the roots, Upload and Download of a browser", () => {
  it("are offered in a browser of the server's disk that may go anywhere", () => {
    expect(offersBrowseRoots("local", undefined)).toBe(true);
    expect(offersBrowseRoots("local", "")).toBe(true);
  });

  it("are not offered in a browser held to one folder, such as the project picker", () => {
    expect(offersBrowseRoots("local", "/data/user/projects/7")).toBe(false);
  });

  it("are not offered in a browser of object storage", () => {
    expect(offersBrowseRoots("cloud", undefined)).toBe(false);
  });
});

describe("the address that opens the Open dialog on the workspace", () => {
  it("is ?open=workspace", () => {
    expect(asksForWorkspace({ open: "workspace" })).toBe(true);
    expect(asksForWorkspace({ open: "catalog" })).toBe(false);
    expect(asksForWorkspace({})).toBe(false);
  });
});

describe("the request to open the Open dialog on the workspace", () => {
  it("is taken once, by the first dialog that shows", () => {
    expect(takeWorkspaceRequest()).toBe(false);
    requestWorkspaceOpen();
    expect(takeWorkspaceRequest()).toBe(true);
    expect(takeWorkspaceRequest()).toBe(false);
  });
});

describe("the files a browser lists", () => {
  const flows = ["flowfile", "yml", "yaml"];

  it("lists every file in a server's folders, whatever the dialog can pick", () => {
    for (const name of ["chart.png", "customers.csv", "README", "flow.yaml"]) {
      expect(listsFile(name, flows, true)).toBe(true);
    }
  });

  it("lists only the dialog's types on a desktop, as before", () => {
    expect(listsFile("flow.yaml", flows, false)).toBe(true);
    expect(listsFile("chart.png", flows, false)).toBe(false);
    expect(listsFile("README", flows, false)).toBe(false);
  });

  it("lists every file where the dialog names no types", () => {
    expect(listsFile("chart.png", [], false)).toBe(true);
    expect(matchesFileTypes("README", [])).toBe(true);
  });

  it("matches a type by the end of the name, in any letter case", () => {
    expect(matchesFileTypes("Flow.YAML", flows)).toBe(true);
    expect(matchesFileTypes("flow.yaml.png", flows)).toBe(false);
  });

  it("says in one sentence which files a dialog can pick", () => {
    expect(pickableTypesNote(["flowfile", "yml", "yaml"])).toBe(
      "Only .flowfile, .yml and .yaml files can be chosen here.",
    );
    expect(pickableTypesNote(["parquet"])).toBe("Only .parquet files can be chosen here.");
  });
});

describe("the question before a delete", () => {
  it("names a file", () => {
    expect(deleteQuestion("a.csv", false, 0)).toBe('Delete "a.csv"? This cannot be undone.');
  });

  it("says how many items go with a folder", () => {
    expect(deleteQuestion("churn", true, 1)).toBe(
      'Delete the folder "churn" and the 1 item in it? This cannot be undone.',
    );
    expect(deleteQuestion("churn", true, 12)).toBe(
      'Delete the folder "churn" and the 12 items in it? This cannot be undone.',
    );
  });

  it("says when a folder is empty", () => {
    expect(deleteQuestion("old", true, 0)).toBe(
      'Delete the empty folder "old"? This cannot be undone.',
    );
  });
});
