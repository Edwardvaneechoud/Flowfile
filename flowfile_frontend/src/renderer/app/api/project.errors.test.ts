import { describe, expect, it } from "vitest";
import {
  ProjectFolderEdited,
  ProjectUnsavedChanges,
  folderEditedFrom,
  unsavedChangesFrom,
} from "./project.errors";

const change = { change: "added", kind: "flow", label: "orders", path: "flows/orders.flow.yaml" };

const axiosError = (status: number, detail: unknown) => ({
  response: { status, data: { detail } },
});

describe("unsavedChangesFrom", () => {
  it("reads a 409 UNSAVED_CHANGES answer", () => {
    const error = unsavedChangesFrom(
      axiosError(409, {
        code: "UNSAVED_CHANGES",
        message: "1 change(s) are not in the last version",
        changes: [change],
        database_snapshot: true,
      }),
    );
    expect(error).toBeInstanceOf(ProjectUnsavedChanges);
    expect(error?.message).toBe("1 change(s) are not in the last version");
    expect(error?.changes).toEqual([change]);
    expect(error?.databaseSnapshot).toBe(true);
  });

  it("reports a server catalog without a snapshot", () => {
    const error = unsavedChangesFrom(
      axiosError(409, {
        code: "UNSAVED_CHANGES",
        message: "m",
        changes: [],
        database_snapshot: false,
      }),
    );
    expect(error?.databaseSnapshot).toBe(false);
  });

  it("ignores any other failure", () => {
    expect(unsavedChangesFrom(axiosError(409, "a plain conflict"))).toBeNull();
    expect(unsavedChangesFrom(axiosError(409, { code: "OTHER" }))).toBeNull();
    expect(
      unsavedChangesFrom(axiosError(422, { code: "UNSAVED_CHANGES", changes: [change] })),
    ).toBeNull();
    expect(unsavedChangesFrom(new Error("network"))).toBeNull();
    expect(unsavedChangesFrom(undefined)).toBeNull();
  });
});

describe("folderEditedFrom", () => {
  it("reads a 409 FOLDER_EDITED answer", () => {
    const error = folderEditedFrom(
      axiosError(409, { code: "FOLDER_EDITED", message: "1 file(s)", changes: [change] }),
    );
    expect(error).toBeInstanceOf(ProjectFolderEdited);
    expect(error?.message).toBe("1 file(s)");
    expect(error?.changes).toEqual([change]);
  });

  it("ignores an unsaved-changes answer and other failures", () => {
    expect(folderEditedFrom(axiosError(409, { code: "UNSAVED_CHANGES" }))).toBeNull();
    expect(folderEditedFrom(axiosError(500, { code: "FOLDER_EDITED" }))).toBeNull();
    expect(folderEditedFrom(undefined)).toBeNull();
  });
});
