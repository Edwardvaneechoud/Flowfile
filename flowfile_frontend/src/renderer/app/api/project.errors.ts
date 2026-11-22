import type { ProjectVersionChange } from "../types";

/** A save refused because the project folder holds edits that were never applied. */
export class ProjectFolderEdited extends Error {
  constructor(
    message: string,
    readonly changes: ProjectVersionChange[],
  ) {
    super(message);
  }
}

/** A restore or reload refused because the last version does not hold some of the current state. */
export class ProjectUnsavedChanges extends Error {
  constructor(
    message: string,
    readonly changes: ProjectVersionChange[],
    readonly databaseSnapshot: boolean,
  ) {
    super(message);
  }
}

const conflict = (error: any, code: string): any => {
  const detail = error?.response?.data?.detail;
  return error?.response?.status === 409 && detail?.code === code ? detail : null;
};

const changesOf = (detail: any): ProjectVersionChange[] =>
  Array.isArray(detail.changes) ? detail.changes : [];

/** The core's 409 `FOLDER_EDITED` answer as an error, or null for any other failure. */
export const folderEditedFrom = (error: any): ProjectFolderEdited | null => {
  const detail = conflict(error, "FOLDER_EDITED");
  return detail ? new ProjectFolderEdited(String(detail.message ?? ""), changesOf(detail)) : null;
};

/** The core's 409 `UNSAVED_CHANGES` answer as an error, or null for any other failure. */
export const unsavedChangesFrom = (error: any): ProjectUnsavedChanges | null => {
  const detail = conflict(error, "UNSAVED_CHANGES");
  return detail
    ? new ProjectUnsavedChanges(
        String(detail.message ?? ""),
        changesOf(detail),
        Boolean(detail.database_snapshot),
      )
    : null;
};

/** A branch, remote, push or pull git refused (409 `GIT_REFUSED`), or a merge waiting (409 `MERGE_IN_PROGRESS`). */
export const gitRefusalFrom = (error: any): Error | null => {
  const detail = conflict(error, "GIT_REFUSED") ?? conflict(error, "MERGE_IN_PROGRESS");
  return detail ? new Error(String(detail.message ?? "")) : null;
};
