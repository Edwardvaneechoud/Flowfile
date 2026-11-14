import type { ProjectVersionChange } from "../../types";

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

/** The core's 409 `UNSAVED_CHANGES` answer as an error, or null for any other failure. */
export const unsavedChangesFrom = (error: any): ProjectUnsavedChanges | null => {
  const detail = error?.response?.data?.detail;
  if (error?.response?.status !== 409 || detail?.code !== "UNSAVED_CHANGES") return null;
  return new ProjectUnsavedChanges(
    String(detail.message ?? ""),
    Array.isArray(detail.changes) ? detail.changes : [],
    Boolean(detail.database_snapshot),
  );
};
