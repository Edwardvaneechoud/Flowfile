// The code pane's modes, shared with the editor store so a request to open the pane can name one.
export type CodeMode = "flowframe" | "polars" | "project" | "notebook";

export const CODE_MODE_KEY = "flowfile.codeGenerator.mode.v1";
export const CODE_MODES: readonly CodeMode[] = ["flowframe", "polars", "project", "notebook"];
