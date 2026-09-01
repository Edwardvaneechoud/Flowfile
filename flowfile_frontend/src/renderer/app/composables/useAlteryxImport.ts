import { ref } from "vue";

import { AlteryxApi, type AlteryxImportResponse } from "../api/alteryx.api";

export type AlteryxImportPhase = "idle" | "confirm" | "converting" | "report" | "error";

export const ALTERYX_EXTENSIONS = ["yxmd", "xml"];
// Mirrors the endpoint's own cap so an oversized file never leaves the browser.
export const MAX_WORKFLOW_BYTES = 20 * 1024 * 1024;

function importErrorMessage(error: unknown): string {
  const detail = (error as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
  return detail || (error as Error)?.message || "Import failed";
}

function extensionOf(name: string): string {
  const dot = name.lastIndexOf(".");
  return dot === -1 ? "" : name.slice(dot + 1).toLowerCase();
}

function megabytes(bytes: number): string {
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export function useAlteryxImport() {
  const phase = ref<AlteryxImportPhase>("idle");
  const fileName = ref("");
  const progress = ref(0);
  const result = ref<AlteryxImportResponse | null>(null);
  const errorMessage = ref("");

  let selected: File | null = null;
  let activeController: AbortController | null = null;

  function reset() {
    activeController?.abort();
    activeController = null;
    selected = null;
    phase.value = "idle";
    fileName.value = "";
    progress.value = 0;
    result.value = null;
    errorMessage.value = "";
  }

  function fail(message: string) {
    errorMessage.value = message;
    phase.value = "error";
  }

  function start(file: File) {
    reset();
    fileName.value = file.name;
    const ext = extensionOf(file.name);
    if (!ALTERYX_EXTENSIONS.includes(ext)) {
      fail(`"${file.name}" is not an Alteryx workflow. Pick a .yxmd or .xml file.`);
      return;
    }
    if (file.size > MAX_WORKFLOW_BYTES) {
      const limit = megabytes(MAX_WORKFLOW_BYTES);
      fail(`That file is ${megabytes(file.size)} — the import limit is ${limit}.`);
      return;
    }
    selected = file;
    phase.value = "confirm";
  }

  async function confirmImport(): Promise<void> {
    if (phase.value !== "confirm" || !selected) return;
    const controller = new AbortController();
    activeController = controller;
    phase.value = "converting";
    progress.value = 0;
    try {
      result.value = await AlteryxApi.importWorkflow(
        selected,
        (percent) => {
          progress.value = percent;
        },
        { signal: controller.signal },
      );
      phase.value = "report";
    } catch (error) {
      // cancel() already reset the state; don't drag it back into an error.
      if (controller.signal.aborted) return;
      fail(importErrorMessage(error));
    } finally {
      if (activeController === controller) activeController = null;
    }
  }

  function cancel() {
    reset();
  }

  return { phase, fileName, progress, result, errorMessage, start, confirmImport, cancel, reset };
}
