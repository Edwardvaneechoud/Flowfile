// A subtle, dismissible "no kernel attached" FYI shown when a user types in a cell that has
// no kernel (so Jedi code intelligence can't run). It's a quiet bottom panel bar — not a
// toast, not modal. Dismissing it stores a flag in sessionStorage so it never nags again
// that session. No-ops when LSP is disabled or a kernel is already attached.
import { StateEffect, StateField, type Extension } from "@codemirror/state";
import { EditorView, ViewPlugin, showPanel, type Panel } from "@codemirror/view";

import { LspApi } from "@/api/lsp.api";
import type { LspContext } from "./lspCompletionSource";

const DISMISS_KEY = "flowfile.lspNoKernelHint.dismissed";

function isDismissed(): boolean {
  try {
    return sessionStorage.getItem(DISMISS_KEY) === "1";
  } catch {
    return false;
  }
}

function markDismissed(): void {
  try {
    sessionStorage.setItem(DISMISS_KEY, "1");
  } catch {
    // sessionStorage unavailable (private mode) — the hint simply re-shows; harmless.
  }
}

const setHintVisible = StateEffect.define<boolean>();

function buildHintPanel(view: EditorView): Panel {
  const dom = document.createElement("div");
  dom.className = "cm-lsp-no-kernel-hint";

  const text = document.createElement("span");
  text.textContent = "No kernel attached — attach one for live code intelligence.";

  const dismiss = document.createElement("button");
  dismiss.type = "button";
  dismiss.className = "cm-lsp-no-kernel-hint-dismiss";
  dismiss.textContent = "✕";
  dismiss.title = "Dismiss for this session";
  dismiss.addEventListener("click", () => {
    markDismissed();
    view.dispatch({ effects: setHintVisible.of(false) });
  });

  dom.appendChild(text);
  dom.appendChild(dismiss);
  return { dom, top: false };
}

export function createNoKernelHint(getCtx: () => LspContext): Extension {
  // Per-editor so the closure captures this cell's kernel context; caps loads once (cached).
  // `ready` gates the hint until the probe resolves, so it never flashes before we know.
  const caps = { enabled: true, ready: false };

  const hintField = StateField.define<boolean>({
    create: () => false,
    update(value, tr) {
      for (const e of tr.effects) if (e.is(setHintVisible)) return e.value;
      // Recompute only on typing, so it never pops up on focus / empty cells.
      if (tr.docChanged) return caps.ready && caps.enabled && !getCtx().kernelId && !isDismissed();
      return value;
    },
    provide: (f) => showPanel.from(f, (on) => (on ? buildHintPanel : null)),
  });

  const capsLoader = ViewPlugin.fromClass(
    class {
      constructor() {
        void LspApi.capabilities()
          .then((c) => {
            caps.enabled = c.enabled;
          })
          .catch(() => {
            // Probe failed — stay optimistic (the hint is harmless if LSP is actually off).
          })
          .finally(() => {
            caps.ready = true;
          });
      }
    },
  );

  return [hintField, capsLoader];
}
