import { tooltips } from "@codemirror/view";
import type { Extension } from "@codemirror/state";

// Mount CM tooltips on <body>: rendered inside the editor, WKWebView (Tauri)
// flips them to position:absolute, and the node-settings panel's overflow
// chain clips them. The body container carries the editor's theme classes.
export const bodyTooltips = (): Extension => tooltips({ parent: document.body });
