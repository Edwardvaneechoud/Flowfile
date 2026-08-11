<template>
  <div class="function-editor-root">
    <codemirror
      v-model="code"
      placeholder="Code goes here..."
      :style="{ height: '250px' }"
      :autofocus="true"
      :indent-with-tab="true"
      :tab-size="2"
      :extensions="extensions"
      @ready="handleReady"
    />
  </div>
</template>

<script setup lang="ts">
import {
  EditorView,
  Decoration,
  DecorationSet,
  ViewPlugin,
  WidgetType,
  hoverTooltip,
} from "@codemirror/view";
import { EditorState, Extension, StateEffect } from "@codemirror/state";
import { RangeSetBuilder } from "@codemirror/state";
import { ref, shallowRef, watch, computed, onMounted } from "vue";
import { Codemirror } from "vue-codemirror";
import {
  autocompletion,
  startCompletion,
  CompletionSource,
  CompletionContext,
} from "@codemirror/autocomplete";
import axios from "axios";

import { bodyTooltips } from "@/utils/codemirrorTooltips";
import type { FlowParameter } from "@/types/flow.types";
import { findParamSpans, paramInsertText, shortType, PARAM_INSERT_VARIANT } from "./paramTokens";

interface Props {
  editorString: string;
  columns?: string[];
  // Column name → data type, used to annotate column completions.
  columnTypes?: Record<string, string>;
  // Flow parameters referenced via ${name}; drives highlight/autocomplete/hover.
  parameters?: FlowParameter[];
  // Render ${name} references as atomic pills (default); false = colored text only.
  renderParamPills?: boolean;
}

const props = withDefaults(defineProps<Props>(), {
  columns: () => [],
  columnTypes: () => ({}),
  parameters: () => [],
  renderParamPills: true,
});

const knownParamNames = computed(() => new Set(props.parameters.map((p) => p.name)));
const paramByName = computed(() => {
  const map = new Map<string, FlowParameter>();
  for (const p of props.parameters) map.set(p.name, p);
  return map;
});

const emit = defineEmits(["update-editor-string"]);

const expressionsList = ref<string[]>([]);
const expressionDocs = ref<Record<string, string>>({});

const fetchExpressions = async () => {
  try {
    const response = await axios.get("editor/expressions");
    expressionsList.value = response.data;
  } catch (error) {
    console.error("Failed to fetch expressions:", error);
  }
};

const fetchExpressionDocs = async () => {
  try {
    const response = await axios.get("editor/expression_doc");
    const docsMap: Record<string, string> = {};

    response.data.forEach((category: any) => {
      category.expressions.forEach((expr: any) => {
        docsMap[expr.name] = expr.doc;
      });
    });

    expressionDocs.value = docsMap;
  } catch (error) {
    console.error("Failed to fetch expression docs:", error);
  }
};

onMounted(() => {
  fetchExpressions();
  fetchExpressionDocs();
});

const polarsCompletions: CompletionSource = (context: CompletionContext) => {
  let functionWord = context.matchBefore(/\w+/);
  let columnWord = context.matchBefore(/\[\w*/);

  if (
    (!functionWord || functionWord.from === functionWord.to) &&
    (!columnWord || columnWord.from === columnWord.to) &&
    !context.explicit
  ) {
    return null;
  }

  const options: Array<{
    label: string;
    type: string;
    detail?: string;
    info: string;
    apply: (view: EditorView) => void;
  }> = [];

  const charBefore = functionWord
    ? context.state.sliceDoc(functionWord.from - 1, functionWord.from)
    : "";
  const twoBefore = functionWord
    ? context.state.sliceDoc(functionWord.from - 2, functionWord.from)
    : "";
  // Skip inside a column ref (`[col`) and inside a parameter ref (`${name`) —
  // those are handled by column and parameter completions respectively.
  if (functionWord && charBefore !== "[" && twoBefore !== "${") {
    const currentText = functionWord.text.toLowerCase();

    expressionsList.value
      .filter((funcName) => funcName.toLowerCase().startsWith(currentText))
      .forEach((funcName) => {
        options.push({
          label: funcName,
          type: "function",
          info: expressionDocs.value[funcName] || `Function: ${funcName}`,
          apply: (editorView: EditorView) => {
            const insert = funcName + "(";
            editorView.dispatch({
              changes: { from: functionWord!.from, to: functionWord!.to, insert },
              selection: { anchor: functionWord!.from + insert.length },
            });
          },
        });
      });
  }

  if (columnWord) {
    const bracketContent = columnWord.text.slice(1).toLowerCase();

    props.columns
      .filter((column) => column.toLowerCase().startsWith(bracketContent))
      .forEach((column) => {
        const dtype = props.columnTypes[column];
        options.push({
          label: column,
          type: "variable",
          detail: dtype,
          info: dtype ? `Column: ${column} (${dtype})` : `Column: ${column}`,
          apply: (editorView: EditorView) => {
            editorView.dispatch({
              changes: {
                from: columnWord!.from + 1,
                to: columnWord!.to,
                insert: column,
              },
              selection: { anchor: columnWord!.from + 1 + column.length },
            });
          },
        });
      });
  }

  return {
    from: functionWord?.from || (columnWord ? columnWord.from + 1 : context.pos),
    options,
  };
};

const paramCompletions: CompletionSource = (context: CompletionContext) => {
  const word = context.matchBefore(/\$\{?[A-Za-z0-9_]*/);
  if (!word) return null;

  // Filter/insert positions: the `from` is placed AFTER the `$`/`${` prefix so
  // CodeMirror filters option labels against the parameter name (not the `$`).
  const prefix = word.text.match(/^\$\{?/)?.[0] ?? "$";
  const nameFrom = word.from + prefix.length;

  const options = props.parameters.map((p) => ({
    label: p.name,
    type: "parameter",
    detail: p.type ?? "string",
    info:
      (p.description || "Parameter") +
      (p.default_value !== "" ? ` — default: ${p.default_value}` : ""),
    apply: (editorView: EditorView, _completion: unknown, _from: number, to: number) => {
      // Scan back to the `$` (works whether or not `${` braces were typed) and
      // swallow a trailing auto-closed `}` so we never double the brace.
      const line = editorView.state.doc.lineAt(to);
      const before = line.text.slice(0, to - line.from);
      const m = before.match(/\$\{?[A-Za-z0-9_]*$/);
      const realFrom = m ? to - m[0].length : nameFrom;
      const realTo = editorView.state.sliceDoc(to, to + 1) === "}" ? to + 1 : to;
      const insert = paramInsertText(p, PARAM_INSERT_VARIANT);
      editorView.dispatch({
        changes: { from: realFrom, to: realTo, insert },
        selection: { anchor: realFrom + insert.length },
      });
    },
  }));

  return { from: nameFrom, options, validFor: /^[A-Za-z0-9_]*$/ };
};

const insertTextAtCursor = (text: string) => {
  if (view.value) {
    view.value.dispatch({
      changes: {
        from: view.value.state.selection.main.head,
        to: view.value.state.selection.main.head,
        insert: text,
      },
    });
    // Return focus to the editor: the sidebar button that triggered the insert
    // would otherwise keep it, sending the user's next keystrokes elsewhere.
    view.value.focus();
  }
};

const code = ref(props.editorString);
const view = shallowRef<EditorView | null>(null);

type HighlightType = "comment" | "param" | "string" | "column" | "function";
interface HighlightSpan {
  start: number;
  end: number;
  type: HighlightType;
  // Optional explicit class (params resolve known/unknown per span).
  className?: string;
}

// Lower number = higher priority when two spans overlap. `param` outranks
// `string` so a quoted reference "${p}" reads as a parameter, not a string.
const HIGHLIGHT_PRIORITY: Record<HighlightType, number> = {
  comment: 0,
  param: 1,
  string: 2,
  column: 3,
  function: 4,
};

const HIGHLIGHT_CLASS: Record<HighlightType, string> = {
  comment: "cm-ff-comment",
  param: "cm-ff-param",
  string: "cm-ff-string",
  column: "cm-ff-column",
  function: "cm-ff-function",
};

// Faithful TS port of the backend `find_comment_spans`
// (polars_expr_transformer/process/expression_validator.py): the single source
// of truth for what counts as a `//` comment. Quote state resets every line; a
// `//` inside a quoted run on its line is not a comment; a comment runs to the
// end of its line. Returns absolute doc offsets. Must run on the full document,
// never a visible-range slice — quote parity is line-local.
const findCommentSpans = (text: string): Array<{ start: number; end: number }> => {
  const spans: Array<{ start: number; end: number }> = [];
  let offset = 0;
  for (const line of text.split("\n")) {
    let insideSingle = false;
    let insideDouble = false;
    const len = line.length;
    for (let pos = 0; pos < len; pos++) {
      const ch = line[pos];
      if (ch === "'" && !insideDouble) {
        insideSingle = !insideSingle;
      } else if (ch === '"' && !insideSingle) {
        insideDouble = !insideDouble;
      } else if (
        ch === "/" &&
        pos + 1 < len &&
        line[pos + 1] === "/" &&
        !insideSingle &&
        !insideDouble
      ) {
        spans.push({ start: offset + pos, end: offset + len });
        break;
      }
    }
    offset += len + 1; // + newline
  }
  return spans;
};

// Dispatched when the parameter list changes so the decoration plugins rebuild
// (they otherwise only refresh on doc/viewport changes).
const refreshParamsEffect = StateEffect.define<void>();
const hasParamRefresh = (update: any): boolean =>
  update.transactions?.some((tr: any) => tr.effects.some((e: any) => e.is(refreshParamsEffect)));

const highlightPlugin = ViewPlugin.fromClass(
  class {
    decorations: DecorationSet;

    constructor(view: EditorView) {
      this.decorations = this.buildDecorations(view);
    }

    update(update: any) {
      if (update.docChanged || update.viewportChanged || hasParamRefresh(update)) {
        this.decorations = this.buildDecorations(update.view);
      }
    }

    buildDecorations(view: EditorView) {
      const builder = new RangeSetBuilder<Decoration>();
      const { doc } = view.state;
      const visible = view.visibleRanges;

      // Comments are computed over the whole document (quote parity is
      // line-local) and take priority: text inside a comment is never re-colored
      // as function/column/string, mirroring the backend masking comments before
      // tokenizing.
      const commentSpans = findCommentSpans(doc.toString());
      const intersectsVisible = (s: number, e: number) =>
        visible.some((r) => s < r.to && e > r.from);
      const overlapsComment = (s: number, e: number) =>
        commentSpans.some((c) => s < c.end && e > c.start);

      const spans: HighlightSpan[] = [];
      for (const c of commentSpans) {
        if (c.end > c.start && intersectsVisible(c.start, c.end)) {
          spans.push({ start: c.start, end: c.end, type: "comment" });
        }
      }

      const regexFunction = /\b([a-zA-Z_]\w*)\(/g;
      const regexColumn = /\[[^\]]+\]/g;
      const regexString = /(["'])(?:(?=(\\?))\2.)*?\1/g;

      const pushToken = (start: number, end: number, type: HighlightType) => {
        if (end > start && !overlapsComment(start, end)) {
          spans.push({ start, end, type });
        }
      };

      for (const { from, to } of visible) {
        const text = doc.sliceString(from, to);
        let match: RegExpExecArray | null;

        regexFunction.lastIndex = 0;
        while ((match = regexFunction.exec(text)) !== null) {
          const start = from + match.index;
          pushToken(start, start + match[1].length, "function");
        }

        regexColumn.lastIndex = 0;
        while ((match = regexColumn.exec(text)) !== null) {
          const start = from + match.index;
          pushToken(start, start + match[0].length, "column");
        }

        regexString.lastIndex = 0;
        while ((match = regexString.exec(text)) !== null) {
          const start = from + match.index;
          pushToken(start, start + match[0].length, "string");
        }

        // Parameter references. Colored inline only when pills aren't rendering
        // them (pills replace the same range via a separate plugin). Known vs
        // unknown params get distinct classes.
        if (!props.renderParamPills) {
          for (const s of findParamSpans(text)) {
            const start = from + s.start;
            const end = from + s.end;
            if (end > start && !overlapsComment(start, end)) {
              const className = knownParamNames.value.has(s.name)
                ? "cm-ff-param"
                : "cm-ff-param cm-ff-param-unknown";
              spans.push({ start, end, type: "param", className });
            }
          }
        }
      }

      // Sort by position, then priority, and sweep to a strictly sorted,
      // non-overlapping set — the two invariants RangeSetBuilder requires.
      spans.sort(
        (a, b) => a.start - b.start || HIGHLIGHT_PRIORITY[a.type] - HIGHLIGHT_PRIORITY[b.type],
      );

      let lastEnd = -1;
      for (const span of spans) {
        if (span.start < lastEnd) continue; // overlaps a claimed region
        const cls = span.className ?? HIGHLIGHT_CLASS[span.type];
        builder.add(span.start, span.end, Decoration.mark({ class: cls }));
        lastEnd = span.end;
      }

      return builder.finish();
    }
  },
  {
    decorations: (v) => v.decorations,
  },
);

// Renders a ${name} reference as an atomic pill with a type badge. The document
// text is unchanged (still `${name}`) — only the rendering is replaced.
class ParamWidget extends WidgetType {
  constructor(
    readonly name: string,
    readonly ptype: FlowParameter["type"],
    readonly known: boolean,
  ) {
    super();
  }

  eq(other: ParamWidget): boolean {
    return other.name === this.name && other.ptype === this.ptype && other.known === this.known;
  }

  toDOM(): HTMLElement {
    const pill = document.createElement("span");
    pill.className = "cm-ff-param-pill" + (this.known ? "" : " cm-ff-param-unknown");
    pill.setAttribute("data-param", this.name);
    const label = document.createElement("span");
    label.className = "cm-ff-param-name";
    label.textContent = this.name;
    pill.appendChild(label);
    const badge = document.createElement("span");
    badge.className = "cm-ff-param-badge";
    badge.textContent = shortType(this.ptype);
    pill.appendChild(badge);
    return pill;
  }

  ignoreEvent(): boolean {
    return false;
  }
}

// Kept in its own plugin (never mix `replace` and `mark` in one RangeSetBuilder).
// atomicRanges makes the caret/Backspace treat each pill as a single unit.
const paramPillPlugin = ViewPlugin.fromClass(
  class {
    decorations: DecorationSet;

    constructor(view: EditorView) {
      this.decorations = this.build(view);
    }

    update(update: any) {
      if (
        update.docChanged ||
        update.viewportChanged ||
        update.selectionSet ||
        hasParamRefresh(update)
      ) {
        this.decorations = this.build(update.view);
      }
    }

    build(view: EditorView) {
      const builder = new RangeSetBuilder<Decoration>();
      if (!props.renderParamPills) return builder.finish();
      const { doc, selection } = view.state;
      for (const { from, to } of view.visibleRanges) {
        const text = doc.sliceString(from, to);
        for (const s of findParamSpans(text)) {
          const start = from + s.start;
          const end = from + s.end;
          // Leave the reference the cursor is inside as plain text so it can be
          // typed/edited; it collapses into a pill once the cursor moves out.
          if (selection.ranges.some((r) => r.from < end && r.to > start)) continue;
          const known = knownParamNames.value.has(s.name);
          const meta = paramByName.value.get(s.name);
          builder.add(
            start,
            end,
            Decoration.replace({
              widget: new ParamWidget(s.name, meta?.type, known),
              inclusive: false,
            }),
          );
        }
      }
      return builder.finish();
    }
  },
  {
    decorations: (v) => v.decorations,
    provide: (plugin) =>
      EditorView.atomicRanges.of((view) => view.plugin(plugin)?.decorations ?? Decoration.none),
  },
);

const paramHover = hoverTooltip((view, pos) => {
  const line = view.state.doc.lineAt(pos);
  for (const s of findParamSpans(line.text)) {
    const from = line.from + s.start;
    const to = line.from + s.end;
    if (pos < from || pos > to) continue;
    const meta = paramByName.value.get(s.name);
    return {
      pos: from,
      end: to,
      above: true,
      create: () => {
        const dom = document.createElement("div");
        dom.className = "cm-ff-param-tooltip";
        if (!meta) {
          dom.textContent = `Unknown parameter "${s.name}"`;
          dom.classList.add("cm-ff-param-unknown");
          return { dom };
        }
        const title = document.createElement("div");
        const name = document.createElement("strong");
        name.textContent = meta.name;
        const type = document.createElement("em");
        type.textContent = " " + (meta.type ?? "string");
        title.append(name, type);
        dom.appendChild(title);
        if (meta.default_value !== "") {
          const def = document.createElement("div");
          def.textContent = `default: ${meta.default_value}`;
          dom.appendChild(def);
        }
        if (meta.description) {
          const desc = document.createElement("div");
          desc.textContent = meta.description;
          dom.appendChild(desc);
        }
        return { dom };
      },
    };
  }
  return null;
});

// CodeMirror only auto-opens completions on word characters, so `$` alone never
// triggers the parameter dropdown. Detect a typed `$` and open it explicitly.
const paramTriggerListener = EditorView.updateListener.of((update) => {
  if (!update.docChanged) return;
  let typedDollar = false;
  for (const tr of update.transactions) {
    if (!tr.isUserEvent("input.type")) continue;
    tr.changes.iterChanges((_fromA, _toA, _fromB, _toB, inserted) => {
      if (inserted.toString().includes("$")) typedDollar = true;
    });
  }
  if (typedDollar) {
    // Defer so we don't dispatch during an update cycle.
    Promise.resolve().then(() => startCompletion(update.view));
  }
});

const extensions: Extension[] = [
  // Syntax colors live here (not a global stylesheet) so CodeMirror scopes them
  // to this editor instance — another editor's styles can no longer repaint our
  // tokens. Classes are namespaced `cm-ff-*` for the same reason.
  EditorView.theme({
    "&": { fontSize: "12px" },
    ".cm-content": { fontSize: "12px" },
    ".cm-gutters": { fontSize: "12px" },
    ".cm-ff-function": { color: "var(--color-code-keyword)", fontWeight: "bold" },
    ".cm-ff-column": { color: "var(--color-code-string)" },
    ".cm-ff-string": { color: "var(--color-code-function)" },
    ".cm-ff-comment": { color: "var(--color-text-tertiary)", fontStyle: "italic" },
    ".cm-ff-param": { color: "var(--color-accent)", fontWeight: "500" },
    ".cm-ff-param.cm-ff-param-unknown": {
      color: "var(--color-danger)",
      textDecoration: "underline wavy",
    },
    ".cm-ff-param-pill": {
      display: "inline-flex",
      alignItems: "center",
      gap: "3px",
      padding: "0 4px",
      margin: "0 1px",
      borderRadius: "4px",
      background: "var(--color-accent-subtle)",
      color: "var(--color-accent)",
      border: "1px solid var(--color-accent)",
      fontSize: "11px",
      lineHeight: "1.4",
      whiteSpace: "nowrap",
    },
    ".cm-ff-param-pill.cm-ff-param-unknown": {
      color: "var(--color-danger)",
      borderColor: "var(--color-danger)",
      background: "var(--color-warning-light)",
    },
    ".cm-ff-param-badge": {
      fontSize: "9px",
      textTransform: "uppercase",
      opacity: "0.75",
      fontWeight: "600",
    },
    ".cm-ff-param-tooltip": { padding: "4px 8px", fontSize: "12px", maxWidth: "260px" },
  }),
  EditorState.tabSize.of(2),
  autocompletion({
    override: [polarsCompletions, paramCompletions],
    defaultKeymap: true,
    activateOnTyping: true,
    icons: false,
  }),
  bodyTooltips(),
  highlightPlugin,
  paramPillPlugin,
  paramHover,
  paramTriggerListener,
];

const handleReady = (payload: { view: EditorView }) => {
  view.value = payload.view;
};

watch(code, (newCode: string) => {
  emit("update-editor-string", newCode);
});

// Rebuild param decorations when the flow's parameter list arrives/changes.
watch(
  () => props.parameters,
  () => view.value?.dispatch({ effects: refreshParamsEffect.of() }),
  { deep: true },
);

defineExpose({ insertTextAtCursor });
</script>

<style>
.function-editor-root {
  display: flex;
  flex-direction: column;
}
/* Token colors are defined in the editor's EditorView.theme (cm-ff-*) so they
   stay scoped to this instance — see the extensions array above. */
</style>
