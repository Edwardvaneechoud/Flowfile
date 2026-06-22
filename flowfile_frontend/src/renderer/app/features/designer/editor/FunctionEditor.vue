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
import { EditorView, Decoration, DecorationSet, ViewPlugin } from "@codemirror/view";
import { EditorState, Extension } from "@codemirror/state";
import { RangeSetBuilder } from "@codemirror/state";
import { ref, shallowRef, watch, onMounted } from "vue";
import { Codemirror } from "vue-codemirror";
import { autocompletion, CompletionSource, CompletionContext } from "@codemirror/autocomplete";
import axios from "axios";

import { bodyTooltips } from "@/utils/codemirrorTooltips";

interface Props {
  editorString: string;
  columns?: string[];
  // Column name → data type, used to annotate column completions.
  columnTypes?: Record<string, string>;
}

const props = withDefaults(defineProps<Props>(), {
  columns: () => [],
  columnTypes: () => ({}),
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

  if (functionWord && context.state.sliceDoc(functionWord.from - 1, functionWord.from) !== "[") {
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

const insertTextAtCursor = (text: string) => {
  if (view.value) {
    view.value.dispatch({
      changes: {
        from: view.value.state.selection.main.head,
        to: view.value.state.selection.main.head,
        insert: text,
      },
    });
  }
};

const code = ref(props.editorString);
const view = shallowRef<EditorView | null>(null);

type HighlightType = "comment" | "string" | "column" | "function";
interface HighlightSpan {
  start: number;
  end: number;
  type: HighlightType;
}

// Lower number = higher priority when two spans overlap.
const HIGHLIGHT_PRIORITY: Record<HighlightType, number> = {
  comment: 0,
  string: 1,
  column: 2,
  function: 3,
};

const HIGHLIGHT_CLASS: Record<HighlightType, string> = {
  comment: "cm-ff-comment",
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

const highlightPlugin = ViewPlugin.fromClass(
  class {
    decorations: DecorationSet;

    constructor(view: EditorView) {
      this.decorations = this.buildDecorations(view);
    }

    update(update: any) {
      if (update.docChanged || update.viewportChanged) {
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
      }

      // Sort by position, then priority, and sweep to a strictly sorted,
      // non-overlapping set — the two invariants RangeSetBuilder requires.
      spans.sort(
        (a, b) => a.start - b.start || HIGHLIGHT_PRIORITY[a.type] - HIGHLIGHT_PRIORITY[b.type],
      );

      let lastEnd = -1;
      for (const span of spans) {
        if (span.start < lastEnd) continue; // overlaps a claimed region
        builder.add(span.start, span.end, Decoration.mark({ class: HIGHLIGHT_CLASS[span.type] }));
        lastEnd = span.end;
      }

      return builder.finish();
    }
  },
  {
    decorations: (v) => v.decorations,
  },
);

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
  }),
  EditorState.tabSize.of(2),
  autocompletion({
    override: [polarsCompletions],
    defaultKeymap: true,
    activateOnTyping: true,
    icons: false,
  }),
  bodyTooltips(),
  highlightPlugin,
];

const handleReady = (payload: { view: EditorView }) => {
  view.value = payload.view;
};

watch(code, (newCode: string) => {
  emit("update-editor-string", newCode);
});

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
