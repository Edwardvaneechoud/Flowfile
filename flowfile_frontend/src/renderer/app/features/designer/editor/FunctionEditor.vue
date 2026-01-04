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

interface Props {
  editorString: string;
  columns?: string[];
}

const props = withDefaults(defineProps<Props>(), {
  columns: () => [],
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
        options.push({
          label: column,
          type: "variable",
          info: `Column: ${column}`,
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

      const regexFunction = /\b([a-zA-Z_]\w*)\(/g;
      const regexColumn = /\[[^\]]+\]/g;
      const regexString = /(["'])(?:(?=(\\?))\2.)*?\1/g;

      // Collect all matches first, then sort them
      const matches = [];

      for (let { from, to } of view.visibleRanges) {
        const text = doc.sliceString(from, to);
        let match;

        // Collect function matches
        regexFunction.lastIndex = 0;
        while ((match = regexFunction.exec(text)) !== null) {
          const start = from + match.index;
          const end = start + match[1].length;
          matches.push({ start, end, type: "function" });
        }

        // Collect column matches
        regexColumn.lastIndex = 0;
        while ((match = regexColumn.exec(text)) !== null) {
          const start = from + match.index;
          const end = start + match[0].length;
          matches.push({ start, end, type: "column" });
        }

        // Collect string matches
        regexString.lastIndex = 0;
        while ((match = regexString.exec(text)) !== null) {
          const start = from + match.index;
          const end = start + match[0].length;
          matches.push({ start, end, type: "string" });
        }
      }

      // Sort matches by their 'from' position
      matches.sort((a, b) => a.start - b.start);

      // Add decorations in sorted order
      for (const match of matches) {
        if (match.type === "function") {
          builder.add(match.start, match.end, Decoration.mark({ class: "cm-function" }));
        } else if (match.type === "column") {
          builder.add(match.start, match.end, Decoration.mark({ class: "cm-column" }));
        } else if (match.type === "string") {
          builder.add(match.start, match.end, Decoration.mark({ class: "cm-string" }));
        }
      }

      return builder.finish();
    }
  },
  {
    decorations: (v) => v.decorations,
  },
);

const extensions: Extension[] = [
  EditorState.tabSize.of(2),
  autocompletion({
    override: [polarsCompletions],
    defaultKeymap: true,
    activateOnTyping: true,
    icons: false,
  }),
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

/* Custom syntax highlighting - these are specific to this editor */
.cm-function {
  color: #f08d49;
  font-weight: bold;
}

.cm-column {
  color: #8ec07c;
}

.cm-string {
  color: #b8bb26;
}
</style>
