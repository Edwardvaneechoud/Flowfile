<template>
  <section class="suggest-box" data-testid="suggest-box">
    <form class="suggest-row" @submit.prevent="submit">
      <el-input
        v-model="request"
        class="suggest-input"
        placeholder="What should be suggested? Leave empty for the whole model."
        :disabled="running"
        maxlength="500"
        data-testid="suggest-box-input"
        @keydown.enter="onEnter"
      />
      <el-tooltip :content="SUGGEST_BUSY_TEXT" :disabled="!busy" placement="top">
        <span>
          <button
            class="btn btn-primary"
            type="submit"
            :disabled="running || busy || pending"
            data-testid="suggest-box-submit"
          >
            <i class="fa-solid fa-wand-magic-sparkles"></i>
            Suggest
          </button>
        </span>
      </el-tooltip>
    </form>
    <TaskModelPicker :surface="SEMANTIC_SUGGEST_SURFACE" label="suggestions" />
    <template v-if="notSetUp">
      <LocalModelSetupCard
        compact
        title="Let AI on this computer make specific suggestions"
        @installed="onInstalled"
        @open-settings="router.push({ name: 'ai' })"
      />
      <p class="note">
        A request in words needs AI. Leave the field empty to suggest the whole model from the
        rules.
      </p>
    </template>
    <p v-else-if="error && !running" class="note warn" role="alert">{{ error }}</p>
    <SemanticSuggestOutcome
      v-else-if="outcome && !running"
      :outcome="outcome"
      @review="emit('review')"
    />
  </section>
</template>

<script setup lang="ts">
import { ref } from "vue";
import { useRouter } from "vue-router";
import LocalModelSetupCard from "../../../components/ai/LocalModelSetupCard.vue";
import TaskModelPicker from "../../../components/ai/TaskModelPicker.vue";
import { SUGGEST_BUSY_TEXT } from "../../../composables/useSemanticActivity";
import { useSemanticSuggest } from "../../../composables/useSemanticSuggest";
import { SEMANTIC_SUGGEST_SURFACE } from "../../../composables/useTaskModels";
import SemanticSuggestOutcome from "./SemanticSuggestOutcome.vue";

withDefaults(defineProps<{ pending?: boolean }>(), { pending: false });
const emit = defineEmits<{ (e: "review"): void; (e: "suggest-model"): void }>();

const router = useRouter();
const { aiStore, running, busy, outcome, notSetUp, error, ask, clear } = useSemanticSuggest();
const request = ref("");

/** Empty: the whole model, which the panel runs; words: a specific request to the AI. */
async function submit() {
  if (running.value || busy.value) return;
  if (!request.value.trim()) {
    clear();
    notSetUp.value = false;
    emit("suggest-model");
    return;
  }
  await ask(request.value);
}

function onEnter(e: KeyboardEvent | Event) {
  const key = e as KeyboardEvent;
  if (!key.metaKey && !key.ctrlKey) return;
  key.preventDefault();
  void submit();
}

function onInstalled() {
  notSetUp.value = false;
  void aiStore.loadProviders();
}
</script>

<style scoped>
.suggest-box {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
  padding: var(--spacing-4);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
}

.suggest-row {
  display: flex;
  gap: var(--spacing-2);
}

.suggest-input {
  flex: 1;
}

.note {
  margin: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.note.warn {
  color: var(--color-warning-dark);
}
</style>
