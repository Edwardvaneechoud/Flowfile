<template>
  <section class="suggest-box" data-testid="suggest-box">
    <template v-if="usable && !notSetUp">
      <form class="suggest-row" @submit.prevent="submit">
        <el-input
          v-model="request"
          class="suggest-input"
          :placeholder="placeholder"
          :disabled="running"
          maxlength="500"
          data-testid="suggest-box-input"
        />
        <button
          class="btn btn-primary"
          type="submit"
          :disabled="!request.trim() || running"
          data-testid="suggest-box-submit"
        >
          <i
            :class="running ? 'fa-solid fa-spinner fa-spin' : 'fa-solid fa-wand-magic-sparkles'"
          ></i>
          Suggest
        </button>
      </form>
      <p v-if="examples.length && !running" class="examples">
        Try:
        <a v-for="ex in examples" :key="ex" href="#" @click.prevent="useExample(ex)">{{ ex }}</a>
      </p>
      <p v-if="running" class="note" role="status">{{ step }}</p>
      <p v-else-if="error" class="note warn" role="alert">{{ error }}</p>
      <SemanticSuggestOutcome
        v-else-if="outcome"
        :outcome="outcome"
        :show-review-link="showReviewLink"
        @review="emit('review')"
      />
    </template>
    <template v-else>
      <LocalModelSetupCard
        v-if="canOfferLocal || notSetUp"
        compact
        title="Let AI on this computer make specific suggestions"
        @installed="onInstalled"
        @open-settings="router.push({ name: 'ai' })"
      />
      <p class="note">
        Asking for something specific needs AI; there is no rules-only version of it. The Suggest
        buttons still propose roles, joins and metrics from rules without AI.
      </p>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { useRouter } from "vue-router";
import LocalModelSetupCard from "../../../components/ai/LocalModelSetupCard.vue";
import { useSemanticSuggest } from "../../../composables/useSemanticSuggest";
import type {
  SemanticField,
  SemanticModel,
  SemanticSuggestKind,
} from "../../../types/semantic.types";
import SemanticSuggestOutcome from "./SemanticSuggestOutcome.vue";

const props = withDefaults(
  defineProps<{
    model: SemanticModel;
    kinds?: SemanticSuggestKind[] | null;
    showReviewLink?: boolean;
  }>(),
  { kinds: null, showReviewLink: true },
);
const emit = defineEmits<{ (e: "review"): void; (e: "done"): void }>();

const router = useRouter();
const { aiStore, usable, canOfferLocal, running, step, outcome, notSetUp, error, ask } =
  useSemanticSuggest();
const request = ref("");

const scope = computed(() => props.kinds?.[0] ?? null);

/** Real table and column names of the open model, so an example is one this model can serve. */
const names = computed(() => {
  const tables = props.model.tables;
  const isAmount = (f: SemanticField, decimal: boolean) =>
    f.facts.role?.value === "amount" &&
    !/_?id$/i.test(f.name) &&
    (!decimal || /^(float|decimal)/i.test(f.dtype ?? ""));
  const pickAmount = (decimal: boolean) => {
    const t = tables.find((table) => table.fields.some((f) => isAmount(f, decimal)));
    return t ? { t, col: t.fields.find((f) => isAmount(f, decimal))?.name } : null;
  };
  const found = pickAmount(true) ?? pickAmount(false);
  const withAmount = found?.t;
  const amount = found?.col;
  const first = withAmount ?? tables[0];
  const second = tables.find((t) => t !== first);
  const category = tables
    .flatMap((t) => t.fields)
    .find((f) => f.facts.role?.value === "category")?.name;
  return { first: first?.name, second: second?.name, amount, category };
});

const examples = computed(() => {
  const { first, second, amount, category } = names.value;
  if (!first) return [];
  const metric = amount
    ? `a metric for the average ${amount} of ${first}`
    : `a metric counting ${first}`;
  const join = second ? `how are ${first} and ${second} related?` : "";
  const describe = `describe the ${first} table`;
  const role = category ? `mark ${category} as a group-by column` : "";
  const byKind: Record<string, string[]> = {
    metric: [metric],
    join: second ? [`a join between ${first} and ${second}`] : [],
    description: [describe, role],
    role: [role, describe],
  };
  const list = scope.value ? byKind[scope.value] : [metric, describe, join];
  return list.filter(Boolean).slice(0, 3);
});

const placeholder = computed(() => {
  const { first, second } = names.value;
  switch (scope.value) {
    case "join":
      return `Ask for a specific join, e.g. a join between ${first ?? "orders"} and ${second ?? "products"}`;
    case "metric":
      return "Ask for a specific metric, e.g. a metric for average order value";
    case "description":
    case "role":
      return `Ask for specific descriptions or roles, e.g. describe the ${first ?? "orders"} table`;
    default:
      return "Ask for a specific suggestion, e.g. a metric for average order value";
  }
});

async function submit() {
  if (!request.value.trim()) return;
  await ask(request.value, props.kinds);
  if (outcome.value) emit("done");
}

function useExample(text: string) {
  request.value = text;
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

.examples,
.note {
  margin: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.examples a {
  margin-left: var(--spacing-2);
}

.note.warn {
  color: var(--color-warning-dark);
}
</style>
