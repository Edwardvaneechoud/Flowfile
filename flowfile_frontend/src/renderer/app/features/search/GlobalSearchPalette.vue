<script setup lang="ts">
// The app-wide search palette (Shift+Cmd/Ctrl+F or the sidebar's search button), mounted once in AppLayout.
import { computed, nextTick, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { useGlobalSearchStore } from "../../stores/global-search-store";
import type { SearchHit, SearchKind, SearchResponse } from "../../types/search.types";
import { IS_MAC, MODIFIER_LABEL, SHIFT_LABEL } from "../../utils/shortcuts";
import SearchResultRow from "./SearchResultRow.vue";
import { inAnotherDialog, isComposingKey, isGlobalSearchShortcut } from "./globalSearchHotkey";
import { flattenGroups, groupHits, totalMatches } from "./searchGroups";
import { SEARCH_KINDS } from "./searchKinds";
import { primaryAction, secondaryAction } from "./searchNavigation";
import { useOpenSearchHit } from "./useOpenSearchHit";
import { MAX_QUERY_LENGTH, useSearchResults } from "./useSearchResults";

const store = useGlobalSearchStore();
const router = useRouter();
const { run } = useOpenSearchHit();

const panelEl = ref<HTMLElement | null>(null);
const inputEl = ref<HTMLInputElement | null>(null);
const listEl = ref<HTMLElement | null>(null);
const kindFilter = ref<SearchKind | null>(null);
let returnFocusTo: HTMLElement | null = null;

const trimmed = computed(() => store.query.trim());
const atMaxLength = computed(() => store.query.length >= MAX_QUERY_LENGTH);
const { response, loading, error, refresh } = useSearchResults(
  () => (store.isOpen ? store.query : ""),
  {
    kinds: () => (kindFilter.value ? [kindFilter.value] : undefined),
    limitPerKind: () => (kindFilter.value ? 25 : 5),
  },
);
// While a kind is picked, the chips' counts come from one cheap all-kinds request.
const countsSearch = useSearchResults(() => (store.isOpen && kindFilter.value ? store.query : ""), {
  limitPerKind: 1,
});

const answersAllKinds = (answer: SearchResponse | null): answer is SearchResponse =>
  !!answer && answer.kinds.length === SEARCH_KINDS.length;
// The answer kept from before a filter change is another filter's, and one kept past a failed search is no longer
// what the input asks, so neither is shown.
const shown = computed<SearchResponse | null>(() => {
  const answer = response.value;
  if (!answer || error.value) return null;
  const picked = kindFilter.value;
  const fits = picked
    ? answer.kinds.length === 1 && answer.kinds[0] === picked
    : answersAllKinds(answer);
  return fits ? answer : null;
});
// The previous query's answer stays on screen while the next one loads, dimmed; Enter waits for the fresh one.
const fresh = computed(() => !!shown.value && shown.value.query === trimmed.value);
const groups = computed(() => groupHits(shown.value));
const flat = computed(() => flattenGroups(groups.value));
const indexByUri = computed(() => new Map(flat.value.map((hit, index) => [hit.uri, index])));
const tokens = computed(() => shown.value?.tokens ?? []);
const activeIndex = ref(0);
const activeHit = computed<SearchHit | null>(() => flat.value[activeIndex.value] ?? null);
const activeSecondary = computed(() => (activeHit.value ? secondaryAction(activeHit.value) : null));
const optionId = (index: number | undefined) => `gsearch-option-${index ?? -1}`;
const activeOptionId = computed(() => (activeHit.value ? optionId(activeIndex.value) : undefined));
const modEnter = `${MODIFIER_LABEL}${IS_MAC ? "" : "+"}↵`;
const shiftEnter = `${SHIFT_LABEL}${IS_MAC ? "" : "+"}↵`;

const allKindsAnswer = ref<{ query: string; counts: Partial<Record<SearchKind, number>> } | null>(
  null,
);
watch([response, countsSearch.response], ([answer, counts]) => {
  for (const candidate of [answer, counts]) {
    if (answersAllKinds(candidate)) {
      allKindsAnswer.value = { query: candidate.query, counts: candidate.counts };
    }
  }
});
// Enter pressed before the answer for what was typed arrived opens that answer's first hit once it lands.
const pendingOpen = ref<{ query: string; kind: SearchKind | null; secondary: boolean } | null>(
  null,
);
watch(shown, () => {
  activeIndex.value = 0;
  const pending = pendingOpen.value;
  if (!pending || !fresh.value) return;
  pendingOpen.value = null;
  if (activeHit.value) void openHit(activeHit.value, pending.secondary);
});
watch([trimmed, kindFilter, error, () => store.isOpen], () => {
  const pending = pendingOpen.value;
  if (!pending) return;
  const stillAsked =
    store.isOpen &&
    !error.value &&
    pending.query === trimmed.value &&
    pending.kind === kindFilter.value;
  if (!stillAsked) pendingOpen.value = null;
});

// The chips keep the last all-kinds counts while the next ones load, so the row does not jump per keystroke.
const chipCounts = computed(() => allKindsAnswer.value?.counts ?? {});
const chipsStale = computed(() => allKindsAnswer.value?.query !== trimmed.value);
const chips = computed(() =>
  SEARCH_KINDS.filter(
    (meta) => (chipCounts.value[meta.kind] ?? 0) > 0 || meta.kind === kindFilter.value,
  ).map((meta) => ({ ...meta, count: chipCounts.value[meta.kind] })),
);
const allCount = computed(() =>
  allKindsAnswer.value
    ? Object.values(chipCounts.value).reduce<number>((sum, n) => sum + (n ?? 0), 0)
    : totalMatches(shown.value),
);

function focusInput(): void {
  inputEl.value?.focus();
}

watch(
  () => store.isOpen,
  async (open) => {
    if (open) {
      returnFocusTo = document.activeElement instanceof HTMLElement ? document.activeElement : null;
      kindFilter.value = null;
      activeIndex.value = 0;
      await nextTick();
      focusInput();
      inputEl.value?.select();
    } else if (returnFocusTo?.isConnected) {
      returnFocusTo.focus();
      returnFocusTo = null;
    }
  },
);

watch(activeIndex, async (index) => {
  await nextTick();
  listEl.value?.querySelector<HTMLElement>(`#${optionId(index)}`)?.scrollIntoView({
    block: "nearest",
  });
});

function move(step: number): void {
  const count = flat.value.length;
  if (count) activeIndex.value = (activeIndex.value + step + count) % count;
}

function setFilter(kind: SearchKind | null): void {
  kindFilter.value = kind;
  focusInput();
}

function cycleFilter(step: number): void {
  const order: Array<SearchKind | null> = [null, ...chips.value.map((chip) => chip.kind)];
  const at = order.indexOf(kindFilter.value);
  kindFilter.value = order[(at + step + order.length) % order.length];
}

function useRecent(query: string): void {
  store.query = query;
  focusInput();
}

function clearRecent(): void {
  store.clearRecent();
  focusInput();
}

function retry(): void {
  refresh();
  focusInput();
}

async function openHit(hit: SearchHit, secondary: boolean): Promise<void> {
  const action = (secondary && secondaryAction(hit)?.action) || primaryAction(hit);
  store.remember(store.query);
  returnFocusTo = null;
  store.close();
  await run(action);
}

function seeAll(kind: SearchKind | null = kindFilter.value): void {
  if (!trimmed.value) return;
  store.remember(store.query);
  returnFocusTo = null;
  store.close();
  void router.push({ name: "search", query: { q: trimmed.value, ...(kind ? { kind } : {}) } });
}

function yieldToDialog(): void {
  returnFocusTo = null;
  store.close();
}

// Focus stays in the palette: anything that would take it out hands it back to the input, except another dialog
// taking focus (one that opened on its own, such as the update prompt), which the palette then makes way for.
function onFocusOut(event: FocusEvent): void {
  const next = event.relatedTarget;
  if (next instanceof Node && panelEl.value?.contains(next)) return;
  if (inAnotherDialog(next, panelEl.value)) return yieldToDialog();
  void nextTick(() => {
    if (!store.isOpen || !document.hasFocus()) return;
    const active = document.activeElement;
    if (panelEl.value?.contains(active)) return;
    if (inAnotherDialog(active, panelEl.value)) return yieldToDialog();
    focusInput();
  });
}

function openActive(secondary: boolean): void {
  if (fresh.value) {
    if (activeHit.value) void openHit(activeHit.value, secondary);
    return;
  }
  if (trimmed.value && !error.value) {
    pendingOpen.value = { query: trimmed.value, kind: kindFilter.value, secondary };
  }
}

function onKeydown(event: KeyboardEvent): void {
  if (isComposingKey(event)) return;
  const onButton = event.target instanceof HTMLButtonElement;
  switch (event.key) {
    case "ArrowDown":
      move(1);
      break;
    case "ArrowUp":
      move(-1);
      break;
    case "Enter":
      if (onButton) return;
      if (event.shiftKey) seeAll();
      else openActive(event.metaKey || event.ctrlKey);
      break;
    case "Escape":
      store.close();
      break;
    case "Tab":
      if (trimmed.value) cycleFilter(event.shiftKey ? -1 : 1);
      focusInput();
      break;
    default:
      // The palette is modal: the app's own shortcuts (Cmd/Ctrl+S, the canvas keys) must not act behind it.
      if (!isGlobalSearchShortcut(event, IS_MAC)) event.stopPropagation();
      return;
  }
  event.preventDefault();
  event.stopPropagation();
}
</script>

<template>
  <Teleport to="body">
    <div
      v-if="store.isOpen"
      class="gsearch__backdrop nokey"
      data-testid="global-search"
      @mousedown.self="store.close()"
    >
      <div
        ref="panelEl"
        class="gsearch__panel"
        role="dialog"
        aria-modal="true"
        aria-label="Search"
        @keydown="onKeydown"
        @focusout="onFocusOut"
      >
        <div class="gsearch__input-row">
          <i class="fa-solid fa-magnifying-glass gsearch__input-icon" aria-hidden="true"></i>
          <input
            ref="inputEl"
            v-model="store.query"
            class="gsearch__input"
            type="text"
            placeholder="Search tables, columns, flows, schedules, notebooks, kernels…"
            spellcheck="false"
            autocomplete="off"
            role="combobox"
            :maxlength="MAX_QUERY_LENGTH"
            :aria-expanded="flat.length > 0"
            aria-controls="gsearch-results"
            aria-autocomplete="list"
            :aria-activedescendant="activeOptionId"
            data-testid="global-search-input"
          />
          <i
            v-if="loading"
            class="fa-solid fa-circle-notch fa-spin gsearch__spinner"
            aria-label="Searching"
          ></i>
          <kbd class="gsearch__kbd">Esc</kbd>
        </div>

        <div
          v-if="trimmed && chips.length"
          class="gsearch__chips"
          :class="{ 'is-stale': chipsStale }"
          data-testid="global-search-chips"
        >
          <button
            type="button"
            class="gsearch__chip"
            :class="{ 'is-active': kindFilter === null }"
            :aria-pressed="kindFilter === null"
            tabindex="-1"
            @click="setFilter(null)"
          >
            All<span v-if="allCount" class="gsearch__chip-count">{{ allCount }}</span>
          </button>
          <button
            v-for="chip in chips"
            :key="chip.kind"
            type="button"
            class="gsearch__chip"
            :class="{ 'is-active': kindFilter === chip.kind }"
            :aria-pressed="kindFilter === chip.kind"
            tabindex="-1"
            :data-testid="`global-search-chip-${chip.kind}`"
            @click="setFilter(chip.kind)"
          >
            <i :class="chip.icon" aria-hidden="true"></i>
            {{ chip.group }}
            <span v-if="chip.count" class="gsearch__chip-count">{{ chip.count }}</span>
          </button>
        </div>

        <div ref="listEl" class="gsearch__results">
          <p v-if="atMaxLength" class="gsearch__state" data-testid="global-search-too-long">
            A search holds at most {{ MAX_QUERY_LENGTH }} characters.
          </p>
          <div v-if="!trimmed" class="gsearch__empty">
            <p class="gsearch__hint">
              Find catalogs and schemas, tables and their columns, flows, schedules, notebooks,
              charts, dashboards, global artifacts, Python kernels and custom nodes. Use dots for
              full names, e.g. <code>sales.orders</code>.
            </p>
            <div v-if="store.recent.length" class="gsearch__recent">
              <div class="gsearch__group-header">
                <span>Recent searches</span>
                <button type="button" class="gsearch__link" tabindex="-1" @click="clearRecent()">
                  Clear
                </button>
              </div>
              <button
                v-for="recent in store.recent"
                :key="recent"
                type="button"
                class="gsearch__recent-item"
                tabindex="-1"
                @click="useRecent(recent)"
              >
                <i class="fa-solid fa-clock-rotate-left" aria-hidden="true"></i>{{ recent }}
              </button>
            </div>
          </div>
          <div v-else-if="error" class="gsearch__state" role="alert">
            {{ error }}
            <button type="button" class="gsearch__link" tabindex="-1" @click="retry()">
              Retry
            </button>
          </div>
          <div
            v-else-if="shown && !flat.length && !loading"
            class="gsearch__state"
            data-testid="global-search-empty"
          >
            Nothing matches “{{ trimmed }}”.
          </div>
          <div
            v-show="trimmed && !error && flat.length"
            id="gsearch-results"
            role="listbox"
            aria-label="Search results"
            :aria-busy="!fresh"
            :class="{ 'is-stale': !fresh }"
          >
            <section
              v-for="group in groups"
              :key="group.kind"
              class="gsearch__group"
              role="group"
              :aria-label="group.label"
            >
              <div class="gsearch__group-header" aria-hidden="true">
                <span><i :class="group.icon"></i> {{ group.label }}</span>
                <button
                  v-if="group.total > group.hits.length"
                  type="button"
                  class="gsearch__link"
                  tabindex="-1"
                  @click="seeAll(group.kind)"
                >
                  See all {{ group.total }}
                </button>
              </div>
              <SearchResultRow
                v-for="hit in group.hits"
                :id="optionId(indexByUri.get(hit.uri))"
                :key="hit.uri"
                :hit="hit"
                :tokens="tokens"
                :active="indexByUri.get(hit.uri) === activeIndex"
                @hover="activeIndex = indexByUri.get(hit.uri) ?? activeIndex"
                @open="(secondary) => openHit(hit, secondary)"
              />
            </section>
          </div>
        </div>

        <footer class="gsearch__footer">
          <span><kbd class="gsearch__kbd">↑</kbd><kbd class="gsearch__kbd">↓</kbd> move</span>
          <span><kbd class="gsearch__kbd">↵</kbd> open</span>
          <span v-if="activeSecondary">
            <kbd class="gsearch__kbd">{{ modEnter }}</kbd> {{ activeSecondary.label.toLowerCase() }}
          </span>
          <span v-if="trimmed"><kbd class="gsearch__kbd">Tab</kbd> filter</span>
          <button
            type="button"
            class="gsearch__see-all"
            :disabled="!trimmed"
            tabindex="-1"
            data-testid="global-search-see-all"
            @click="seeAll()"
          >
            See all results <kbd class="gsearch__kbd">{{ shiftEnter }}</kbd>
          </button>
        </footer>
      </div>
    </div>
  </Teleport>
</template>

<style scoped>
.gsearch__backdrop {
  position: fixed;
  inset: 0;
  z-index: 4000;
  display: flex;
  align-items: flex-start;
  justify-content: center;
  padding: 12vh var(--spacing-4) var(--spacing-4);
  background: rgba(20, 24, 32, 0.36);
}

.gsearch__panel {
  display: flex;
  flex-direction: column;
  width: min(720px, 100%);
  max-height: 72vh;
  overflow: hidden;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-lg);
  background: var(--color-background-primary);
  box-shadow: var(--shadow-xl);
  color: var(--color-text-primary);
}

.gsearch__input-row,
.gsearch__chips,
.gsearch__footer {
  flex-shrink: 0;
}

.gsearch__input-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  padding: var(--spacing-3) var(--spacing-4);
  border-bottom: 1px solid var(--color-border-light, var(--color-border-primary));
}

.gsearch__input-icon {
  color: var(--color-text-secondary);
}

.gsearch__input {
  flex: 1;
  min-width: 0;
  border: none;
  outline: none;
  background: transparent;
  color: var(--color-text-primary);
  font-size: var(--font-size-lg);
}

.gsearch__input::placeholder {
  color: var(--color-text-tertiary, var(--color-text-secondary));
}

.gsearch__spinner {
  color: var(--color-text-secondary);
}

.gsearch__kbd {
  display: inline-block;
  min-width: 18px;
  padding: 0 4px;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-sm);
  background: var(--color-background-secondary);
  color: var(--color-text-secondary);
  font-family: inherit;
  font-size: var(--font-size-2xs, 10px);
  line-height: 16px;
  text-align: center;
}

.gsearch__chips {
  display: flex;
  gap: var(--spacing-1);
  padding: var(--spacing-2) var(--spacing-4);
  overflow-x: auto;
  scrollbar-width: thin;
  border-bottom: 1px solid var(--color-border-light, var(--color-border-primary));
}

.gsearch__chip {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  flex: 0 0 auto;
  padding: 2px var(--spacing-2);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-full);
  background: var(--color-background-primary);
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
  cursor: pointer;
}

.gsearch__chip.is-active {
  border-color: var(--color-accent);
  background: var(--color-accent-subtle);
  color: var(--color-accent);
}

.gsearch__chip-count {
  color: var(--color-text-tertiary, var(--color-text-secondary));
}

.is-stale {
  opacity: 0.6;
  transition: opacity 120ms ease;
}

.gsearch__results {
  flex: 1;
  min-height: 80px;
  overflow-y: auto;
  padding: var(--spacing-2);
}

.gsearch__group + .gsearch__group {
  margin-top: var(--spacing-2);
}

.gsearch__group-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--spacing-1) var(--spacing-3);
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-semibold);
}

.gsearch__link {
  border: none;
  background: none;
  color: var(--color-accent);
  font-size: var(--font-size-xs);
  cursor: pointer;
}

.gsearch__empty,
.gsearch__state {
  padding: var(--spacing-3);
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
}

.gsearch__hint {
  margin: 0 0 var(--spacing-3);
  line-height: 1.5;
}

.gsearch__hint code {
  padding: 0 4px;
  border-radius: var(--border-radius-sm);
  background: var(--color-background-secondary);
}

.gsearch__recent-item {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  width: 100%;
  padding: var(--spacing-2) var(--spacing-3);
  border: none;
  border-radius: var(--border-radius-md);
  background: none;
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
  text-align: left;
  cursor: pointer;
}

.gsearch__recent-item:hover {
  background: var(--color-background-hover);
}

.gsearch__recent-item i {
  color: var(--color-text-secondary);
}

.gsearch__footer {
  display: flex;
  align-items: center;
  gap: var(--spacing-4);
  padding: var(--spacing-2) var(--spacing-4);
  border-top: 1px solid var(--color-border-light, var(--color-border-primary));
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
}

.gsearch__footer > span {
  display: inline-flex;
  align-items: center;
  gap: 4px;
}

.gsearch__see-all {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  margin-left: auto;
  border: none;
  background: none;
  color: var(--color-accent);
  font-size: var(--font-size-xs);
  cursor: pointer;
}

.gsearch__see-all:disabled {
  color: var(--color-text-tertiary, var(--color-text-secondary));
  cursor: default;
}

@media (max-width: 600px) {
  .gsearch__footer > span {
    display: none;
  }
}
</style>
