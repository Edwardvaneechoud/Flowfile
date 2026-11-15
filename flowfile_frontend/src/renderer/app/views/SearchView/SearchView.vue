<script setup lang="ts">
// The full search results page (`/main/search?q=&kind=`), opened from the palette's "See all results".
import { computed, onMounted, onUnmounted, ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import SearchResultRow from "../../features/search/SearchResultRow.vue";
import { groupHits, totalMatches } from "../../features/search/searchGroups";
import { KIND_META, SEARCH_KINDS } from "../../features/search/searchKinds";
import { primaryAction, secondaryAction } from "../../features/search/searchNavigation";
import { useOpenSearchHit } from "../../features/search/useOpenSearchHit";
import { MAX_QUERY_LENGTH, useSearchResults } from "../../features/search/useSearchResults";
import { useGlobalSearchStore } from "../../stores/global-search-store";
import type { SearchHit, SearchKind } from "../../types/search.types";

const KIND_LIMIT = 100;
const OVERVIEW_LIMIT = 6;

const route = useRoute();
const router = useRouter();
const store = useGlobalSearchStore();
const { run } = useOpenSearchHit();

const routeQuery = (): string => (typeof route.query.q === "string" ? route.query.q : "");
const routeKind = (): SearchKind | null => {
  const kind = route.query.kind;
  return typeof kind === "string" && kind in KIND_META ? (kind as SearchKind) : null;
};

const input = ref(routeQuery());
const inputEl = ref<HTMLInputElement | null>(null);
const kind = computed(routeKind);
const trimmed = computed(() => input.value.trim());

const overview = useSearchResults(input, { limitPerKind: OVERVIEW_LIMIT, debounceMs: 200 });
const single = useSearchResults(() => (kind.value ? input.value : ""), {
  kinds: () => (kind.value ? [kind.value] : undefined),
  limitPerKind: KIND_LIMIT,
  debounceMs: 200,
});

const counts = computed(() => overview.response.value?.counts ?? {});
const total = computed(() => totalMatches(overview.response.value));
const facets = computed(() =>
  SEARCH_KINDS.map((meta) => ({ ...meta, count: counts.value[meta.kind] ?? 0 })),
);
const groups = computed(() => groupHits(overview.response.value));
// While another kind's answer is still shown, its hits are not this kind's.
const singleAnswer = computed(() => {
  const answer = single.response.value;
  return answer && answer.kinds.length === 1 && answer.kinds[0] === kind.value ? answer : null;
});
const kindHits = computed<SearchHit[]>(() => singleAnswer.value?.hits ?? []);
const kindTotal = computed(() =>
  kind.value ? (singleAnswer.value?.counts[kind.value] ?? kindHits.value.length) : 0,
);
const active = computed(() => (kind.value ? single : overview));
const activeAnswer = computed(() => (kind.value ? singleAnswer.value : overview.response.value));
const tokens = computed(() => activeAnswer.value?.tokens ?? []);
const truncatedGroups = computed(() =>
  (activeAnswer.value?.truncated_kinds ?? []).map((k) => KIND_META[k].group.toLowerCase()),
);
const atMaxLength = computed(() => input.value.length >= MAX_QUERY_LENGTH);

// The URL follows the input 300 ms after typing stops; Back and Forward (and links) set the input from the URL.
let routeTimer: ReturnType<typeof setTimeout> | null = null;
let lastWritten: string | null = null;
let adoptingRoute = false;

function cancelRouteUpdate(): void {
  if (routeTimer !== null) clearTimeout(routeTimer);
  routeTimer = null;
}

function writeRoute(value: string) {
  lastWritten = value;
  return router.replace({ query: { ...route.query, q: value || undefined } });
}

async function flushRouteUpdate(): Promise<void> {
  if (routeTimer === null) return;
  cancelRouteUpdate();
  if (route.name === "search" && input.value !== routeQuery()) await writeRoute(input.value);
}

watch(input, (value) => {
  cancelRouteUpdate();
  if (adoptingRoute) {
    adoptingRoute = false;
    return;
  }
  routeTimer = setTimeout(() => {
    routeTimer = null;
    if (route.name !== "search" || value === routeQuery()) return;
    void writeRoute(value);
  }, 300);
});

watch(
  () => route.query.q,
  () => {
    if (route.name !== "search") return;
    const q = routeQuery();
    if (lastWritten !== null && q === lastWritten) {
      lastWritten = null;
      return;
    }
    cancelRouteUpdate();
    if (q !== input.value) {
      adoptingRoute = true;
      input.value = q;
    }
  },
);

function showKind(next: SearchKind | null): void {
  void router.push({
    query: { ...route.query, q: trimmed.value || undefined, kind: next ?? undefined },
  });
}

async function openHit(hit: SearchHit, secondary: boolean): Promise<void> {
  // Back from the opened item returns to this query, not to one typed before it.
  await flushRouteUpdate();
  store.remember(input.value);
  await run((secondary && secondaryAction(hit)?.action) || primaryAction(hit));
}

function useRecent(query: string): void {
  input.value = query;
  inputEl.value?.focus();
}

onMounted(() => inputEl.value?.focus());
onUnmounted(cancelRouteUpdate);
</script>

<template>
  <div class="search-view" data-testid="search-view">
    <header class="search-view__header">
      <h1 class="search-view__title">
        <i class="fa-solid fa-magnifying-glass" aria-hidden="true"></i>
        Search
      </h1>
      <div class="search-view__input-row">
        <input
          ref="inputEl"
          v-model="input"
          class="search-view__input"
          type="search"
          placeholder="Search tables, columns, flows, schedules, notebooks, kernels…"
          spellcheck="false"
          autocomplete="off"
          aria-label="Search"
          :maxlength="MAX_QUERY_LENGTH"
          data-testid="search-view-input"
        />
        <i
          v-if="active.loading.value"
          class="fa-solid fa-circle-notch fa-spin search-view__spinner"
          aria-label="Searching"
        ></i>
      </div>
      <p v-if="atMaxLength" class="search-view__note" data-testid="search-view-too-long">
        A search holds at most {{ MAX_QUERY_LENGTH }} characters.
      </p>
    </header>

    <div v-if="!trimmed" class="search-view__intro">
      <p>
        Search across everything you can see: catalogs and schemas, tables and their columns, flows,
        schedules, notebooks, charts, dashboards, global artifacts, Python kernels and custom nodes.
        Every word has to match a name, a <code>catalog.schema</code> path, a description, a column,
        a package or a tag.
      </p>
      <div v-if="store.recent.length" class="search-view__recent">
        <span class="search-view__recent-label">Recent</span>
        <button
          v-for="recent in store.recent"
          :key="recent"
          type="button"
          class="search-view__recent-item"
          @click="useRecent(recent)"
        >
          {{ recent }}
        </button>
      </div>
    </div>

    <div v-else class="search-view__layout">
      <nav class="search-view__facets" aria-label="Filter by kind">
        <button
          type="button"
          class="search-view__facet"
          :class="{ 'is-active': kind === null }"
          @click="showKind(null)"
        >
          <span>All results</span>
          <span class="search-view__facet-count">{{ total }}</span>
        </button>
        <button
          v-for="facet in facets"
          :key="facet.kind"
          type="button"
          class="search-view__facet"
          :class="{ 'is-active': kind === facet.kind, 'is-empty': !facet.count }"
          :disabled="!facet.count && kind !== facet.kind"
          :data-testid="`search-facet-${facet.kind}`"
          @click="showKind(facet.kind)"
        >
          <span><i :class="facet.icon" aria-hidden="true"></i> {{ facet.group }}</span>
          <span class="search-view__facet-count">{{ facet.count }}</span>
        </button>
      </nav>

      <main class="search-view__results">
        <div v-if="active.error.value" class="search-view__state">
          {{ active.error.value }}
          <button type="button" class="search-view__link" @click="active.refresh()">Retry</button>
        </div>

        <template v-else-if="kind">
          <section class="search-view__section">
            <div class="search-view__section-header">
              <h2>
                <i :class="KIND_META[kind].icon" aria-hidden="true"></i> {{ KIND_META[kind].group }}
              </h2>
              <span class="search-view__section-count">
                {{
                  kindTotal > kindHits.length
                    ? `First ${kindHits.length} of ${kindTotal}`
                    : kindTotal
                }}
              </span>
            </div>
            <div v-if="!kindHits.length && singleAnswer" class="search-view__state">
              No {{ KIND_META[kind].group.toLowerCase() }} match “{{ trimmed }}”.
            </div>
            <div class="search-view__list">
              <SearchResultRow
                v-for="hit in kindHits"
                :key="hit.uri"
                :hit="hit"
                :tokens="tokens"
                detailed
                @open="(secondary) => openHit(hit, secondary)"
              />
            </div>
          </section>
        </template>

        <template v-else>
          <div
            v-if="overview.response.value && !groups.length && !overview.loading.value"
            class="search-view__state"
            data-testid="search-view-empty"
          >
            Nothing matches “{{ trimmed }}”.
          </div>
          <section v-for="group in groups" :key="group.kind" class="search-view__section">
            <div class="search-view__section-header">
              <h2><i :class="group.icon" aria-hidden="true"></i> {{ group.label }}</h2>
              <button
                v-if="group.total > group.hits.length"
                type="button"
                class="search-view__link"
                @click="showKind(group.kind)"
              >
                Show all {{ group.total }}
              </button>
              <span v-else class="search-view__section-count">{{ group.total }}</span>
            </div>
            <div class="search-view__list">
              <SearchResultRow
                v-for="hit in group.hits"
                :key="hit.uri"
                :hit="hit"
                :tokens="tokens"
                detailed
                @open="(secondary) => openHit(hit, secondary)"
              />
            </div>
          </section>
        </template>

        <p
          v-if="truncatedGroups.length"
          class="search-view__note"
          data-testid="search-view-truncated"
        >
          More {{ truncatedGroups.join(", ") }} matched than the search examines, so some may be
          missing; add a word to narrow it.
        </p>
      </main>
    </div>
  </div>
</template>

<style scoped>
.search-view {
  max-width: 1200px;
  margin: 0 auto;
  padding: 32px;
  color: var(--color-text-primary);
}

.search-view__header {
  margin-bottom: var(--spacing-6, 24px);
}

.search-view__title {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  margin: 0 0 var(--spacing-4);
  font-size: 24px;
  font-weight: var(--font-weight-semibold);
}

.search-view__title i {
  color: var(--color-accent);
  font-size: 20px;
}

.search-view__input-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  padding: var(--spacing-3) var(--spacing-4);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-lg);
  background: var(--color-background-primary);
}

.search-view__input-row:focus-within {
  border-color: var(--color-accent);
}

.search-view__input {
  flex: 1;
  min-width: 0;
  border: none;
  outline: none;
  background: transparent;
  color: var(--color-text-primary);
  font-size: var(--font-size-lg);
}

.search-view__spinner {
  color: var(--color-text-secondary);
}

.search-view__intro {
  max-width: 720px;
  color: var(--color-text-secondary);
  line-height: 1.6;
}

.search-view__intro code {
  padding: 0 4px;
  border-radius: var(--border-radius-sm);
  background: var(--color-background-secondary);
}

.search-view__recent {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: var(--spacing-2);
  margin-top: var(--spacing-4);
}

.search-view__recent-label {
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-semibold);
}

.search-view__recent-item {
  padding: 2px var(--spacing-3);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-full);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  font-size: var(--font-size-xs);
  cursor: pointer;
}

.search-view__recent-item:hover {
  border-color: var(--color-accent);
}

.search-view__layout {
  display: grid;
  grid-template-columns: 220px minmax(0, 1fr);
  gap: var(--spacing-6, 24px);
  align-items: start;
}

.search-view__facets {
  position: sticky;
  top: var(--spacing-4);
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.search-view__facet {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  border: none;
  border-radius: var(--border-radius-md);
  background: none;
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
  text-align: left;
  cursor: pointer;
}

.search-view__facet i {
  width: 16px;
  color: var(--color-text-secondary);
}

.search-view__facet:hover:not(:disabled) {
  background: var(--color-background-hover);
}

.search-view__facet.is-active {
  background: var(--color-accent-subtle);
  color: var(--color-accent);
}

.search-view__facet.is-active i {
  color: var(--color-accent);
}

.search-view__facet.is-empty {
  color: var(--color-text-tertiary, var(--color-text-secondary));
  cursor: default;
}

.search-view__facet-count {
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.search-view__results {
  min-width: 0;
}

.search-view__section + .search-view__section {
  margin-top: var(--spacing-6, 24px);
}

.search-view__section-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: var(--spacing-2);
}

.search-view__section-header h2 {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  margin: 0;
  font-size: var(--font-size-md, 15px);
  font-weight: var(--font-weight-semibold);
}

.search-view__section-header h2 i {
  color: var(--color-text-secondary);
}

.search-view__section-count {
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
}

.search-view__list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.search-view__link {
  border: none;
  background: none;
  color: var(--color-accent);
  font-size: var(--font-size-sm);
  cursor: pointer;
}

.search-view__state,
.search-view__note {
  padding: var(--spacing-3) 0;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
}

@media (max-width: 760px) {
  .search-view {
    padding: var(--spacing-4);
  }

  .search-view__layout {
    grid-template-columns: minmax(0, 1fr);
  }

  .search-view__facets {
    position: static;
    flex-direction: row;
    flex-wrap: wrap;
  }
}
</style>
