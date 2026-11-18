<script setup lang="ts">
import { computed } from "vue";
import type { SearchHit } from "../../types/search.types";
import HighlightedText from "./HighlightedText.vue";
import { extraMatches } from "./searchGroups";
import { hitIcon, hitKindLabel } from "./searchKinds";
import { secondaryAction } from "./searchNavigation";

const props = withDefaults(
  defineProps<{
    hit: SearchHit;
    tokens: readonly string[];
    active?: boolean;
    detailed?: boolean;
  }>(),
  { active: false, detailed: false },
);

const emit = defineEmits<{
  (e: "open", secondary: boolean): void;
  (e: "hover"): void;
}>();

const location = computed(() => (props.hit.path.length ? props.hit.path.join(" › ") : null));
const matches = computed(() => extraMatches(props.hit).slice(0, props.detailed ? 6 : 3));
const secondary = computed(() => secondaryAction(props.hit));
// In the palette a row is an option of its listbox; on the results page its name is the button that opens it.
const openEvent = (event: MouseEvent) => event.metaKey || event.ctrlKey;
const updated = computed(() => {
  const raw = props.hit.updated_at;
  if (!props.detailed || !raw) return null;
  const date = new Date(raw);
  return Number.isNaN(date.getTime()) ? null : date.toLocaleDateString();
});
</script>

<template>
  <div
    class="search-row"
    :class="{ 'is-active': active, 'is-detailed': detailed }"
    :role="detailed ? undefined : 'option'"
    :aria-selected="detailed ? undefined : active"
    :data-testid="`search-hit-${hit.kind}-${hit.id}`"
    @mousemove="emit('hover')"
    @click="emit('open', openEvent($event))"
  >
    <span class="search-row__icon" aria-hidden="true"><i :class="hitIcon(hit)"></i></span>
    <div class="search-row__body">
      <div class="search-row__title">
        <button
          v-if="detailed"
          type="button"
          class="search-row__open"
          :data-testid="`search-hit-open-${hit.kind}-${hit.id}`"
          @click.stop="emit('open', openEvent($event))"
        >
          <HighlightedText class="search-row__name" :text="hit.name" :tokens="tokens" />
        </button>
        <HighlightedText v-else class="search-row__name" :text="hit.name" :tokens="tokens" />
        <span class="search-row__kind">{{ hitKindLabel(hit) }}</span>
      </div>
      <div class="search-row__meta">
        <HighlightedText
          v-if="location"
          class="search-row__path"
          :text="location"
          :tokens="tokens"
        />
        <span v-if="location && hit.subtitle" class="search-row__dot" aria-hidden="true">·</span>
        <span v-if="hit.subtitle" class="search-row__subtitle">{{ hit.subtitle }}</span>
      </div>
      <div v-if="detailed && hit.description" class="search-row__description">
        <HighlightedText :text="hit.description" :tokens="tokens" />
      </div>
      <div v-if="matches.length" class="search-row__matches">
        <span
          v-for="match in matches"
          :key="`${match.field}:${match.value}`"
          class="search-row__match"
        >
          <span class="search-row__match-label">{{ match.label }}</span>
          <HighlightedText :text="match.value" :tokens="tokens" />
        </span>
      </div>
    </div>
    <div class="search-row__aside">
      <span v-if="updated" class="search-row__updated">{{ updated }}</span>
      <button
        v-if="secondary && (active || detailed)"
        type="button"
        class="search-row__secondary"
        :tabindex="detailed ? 0 : -1"
        :data-testid="`search-hit-secondary-${hit.kind}-${hit.id}`"
        @click.stop="emit('open', true)"
      >
        {{ secondary.label }}
      </button>
    </div>
  </div>
</template>

<style scoped>
.search-row {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-3);
  padding: var(--spacing-2) var(--spacing-3);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  color: var(--color-text-primary);
}

.search-row.is-active {
  background: var(--color-background-selected, var(--color-background-hover));
}

.search-row.is-detailed {
  padding: var(--spacing-3);
  border: 1px solid var(--color-border-light, var(--color-border-primary));
  background: var(--color-background-primary);
}

.search-row.is-detailed:hover {
  border-color: var(--color-accent);
}

.search-row__icon {
  flex: 0 0 28px;
  height: 28px;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: var(--border-radius-md);
  background: var(--color-background-secondary);
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
}

.search-row.is-active .search-row__icon {
  color: var(--color-accent);
}

.search-row__body {
  flex: 1;
  min-width: 0;
}

.search-row__title {
  display: flex;
  align-items: baseline;
  gap: var(--spacing-2);
  min-width: 0;
}

.search-row__name {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.search-row__kind {
  flex: 0 0 auto;
  font-size: var(--font-size-2xs, 10px);
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: var(--color-text-tertiary, var(--color-text-secondary));
}

.search-row__meta {
  display: flex;
  gap: var(--spacing-1);
  min-width: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  white-space: nowrap;
  overflow: hidden;
}

.search-row__path,
.search-row__subtitle {
  overflow: hidden;
  text-overflow: ellipsis;
}

.search-row__path {
  flex: 0 1 auto;
  max-width: 60%;
}

.search-row__description {
  margin-top: var(--spacing-1);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.search-row__matches {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-1);
  margin-top: var(--spacing-1);
}

.search-row__match {
  display: inline-block;
  max-width: 100%;
  padding: 0 var(--spacing-2);
  border-radius: var(--border-radius-full);
  background: var(--color-background-secondary);
  font-size: var(--font-size-2xs, 10px);
  line-height: 18px;
  color: var(--color-text-secondary);
  overflow: hidden;
  white-space: nowrap;
  text-overflow: ellipsis;
}

.search-row__match-label {
  margin-right: var(--spacing-1);
  color: var(--color-text-tertiary, var(--color-text-secondary));
}

.search-row__open {
  min-width: 0;
  max-width: 100%;
  padding: 0;
  border: none;
  background: none;
  color: inherit;
  font: inherit;
  text-align: left;
  cursor: pointer;
  overflow: hidden;
}

.search-row__open .search-row__name {
  display: block;
}

.search-row__open:focus-visible {
  outline: 2px solid var(--color-accent);
  outline-offset: 2px;
  border-radius: var(--border-radius-sm);
}

.search-row__aside {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: var(--spacing-1);
  flex: 0 0 auto;
}

.search-row__updated {
  font-size: var(--font-size-2xs, 10px);
  color: var(--color-text-tertiary, var(--color-text-secondary));
}

.search-row__secondary {
  padding: 2px var(--spacing-2);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-sm);
  background: var(--color-background-primary);
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
  cursor: pointer;
  white-space: nowrap;
}

.search-row__secondary:hover {
  border-color: var(--color-accent);
  color: var(--color-accent);
}
</style>
