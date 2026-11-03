<template>
  <section
    class="compute-card"
    :class="{ 'is-collapsed': !open }"
    :aria-labelledby="`${cardId}-title`"
  >
    <header class="compute-card__header compute-card__header--toggle">
      <span class="compute-card__icon"><i :class="icon" aria-hidden="true"></i></span>
      <div class="compute-card__intro">
        <h3 :id="`${cardId}-title`" class="compute-card__title">
          <button
            type="button"
            class="compute-card__toggle"
            :aria-expanded="open"
            :aria-controls="`${cardId}-content`"
            :data-testid="`${cardId}-toggle`"
            @click="toggle"
          >
            {{ title }}
          </button>
        </h3>
        <p v-if="$slots.description" v-show="open" class="compute-card__description">
          <slot name="description" />
        </p>
      </div>
      <div v-if="$slots.meta" class="compute-card__meta">
        <slot name="meta" :open="open" />
      </div>
      <i class="fa-solid fa-chevron-down compute-card__chevron" aria-hidden="true"></i>
    </header>
    <el-collapse-transition>
      <div v-show="open" :id="`${cardId}-content`">
        <div class="compute-card__body"><slot /></div>
        <footer v-if="$slots.footer" class="compute-card__footer">
          <slot name="footer" />
        </footer>
      </div>
    </el-collapse-transition>
  </section>
</template>

<script setup lang="ts">
import { useSectionToggle } from "../../components/common/CollapsibleSection/useSectionToggle";

/** A Backends card whose header folds its body; the open state is remembered per card in this browser. */
const props = withDefaults(
  defineProps<{ cardId: string; title: string; icon: string; defaultOpen?: boolean }>(),
  { defaultOpen: true },
);

const { open, toggle } = useSectionToggle(`compute-backends.${props.cardId}`, props.defaultOpen);
</script>

<style scoped src="./computeSettings.css"></style>
<style scoped>
.compute-card__header--toggle {
  position: relative;
  transition:
    background-color var(--transition-fast),
    border-color var(--transition-fast);
}
.compute-card__header--toggle:hover {
  background: var(--color-background-hover);
}
.is-collapsed .compute-card__header--toggle {
  border-bottom-color: transparent;
}
.is-collapsed .compute-card__title {
  margin-bottom: 0;
}
.compute-card__toggle {
  appearance: none;
  margin: 0;
  padding: 0;
  border: 0;
  background: none;
  font: inherit;
  color: inherit;
  text-align: left;
  cursor: pointer;
}
/* Stretches the title's button over the whole header, so the header is one click target. */
.compute-card__toggle::after {
  content: "";
  position: absolute;
  inset: 0;
}
.compute-card__toggle:focus-visible {
  outline: none;
}
.compute-card__toggle:focus-visible::after {
  outline: 2px solid var(--color-border-focus);
  outline-offset: -2px;
}
.compute-card__meta {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  flex-wrap: wrap;
  gap: var(--spacing-2);
  min-width: 0;
}
.compute-card__chevron {
  flex-shrink: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-tertiary);
  transition: transform var(--transition-fast);
}
.is-collapsed .compute-card__chevron {
  transform: rotate(-90deg);
}
.compute-card__header--toggle:hover .compute-card__chevron {
  color: var(--color-accent-dark);
}
</style>
