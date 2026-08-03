<template>
  <div class="card km-form-card mb-3">
    <button
      type="button"
      class="card-header km-form-card__header"
      :class="{ 'km-form-card__header--expanded': isExpanded }"
      :aria-expanded="isExpanded"
      @click="isExpanded = !isExpanded"
    >
      <h3 class="card-title km-form-card__title">
        <span class="km-form-card__title-icon" aria-hidden="true">
          <i class="fa-solid fa-plus"></i>
        </span>
        Create new kernel
      </h3>
      <i
        class="km-form-card__chevron"
        :class="isExpanded ? 'fa-solid fa-chevron-up' : 'fa-solid fa-chevron-down'"
      ></i>
    </button>
    <transition name="km-collapse">
      <div v-if="isExpanded" class="card-content card-content--relative">
        <KernelCreateForm
          :flavour-info="flavourInfo"
          :image-statuses="imageStatuses"
          :on-create="onCreate"
          @success="isExpanded = false"
        />
      </div>
    </transition>
  </div>
</template>

<script setup lang="ts">
import { ref } from "vue";
import KernelCreateForm from "../../components/kernel/KernelCreateForm.vue";
import type { FlavourInfo, ImageFlavour, KernelConfig, KernelImageStatus } from "../../types";

defineProps<{
  flavourInfo: Map<ImageFlavour, FlavourInfo>;
  imageStatuses: KernelImageStatus[];
  onCreate: (config: KernelConfig) => Promise<void>;
}>();

const isExpanded = ref(true);
</script>

<style scoped>
/* ─── Modern form-card header ────────────────────────────────────────── */
.km-form-card {
  border-radius: var(--border-radius-lg);
  box-shadow: var(--shadow-sm);
  overflow: hidden;
  transition: box-shadow var(--transition-base) var(--transition-timing);
}

.km-form-card:hover {
  box-shadow: var(--shadow-md);
}

.km-form-card__header {
  width: 100%;
  cursor: pointer;
  border: none;
  text-align: left;
  background: linear-gradient(135deg, rgba(8, 145, 178, 0.08) 0%, rgba(102, 126, 234, 0.06) 100%);
  transition: background var(--transition-base) var(--transition-timing);
}

[data-theme="dark"] .km-form-card__header {
  background: linear-gradient(135deg, rgba(8, 145, 178, 0.18) 0%, rgba(102, 126, 234, 0.14) 100%);
}

.km-form-card__header:hover {
  background: linear-gradient(135deg, rgba(8, 145, 178, 0.14) 0%, rgba(102, 126, 234, 0.1) 100%);
}

[data-theme="dark"] .km-form-card__header:hover {
  background: linear-gradient(135deg, rgba(8, 145, 178, 0.24) 0%, rgba(102, 126, 234, 0.2) 100%);
}

.km-form-card__header--expanded {
  background: var(--color-background-muted);
}

[data-theme="dark"] .km-form-card__header--expanded {
  background: var(--color-background-secondary);
}

.km-form-card__title {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
}

.km-form-card__title-icon {
  width: 24px;
  height: 24px;
  border-radius: var(--border-radius-md);
  display: inline-flex;
  align-items: center;
  justify-content: center;
  background: linear-gradient(
    135deg,
    var(--color-accent) 0%,
    var(--color-gradient-purple-start) 100%
  );
  color: #fff;
  font-size: var(--font-size-2xs);
}

.km-form-card__chevron {
  color: var(--color-text-muted);
  transition: transform var(--transition-base) var(--transition-timing);
}

/* Collapse transition */
.km-collapse-enter-active,
.km-collapse-leave-active {
  transition:
    max-height 0.25s var(--transition-timing),
    opacity 0.2s var(--transition-timing);
  overflow: hidden;
}

.km-collapse-enter-from,
.km-collapse-leave-to {
  max-height: 0;
  opacity: 0;
}

.km-collapse-enter-to,
.km-collapse-leave-from {
  max-height: 2000px;
  opacity: 1;
}

.card-content--relative {
  position: relative;
}
</style>
