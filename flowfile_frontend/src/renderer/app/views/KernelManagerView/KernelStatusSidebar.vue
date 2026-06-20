<template>
  <aside class="km-sidebar">
    <!-- Section A: image-availability rows -->
    <section class="km-sidebar__section">
      <header class="km-sidebar__header">
        <span class="km-sidebar__title-icon">
          <i class="fa-solid fa-layer-group"></i>
        </span>
        <h3 class="km-sidebar__title">Base images</h3>
        <span class="km-sidebar__count">{{ imageRows.length }}</span>
      </header>

      <ul class="km-image-list">
        <li
          v-for="row in imageRows"
          :key="row.image"
          class="km-image-row"
          :class="`km-image-row--${row.state}`"
        >
          <span class="km-image-row__dot" :title="row.dotTitle"></span>
          <div class="km-image-row__body">
            <div class="km-image-row__header">
              <span class="km-image-row__label">{{ row.label }}</span>
              <span v-if="row.chip" class="km-image-row__chip">{{ row.chip }}</span>
            </div>
            <code v-if="row.subline" class="km-image-row__tag">{{ row.subline }}</code>
            <p v-if="row.state === 'error'" class="km-image-row__error">
              {{ row.errorMessage }}
            </p>
          </div>

          <div class="km-image-row__actions">
            <span v-if="row.state === 'pulling'" class="km-image-row__pulling">
              <span class="loading-spinner loading-spinner--inline"></span>
              Installing…
            </span>
            <button
              v-else-if="row.state === 'update'"
              type="button"
              class="km-image-row__install km-image-row__update"
              :disabled="pendingInstalls.has(row.flavour)"
              title="Pull the new image. Restart existing kernels to use it."
              @click="installImage(row.flavour)"
            >
              <i class="fa-solid fa-circle-up"></i>
              Update
            </button>
            <template v-else-if="row.state === 'missing' || row.state === 'error'">
              <button
                type="button"
                class="km-image-row__install"
                :disabled="pendingInstalls.has(row.flavour)"
                @click="installImage(row.flavour)"
              >
                <i class="fa-solid fa-download"></i>
                {{ row.state === "error" ? "Retry" : "Install" }}
              </button>
              <button
                v-if="canDismiss(row)"
                type="button"
                class="km-image-row__dismiss"
                :title="`Hide ${row.label} for now`"
                @click="dismissImage(row.image)"
              >
                <i class="fa-solid fa-xmark"></i>
              </button>
            </template>
            <span
              v-else-if="row.state === 'installed'"
              class="km-image-row__uptodate"
              title="You're on the latest version"
            >
              <i class="fa-solid fa-circle-check"></i>
              Up to date
            </span>
          </div>
        </li>
      </ul>

      <p v-if="hiddenMissingImages.length > 0" class="km-image-list__hidden">
        Hidden:
        <button
          v-for="img in hiddenMissingImages"
          :key="img.image"
          type="button"
          class="km-image-list__restore"
          :title="`Show ${flavourLabel(img.flavour)} again`"
          @click="undismissImage(img.image)"
        >
          {{ flavourLabel(img.flavour) }}
          <i class="fa-solid fa-rotate-left"></i>
        </button>
      </p>
    </section>

    <!-- Section B: existing kernels -->
    <section class="km-sidebar__section km-sidebar__section--kernels">
      <header class="km-sidebar__header">
        <span class="km-sidebar__title-icon">
          <i class="fa-solid fa-cube"></i>
        </span>
        <h3 class="km-sidebar__title">Your kernels</h3>
        <span class="km-sidebar__count">{{ kernels.length }}</span>
      </header>

      <div v-if="isLoading" class="km-sidebar__loading">
        <div class="loading-spinner"></div>
        <p>Loading kernels…</p>
      </div>

      <div v-else-if="kernels.length === 0" class="km-sidebar__empty">
        <i class="fa-solid fa-server"></i>
        <p>No kernels yet</p>
        <p class="km-sidebar__empty-hint">Fill out the form on the right to create one.</p>
      </div>

      <div v-else class="km-sidebar__kernels">
        <KernelCard
          v-for="kernel in kernels"
          :key="kernel.id"
          :kernel="kernel"
          :busy="isActionInProgress(kernel.id)"
          :memory-info="memoryStats[kernel.id] ?? null"
          @start="(id) => $emit('start', id)"
          @stop="(id) => $emit('stop', id)"
          @details="(id) => $emit('details', id)"
          @delete="(id, name) => $emit('delete', id, name)"
        />
      </div>
    </section>
  </aside>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";
import { ElMessage } from "element-plus";
import {
  KERNEL_FLAVOURS,
  type DockerStatus,
  type ImageFlavour,
  type KernelImageStatus,
  type KernelInfo,
  type KernelMemoryInfo,
} from "../../types";
import { KernelApi } from "../../api/kernel.api";
import KernelCard from "./KernelCard.vue";

const props = defineProps<{
  dockerStatus: DockerStatus | null;
  kernels: KernelInfo[];
  memoryStats: Record<string, KernelMemoryInfo | null>;
  isActionInProgress: (kernelId: string) => boolean;
  isLoading: boolean;
}>();

const emit = defineEmits<{
  (e: "start", id: string): void;
  (e: "stop", id: string): void;
  (e: "details", id: string): void;
  (e: "delete", id: string, name: string): void;
  (e: "refresh-status"): void;
}>();

// ---- image rows: derive a single row state per flavour -------------------

const flavourLabel = (flavour: ImageFlavour): string =>
  KERNEL_FLAVOURS.find((f) => f.value === flavour)?.label ?? flavour;

const baseAvailable = computed<boolean>(() =>
  (props.dockerStatus?.images ?? []).some((i) => i.flavour === "base" && i.available),
);

const canDismiss = (row: ImageRow): boolean =>
  // Don't allow hiding base unless we have at least one baked flavour installed
  // — otherwise the user can dismiss their only path to a working kernel.
  row.flavour !== "base" || baseAvailable.value;

type RowState = "installed" | "local" | "missing" | "pulling" | "error" | "update";
interface ImageRow {
  flavour: ImageFlavour;
  label: string;
  image: string;
  state: RowState;
  subline: string | null;
  chip: string | null;
  errorMessage: string | null;
  dotTitle: string;
}

const imageRows = computed<ImageRow[]>(() => {
  const all = props.dockerStatus?.images ?? [];
  // Only the three baked flavours have a registry image to install/show.
  // Custom is configured per-kernel via its URI, not as a global flavour.
  const baked: ImageFlavour[] = ["base", "lite", "ml"];
  const rows: ImageRow[] = [];
  for (const flavour of baked) {
    const status = all.find((s) => s.flavour === flavour);
    if (!status) continue;
    const label = flavourLabel(flavour);
    if (status.pull_state === "pulling") {
      rows.push({
        flavour,
        label,
        image: status.image,
        state: "pulling",
        subline: null,
        chip: null,
        errorMessage: null,
        dotTitle: "Installing",
      });
      continue;
    }
    if (status.pull_state && status.pull_state.startsWith("error:")) {
      rows.push({
        flavour,
        label,
        image: status.image,
        state: "error",
        subline: status.image,
        chip: null,
        errorMessage: status.pull_state.slice("error:".length),
        dotTitle: "Last install attempt failed",
      });
      continue;
    }
    if (status.available && status.resolved_image && status.resolved_image !== status.image) {
      // A local dev build is in use — respect it, don't nag about updates.
      rows.push({
        flavour,
        label,
        image: status.image,
        state: "local",
        subline: status.resolved_image,
        chip: "local",
        errorMessage: null,
        dotTitle: `Using local build (${status.resolved_image})`,
      });
      continue;
    }
    if (status.update_available) {
      rows.push({
        flavour,
        label,
        image: status.image,
        state: "update",
        subline: status.image,
        chip: "update",
        errorMessage: null,
        dotTitle: `A newer image is available: ${status.image}`,
      });
      continue;
    }
    if (status.available) {
      rows.push({
        flavour,
        label,
        image: status.image,
        state: "installed",
        subline: null,
        chip: null,
        errorMessage: null,
        dotTitle: "Installed",
      });
      continue;
    }
    rows.push({
      flavour,
      label,
      image: status.image,
      state: "missing",
      subline: status.image,
      chip: null,
      errorMessage: null,
      dotTitle: "Not installed",
    });
  }
  // Dismissals only hide rows that are actionable-bad (missing / failed
  // install). Once a flavour is installed — registry default *or* a local
  // build — always show it; old "I don't care about ML" dismissals
  // shouldn't make a now-installed ML invisible.
  return rows.filter(
    (r) => !(r.state === "missing" || r.state === "error") || !dismissedImages.value.has(r.image),
  );
});

// ---- dismissals (carried over from the old banner) -----------------------

const DISMISSED_KEY = "flowfile.kernel.dismissedMissingImages";

const loadDismissed = (): Set<string> => {
  try {
    const raw = localStorage.getItem(DISMISSED_KEY);
    if (!raw) return new Set();
    const parsed = JSON.parse(raw);
    return new Set(Array.isArray(parsed) ? parsed : []);
  } catch {
    return new Set();
  }
};

const dismissedImages = ref<Set<string>>(loadDismissed());

const persistDismissed = () => {
  try {
    localStorage.setItem(DISMISSED_KEY, JSON.stringify([...dismissedImages.value]));
  } catch (err) {
    console.warn("Could not persist dismissed kernel image set:", err);
  }
};

const dismissImage = (image: string) => {
  const next = new Set(dismissedImages.value);
  next.add(image);
  dismissedImages.value = next;
  persistDismissed();
};

const undismissImage = (image: string) => {
  const next = new Set(dismissedImages.value);
  next.delete(image);
  dismissedImages.value = next;
  persistDismissed();
};

const hiddenMissingImages = computed<KernelImageStatus[]>(() =>
  (props.dockerStatus?.images ?? []).filter(
    (i) => !i.available && dismissedImages.value.has(i.image),
  ),
);

// ---- install button -------------------------------------------------------

const pendingInstalls = ref<Set<ImageFlavour>>(new Set());

const installImage = async (flavour: ImageFlavour) => {
  if (pendingInstalls.value.has(flavour)) return;
  pendingInstalls.value = new Set(pendingInstalls.value).add(flavour);
  try {
    await KernelApi.pullImage(flavour);
  } catch (err: any) {
    ElMessage.error({ message: `Could not start install: ${err.message || err}` });
  } finally {
    pendingInstalls.value.delete(flavour);
    pendingInstalls.value = new Set(pendingInstalls.value);
    emit("refresh-status");
  }
};

// ---- pull-state polling (matches old behaviour) --------------------------

const PULL_POLL_INTERVAL_MS = 3000;
let pullPollTimer: ReturnType<typeof setInterval> | null = null;

const stopPullPoll = () => {
  if (pullPollTimer !== null) {
    clearInterval(pullPollTimer);
    pullPollTimer = null;
  }
};

watch(
  () => (props.dockerStatus?.images ?? []).some((i) => i.pull_state === "pulling"),
  (hasActivePull) => {
    if (hasActivePull && pullPollTimer === null) {
      pullPollTimer = setInterval(() => emit("refresh-status"), PULL_POLL_INTERVAL_MS);
    } else if (!hasActivePull) {
      stopPullPoll();
    }
  },
);

onBeforeUnmount(stopPullPoll);
</script>

<style scoped>
.km-sidebar {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
}

.km-sidebar__section {
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg);
  padding: var(--spacing-3);
  box-shadow: var(--shadow-xs);
}

.km-sidebar__header {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  margin-bottom: var(--spacing-2-5);
}

.km-sidebar__title-icon {
  width: 22px;
  height: 22px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border-radius: var(--border-radius-sm);
  background-color: var(--color-background-tertiary);
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
}

.km-sidebar__title {
  margin: 0;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
  flex: 1;
}

.km-sidebar__count {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 22px;
  height: 20px;
  padding: 0 var(--spacing-1-5);
  border-radius: var(--border-radius-full);
  background-color: var(--color-background-tertiary);
  color: var(--color-text-secondary);
  font-size: var(--font-size-2xs);
  font-weight: var(--font-weight-semibold);
}

/* ---- image rows ---- */
.km-image-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.km-image-row {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-2-5);
  background: var(--color-background-secondary);
  border-radius: var(--border-radius-md);
  border: 1px solid transparent;
  transition:
    background-color var(--transition-base) var(--transition-timing),
    border-color var(--transition-base) var(--transition-timing);
}

.km-image-row:hover {
  background-color: var(--color-background-tertiary);
}

.km-image-row--missing {
  border-color: var(--color-warning, rgba(220, 150, 0, 0.4));
  background: var(--color-warning-light, rgba(220, 150, 0, 0.06));
}
.km-image-row--error {
  border-color: var(--color-danger, rgba(200, 50, 50, 0.4));
  background: var(--color-danger-light, rgba(200, 50, 50, 0.06));
}
.km-image-row--local {
  border-color: var(--color-border-light);
  background: var(--color-background-secondary);
}
.km-image-row--update {
  border-color: var(--color-primary, rgba(59, 130, 246, 0.4));
  background: var(--color-primary-light, rgba(59, 130, 246, 0.06));
}
.km-image-row--update .km-image-row__dot {
  background: var(--color-primary, #3b82f6);
}

/* Dark: saturated amber/red fills go muddy on navy, so soften the border + drop the fill. */
[data-theme="dark"] .km-image-row--missing {
  border-color: rgba(245, 158, 11, 0.5);
  background: rgba(245, 158, 11, 0.08);
}
[data-theme="dark"] .km-image-row--error {
  border-color: rgba(239, 68, 68, 0.5);
  background: rgba(239, 68, 68, 0.08);
}

.km-image-row__dot {
  flex: 0 0 auto;
  width: 0.6em;
  height: 0.6em;
  border-radius: 50%;
  background: var(--color-text-tertiary, #888);
  margin-top: 0.45em;
}
.km-image-row--installed .km-image-row__dot {
  background: var(--color-success, #2e7d32);
}
.km-image-row--local .km-image-row__dot {
  background: var(--color-success);
}
.km-image-row--missing .km-image-row__dot {
  background: var(--color-warning, rgba(220, 150, 0, 0.9));
}
.km-image-row--pulling .km-image-row__dot {
  background: var(--color-warning, rgba(220, 150, 0, 0.9));
  animation: km-dot-pulse 1.2s ease-in-out infinite;
  box-shadow: 0 0 0 4px rgba(245, 158, 11, 0.18);
}
.km-image-row--error .km-image-row__dot {
  background: var(--color-danger, rgba(200, 50, 50, 0.9));
}

@keyframes km-dot-pulse {
  0%,
  100% {
    opacity: 0.4;
  }
  50% {
    opacity: 1;
  }
}

.km-image-row__body {
  flex: 1 1 auto;
  min-width: 0;
}

.km-image-row__header {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
}

.km-image-row__chip {
  display: inline-block;
  padding: 0 var(--spacing-1);
  border-radius: var(--border-radius-sm);
  background-color: var(--color-background-tertiary);
  color: var(--color-text-secondary);
  font-size: var(--font-size-2xs);
  font-weight: var(--font-weight-semibold);
  text-transform: uppercase;
  letter-spacing: 0.04em;
}

.km-image-row__tag {
  display: block;
  margin-top: var(--spacing-0-5);
  font-family: var(--font-family-mono);
  font-size: var(--font-size-2xs);
  color: var(--color-text-secondary);
  word-break: break-all;
}

.km-image-row__error {
  margin: var(--spacing-0-5) 0 0;
  font-size: var(--font-size-2xs);
  color: var(--color-danger);
  line-height: 1.4;
  overflow-wrap: anywhere;
}

.km-image-row__actions {
  flex: 0 0 auto;
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-1);
}

.km-image-row__install {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-0-5);
  background: var(--color-warning-dark, var(--color-warning, #b07b00));
  color: #fff;
  border: none;
  border-radius: var(--border-radius-md);
  padding: var(--spacing-1) var(--spacing-2);
  cursor: pointer;
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-semibold);
  transition:
    filter var(--transition-base) var(--transition-timing),
    transform var(--transition-base) var(--transition-timing);
}
.km-image-row__install:hover:not(:disabled) {
  filter: brightness(1.1);
  transform: translateY(-1px);
}
.km-image-row__install:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}
.km-image-row__update {
  background: var(--color-primary, #3b82f6);
}
/* In dark mode --color-warning-dark is a light tint (for text), so the install
   button can't use it as a fill. Keep it a solid amber CTA with dark text. */
[data-theme="dark"] .km-image-row__install:not(.km-image-row__update) {
  background: var(--color-warning);
  color: var(--color-text-inverse);
}

.km-image-row__dismiss {
  background: transparent;
  border: 1px dashed var(--color-warning);
  color: var(--color-warning-dark);
  border-radius: var(--border-radius-sm);
  padding: var(--spacing-0-5) var(--spacing-1);
  cursor: pointer;
  font-size: var(--font-size-xs);
  opacity: 0.7;
}
.km-image-row__dismiss:hover {
  opacity: 1;
}

.km-image-row__pulling {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.km-image-row__uptodate {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-0-5);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  color: var(--color-success, #2e7d32);
  white-space: nowrap;
}

.loading-spinner--inline {
  width: 0.9em;
  height: 0.9em;
  border-width: 2px;
}

.km-image-list__hidden {
  margin: var(--spacing-2) 0 0;
  font-size: var(--font-size-2xs);
  color: var(--color-text-secondary);
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-1);
  align-items: center;
}

.km-image-list__restore {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-0-5);
  background: transparent;
  border: 1px dashed var(--color-warning);
  color: var(--color-warning-dark);
  border-radius: var(--border-radius-sm);
  padding: var(--spacing-0-25) var(--spacing-1);
  cursor: pointer;
  font-size: var(--font-size-2xs);
  font-weight: var(--font-weight-medium);
  text-transform: uppercase;
  letter-spacing: 0.04em;
}
.km-image-list__restore:hover {
  background-color: rgba(0, 0, 0, 0.05);
}
/* On dark, the dashed amber pill reads like a stray focus ring and the black
   hover wash is invisible — quiet it to a neutral restore affordance. */
[data-theme="dark"] .km-image-list__restore {
  border-color: var(--color-border-secondary);
  color: var(--color-text-secondary);
}
[data-theme="dark"] .km-image-list__restore:hover {
  background-color: var(--color-background-tertiary);
  color: var(--color-text-primary);
}

/* ---- kernels section ---- */
.km-sidebar__kernels {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.km-sidebar__loading,
.km-sidebar__empty {
  text-align: center;
  padding: var(--spacing-3) var(--spacing-1);
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
}

.km-sidebar__empty i {
  font-size: 1.5em;
  opacity: 0.4;
  display: block;
  margin-bottom: var(--spacing-1);
}

.km-sidebar__empty p {
  margin: 0;
}

.km-sidebar__empty-hint {
  margin-top: var(--spacing-0-5);
  font-size: var(--font-size-xs);
  opacity: 0.75;
}
</style>
