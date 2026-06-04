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
        <!-- In-progress overlay: covers the form while the backend builds the
           derived image (~30 s for the bake step), so the user gets a clear
           "we're working" signal instead of a frozen-looking form. -->
        <div v-if="isSubmitting" class="creating-overlay">
          <div class="creating-overlay__spinner"></div>
          <p class="creating-overlay__title">{{ submitLabel }}</p>
          <p class="creating-overlay__hint">
            {{
              packages.length > 0
                ? "Building a per-kernel Docker image with your extra packages — this can take ~30 s."
                : "Provisioning kernel…"
            }}
          </p>
        </div>

        <form class="form" @submit.prevent="handleSubmit">
          <div class="form-grid">
            <div class="form-field">
              <label for="kernel-id" class="form-label">Kernel ID</label>
              <input
                id="kernel-id"
                v-model="form.id"
                type="text"
                class="form-input"
                :class="{ 'form-input--error': kernelIdError }"
                placeholder="my-kernel"
                required
              />
              <p v-if="kernelIdError" class="form-error">{{ kernelIdError }}</p>
            </div>

            <div class="form-field">
              <label for="kernel-name" class="form-label">Display Name</label>
              <input
                id="kernel-name"
                v-model="form.name"
                type="text"
                class="form-input"
                placeholder="My Data Science Kernel"
                required
              />
            </div>
          </div>

          <div class="form-field">
            <label for="kernel-flavour" class="form-label">Image flavour</label>
            <select id="kernel-flavour" v-model="form.image_flavour" class="form-input">
              <option
                v-for="flavour in KERNEL_FLAVOURS"
                :key="flavour.value"
                :value="flavour.value"
                :disabled="!isFlavourAvailable(flavour.value)"
              >
                {{ flavour.label
                }}{{
                  isFlavourAvailable(flavour.value) ? "" : " — not installed (use Install above)"
                }}
              </option>
            </select>
            <p class="form-help">
              {{ activeFlavour.description }}
              <span v-if="activeImage" class="form-help-image">
                → <code>{{ activeResolvedImage || activeImage }}</code>
                <span
                  v-if="activeResolvedImage"
                  class="form-help-local"
                  title="Resolved via local-image fallback"
                >
                  local build
                </span>
              </span>
            </p>
          </div>

          <div v-if="form.image_flavour === 'custom'" class="form-field">
            <label for="kernel-custom-image" class="form-label">
              Custom image URI
              <span class="form-label-hint">(must include a version tag)</span>
            </label>
            <input
              id="kernel-custom-image"
              v-model="form.custom_image"
              type="text"
              class="form-input"
              :class="{ 'form-input--error': customImageError }"
              placeholder="myorg/flowfile-kernel-custom:1.0.0"
              required
            />
            <p v-if="customImageError" class="form-error">{{ customImageError }}</p>
            <p v-else class="form-help">
              Must be a Docker image that follows the Flowfile kernel runtime contract. Tags like
              <code>:1.2.3</code> or digests <code>@sha256:…</code> are required so kernel runs are
              reproducible.
            </p>
          </div>

          <div v-if="activePackages.length > 0" class="prebaked-hint">
            <i class="fa-solid fa-check-circle" style="margin-right: 6px"></i>
            <div class="prebaked-content">
              <span class="prebaked-label">Pre-installed in this image:</span>
              <span class="prebaked-list">
                <span v-for="pkg in activePackages" :key="pkg.name" class="prebaked-pkg">
                  <span class="prebaked-pkg__name">{{ pkg.name }}</span>
                  <span class="prebaked-pkg__version">{{ pkg.version }}</span>
                </span>
              </span>
            </div>
          </div>

          <div class="form-field">
            <label for="kernel-packages" class="form-label">
              Extra Python packages
              <span class="form-label-hint">(optional, version pins encouraged)</span>
            </label>
            <div class="chip-input">
              <el-tag
                v-for="(pkg, i) in packages"
                :key="`${pkg}-${i}`"
                closable
                :disable-transitions="false"
                class="chip-input__tag"
                @close="handleRemovePackage(i)"
              >
                {{ pkg }}
              </el-tag>
              <input
                id="kernel-packages"
                v-model="newPackage"
                type="text"
                class="chip-input__input"
                :placeholder="
                  packages.length === 0
                    ? `${extraPackagesPlaceholder} — press Enter to add`
                    : 'Add another…'
                "
                @keydown.enter.prevent="handleAddPackage"
                @keydown.delete="handleBackspaceTrim"
              />
            </div>
            <p class="form-help">
              Press Enter to add each package. Each chip is one full spec — commas inside a spec are
              fine, so ranges like <code>name&gt;=1.0,&lt;2.0</code> work. Pin versions with
              <code>name==1.2.3</code> for reproducibility. Specifiers are validated against the
              flavour's constraints file — Base and ML lock the full transitive closure; Lite only
              pins <code>polars</code> and the kernel-runtime libs, so transitives like
              <code>numpy</code> and <code>pyarrow</code> can move to satisfy what you install.
            </p>
            <p class="form-help">
              Baked into a per-kernel Docker image at creation (one-time, ~30 s). Subsequent kernel
              starts reuse the image — no pip install at startup.
            </p>
          </div>

          <div class="form-grid form-grid--3col">
            <div class="form-field">
              <label for="kernel-memory" class="form-label">Memory (GB)</label>
              <input
                id="kernel-memory"
                v-model.number="form.memory_gb"
                type="number"
                class="form-input"
                min="0.5"
                max="64"
                step="0.5"
              />
            </div>

            <div class="form-field">
              <label for="kernel-cpu" class="form-label">CPU Cores</label>
              <input
                id="kernel-cpu"
                v-model.number="form.cpu_cores"
                type="number"
                class="form-input"
                min="0.5"
                max="32"
                step="0.5"
              />
            </div>

            <div class="form-field">
              <label class="form-label">GPU</label>
              <label class="toggle-label">
                <input v-model="form.gpu" type="checkbox" class="toggle-checkbox" />
                <span class="toggle-text">{{ form.gpu ? "Enabled" : "Disabled" }}</span>
              </label>
            </div>
          </div>

          <div class="form-actions">
            <button
              type="submit"
              class="btn btn-primary km-form-card__submit"
              :disabled="!isValid || isSubmitting"
            >
              <i :class="isSubmitting ? 'fa-solid fa-spinner fa-spin' : 'fa-solid fa-plus'"></i>
              {{ submitLabel }}
            </button>
          </div>
        </form>
      </div>
    </transition>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from "vue";
import { ElTag } from "element-plus";
import {
  KERNEL_FLAVOURS,
  type FlavourInfo,
  type FlavourPackage,
  type ImageFlavour,
  type KernelConfig,
  type KernelImageStatus,
} from "../../types";

const props = defineProps<{
  flavourInfo: Map<ImageFlavour, FlavourInfo>;
  imageStatuses: KernelImageStatus[];
  onCreate: (config: KernelConfig) => Promise<void>;
}>();

// Lookup set of baked flavours that are actually present locally. ``custom``
// is always selectable because the user supplies their own image URI.
const installedFlavours = computed<Set<ImageFlavour>>(
  () => new Set(props.imageStatuses.filter((i) => i.available).map((i) => i.flavour)),
);

const isFlavourAvailable = (flavour: ImageFlavour): boolean =>
  flavour === "custom" || installedFlavours.value.has(flavour);

const isExpanded = ref(true);
const isSubmitting = ref(false);
const packages = ref<string[]>([]);
const newPackage = ref("");

const form = ref({
  id: "",
  name: "",
  memory_gb: 4.0,
  cpu_cores: 2.0,
  gpu: false,
  image_flavour: "base" as ImageFlavour,
  custom_image: "" as string,
});

// Snap the default to a flavour the user can actually pick: prefer base when
// installed, else the first available baked flavour, else custom. Skipped
// until docker-status arrives (otherwise the immediate first run sees an
// empty image list, falls back to "custom", and gets stuck there because
// custom is always available — so subsequent runs short-circuit).
watch(
  installedFlavours,
  (set) => {
    if (props.imageStatuses.length === 0) return;
    if (isFlavourAvailable(form.value.image_flavour)) return;
    const fallback = (["base", "lite", "ml"] as ImageFlavour[]).find((f) => set.has(f)) ?? "custom";
    form.value.image_flavour = fallback;
  },
  { immediate: true },
);

const activeFlavour = computed(
  () => KERNEL_FLAVOURS.find((f) => f.value === form.value.image_flavour) ?? KERNEL_FLAVOURS[0],
);

const activeImage = computed<string | null>(
  () => props.flavourInfo.get(form.value.image_flavour)?.image ?? null,
);

// When the backend resolved a different tag than the registry default (i.e.
// the local-image fallback found a docker compose build), surface that tag
// here so users see exactly which image their kernel will run on.
const activeResolvedImage = computed<string | null>(() => {
  const status = props.imageStatuses.find((s) => s.flavour === form.value.image_flavour);
  return status?.resolved_image ?? null;
});

const activePackages = computed<FlavourPackage[]>(
  () => props.flavourInfo.get(form.value.image_flavour)?.packages ?? [],
);

const extraPackagesPlaceholder = computed(() => {
  if (form.value.image_flavour === "ml") return "matplotlib==3.8.0";
  if (form.value.image_flavour === "base") return "scikit-learn==1.7.2";
  return "opencv-python==4.10.0";
});

const _DIGEST_RE = /@sha256:[a-fA-F0-9]{12,}$/;
const _TAG_RE = /^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$/;

const customImageError = computed<string>(() => {
  if (form.value.image_flavour !== "custom") return "";
  const ref_ = form.value.custom_image.trim();
  if (!ref_) return "";
  if (_DIGEST_RE.test(ref_)) return "";
  const lastSlash = ref_.lastIndexOf("/");
  const lastSegment = lastSlash >= 0 ? ref_.slice(lastSlash + 1) : ref_;
  const colonIdx = lastSegment.indexOf(":");
  if (colonIdx === -1) {
    return "Add an explicit tag (e.g. ':1.2.3') or use an '@sha256:…' digest.";
  }
  const tag = lastSegment.slice(colonIdx + 1).trim();
  if (!tag) return "Tag is empty after the colon.";
  if (!_TAG_RE.test(tag)) return "Invalid tag — letters, digits, '.-_' only.";
  return "";
});

// Kernel IDs become Docker container/network identifiers, so we restrict
// them to safe characters (letters, numbers, hyphens, underscores). Live
// validation — fires as the user types, not just on submit.
const KERNEL_ID_RE = /^[A-Za-z0-9_-]+$/;
const kernelIdError = computed<string>(() => {
  const v = form.value.id.trim();
  if (v === "") return "";
  if (!KERNEL_ID_RE.test(v)) {
    return "Only letters, numbers, hyphens, and underscores.";
  }
  return "";
});

const isValid = computed(() => {
  if (form.value.id.trim() === "" || form.value.name.trim() === "") return false;
  if (kernelIdError.value !== "") return false;
  if (!isFlavourAvailable(form.value.image_flavour)) return false;
  if (form.value.image_flavour === "custom") {
    if (form.value.custom_image.trim() === "") return false;
    if (customImageError.value !== "") return false;
  }
  return true;
});

const submitLabel = computed(() => {
  if (!isSubmitting.value) return "Create Kernel";
  // Building a derived image takes ~30 s; surface that so users don't think it hung.
  if (packages.value.length > 0) return "Baking packages…";
  return "Creating…";
});

const handleAddPackage = () => {
  const candidate = newPackage.value.trim();
  if (!candidate) return;
  if (packages.value.includes(candidate)) {
    newPackage.value = "";
    return;
  }
  packages.value = [...packages.value, candidate];
  newPackage.value = "";
};

const handleRemovePackage = (index: number) => {
  packages.value = packages.value.filter((_, i) => i !== index);
};

const handleBackspaceTrim = (event: KeyboardEvent) => {
  // Backspace on an empty input pops the last chip — common chip-input UX.
  if (event.key !== "Backspace") return;
  if (newPackage.value.length > 0) return;
  if (packages.value.length === 0) return;
  event.preventDefault();
  packages.value = packages.value.slice(0, -1);
};

const handleSubmit = async () => {
  if (!isValid.value || isSubmitting.value) return;

  // Auto-commit a pending typed-but-unconfirmed package so a Create click
  // doesn't silently drop the user's last entry.
  if (newPackage.value.trim()) {
    handleAddPackage();
  }

  const config: KernelConfig = {
    id: form.value.id.trim(),
    name: form.value.name.trim(),
    packages: [...packages.value],
    memory_gb: form.value.memory_gb,
    cpu_cores: form.value.cpu_cores,
    gpu: form.value.gpu,
    image_flavour: form.value.image_flavour,
    custom_image: form.value.image_flavour === "custom" ? form.value.custom_image.trim() : null,
  };

  isSubmitting.value = true;
  try {
    await props.onCreate(config);
    // Success: reset and collapse so the user can see their new kernel.
    form.value = {
      id: "",
      name: "",
      memory_gb: 4.0,
      cpu_cores: 2.0,
      gpu: false,
      image_flavour: "base",
      custom_image: "",
    };
    packages.value = [];
    newPackage.value = "";
    isExpanded.value = false;
  } catch {
    // Parent has already shown an error alert. Keep form populated for retry.
  } finally {
    isSubmitting.value = false;
  }
};
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

.km-form-card__submit {
  background: linear-gradient(135deg, var(--color-accent) 0%, var(--color-accent-hover) 100%);
  border: none;
  color: #fff;
  font-weight: var(--font-weight-semibold);
  box-shadow: 0 4px 10px rgba(8, 145, 178, 0.25);
  transition:
    transform var(--transition-base) var(--transition-timing),
    box-shadow var(--transition-base) var(--transition-timing),
    filter var(--transition-base) var(--transition-timing);
}

.km-form-card__submit:hover:not(:disabled) {
  transform: translateY(-1px);
  box-shadow: 0 6px 14px rgba(8, 145, 178, 0.32);
  filter: brightness(1.05);
}

.km-form-card__submit:disabled {
  opacity: 0.6;
  cursor: not-allowed;
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

.form-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: var(--spacing-3);
}

.form-grid--3col {
  grid-template-columns: 1fr 1fr 1fr;
}

.form-label-hint {
  font-weight: var(--font-weight-normal);
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
}

.form-help {
  margin: var(--spacing-1) 0 0;
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
}

.toggle-label {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  cursor: pointer;
  height: 36px;
}

.toggle-checkbox {
  width: 16px;
  height: 16px;
  accent-color: var(--color-accent);
  cursor: pointer;
}

.toggle-text {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.prebaked-hint {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  /* Mode-agnostic palette mirrors .status-banner--warning: a constant
     light/dark pair that reads on either theme background. */
  background: var(--color-success-light);
  border-left: 2px solid var(--color-success);
  border-radius: var(--border-radius-sm);
  margin-bottom: var(--spacing-3);
  font-size: var(--font-size-xs);
  color: var(--color-success-hover);
}

.prebaked-hint > i {
  color: var(--color-success-hover);
}

.prebaked-content {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
  flex: 1;
}

.prebaked-label {
  font-weight: var(--font-weight-medium);
  color: var(--color-success-hover);
}

.prebaked-list {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-1);
}

.prebaked-pkg {
  display: inline-flex;
  align-items: stretch;
  font-family: var(--font-family-mono);
  /* The hint banner has a constant pale-green bg, so stay neutral here. */
  border: 1px solid rgba(0, 0, 0, 0.12);
  border-radius: var(--border-radius-sm);
  overflow: hidden;
  font-size: var(--font-size-2xs);
  line-height: 1.4;
}

.prebaked-pkg__name {
  padding: 1px 6px;
  background: rgba(255, 255, 255, 0.55);
  color: var(--color-success-hover);
}

.prebaked-pkg__version {
  padding: 1px 6px;
  background: var(--color-success);
  color: #ffffff;
  font-weight: var(--font-weight-medium);
}

.form-help-image {
  margin-left: var(--spacing-1);
  color: var(--color-text-tertiary);
}

.form-help-image code {
  font-family: var(--font-mono, monospace);
  font-size: inherit;
}

.form-help-local {
  display: inline-block;
  margin-left: var(--spacing-1);
  padding: 0 var(--spacing-1);
  border-radius: var(--border-radius-sm);
  background-color: var(--color-info-light, rgba(40, 110, 200, 0.12));
  color: var(--color-info-dark, var(--color-text-primary));
  font-size: var(--font-size-2xs);
  font-weight: var(--font-weight-semibold);
  text-transform: uppercase;
  letter-spacing: 0.04em;
  vertical-align: middle;
}

.form-error {
  margin: var(--spacing-1) 0 0;
  color: var(--color-danger);
  font-size: var(--font-size-xs);
}

.form-input--error {
  border-color: var(--color-danger);
}

.card-content--relative {
  position: relative;
}

.creating-overlay {
  position: absolute;
  inset: 0;
  background-color: rgba(255, 255, 255, 0.92);
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-2);
  z-index: 5;
  border-radius: inherit;
  text-align: center;
  padding: var(--spacing-4);
}

[data-theme="dark"] .creating-overlay {
  background-color: rgba(26, 26, 46, 0.92);
}

.creating-overlay__spinner {
  width: 40px;
  height: 40px;
  border-radius: 50%;
  border: 3px solid var(--color-border-light);
  border-top-color: var(--color-accent);
  animation: creating-spin 0.9s linear infinite;
}

@keyframes creating-spin {
  to {
    transform: rotate(360deg);
  }
}

.creating-overlay__title {
  margin: 0;
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.creating-overlay__hint {
  margin: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  max-width: 360px;
}

.chip-input {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-2);
  padding: var(--spacing-2);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background-color: var(--color-background-primary);
  min-height: 38px;
  align-items: center;
}

.chip-input__tag {
  margin: 0;
}

.chip-input__input {
  flex: 1;
  min-width: 200px;
  border: none;
  outline: none;
  background: transparent;
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  padding: 4px 6px;
}
</style>
