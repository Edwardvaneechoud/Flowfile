<template>
  <div class="modal-overlay" @click="$emit('close')">
    <div class="modal-container modal-container--lg km-details-modal" @click.stop>
      <div class="modal-header km-details-modal__header">
        <div class="modal-title-row">
          <h3 class="modal-title">{{ kernel.name }}</h3>
          <KernelStatusBadge :state="kernel.state" />
          <span
            class="kernel-card__flavour"
            :class="`kernel-card__flavour--${kernel.image_flavour}`"
          >
            {{ flavourLabel }}
          </span>
        </div>
        <button class="modal-close" aria-label="Close" @click="$emit('close')">
          <i class="fa-solid fa-times"></i>
        </button>
      </div>

      <div class="modal-content kernel-details">
        <!-- Identity -->
        <section class="detail-section">
          <h4 class="detail-section__title">Identity</h4>
          <dl class="detail-grid">
            <dt>Kernel ID</dt>
            <dd>
              <code>{{ kernel.id }}</code>
            </dd>
            <dt>Image flavour</dt>
            <dd>{{ flavourLabel }}</dd>
            <dt>Image tag</dt>
            <dd>
              <code class="break">{{ kernel.image || resolvedImage || "—" }}</code>
              <span
                v-if="updateInfo?.available"
                class="km-update-badge"
                :title="`Latest is ${updateInfo.latest}. Update the image and recreate this kernel to use it.`"
              >
                <i class="fa-solid fa-circle-up"></i> Update available
              </span>
              <span v-else-if="updateInfo" class="km-uptodate">
                <i class="fa-solid fa-circle-check"></i> Up to date
              </span>
            </dd>
            <dt>Kernel runtime</dt>
            <dd>{{ kernel.kernel_version ? `v${kernel.kernel_version}` : "—" }}</dd>
            <dt>Created</dt>
            <dd>{{ formattedCreatedAt }}</dd>
          </dl>
        </section>

        <!-- Resources -->
        <section class="detail-section">
          <h4 class="detail-section__title">Resources</h4>
          <dl class="detail-grid">
            <dt>CPU</dt>
            <dd>{{ kernel.cpu_cores }} cores</dd>
            <dt>Memory limit</dt>
            <dd>{{ kernel.memory_gb }} GB</dd>
            <dt>GPU</dt>
            <dd>{{ kernel.gpu ? "Enabled" : "Disabled" }}</dd>
          </dl>
        </section>

        <!-- Pre-installed packages (read-only, from flavour info) -->
        <section v-if="preinstalled.length > 0" class="detail-section">
          <h4 class="detail-section__title">Pre-installed in this image</h4>
          <div class="prebaked-list">
            <span v-for="pkg in preinstalled" :key="pkg.name" class="prebaked-pkg">
              <span class="prebaked-pkg__name">{{ pkg.name }}</span>
              <span class="prebaked-pkg__version">{{ pkg.version }}</span>
            </span>
          </div>
        </section>

        <!-- Extra packages (editable) -->
        <section class="detail-section">
          <div class="detail-section__header">
            <h4 class="detail-section__title">Extra packages</h4>
            <button
              v-if="!editing && canEdit"
              type="button"
              class="btn btn-secondary btn-sm"
              @click="startEdit"
            >
              <i class="fa-solid fa-pen"></i> Edit
            </button>
          </div>

          <div v-if="!canEdit && !editing" class="lock-hint">
            <i class="fa-solid fa-lock"></i>
            Stop the kernel to edit packages.
          </div>

          <div v-if="!editing">
            <p v-if="kernel.packages.length === 0" class="empty-line">No extra packages.</p>
            <template v-else>
              <!-- When the bake captured resolved versions, render them in the
                   same name+version chip style as the pre-installed list. -->
              <div v-if="kernel.resolved_packages.length > 0" class="prebaked-list">
                <span
                  v-for="pkg in kernel.resolved_packages"
                  :key="pkg.name"
                  class="prebaked-pkg prebaked-pkg--user"
                >
                  <span class="prebaked-pkg__name">{{ pkg.name }}</span>
                  <span class="prebaked-pkg__version">{{ pkg.version }}</span>
                </span>
              </div>
              <!-- Fallback for legacy kernels (created before resolved_packages
                   was captured) — show the user's original specs. -->
              <div v-else class="extra-pkg-list">
                <span v-for="p in kernel.packages" :key="p" class="extra-pkg">{{ p }}</span>
                <p class="empty-line">
                  Resolved versions not recorded for this kernel — edit and save to refresh.
                </p>
              </div>
            </template>
          </div>

          <div v-else class="edit-pkgs">
            <div class="chip-input">
              <el-tag
                v-for="(pkg, i) in editPackages"
                :key="`${pkg}-${i}`"
                closable
                :disable-transitions="false"
                class="chip-input__tag"
                @close="handleRemovePackage(i)"
              >
                {{ pkg }}
              </el-tag>
              <input
                v-model="editNewPackage"
                type="text"
                class="chip-input__input"
                :placeholder="
                  editPackages.length === 0
                    ? 'matplotlib==3.8.0 — press Enter to add'
                    : 'Add another…'
                "
                :disabled="saving"
                @keydown.enter.prevent="handleAddPackage"
                @keydown.delete="handleBackspaceTrim"
              />
            </div>
            <p class="form-help">
              Press Enter to add each package. Each chip is one full spec — ranges like
              <code>name&gt;=1.0,&lt;2.0</code> work. Saving rebuilds the kernel image (~30 s) so
              transitive deps stay pinned against the flavour's constraints.
            </p>
            <p v-if="saveError" class="form-error">{{ saveError }}</p>
            <div class="edit-actions">
              <button
                type="button"
                class="btn btn-secondary btn-sm"
                :disabled="saving"
                @click="cancelEdit"
              >
                Cancel
              </button>
              <button type="button" class="btn btn-primary btn-sm" :disabled="saving" @click="save">
                <i v-if="saving" class="fa-solid fa-spinner fa-spin"></i>
                {{ saving ? "Rebuilding…" : "Save" }}
              </button>
            </div>
          </div>
        </section>

        <!-- Error if present -->
        <section v-if="kernel.error_message" class="detail-section detail-section--error">
          <h4 class="detail-section__title">Error</h4>
          <p>{{ kernel.error_message }}</p>
        </section>
      </div>

      <div class="modal-actions">
        <button type="button" class="btn btn-secondary" @click="$emit('close')">Close</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { ElTag } from "element-plus";
import {
  KERNEL_FLAVOURS,
  type FlavourInfo,
  type FlavourPackage,
  type ImageFlavour,
  type KernelInfo,
} from "../../types";
import KernelStatusBadge from "./KernelStatusBadge.vue";

const props = defineProps<{
  kernel: KernelInfo;
  flavourInfo: Map<ImageFlavour, FlavourInfo>;
  onSave: (kernelId: string, packages: string[]) => Promise<void>;
}>();

const emit = defineEmits<{
  (e: "close"): void;
}>();

const editing = ref(false);
const editPackages = ref<string[]>([]);
const editNewPackage = ref("");
const saving = ref(false);
const saveError = ref<string | null>(null);

const flavourLabel = computed(
  () =>
    KERNEL_FLAVOURS.find((f) => f.value === props.kernel.image_flavour)?.label ??
    props.kernel.image_flavour,
);

const resolvedImage = computed(
  () => props.flavourInfo.get(props.kernel.image_flavour)?.image ?? null,
);

// Compare this kernel's own image tag against the flavour's latest registry tag
// so the user can see whether the kernel is running an outdated image. Skips
// non-version tags (e.g. :local dev builds) and mismatched/custom repos.
function parseImageVersion(tag: string): number[] | null {
  const idx = tag.lastIndexOf(":");
  if (idx === -1) return null;
  const nums = tag
    .slice(idx + 1)
    .split(".")
    .map(Number);
  return nums.some((n) => !Number.isInteger(n)) ? null : nums;
}

function isOlder(a: number[], b: number[]): boolean {
  const len = Math.max(a.length, b.length);
  for (let i = 0; i < len; i++) {
    const av = a[i] ?? 0;
    const bv = b[i] ?? 0;
    if (av !== bv) return av < bv;
  }
  return false;
}

const updateInfo = computed<{ available: boolean; latest: string } | null>(() => {
  const current = props.kernel.image;
  const latest = resolvedImage.value;
  if (!current || !latest) return null;
  const repo = (t: string) => t.slice(0, t.lastIndexOf(":"));
  if (repo(current) !== repo(latest)) return null;
  const cv = parseImageVersion(current);
  const lv = parseImageVersion(latest);
  if (!cv || !lv) return null;
  return { available: isOlder(cv, lv), latest };
});

const preinstalled = computed<FlavourPackage[]>(
  () => props.flavourInfo.get(props.kernel.image_flavour)?.packages ?? [],
);

const canEdit = computed(() => {
  // Block while running, starting, or executing — saving rebuilds the image.
  return !["idle", "executing", "starting"].includes(props.kernel.state);
});

const formattedCreatedAt = computed(() => {
  if (!props.kernel.created_at) return "—";
  try {
    return new Date(props.kernel.created_at).toLocaleString();
  } catch {
    return props.kernel.created_at;
  }
});

const startEdit = () => {
  editPackages.value = [...props.kernel.packages];
  editNewPackage.value = "";
  saveError.value = null;
  editing.value = true;
};

const cancelEdit = () => {
  editing.value = false;
  editPackages.value = [];
  editNewPackage.value = "";
  saveError.value = null;
};

const handleAddPackage = () => {
  const candidate = editNewPackage.value.trim();
  if (!candidate) return;
  if (editPackages.value.includes(candidate)) {
    editNewPackage.value = "";
    return;
  }
  editPackages.value = [...editPackages.value, candidate];
  editNewPackage.value = "";
};

const handleRemovePackage = (index: number) => {
  editPackages.value = editPackages.value.filter((_, i) => i !== index);
};

const handleBackspaceTrim = (event: KeyboardEvent) => {
  // Backspace on an empty input pops the last chip — common chip-input UX.
  if (event.key !== "Backspace") return;
  if (editNewPackage.value.length > 0) return;
  if (editPackages.value.length === 0) return;
  event.preventDefault();
  editPackages.value = editPackages.value.slice(0, -1);
};

const save = async () => {
  if (saving.value) return;
  // Auto-commit a pending typed-but-unconfirmed package so a Save click
  // doesn't silently drop the user's last entry.
  if (editNewPackage.value.trim()) {
    handleAddPackage();
  }
  saving.value = true;
  saveError.value = null;
  try {
    await props.onSave(props.kernel.id, [...editPackages.value]);
    editing.value = false;
  } catch (err: any) {
    saveError.value = err?.message ?? "Failed to update packages.";
  } finally {
    saving.value = false;
  }
};
</script>

<style scoped>
.modal-container--lg {
  max-width: 720px;
  width: 90vw;
}

.km-details-modal {
  border-radius: var(--border-radius-xl);
  box-shadow: var(--shadow-lg);
  overflow: hidden;
}

.km-details-modal__header {
  background: linear-gradient(135deg, rgba(8, 145, 178, 0.08) 0%, rgba(102, 126, 234, 0.06) 100%);
}

[data-theme="dark"] .km-details-modal__header {
  background: linear-gradient(135deg, rgba(8, 145, 178, 0.18) 0%, rgba(102, 126, 234, 0.14) 100%);
}

.modal-title-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  flex-wrap: wrap;
}

.kernel-details {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4);
}

.detail-section {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.detail-section__title {
  margin: 0;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-secondary);
  text-transform: uppercase;
  letter-spacing: 0.04em;
}

.detail-section__header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-2);
}

.detail-section--error {
  background-color: var(--color-danger-light);
  color: var(--color-danger);
  padding: var(--spacing-2) var(--spacing-3);
  border-radius: var(--border-radius-sm);
}

.detail-grid {
  display: grid;
  grid-template-columns: 140px 1fr;
  gap: var(--spacing-1) var(--spacing-3);
  margin: 0;
  font-size: var(--font-size-sm);
}

.detail-grid dt {
  color: var(--color-text-tertiary);
}

.detail-grid dd {
  margin: 0;
  color: var(--color-text-primary);
  word-break: break-word;
}

.detail-grid code {
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
  background-color: var(--color-background-tertiary);
  color: var(--color-text-primary);
  padding: 1px 6px;
  border-radius: var(--border-radius-sm);
}

.detail-grid code.break {
  word-break: break-all;
}

.km-update-badge,
.km-uptodate {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-0-5);
  margin-left: var(--spacing-2);
  font-size: var(--font-size-2xs);
  font-weight: var(--font-weight-semibold);
  white-space: nowrap;
}

.km-update-badge {
  color: var(--color-primary, #3b82f6);
}

.km-uptodate {
  color: var(--color-text-tertiary);
}

.kernel-card__flavour {
  font-size: var(--font-size-2xs);
  font-weight: var(--font-weight-medium);
  text-transform: uppercase;
  letter-spacing: 0.04em;
  padding: 0 var(--spacing-1);
  border-radius: var(--border-radius-sm);
  border: 1px solid currentColor;
}

.kernel-card__flavour--base {
  color: var(--color-text-secondary);
}

.kernel-card__flavour--ml {
  color: var(--color-success);
}

.kernel-card__flavour--custom {
  color: var(--color-warning);
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
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-sm);
  overflow: hidden;
  font-size: var(--font-size-2xs);
  line-height: 1.4;
}

.prebaked-pkg__name {
  padding: 1px 6px;
  background-color: var(--color-background-tertiary);
  color: var(--color-text-primary);
}

.prebaked-pkg__version {
  padding: 1px 6px;
  background-color: var(--color-success);
  color: #ffffff;
  font-weight: var(--font-weight-medium);
}

/* Distinguish user-installed extras from the flavour's pre-installed set
   so users can see at a glance which versions came from their request. */
.prebaked-pkg--user .prebaked-pkg__version {
  background-color: var(--color-accent);
}

.extra-pkg-list {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-1);
}

.extra-pkg {
  display: inline-block;
  background-color: var(--color-accent-subtle);
  color: var(--color-accent);
  font-family: var(--font-family-mono);
  font-size: var(--font-size-2xs);
  font-weight: var(--font-weight-medium);
  padding: var(--spacing-0-5) var(--spacing-1-5);
  border-radius: var(--border-radius-sm);
}

.empty-line {
  margin: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
  font-style: italic;
}

.lock-hint {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
}

.edit-pkgs {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
}

.edit-actions {
  display: flex;
  gap: var(--spacing-2);
  justify-content: flex-end;
  margin-top: var(--spacing-1);
}

.form-help {
  margin: 0;
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
}

.form-error {
  margin: 0;
  color: var(--color-danger);
  font-size: var(--font-size-xs);
}

.btn-sm {
  padding: var(--spacing-1) var(--spacing-2-5);
  font-size: var(--font-size-xs);
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
