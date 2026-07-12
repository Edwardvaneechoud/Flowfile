<template>
  <el-dialog
    :model-value="modelValue"
    :title="detail?.node_name || 'Community node'"
    width="720px"
    top="6vh"
    append-to-body
    @update:model-value="(v: boolean) => emit('update:modelValue', v)"
  >
    <div v-loading="loading" class="detail-body">
      <EmptyState
        v-if="!loading && loadError"
        icon="fa-solid fa-triangle-exclamation"
        title="Could not load node"
        :description="loadError"
      />

      <template v-else-if="detail">
        <!-- Header meta -->
        <div class="detail-header">
          <img :src="iconUrl" alt="" class="detail-icon" />
          <div class="detail-meta">
            <div class="detail-title-row">
              <span class="detail-name">{{ detail.node_name }}</span>
              <span
                :class="['chip', detail.environment === 'kernel' ? 'chip-kernel' : 'chip-local']"
              >
                {{ detail.environment === "kernel" ? "Kernel" : "Local" }}
              </span>
            </div>
            <div class="detail-sub">
              <span v-if="detail.author.github">@{{ detail.author.github }}</span>
              <span class="dot">·</span>
              <span>v{{ detail.version }}</span>
              <template v-if="detail.license">
                <span class="dot">·</span>
                <span>{{ detail.license }}</span>
              </template>
              <template v-if="detail.popularity">
                <span class="dot">·</span>
                <span class="thumbs"
                  ><i class="fa-solid fa-thumbs-up"></i> {{ detail.popularity.thumbs_up }}</span
                >
              </template>
            </div>
            <div v-if="detail.tags.length" class="detail-tags">
              <span v-for="tag in detail.tags" :key="tag" class="chip chip-tag">{{ tag }}</span>
            </div>
          </div>
        </div>

        <p class="detail-description">{{ detail.description }}</p>

        <!-- Screenshots -->
        <div v-if="screenshotUrls.length" class="shot-strip">
          <el-image
            v-for="(url, i) in screenshotUrls"
            :key="i"
            :src="url"
            :preview-src-list="screenshotUrls"
            :initial-index="i"
            fit="cover"
            class="shot-thumb"
            preview-teleported
          />
        </div>

        <!-- Readme: renderSafeMarkdown sanitises via DOMPurify before v-html. -->
        <!-- eslint-disable-next-line vue/no-v-html -->
        <div v-if="readmeHtml" class="readme markdown-body" v-html="readmeHtml"></div>
      </template>
    </div>

    <template #footer>
      <div class="footer-row">
        <button class="btn btn-secondary" @click="emit('update:modelValue', false)">Close</button>
        <template v-if="detail">
          <button
            v-if="
              detail.install_state === 'installed' || detail.install_state === 'modified_locally'
            "
            class="btn btn-ghost"
            :disabled="busy"
            @click="emit('uninstall', detail.id)"
          >
            <i class="fa-solid fa-trash"></i> Uninstall
          </button>
          <button
            v-if="canInstall"
            class="btn btn-primary"
            :disabled="busy"
            @click="emit('install', detail)"
          >
            <i class="fa-solid fa-download"></i>
            {{ detail.install_state === "update_available" ? "Update" : "Install" }}
          </button>
          <span
            v-else-if="detail.install_state === 'incompatible'"
            class="status-badge status-badge--warning"
          >
            Requires Flowfile {{ detail.min_flowfile_version }}+
          </span>
        </template>
      </div>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { ElDialog, ElImage } from "element-plus";
import type { CommunityNodeDetail } from "../../api/communityNodes";
import { useCommunityNodesStore, describeError } from "../../stores/community-nodes-store";
import { useCommunityMediaUrl, loadScreenshotUrls } from "../../composables/useCommunityMedia";
import { renderSafeMarkdown } from "../../lib/markdown";
import { EmptyState } from "../../components/common";

const props = defineProps<{
  modelValue: boolean;
  nodeId: string | null;
  busy?: boolean;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: boolean): void;
  (e: "install", node: CommunityNodeDetail): void;
  (e: "uninstall", nodeId: string): void;
}>();

const store = useCommunityNodesStore();

const detail = ref<CommunityNodeDetail | null>(null);
const loading = ref(false);
const loadError = ref<string | null>(null);
const screenshotUrls = ref<string[]>([]);

const iconUrl = useCommunityMediaUrl(
  () => detail.value?.id ?? "",
  () => detail.value?.icon_file ?? null,
);

const readmeHtml = computed(() =>
  detail.value?.readme_text ? renderSafeMarkdown(detail.value.readme_text) : "",
);

const canInstall = computed(
  () =>
    detail.value?.install_state === "not_installed" ||
    detail.value?.install_state === "update_available",
);

async function load(nodeId: string) {
  loading.value = true;
  loadError.value = null;
  detail.value = null;
  screenshotUrls.value = [];
  try {
    const d = await store.loadDetail(nodeId);
    detail.value = d;
    if (d.screenshots.length) {
      screenshotUrls.value = await loadScreenshotUrls(d.id, d.screenshots);
    }
  } catch (e) {
    loadError.value = describeError(e);
  } finally {
    loading.value = false;
  }
}

watch(
  () => [props.modelValue, props.nodeId] as const,
  ([open, id]) => {
    if (open && id) void load(id);
  },
  { immediate: true },
);
</script>

<style scoped>
.detail-body {
  min-height: 120px;
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4);
}
.detail-header {
  display: flex;
  gap: var(--spacing-3);
  align-items: flex-start;
}
.detail-icon {
  width: 48px;
  height: 48px;
  border-radius: var(--border-radius-md);
  object-fit: contain;
  background: var(--color-background-secondary);
  flex-shrink: 0;
}
.detail-title-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}
.detail-name {
  font-size: var(--font-size-lg);
  font-weight: var(--font-weight-semibold);
}
.detail-sub {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  margin-top: 2px;
  flex-wrap: wrap;
}
.dot {
  opacity: 0.5;
}
.thumbs i {
  color: var(--color-accent);
}
.detail-tags {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-1);
  margin-top: var(--spacing-2);
}
.chip {
  display: inline-flex;
  align-items: center;
  font-size: var(--font-size-xs);
  padding: 1px 8px;
  border-radius: var(--border-radius-full);
}
.chip-tag {
  background: var(--color-background-secondary);
  color: var(--color-text-secondary);
}
.chip-kernel {
  background: var(--color-warning-light);
  color: var(--color-warning);
}
.chip-local {
  background: var(--color-success-light);
  color: var(--color-success);
}
.detail-description {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  line-height: 1.5;
}
.shot-strip {
  display: flex;
  gap: var(--spacing-2);
  flex-wrap: wrap;
}
.shot-thumb {
  width: 160px;
  height: 100px;
  border-radius: var(--border-radius-md);
  border: 1px solid var(--color-border-primary);
  cursor: zoom-in;
  overflow: hidden;
}
.readme {
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  line-height: 1.6;
  max-height: 40vh;
  overflow-y: auto;
  border-top: 1px solid var(--color-border-primary);
  padding-top: var(--spacing-3);
}
.readme :deep(pre) {
  background: var(--color-background-secondary);
  padding: var(--spacing-2);
  border-radius: var(--border-radius-md);
  overflow-x: auto;
}
.readme :deep(code) {
  font-family: var(--font-family-mono);
}
.footer-row {
  display: flex;
  justify-content: flex-end;
  gap: var(--spacing-2);
  align-items: center;
}
</style>
