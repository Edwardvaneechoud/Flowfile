<template>
  <el-dialog
    :model-value="modelValue"
    title="Install community node"
    width="560px"
    align-center
    append-to-body
    @update:model-value="(v: boolean) => emit('update:modelValue', v)"
    @closed="acknowledged = false"
  >
    <div v-if="node" class="consent-body">
      <!-- Identity -->
      <div class="identity">
        <img :src="iconUrl" alt="" class="identity-icon" />
        <div class="identity-text">
          <div class="identity-name">{{ node.node_name }}</div>
          <div class="identity-meta">
            <span>{{ node.author.github ? "@" + node.author.github : "unknown author" }}</span>
            <span class="dot">·</span>
            <span>v{{ node.version }}</span>
          </div>
        </div>
      </div>

      <!-- Capabilities -->
      <section class="consent-section">
        <h4>What this node can do</h4>
        <p v-if="lines.length === 0" class="no-caps">{{ NO_CAPABILITIES_NOTE }}</p>
        <ul v-else class="cap-list">
          <li v-for="line in lines" :key="line.flag" class="cap-item">
            <span class="chip cap-chip">{{ line.label }}</span>
            <span class="cap-desc">{{ line.description }}</span>
          </li>
        </ul>
      </section>

      <!-- Environment -->
      <p class="env-note">
        <i :class="node.environment === 'kernel' ? 'fa-solid fa-cube' : 'fa-solid fa-desktop'"></i>
        {{ envNote }}
      </p>

      <!-- Risk statement -->
      <p class="risk-copy">
        Community nodes are reviewed and scanned before they are published, but they are code
        written by third parties that runs with your data and any secrets you assign. Scanning
        surfaces capabilities — it is not a safety guarantee. Install only nodes from authors you
        trust.
      </p>

      <!-- Provenance -->
      <div class="provenance">
        <span class="sha">sha256 {{ sha }}</span>
        <button class="link-btn" @click="openSource">
          <i class="fa-solid fa-arrow-up-right-from-square"></i> View source on GitHub
        </button>
      </div>

      <el-checkbox v-model="acknowledged" class="ack-check">
        I understand this node runs third-party code with the capabilities listed above, and I
        choose to install it.
      </el-checkbox>
    </div>

    <template #footer>
      <button class="btn btn-secondary" @click="emit('update:modelValue', false)">Cancel</button>
      <button class="btn btn-primary" :disabled="!acknowledged || installing" @click="confirm">
        <i v-if="installing" class="fa-solid fa-spinner fa-spin"></i>
        Install
      </button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { ElCheckbox, ElDialog } from "element-plus";
import type { CommunityNodeSummary } from "../../api/communityNodes";
import { mediaFileName, communityNodeSourceUrl } from "../../api/communityNodes";
import { useCommunityMediaUrl } from "../../composables/useCommunityMedia";
import { desktop } from "../../../lib/desktop";
import {
  NO_CAPABILITIES_NOTE,
  capabilityLines,
  environmentNote,
  shortSha,
} from "./communityCapabilities";

const props = defineProps<{
  modelValue: boolean;
  node: CommunityNodeSummary | null;
  installing?: boolean;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: boolean): void;
  (e: "confirm", acknowledgedCapabilities: string[]): void;
}>();

const acknowledged = ref(false);

const lines = computed(() => capabilityLines(props.node?.risk_flags ?? []));
const envNote = computed(() => environmentNote(props.node?.environment ?? "local"));
const sha = computed(() => shortSha(props.node?.artifacts.node.sha256 ?? ""));

const iconUrl = useCommunityMediaUrl(
  () => props.node?.id ?? "",
  () => (props.node?.artifacts.icon ? mediaFileName(props.node.artifacts.icon.path) : null),
);

function openSource() {
  if (props.node) void desktop.openExternal(communityNodeSourceUrl(props.node.id));
}

function confirm() {
  if (!props.node || !acknowledged.value) return;
  // acknowledged_capabilities must superset what core re-scans → send risk_flags.
  emit("confirm", [...props.node.risk_flags]);
}
</script>

<style scoped>
.consent-body {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4);
}
.identity {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
}
.identity-icon {
  width: 40px;
  height: 40px;
  border-radius: var(--border-radius-md);
  object-fit: contain;
  background: var(--color-background-secondary);
}
.identity-name {
  font-weight: var(--font-weight-semibold);
  font-size: var(--font-size-base);
}
.identity-meta {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  display: flex;
  gap: var(--spacing-1);
}
.dot {
  opacity: 0.5;
}
.consent-section h4 {
  margin: 0 0 var(--spacing-2);
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
}
.no-caps {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}
.cap-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}
.cap-item {
  display: flex;
  align-items: baseline;
  gap: var(--spacing-2);
}
.chip {
  display: inline-flex;
  align-items: center;
  font-size: var(--font-size-xs);
  padding: 1px 8px;
  border-radius: var(--border-radius-full);
  white-space: nowrap;
}
.cap-chip {
  background: var(--color-warning-light);
  color: var(--color-warning);
  font-weight: var(--font-weight-medium);
}
.cap-desc {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  line-height: 1.4;
}
.env-note {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}
.risk-copy {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  line-height: 1.5;
  padding: var(--spacing-3);
  background: var(--color-background-secondary);
  border-radius: var(--border-radius-md);
}
.provenance {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-2);
}
.sha {
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}
.link-btn {
  background: none;
  border: none;
  padding: 0;
  cursor: pointer;
  color: var(--color-accent);
  font-size: var(--font-size-sm);
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
}
.link-btn:hover {
  text-decoration: underline;
}
.ack-check {
  align-items: flex-start;
  height: auto;
  white-space: normal;
}
.ack-check :deep(.el-checkbox__label) {
  white-space: normal;
  line-height: 1.4;
  font-size: var(--font-size-sm);
}
</style>
