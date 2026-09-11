<template>
  <el-dialog
    :model-value="visible"
    title="Request a node"
    width="560px"
    align-center
    append-to-body
    class="high-z-index-dialog"
    @update:model-value="onModelUpdate"
  >
    <p class="nr-intro">
      Search the open requests first. An upvote on an existing request counts for more than a
      duplicate.
    </p>
    <el-input v-model="query" placeholder="Search open requests, e.g. pivot or DateTime" clearable>
      <template #prefix>
        <span class="material-icons nr-search-icon">search</span>
      </template>
    </el-input>

    <p v-if="loading" class="nr-state">Loading open requests…</p>
    <p v-else-if="loadFailed" class="nr-state">
      The open requests could not be loaded from GitHub. You can still file a new one.
    </p>
    <p v-else-if="matches.length === 0" class="nr-state">
      No open request matches{{ query.trim() ? ` "${query.trim()}"` : "" }}.
    </p>
    <ul v-else class="nr-list">
      <li v-for="request in matches" :key="request.number" class="nr-row">
        <span class="nr-title" :title="request.title">{{ request.title }}</span>
        <span
          class="status-badge nr-kind"
          :class="request.kind === 'alteryx' ? 'status-badge--info' : 'status-badge--muted'"
        >
          {{ request.kind === "alteryx" ? "Alteryx" : "General" }}
        </span>
        <span class="nr-upvotes" title="Upvotes on GitHub">
          <span class="material-icons nr-upvote-icon">thumb_up</span>{{ request.upvotes }}
        </span>
        <el-button link type="primary" size="small" @click="open(request.url)">
          Upvote
          <span class="material-icons nr-ext">open_in_new</span>
        </el-button>
      </li>
    </ul>

    <template #footer>
      <div class="nr-footer">
        <el-button @click="close">Close</el-button>
        <el-button type="primary" @click="open(newRequestUrl(query))">
          Request a new node
          <span class="material-icons nr-ext">open_in_new</span>
        </el-button>
      </div>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";

import { NodeRequestsApi, type NodeRequest } from "../../../api/nodeRequests.api";
import { desktop } from "../../../../lib/desktop";
import { filterRequests, newRequestUrl } from "./nodeRequests";

const props = defineProps<{ visible: boolean }>();
const emit = defineEmits<{ (e: "update:visible", value: boolean): void }>();

const query = ref("");
const requests = ref<NodeRequest[]>([]);
const loading = ref(false);
const loadFailed = ref(false);

const matches = computed(() => filterRequests(requests.value, query.value));

async function load() {
  loading.value = true;
  loadFailed.value = false;
  try {
    requests.value = (await NodeRequestsApi.fetchOpen()).requests;
  } catch {
    requests.value = [];
    loadFailed.value = true;
  } finally {
    loading.value = false;
  }
}

function open(url: string) {
  void desktop.openExternal(url);
}

function close() {
  emit("update:visible", false);
}

function onModelUpdate(open: boolean) {
  if (!open) close();
}

watch(
  () => props.visible,
  (open) => {
    if (open) {
      query.value = "";
      void load();
    }
  },
  { immediate: true },
);
</script>

<style scoped>
.nr-intro {
  margin: 0 0 var(--spacing-3);
  font-size: 13px;
  color: var(--color-text-secondary);
}

.nr-search-icon {
  font-size: 18px;
}

.nr-state {
  margin: var(--spacing-4) 0 0;
  font-size: 13px;
  color: var(--color-text-secondary);
}

.nr-list {
  list-style: none;
  margin: var(--spacing-3) 0 0;
  padding: 0;
  max-height: 320px;
  overflow-y: auto;
}

.nr-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) 0;
  border-top: 1px solid var(--color-border-secondary);
  font-size: 13px;
}

.nr-title {
  flex: 1;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.nr-kind {
  flex-shrink: 0;
}

.nr-upvotes {
  display: inline-flex;
  align-items: center;
  gap: 2px;
  flex-shrink: 0;
  color: var(--color-text-secondary);
  font-variant-numeric: tabular-nums;
}

.nr-upvote-icon,
.nr-ext {
  font-size: 14px;
}

.nr-ext {
  margin-left: 2px;
}

.nr-footer {
  display: flex;
  justify-content: flex-end;
  gap: var(--spacing-2);
}
</style>
