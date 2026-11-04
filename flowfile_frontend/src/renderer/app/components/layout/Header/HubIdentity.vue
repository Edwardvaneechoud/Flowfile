<template>
  <button
    v-if="identity"
    type="button"
    class="hub-identity"
    data-testid="hub-identity"
    :title="title"
    :aria-label="title"
    @click="openHub"
  >
    <i class="fa-solid fa-building hub-identity__icon" aria-hidden="true"></i>
    <span v-if="identity.who" class="hub-identity__who">{{ identity.who }}</span>
    <span v-if="identity.who" class="hub-identity__dot" aria-hidden="true">·</span>
    <span class="hub-identity__where">{{ identity.where }}</span>
    <span class="material-icons hub-identity__open" aria-hidden="true">open_in_new</span>
  </button>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { getHubConnection, type HubConnection } from "../../../api/hubConnection.api";
import { desktop } from "../../../../lib/desktop";
import { hubIdentity, hubIdentityTitle } from "./hubIdentity";

const connection = ref<HubConnection | null>(null);
const identity = computed(() => hubIdentity(connection.value));
const title = computed(() => (identity.value ? hubIdentityTitle(identity.value) : ""));

onMounted(async () => {
  try {
    connection.value = await getHubConnection();
  } catch {
    // No hub connection to read: the header says nothing about one.
    connection.value = null;
  }
});

const openHub = () => {
  if (identity.value) void desktop.openExternal(identity.value.url);
};
</script>

<style scoped>
.hub-identity {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  max-width: 320px;
  height: 28px;
  padding: 0 10px;
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: 6px;
  box-shadow: var(--shadow-xs);
  color: var(--color-text-primary);
  font-family: var(--font-family-base);
  font-size: 12px;
  white-space: nowrap;
  cursor: pointer;
  transition: all var(--transition-fast);
}

.hub-identity:hover {
  border-color: var(--el-color-primary, #409eff);
}

.hub-identity__icon {
  font-size: 11px;
  color: var(--el-color-primary, #409eff);
}

.hub-identity__who {
  font-weight: var(--font-weight-medium);
  overflow: hidden;
  text-overflow: ellipsis;
}

.hub-identity__dot,
.hub-identity__where {
  color: var(--color-text-secondary);
}

.hub-identity__where {
  overflow: hidden;
  text-overflow: ellipsis;
}

.hub-identity__open {
  font-size: 13px;
  color: var(--color-text-secondary);
}
</style>
