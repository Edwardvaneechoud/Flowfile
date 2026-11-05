<template>
  <button
    v-if="identity"
    type="button"
    class="header-btn hub-identity"
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
import { computed, onMounted } from "vue";
import { desktop } from "../../../../lib/desktop";
import { useHubConnectionStore } from "../../../stores/hub-connection-store";
import { hubIdentity, hubIdentityTitle } from "./hubIdentity";

const hub = useHubConnectionStore();
const identity = computed(() => hubIdentity(hub.connection));
const title = computed(() => (identity.value ? hubIdentityTitle(identity.value) : ""));

onMounted(() => void hub.load());

const openHub = () => {
  if (identity.value) void desktop.openExternal(identity.value.url);
};
</script>

<style scoped>
.hub-identity {
  max-width: 320px;
  font-family: var(--font-family-base);
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
