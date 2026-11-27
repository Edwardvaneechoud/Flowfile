<template>
  <el-popover
    v-model:visible="open"
    trigger="click"
    placement="bottom-start"
    :width="360"
    :persistent="false"
    popper-class="external-badge-popover"
  >
    <template #reference>
      <span
        ref="badgeRef"
        class="external-badge"
        data-testid="external-badge"
        role="button"
        tabindex="0"
        aria-haspopup="dialog"
        :aria-expanded="open"
        @click.stop
        @keydown.enter.stop
        @keydown.space.stop
      >
        <i class="fa-solid fa-link"></i>
        Iceberg REST
        <i class="fa-solid fa-circle-info info-icon"></i>
      </span>
    </template>
    <div class="external-explainer" data-testid="external-badge-popover">
      <section v-for="section in sections" :key="section.heading">
        <h4>{{ section.heading }}</h4>
        <p v-if="section.text">{{ section.text }}</p>
        <ul v-if="section.items">
          <li v-for="item in section.items" :key="item">{{ item }}</li>
        </ul>
      </section>
      <a class="docs-link" :href="OUTSIDE_CATALOG_DOCS_URL" @click.prevent="openDocs">
        More in the docs <i class="fa-solid fa-arrow-up-right-from-square"></i>
      </a>
    </div>
  </el-popover>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";
import { desktop } from "../../../lib/desktop";
import type { ExternalCatalogInfo } from "../../types";
import { OUTSIDE_CATALOG_DOCS_URL, outsideCatalogExplainer } from "./externalCatalog";

const props = defineProps<{ external: ExternalCatalogInfo }>();

const open = ref(false);
const badgeRef = ref<HTMLElement | null>(null);
const sections = computed(() => outsideCatalogExplainer(props.external));

// Taken in capture so a drawer or dialog around the badge does not also close on this Esc.
function onKeydown(event: KeyboardEvent) {
  if (event.key !== "Escape" || !open.value) return;
  event.stopPropagation();
  open.value = false;
  badgeRef.value?.focus();
}

watch(open, (isOpen) => {
  if (isOpen) document.addEventListener("keydown", onKeydown, true);
  else document.removeEventListener("keydown", onKeydown, true);
});

onBeforeUnmount(() => document.removeEventListener("keydown", onKeydown, true));

function openDocs() {
  open.value = false;
  void desktop.openExternal(OUTSIDE_CATALOG_DOCS_URL);
}
</script>

<style scoped>
.external-badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  margin-left: 8px;
  padding: 1px 8px;
  font-size: 11px;
  font-weight: 600;
  color: var(--el-color-success, #67c23a);
  background: var(--el-color-success-light-9, rgba(103, 194, 58, 0.1));
  border-radius: 999px;
  white-space: nowrap;
  cursor: pointer;
}
.external-badge:focus-visible {
  outline: 2px solid var(--el-color-success, #67c23a);
  outline-offset: 1px;
}
.external-badge i {
  font-size: 10px;
}
.external-badge .info-icon {
  opacity: 0.8;
}
.external-explainer {
  font-size: var(--font-size-sm, 13px);
  line-height: 1.45;
  color: var(--color-text-primary);
  overflow-wrap: anywhere;
}
.external-explainer section + section {
  margin-top: 10px;
}
.external-explainer h4 {
  margin: 0 0 4px;
  font-size: var(--font-size-sm, 13px);
  font-weight: 600;
}
.external-explainer p {
  margin: 0;
}
.external-explainer ul {
  margin: 0;
  padding-left: 18px;
}
.external-explainer li + li {
  margin-top: 2px;
}
.docs-link {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  margin-top: 10px;
  color: var(--el-color-primary);
  text-decoration: none;
}
.docs-link:hover {
  text-decoration: underline;
}
.docs-link i {
  font-size: 10px;
}
</style>
