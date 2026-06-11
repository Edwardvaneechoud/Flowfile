<script setup lang="ts">
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import ChartsPanel from './ChartsPanel.vue'
import DashboardsPanel from './DashboardsPanel.vue'

// One "Visuals" overview combining charts + dashboards behind a segmented
// toggle, mirroring flowfile_frontend's VisualsPanel. The active sub-view is
// reflected in the `kind` query param (?tab=visuals&kind=charts|dashboards).
type Kind = 'charts' | 'dashboards'

const route = useRoute()
const router = useRouter()

const kind = computed<Kind>(() => (route.query.kind === 'dashboards' ? 'dashboards' : 'charts'))

function setKind(k: Kind) {
  if (kind.value === k) return
  router.replace({ query: { ...route.query, kind: k } })
}
</script>

<template>
  <div class="visuals-overview">
    <div class="visuals-toggle">
      <button class="seg" :class="{ active: kind === 'charts' }" @click="setKind('charts')">
        <i class="fa-solid fa-chart-column"></i>
        <span>Charts</span>
      </button>
      <button class="seg" :class="{ active: kind === 'dashboards' }" @click="setKind('dashboards')">
        <i class="fa-solid fa-table-cells-large"></i>
        <span>Dashboards</span>
      </button>
    </div>
    <div class="visuals-overview-body">
      <ChartsPanel v-if="kind === 'charts'" />
      <DashboardsPanel v-else />
    </div>
  </div>
</template>

<style scoped>
.visuals-overview {
  display: flex;
  flex-direction: column;
  height: 100%;
  min-height: 0;
}
.visuals-toggle {
  display: flex;
  gap: 4px;
  padding: var(--spacing-3, 12px) var(--spacing-6, 24px) 0;
  flex-shrink: 0;
}
.seg {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 8px 16px;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md, 8px);
  background: var(--color-background-secondary);
  color: var(--color-text-secondary);
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all var(--transition-fast, 0.15s);
}
.seg:hover {
  color: var(--color-text-primary);
  background: var(--color-background-hover);
}
.seg.active {
  background: var(--color-accent);
  border-color: var(--color-accent);
  color: #fff;
}
.seg i {
  font-size: 13px;
}
.visuals-overview-body {
  flex: 1;
  min-height: 0;
  overflow: hidden;
}
</style>
