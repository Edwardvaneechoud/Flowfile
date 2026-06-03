<template>
  <el-menu
    :default-active="activeIndex"
    class="el-menu-vertical-demo"
    :collapse="isCollapse"
    router
  >
    <template v-for="(routeItem, idx) in items" :key="idx">
      <!-- Regular menu items (no children) -->
      <el-menu-item
        v-if="!routeItem.children"
        :index="routeItem.name"
        :route="{ name: routeItem.name }"
        :disabled="routeItem.disabled"
      >
        <i v-if="routeItem.meta?.icon" :class="routeItem.meta.icon"></i>
        <template #title>
          <span>{{ t(routeItem.displayName) }}</span>
        </template>
      </el-menu-item>

      <!-- Items with children -->
      <el-sub-menu v-else :index="routeItem.name" popper-class="sidebar-submenu-popper">
        <template #title>
          <i
            v-if="routeItem.meta?.icon"
            :class="routeItem.meta.icon"
            @click="handleParentClick(routeItem)"
          ></i>
          <span @click="handleParentClick(routeItem)">{{ t(routeItem.displayName) }}</span>
          <i
            v-if="isCollapse"
            class="submenu-caret fa-solid fa-angle-right"
            @click="handleParentClick(routeItem)"
          ></i>
        </template>

        <el-menu-item-group :title="t(routeItem.displayName)">
          <el-menu-item
            v-for="child in routeItem.children"
            :key="child.index ?? child.name"
            :index="child.index ?? child.name"
            :route="child.query ? { name: child.name, query: child.query } : { name: child.name }"
            :disabled="child.disabled"
          >
            <i v-if="child.meta?.icon" :class="child.meta.icon"></i>
            <template #title>
              <span>{{ t(child.displayName) }}</span>
            </template>
          </el-menu-item>
        </el-menu-item-group>
      </el-sub-menu>
    </template>
  </el-menu>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { INavigationRoute } from "../NavigationRoutes";
import { useRoute, useRouter } from "vue-router";
import { useI18n } from "vue-i18n";

const { t } = useI18n();
const route = useRoute();
const router = useRouter();

function handleParentClick(routeItem: INavigationRoute) {
  // A sub-menu parent with its own destination (marked via `query`) navigates to
  // it on click, in addition to toggling the fly-out — Element's sub-menu title
  // does not navigate on its own in router mode. For Connections this opens the
  // overview landing page.
  if (!routeItem.query) return;
  router.push({ name: routeItem.name, query: routeItem.query }).catch(() => {
    // ignore redundant navigation (already on this route)
  });
}

const props = withDefaults(
  defineProps<{
    items?: INavigationRoute[];
    isCollapse?: boolean;
  }>(),
  {
    items: () => [],
    isCollapse: false,
  },
);

const activeIndex = computed(() => {
  const name = route.name as string;
  // Connection sub-items share the "connections" route but differ by ?tab=, so
  // match the composite child index instead of the bare route name.
  if (name === "connections") {
    const tab = (route.query.tab as string) || "overview";
    return `connections:${tab}`;
  }
  // Catalog sub-items likewise share the "catalog" route but differ by ?tab=.
  if (name === "catalog") {
    const tab = (route.query.tab as string) || "catalog";
    return `catalog:${tab}`;
  }
  return name;
});

const accordionValue = ref<boolean[]>([]);
onMounted(() => {
  accordionValue.value = props.items.map((item) => isItemExpanded(item));
});

function isRouteActive(item: INavigationRoute) {
  return item.name === route.name;
}

function isItemExpanded(item: INavigationRoute): boolean {
  if (!item.children) {
    return false;
  }

  const isCurrentItemActive = isRouteActive(item);
  const isChildActive = !!item.children.find((child) =>
    child.children ? isItemExpanded(child) : isRouteActive(child),
  );

  return isCurrentItemActive || isChildActive;
}
</script>

<style scoped>
.el-menu-vertical-demo:not(.el-menu--collapse) {
  width: 200px;
}

.el-menu-item [class^="fa-"],
.el-sub-menu [class^="fa-"] {
  margin-right: 5px;
  width: 24px;
  text-align: center;
  font-size: 18px;
  vertical-align: middle;
}

.el-menu--collapse {
  .el-menu-item [class^="fa-"],
  .el-sub-menu [class^="fa-"] {
    margin-right: 0;
  }
}

/* Expand/collapse affordance on the collapsed rail icon */
:deep(.el-sub-menu__title) {
  position: relative;
}

.submenu-caret {
  position: absolute;
  right: 5px;
  top: 50%;
  width: auto;
  transform: translateY(-50%);
  font-size: 9px;
  color: var(--color-text-tertiary);
  cursor: pointer;
}
</style>

<!-- Global: the fly-out popover is teleported to <body>, so scoped styles can't
     reach it. Gives the collapsed sub-menu fly-out a clear header and keeps the
     item list compact — the default Element item height (56px) makes a 7-item
     menu very tall. Shared by the Connections and Catalog sub-menus. -->
<style>
.sidebar-submenu-popper {
  --el-menu-item-height: 34px;
  --el-menu-sub-item-height: 34px;
}

.sidebar-submenu-popper .el-menu--popup {
  min-width: 168px;
  padding: var(--spacing-1);
}

.sidebar-submenu-popper .el-menu-item {
  height: 34px;
  line-height: 34px;
  padding: 0 var(--spacing-3);
  border-radius: var(--border-radius-md);
}

.sidebar-submenu-popper .el-menu-item-group__title {
  padding: var(--spacing-2) var(--spacing-3) var(--spacing-1);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  color: var(--color-text-tertiary);
}
</style>
