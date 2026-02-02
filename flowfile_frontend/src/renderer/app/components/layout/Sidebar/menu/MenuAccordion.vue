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
      <el-sub-menu v-else :index="routeItem.name">
        <template #title>
          <i v-if="routeItem.meta?.icon" :class="routeItem.meta.icon"></i>
          <span>{{ t(routeItem.displayName) }}</span>
        </template>

        <el-menu-item
          v-for="child in routeItem.children"
          :key="child.name"
          :index="child.name"
          :route="{ name: child.name }"
          :disabled="child.disabled"
        >
          <i v-if="child.meta?.icon" :class="child.meta.icon"></i>
          <template #title>
            <span>{{ t(child.displayName) }}</span>
          </template>
        </el-menu-item>
      </el-sub-menu>
    </template>
  </el-menu>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { INavigationRoute } from "../NavigationRoutes";
import { useRoute } from "vue-router";
import { useI18n } from "vue-i18n";

const { t } = useI18n();
const route = useRoute();

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

const activeIndex = computed(() => route.name as string);

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
</style>
