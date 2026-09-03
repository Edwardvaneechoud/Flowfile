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
        :route="
          routeItem.query
            ? { name: routeItem.name, query: routeItem.query }
            : { name: routeItem.name }
        "
        :disabled="routeItem.disabled"
      >
        <span class="nav-icon">
          <i v-if="routeItem.meta?.icon" :class="routeItem.meta.icon"></i>
          <span
            v-if="routeItem.statusDot"
            class="nav-status-dot"
            :class="`is-${routeItem.statusDot}`"
          ></span>
        </span>
        <template #title>
          <span>{{ t(routeItem.displayName) }}</span>
        </template>
      </el-menu-item>

      <!-- Items with children -->
      <el-sub-menu v-else :index="itemKey(routeItem)" popper-class="sidebar-submenu-popper">
        <template #title>
          <!-- A div, not a span: Element hides every span in a collapsed sub-menu title. -->
          <div class="nav-icon" @click="handleParentClick(routeItem)">
            <i v-if="routeItem.meta?.icon" :class="routeItem.meta.icon"></i>
            <i
              v-if="routeItem.statusDot"
              class="nav-status-dot"
              :class="`is-${routeItem.statusDot}`"
            ></i>
          </div>
          <span @click="handleParentClick(routeItem)">{{ t(routeItem.displayName) }}</span>
          <i
            v-if="isCollapse"
            class="submenu-caret fa-solid fa-angle-right"
            @click="handleParentClick(routeItem)"
          ></i>
        </template>

        <template
          v-for="section in childSections(routeItem)"
          :key="section.key ?? `${itemKey(routeItem)}-ungrouped`"
        >
          <el-menu-item-group v-if="!section.labelKey" :title="t(routeItem.displayName)">
            <template v-for="child in section.children" :key="itemKey(child)">
              <el-sub-menu
                v-if="child.children"
                :index="itemKey(child)"
                popper-class="sidebar-submenu-popper"
              >
                <template #title>
                  <span class="nav-icon" @click="handleParentClick(child)">
                    <i v-if="child.meta?.icon" :class="child.meta.icon"></i>
                  </span>
                  <span @click="handleParentClick(child)">{{ t(child.displayName) }}</span>
                </template>
                <el-menu-item
                  v-for="leaf in child.children"
                  :key="itemKey(leaf)"
                  :index="itemKey(leaf)"
                  :route="leaf.query ? { name: leaf.name, query: leaf.query } : { name: leaf.name }"
                  :disabled="leaf.disabled"
                >
                  <span class="nav-icon">
                    <i v-if="leaf.meta?.icon" :class="leaf.meta.icon"></i>
                  </span>
                  <template #title>
                    <span>{{ t(leaf.displayName) }}</span>
                  </template>
                </el-menu-item>
              </el-sub-menu>
              <el-menu-item
                v-else
                :index="itemKey(child)"
                :route="
                  child.query ? { name: child.name, query: child.query } : { name: child.name }
                "
                :disabled="child.disabled"
              >
                <span class="nav-icon">
                  <i v-if="child.meta?.icon" :class="child.meta.icon"></i>
                  <span
                    v-if="child.statusDot"
                    class="nav-status-dot"
                    :class="`is-${child.statusDot}`"
                  ></span>
                </span>
                <template #title>
                  <span>{{ t(child.displayName) }}</span>
                </template>
              </el-menu-item>
            </template>
          </el-menu-item-group>
          <div v-else class="menu-group">
            <button
              type="button"
              class="menu-group-header"
              :aria-expanded="!isGroupCollapsed(itemKey(routeItem), section.key)"
              @click="toggleGroup(itemKey(routeItem), section.key)"
            >
              <span>{{ t(section.labelKey) }}</span>
              <i
                class="fa-solid fa-chevron-down menu-group-chevron"
                :class="{ 'is-collapsed': isGroupCollapsed(itemKey(routeItem), section.key) }"
              ></i>
            </button>
            <el-collapse-transition>
              <div v-show="!isGroupCollapsed(itemKey(routeItem), section.key)">
                <template v-for="child in section.children" :key="itemKey(child)">
                  <el-sub-menu
                    v-if="child.children"
                    :index="itemKey(child)"
                    popper-class="sidebar-submenu-popper"
                  >
                    <template #title>
                      <span class="nav-icon" @click="handleParentClick(child)">
                        <i v-if="child.meta?.icon" :class="child.meta.icon"></i>
                      </span>
                      <span @click="handleParentClick(child)">{{ t(child.displayName) }}</span>
                    </template>
                    <el-menu-item
                      v-for="leaf in child.children"
                      :key="itemKey(leaf)"
                      :index="itemKey(leaf)"
                      :route="
                        leaf.query ? { name: leaf.name, query: leaf.query } : { name: leaf.name }
                      "
                      :disabled="leaf.disabled"
                    >
                      <span class="nav-icon">
                        <i v-if="leaf.meta?.icon" :class="leaf.meta.icon"></i>
                      </span>
                      <template #title>
                        <span>{{ t(leaf.displayName) }}</span>
                      </template>
                    </el-menu-item>
                  </el-sub-menu>
                  <el-menu-item
                    v-else
                    :index="itemKey(child)"
                    :route="
                      child.query ? { name: child.name, query: child.query } : { name: child.name }
                    "
                    :disabled="child.disabled"
                  >
                    <span class="nav-icon">
                      <i v-if="child.meta?.icon" :class="child.meta.icon"></i>
                      <span
                        v-if="child.statusDot"
                        class="nav-status-dot"
                        :class="`is-${child.statusDot}`"
                      ></span>
                    </span>
                    <template #title>
                      <span>{{ t(child.displayName) }}</span>
                    </template>
                  </el-menu-item>
                </template>
              </div>
            </el-collapse-transition>
          </div>
        </template>
      </el-sub-menu>
    </template>
  </el-menu>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { INavigationRoute } from "../NavigationRoutes";
import { useRoute, useRouter } from "vue-router";
import { useI18n } from "vue-i18n";

const { t } = useI18n();
const route = useRoute();
const router = useRouter();

function handleParentClick(routeItem: INavigationRoute) {
  // A sub-menu parent with its own destination (marked via `query`) navigates to
  // it on click, in addition to toggling the fly-out — Element's sub-menu title
  // does not navigate on its own in router mode. For Settings this opens the
  // connections overview, for the nested All connections entry likewise.
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

// el-menu index of an entry; parents that share a route name carry their own `index`.
const itemKey = (item: INavigationRoute) => item.index ?? item.name;

// Every entry below the rail, nested levels included (one extra level is supported).
const descendants = (items: INavigationRoute[]): INavigationRoute[] =>
  items.flatMap((item) => (item.children ? [...item.children, ...descendants(item.children)] : []));

const activeIndex = computed(() => {
  const name = route.name as string;
  const tab = route.query.tab as string | undefined;
  const children = descendants(props.items);
  // Sub-items share a route name but differ by ?tab=, so try the composite index
  // first, then the bare name (a child without a tab, or a flattened parent).
  const candidates = tab ? [`${name}:${tab}`, name] : [name];
  for (const candidate of candidates) {
    if (children.some((child) => itemKey(child) === candidate)) return candidate;
  }
  // Unknown tab (e.g. legacy catalog ?tab=dashboards): highlight the first entry on
  // this route so its parent still lights up.
  const first = children.find((child) => child.name === name);
  return first ? itemKey(first) : name;
});

const COLLAPSED_GROUPS_KEY = "flowfile-sidebar-collapsed-groups";

function readCollapsedGroups(): Set<string> {
  try {
    const raw = localStorage.getItem(COLLAPSED_GROUPS_KEY);
    return new Set(raw ? (JSON.parse(raw) as string[]) : []);
  } catch {
    return new Set();
  }
}

// Must precede the immediate activeIndex watcher below, which reads it during setup.
const collapsedGroups = ref<Set<string>>(readCollapsedGroups());

function setCollapsedGroups(next: Set<string>) {
  collapsedGroups.value = next;
  try {
    localStorage.setItem(COLLAPSED_GROUPS_KEY, JSON.stringify([...next]));
  } catch {
    // private mode / blocked storage: state just won't persist
  }
}

// Navigating to a child inside a collapsed group re-opens that group so the
// active item is never invisibly selected. Immediate, so a deep link into a
// collapsed group is uncovered on first mount too.
watch(
  activeIndex,
  (idx) => {
    for (const item of props.items) {
      for (const child of item.children ?? []) {
        if (itemKey(child) === idx && child.group) {
          const id = `${itemKey(item)}:${child.group.key}`;
          if (collapsedGroups.value.has(id)) {
            const next = new Set(collapsedGroups.value);
            next.delete(id);
            setCollapsedGroups(next);
          }
        }
      }
    }
  },
  { immediate: true },
);

const accordionValue = ref<boolean[]>([]);
onMounted(() => {
  accordionValue.value = props.items.map((item) => isItemExpanded(item));
});

interface ChildSection {
  key: string | null;
  labelKey: string | null;
  children: INavigationRoute[];
}

/** Adjacent children sharing a group key become one collapsible section. */
function childSections(routeItem: INavigationRoute): ChildSection[] {
  const sections: ChildSection[] = [];
  for (const child of routeItem.children ?? []) {
    const key = child.group?.key ?? null;
    const last = sections[sections.length - 1];
    if (last && last.key === key) last.children.push(child);
    else sections.push({ key, labelKey: child.group?.labelKey ?? null, children: [child] });
  }
  return sections;
}

function isGroupCollapsed(parent: string, key: string | null): boolean {
  return key !== null && collapsedGroups.value.has(`${parent}:${key}`);
}

function toggleGroup(parent: string, key: string | null) {
  if (key === null) return;
  const id = `${parent}:${key}`;
  const next = new Set(collapsedGroups.value);
  if (next.has(id)) next.delete(id);
  else next.add(id);
  setCollapsedGroups(next);
}

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
  margin-right: 8px;
  width: 22px;
  text-align: center;
  font-size: 16px;
  vertical-align: middle;
}

.el-menu--collapse {
  .el-menu-item [class^="fa-"],
  .el-sub-menu [class^="fa-"] {
    margin-right: 0;
  }
}

/* Icon wrapper so a status dot can anchor to the icon corner in both the
   expanded and collapsed (rail) layouts. */
.nav-icon {
  position: relative;
  display: inline-flex;
  align-items: center;
  margin-right: 8px;
}

.nav-icon [class^="fa-"] {
  margin-right: 0;
}

/* The dot is an <i> inside the sub-menu title (spans are hidden there); undo the icon sizing. */
.nav-icon i.nav-status-dot {
  width: 8px;
  margin: 0;
  font-size: 0;
}

.el-menu--collapse .nav-icon {
  margin-right: 0;
}

.nav-status-dot {
  position: absolute;
  top: -1px;
  right: -1px;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  /* ring in the sidebar color so the dot stays crisp over the glyph */
  box-shadow: 0 0 0 1.5px var(--color-background-primary);
}

.nav-status-dot.is-clean {
  background: var(--color-success, #16a34a);
}

.nav-status-dot.is-unsaved {
  background: var(--color-warning, #d97706);
}

.nav-status-dot.is-external {
  background: var(--color-danger, #ef4444);
}

/* Expand/collapse affordance on the collapsed rail icon */
:deep(.el-sub-menu__title) {
  position: relative;
}

.menu-group-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 100%;
  padding: var(--spacing-2) var(--spacing-4) var(--spacing-1);
  border: none;
  background: transparent;
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  color: var(--color-text-tertiary);
  cursor: pointer;
  user-select: none;
}

.menu-group-header:hover {
  color: var(--color-text-secondary);
}

/* Beats the generic sub-menu fa- sizing rule above (element + two classes). */
.menu-group-header i.menu-group-chevron {
  width: auto;
  margin-right: 0;
  font-size: 9px;
  transition: transform var(--transition-fast, 0.15s ease);
}

.menu-group-chevron.is-collapsed {
  transform: rotate(-90deg);
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
  --el-menu-item-height: 38px;
  --el-menu-sub-item-height: 38px;
}

.sidebar-submenu-popper .el-menu--popup {
  min-width: 184px;
  padding: var(--spacing-2);
}

.sidebar-submenu-popper .el-menu-item {
  height: 38px;
  line-height: 38px;
  padding: 0 var(--spacing-4);
  border-radius: var(--border-radius-md);
}

/* Smaller, lighter icons with real breathing room between icon and label —
   the teleported popper doesn't inherit MenuAccordion's scoped icon rule. */
.sidebar-submenu-popper .el-menu-item .nav-icon {
  position: relative;
  display: inline-flex;
  align-items: center;
  margin-right: 12px;
}

.sidebar-submenu-popper .el-menu-item [class^="fa-"] {
  width: 18px;
  font-size: 14px;
  text-align: center;
  vertical-align: middle;
}

.sidebar-submenu-popper .nav-status-dot {
  position: absolute;
  top: -1px;
  right: -1px;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  box-shadow: 0 0 0 1.5px var(--color-background-primary);
}

.sidebar-submenu-popper .nav-status-dot.is-clean {
  background: var(--color-success, #16a34a);
}

.sidebar-submenu-popper .nav-status-dot.is-unsaved {
  background: var(--color-warning, #d97706);
}

.sidebar-submenu-popper .nav-status-dot.is-external {
  background: var(--color-danger, #ef4444);
}

.sidebar-submenu-popper .el-sub-menu__title {
  height: 38px;
  line-height: 38px;
  padding: 0 var(--spacing-4);
  border-radius: var(--border-radius-md);
}

.sidebar-submenu-popper .el-sub-menu__title .nav-icon {
  position: relative;
  display: inline-flex;
  align-items: center;
  margin-right: 12px;
}

.sidebar-submenu-popper .el-sub-menu__title [class^="fa-"] {
  width: 18px;
  font-size: 14px;
  text-align: center;
  vertical-align: middle;
}

.sidebar-submenu-popper .el-menu-item-group__title {
  padding: var(--spacing-2) var(--spacing-4) var(--spacing-1);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  color: var(--color-text-tertiary);
}
</style>
