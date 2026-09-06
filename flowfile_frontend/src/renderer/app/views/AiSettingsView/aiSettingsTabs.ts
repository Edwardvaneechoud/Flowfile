// Single source of truth for the AI settings sections shown in the
// AiSettingsView header tab bar and the sidebar sub-menu (NavigationRoutes).
// Keep this file dependency-free (pure data) so it can be imported from the
// sidebar without risking an import cycle.

export const AI_SETTINGS_TAB_KEYS = ["providers", "assistant"] as const;

export type AiSettingsTabKey = (typeof AI_SETTINGS_TAB_KEYS)[number];

/** Old ?tab= values that still resolve (the on-device model used to be its own tab). */
export const AI_SETTINGS_TAB_ALIASES: Record<string, AiSettingsTabKey> = {
  local: "providers",
};

/** Sidebar group-header label (i18n key) the AI children render under. */
export const AI_SETTINGS_GROUP_KEY = "menu.settingsGroupAi";

export interface AiSettingsTabDef {
  key: AiSettingsTabKey; // also the ?tab= query value
  label: string; // header tab label
  icon: string; // FontAwesome class
  sidebarKey: string; // i18n key for the sidebar child label
}

export const aiSettingsTabs: AiSettingsTabDef[] = [
  {
    key: "providers",
    label: "Providers",
    icon: "fa-solid fa-plug",
    sidebarKey: "menu.aiProviders",
  },
  {
    key: "assistant",
    label: "Assistant",
    icon: "fa-solid fa-wand-magic-sparkles",
    sidebarKey: "menu.aiAssistant",
  },
];
