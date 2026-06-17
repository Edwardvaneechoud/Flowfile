import type { PageHelpContent } from "../../components/common/PageHelpModal/types";

export const catalogHelp: PageHelpContent = {
  title: "Data Catalog",
  icon: "fa-solid fa-folder-tree",
  sections: [
    {
      title: "Overview",
      icon: "fa-solid fa-info-circle",
      description:
        "The Data Catalog is your central hub for managing tables, flows, and schedules. Organize assets into namespaces and explore data with the built-in SQL editor.",
      features: [
        {
          icon: "fa-solid fa-table",
          title: "Register Tables",
          description: "Register external data sources as catalog tables for easy discovery and reuse",
        },
        {
          icon: "fa-solid fa-diagram-project",
          title: "Register Flows",
          description: "Publish flows to the catalog so they can be scheduled and monitored",
        },
        {
          icon: "fa-solid fa-clock",
          title: "Schedules",
          description: "Set up cron-based schedules to run registered flows automatically",
        },
        {
          icon: "fa-solid fa-database",
          title: "SQL Explorer",
          description: "Query catalog tables directly using SQL to explore and validate your data",
        },
      ],
    },
    {
      title: "Quick Tips",
      icon: "fa-solid fa-lightbulb",
      tips: [
        {
          type: "success",
          title: "Use namespaces to organize assets",
          description: "Group related tables and flows into namespaces for a cleaner catalog structure.",
        },
        {
          type: "success",
          title: "Check run history for debugging",
          description: "The run history panel shows execution logs and errors for each registered flow.",
        },
        {
          type: "warning",
          title: "Flows must be saved before registering",
          description: "Save your flow in the Designer first, then register it here for scheduling.",
        },
      ],
    },
  ],
};
