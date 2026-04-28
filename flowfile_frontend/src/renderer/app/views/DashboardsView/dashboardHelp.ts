import type { PageHelpContent } from "../../components/common/PageHelpModal/types";

export const dashboardHelp: PageHelpContent = {
  title: "Dashboards",
  icon: "fa-solid fa-table-cells-large",
  sections: [
    {
      title: "Overview",
      icon: "fa-solid fa-info-circle",
      description:
        "A dashboard composes multiple saved visualizations on a single canvas so you can read them side-by-side and steer them with shared filters. Tiles live on a 12-column grid; every chart is computed live from the catalog table or SQL query behind its visualization.",
      features: [
        {
          icon: "fa-solid fa-table-cells-large",
          title: "Library",
          description:
            "Browse, open, edit, or delete dashboards from the Dashboards tab in the catalog. Each card shows tile count, namespace, and last-updated.",
        },
        {
          icon: "fa-solid fa-border-all",
          title: "Canvas",
          description:
            "A 12-column grid with a 40px row height. Drag tiles by their header, resize from any corner, and the layout vertically compacts automatically.",
        },
        {
          icon: "fa-solid fa-shapes",
          title: "Tiles",
          description:
            "Two types: visualization tiles render a saved chart from the catalog, and text tiles hold sanitized Markdown for headings and notes.",
        },
        {
          icon: "fa-solid fa-filter",
          title: "Filters",
          description:
            "Dashboard-wide filters bound to a catalog table. When active, every tile that reads from the same table re-renders with the filter applied.",
        },
      ],
    },
    {
      title: "Canvas & grid",
      icon: "fa-solid fa-border-all",
      description:
        "Tiles are positioned on a 12-column grid in row units of 40px. The grid uses vertical compaction, so empty rows above a tile are squeezed out as you move things around. The hatched background only appears in edit mode — it's a guideline grid that disappears in the viewer.",
      features: [
        {
          icon: "fa-solid fa-up-down-left-right",
          title: "Drag to move",
          description:
            "Click and hold any tile's header (the move icon appears in edit mode) to reposition it on the grid.",
        },
        {
          icon: "fa-solid fa-up-right-and-down-left-from-center",
          title: "Drag to resize",
          description:
            "Grab the bottom-right corner of a tile to resize. Minimum size is 2×2 grid cells.",
        },
        {
          icon: "fa-solid fa-arrows-up-to-line",
          title: "Auto-stacking",
          description:
            "New tiles are placed below the lowest existing tile, so adding never overlaps the current layout.",
        },
        {
          icon: "fa-solid fa-ruler-combined",
          title: "Default sizes",
          description:
            "New visualization tiles spawn as 6×6; new text tiles spawn full width (12×3). Reshape them right after dropping.",
        },
      ],
    },
    {
      title: "Tile types",
      icon: "fa-solid fa-shapes",
      description:
        "Each tile is either a visualization (a saved chart from the catalog) or a text block (Markdown). The three-dot menu in edit mode lets you edit the underlying chart, edit text, or remove the tile from the dashboard. Removing a tile here does not delete the visualization from the catalog.",
      features: [
        {
          icon: "fa-solid fa-chart-column",
          title: "Visualization tile",
          description:
            "Renders a saved visualization. Re-fetches data when the underlying viz is edited and applies any dashboard filters that target it.",
        },
        {
          icon: "fa-solid fa-pen-to-square",
          title: "Text tile",
          description:
            "Markdown with headings, lists, code, links, tables, images, blockquotes, horizontal rules. Sanitized through DOMPurify.",
        },
        {
          icon: "fa-solid fa-mouse-pointer",
          title: "Edit text",
          description:
            "Double-click a text tile (or click the pen icon) in edit mode to enter the editor; click outside or press the check icon to commit.",
        },
        {
          icon: "fa-solid fa-eye",
          title: "View-mode prose",
          description:
            "In view mode, text tiles render borderless and transparent so they read like part of the page rather than a card.",
        },
      ],
    },
    {
      title: "Datasources & filters",
      icon: "fa-solid fa-filter",
      description:
        "Each visualization tile resolves to one of two source types: a catalog table (filterable from the dashboard) or a SQL query (not filterable). The dashboard walks viz → catalog table on mount and surfaces the unique tables as datasources. Filters are bound to one datasource; when active, every tile that reads from the same table receives a filter step prepended to its query.",
      features: [
        {
          icon: "fa-solid fa-calendar-days",
          title: "Date range",
          description:
            "Inferred for temporal columns. Pick a start and end; an empty range disables the filter.",
        },
        {
          icon: "fa-solid fa-arrows-left-right",
          title: "Numeric range",
          description:
            "Default for numeric columns. Min/max placeholders show the column's actual extents. You can switch to categorical for one-of selection.",
        },
        {
          icon: "fa-solid fa-list-check",
          title: "Categorical",
          description:
            "Multi-select with type-ahead. Up to 100 distinct values are loaded; if more exist, a +more hint lets you type custom values.",
        },
        {
          icon: "fa-solid fa-bullseye",
          title: "Apply to",
          description:
            "Target every tile backed by the datasource, or pick specific tiles. Empty selection means all.",
        },
      ],
      tips: [
        {
          type: "warning",
          title: "Add filter is disabled until a tile is bound to a catalog table",
          description:
            "SQL-source tiles render but don't participate in dashboard filtering. Add at least one catalog-backed visualization first.",
        },
        {
          type: "warning",
          title: "Untied filters need a datasource",
          description:
            "A filter showing the untied badge has no datasource bound — click the pencil to bind one.",
        },
      ],
    },
    {
      title: "Editor workflow",
      icon: "fa-solid fa-pen-ruler",
      description:
        "In edit mode the sidebar lets you insert tiles, the filter bar manages dashboard-wide filters, and the toolbar saves or discards your changes.",
      features: [
        {
          icon: "fa-solid fa-plus",
          title: "Insert sidebar",
          description:
            "Add a Markdown text block with one click, or pick any saved visualization. Tiles already on the canvas dim but can still be added a second time.",
        },
        {
          icon: "fa-solid fa-sliders",
          title: "Filter bar",
          description:
            "Add, edit, or remove filters. Changing a filter's field or kind clears its current state to avoid mismatched values.",
        },
        {
          icon: "fa-solid fa-pen-to-square",
          title: "Edit chart",
          description:
            "From the tile menu, open the saved-visualization viewer. On close, every tile bound to that viz is remounted and re-fetched.",
        },
        {
          icon: "fa-solid fa-floppy-disk",
          title: "Save & discard",
          description:
            "Edits are local until you click Save. The unsaved-changes badge tracks divergence from the persisted copy.",
        },
      ],
      tips: [
        {
          type: "warning",
          title: "Closing with unsaved changes prompts for confirmation",
          description:
            "Leaving the editor with the unsaved-changes badge active asks you to confirm before discarding.",
        },
        {
          type: "success",
          title: "Tile state survives layout edits",
          description:
            "Tile IDs are stable across saves, so text-editor focus and filter values stick when you reposition.",
        },
      ],
    },
    {
      title: "Viewing",
      icon: "fa-solid fa-eye",
      description:
        "The viewer is read-only: you can't move tiles, add filters, or edit text. You can change filter values to explore the data — those changes apply immediately to every tile but are scoped to your session and reset on reload.",
      tips: [
        {
          type: "success",
          title: "Click Edit to switch to the editor",
          description:
            "The toolbar's Edit button takes you into the editor with the same dashboard loaded.",
        },
        {
          type: "warning",
          title: "Filter changes are session-only here",
          description:
            "Reload the page or click Back and your filters reset to the saved state. Save them from the editor if you want them to stick.",
        },
      ],
    },
    {
      title: "Quick reference",
      icon: "fa-solid fa-keyboard",
      tips: [
        {
          type: "success",
          title: "Drag tile by its header, resize from the bottom-right",
          description: "Use the move icon in the tile header to drag, and any corner to resize.",
        },
        {
          type: "success",
          title: "Double-click a text tile to edit",
          description:
            "Or click the pen icon in its header. Click outside the textarea or press the check icon to commit.",
        },
        {
          type: "success",
          title: "Tile menu → Edit chart, Remove from dashboard",
          description:
            "The three-dot menu in the tile header offers chart edit and tile removal in edit mode.",
        },
      ],
    },
  ],
};
