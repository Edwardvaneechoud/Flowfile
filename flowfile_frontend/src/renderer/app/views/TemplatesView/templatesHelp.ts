import type { PageHelpContent } from "../../components/common/PageHelpModal/types";

export const templatesHelp: PageHelpContent = {
  title: "Templates",
  icon: "fa-solid fa-layer-group",
  sections: [
    {
      title: "Overview",
      icon: "fa-solid fa-info-circle",
      description:
        "Browse pre-built flow templates to get started quickly. Templates include sample data and are organized by difficulty level.",
      features: [
        {
          icon: "fa-solid fa-graduation-cap",
          title: "Beginner",
          description: "Simple flows to learn the basics — reading files, filtering, and writing output",
        },
        {
          icon: "fa-solid fa-chart-line",
          title: "Intermediate",
          description: "Multi-step flows with joins, aggregations, and multiple data sources",
        },
        {
          icon: "fa-solid fa-rocket",
          title: "Advanced",
          description: "Complex pipelines with Python scripts, custom nodes, and external connections",
        },
        {
          icon: "fa-solid fa-download",
          title: "Sample Data",
          description: "Each template includes downloadable sample datasets to run immediately",
        },
      ],
    },
    {
      title: "Quick Tips",
      icon: "fa-solid fa-lightbulb",
      tips: [
        {
          type: "success",
          title: "Load a template to open it in the Designer",
          description: "Click any template card to load it as a new flow in the Flow Designer.",
        },
        {
          type: "success",
          title: "Modify templates freely",
          description: "Templates are starting points — customize them to fit your own data and needs.",
        },
      ],
    },
  ],
};
