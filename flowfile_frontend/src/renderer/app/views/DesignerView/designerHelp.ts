import type { PageHelpContent } from "../../components/common/PageHelpModal/types";

export const designerHelp: PageHelpContent = {
  title: "Flow Designer",
  icon: "fa-solid fa-diagram-project",
  sections: [
    {
      title: "Overview",
      icon: "fa-solid fa-info-circle",
      description:
        "The Flow Designer is your visual workspace for building data pipelines. Drag nodes onto the canvas, connect them together, and run your flow to process data.",
      features: [
        {
          icon: "fa-solid fa-plus-circle",
          title: "Add Nodes",
          description: "Right-click the canvas or drag from the node panel to add transformation steps",
        },
        {
          icon: "fa-solid fa-link",
          title: "Connect Nodes",
          description: "Drag from an output handle to an input handle to define data flow between steps",
        },
        {
          icon: "fa-solid fa-play",
          title: "Run Flows",
          description: "Execute the entire flow or run individual nodes to preview intermediate results",
        },
        {
          icon: "fa-solid fa-floppy-disk",
          title: "Save & Reuse",
          description: "Save flows to disk and register them in the catalog for scheduling",
        },
      ],
    },
    {
      title: "Quick Tips",
      icon: "fa-solid fa-lightbulb",
      tips: [
        {
          type: "success",
          title: "Double-click a node to configure it",
          description:
            "Each node has settings you can customize — column selections, filters, join keys, and more.",
        },
        {
          type: "success",
          title: "Use Ctrl+S to save and Ctrl+R to run",
          description: "Keyboard shortcuts help you work faster without reaching for buttons.",
        },
        {
          type: "warning",
          title: "Connect nodes before running",
          description: "Unconnected nodes won't receive data. Make sure your pipeline is fully wired.",
        },
      ],
    },
  ],
};
