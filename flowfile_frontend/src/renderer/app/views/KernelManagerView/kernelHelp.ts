import type { PageHelpContent } from "../../components/common/PageHelpModal/types";

export const kernelHelp: PageHelpContent = {
  title: "Kernel Manager",
  icon: "fa-solid fa-server",
  sections: [
    {
      title: "Overview",
      icon: "fa-solid fa-info-circle",
      description:
        "Kernels are isolated Python environments running in Docker containers. They execute Python Script nodes and custom code in a sandboxed environment with full package support.",
      features: [
        {
          icon: "fa-solid fa-box",
          title: "Isolated Execution",
          description: "Each kernel runs in its own Docker container with a separate Python environment",
        },
        {
          icon: "fa-solid fa-code",
          title: "Python Scripts",
          description: "Required for running Python Script nodes and custom transformation code",
        },
        {
          icon: "fa-solid fa-cubes",
          title: "Package Support",
          description: "Install additional Python packages (scikit-learn, etc.) inside the kernel",
        },
        {
          icon: "fa-solid fa-arrows-rotate",
          title: "Persistent State",
          description: "Artifacts and variables persist between node executions within the same kernel",
        },
      ],
    },
    {
      title: "Quick Tips",
      icon: "fa-solid fa-lightbulb",
      tips: [
        {
          type: "success",
          title: "Docker must be running",
          description: "Kernels require Docker to be installed and running on your machine.",
        },
        {
          type: "warning",
          title: "Kernels use system resources",
          description: "Each kernel is a running container. Stop unused kernels to free memory and CPU.",
        },
      ],
    },
  ],
};
