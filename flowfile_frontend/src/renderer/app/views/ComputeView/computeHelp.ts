import type { PageHelpContent } from "../../components/common/PageHelpModal/types";

export const computeHelp: PageHelpContent = {
  title: "Compute",
  icon: "fa-solid fa-server",
  sections: [
    {
      title: "Two kinds of compute",
      icon: "fa-solid fa-info-circle",
      description:
        "Flowfile runs your flows with two separate systems. This page manages both — each on its own tab.",
      features: [
        {
          icon: "fa-brands fa-python",
          title: "Python kernels",
          description:
            "Docker containers that run Python Script and custom nodes. You create them, pick their packages, and start or stop them on the Python Kernels tab.",
        },
        {
          icon: "fa-solid fa-gauge-high",
          title: "Warm worker pool",
          description:
            "A speed setting for all the regular data nodes (joins, filters, formulas, …). Keeping worker processes warm skips a start-up cost per node. Admins manage it on the Performance tab.",
        },
      ],
    },
    {
      title: "Python kernels",
      icon: "fa-brands fa-python",
      description:
        "Kernels are isolated Python environments running in Docker containers. They execute Python Script nodes and custom code in a sandboxed environment with full package support.",
      features: [
        {
          icon: "fa-solid fa-box",
          title: "Isolated Execution",
          description:
            "Each kernel runs in its own Docker container with a separate Python environment",
        },
        {
          icon: "fa-solid fa-code",
          title: "Python Scripts",
          description: "Required for running Python Script nodes and custom transformation code",
        },
        {
          icon: "fa-solid fa-layer-group",
          title: "Image flavours",
          description:
            "Pick Base (Polars/PyArrow/NumPy) or ML (sklearn, xgboost, lightgbm, statsmodels pre-baked). Use Custom for your own image.",
        },
        {
          icon: "fa-solid fa-cubes",
          title: "Extra packages",
          description:
            "Pip packages listed here are baked into a per-kernel Docker image at creation (one-time, ~30 s) and pinned against the flavour's constraints. Subsequent kernel starts skip the install.",
        },
        {
          icon: "fa-solid fa-arrows-rotate",
          title: "Persistent State",
          description:
            "Artifacts and variables persist between node executions within the same kernel",
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
          description:
            "Each kernel is a running container. Stop unused kernels to free memory and CPU.",
        },
        {
          type: "warning",
          title: "Pull the image first",
          description:
            "On first use, Docker pulls the chosen kernel image (~500 MB base, ~720 MB ML). Subsequent kernels start in seconds.",
        },
        {
          type: "success",
          title: "The pool needs no Docker",
          description:
            "The warm worker pool is independent of kernels and Docker — it speeds up regular nodes even when Docker is off.",
        },
      ],
    },
  ],
};
