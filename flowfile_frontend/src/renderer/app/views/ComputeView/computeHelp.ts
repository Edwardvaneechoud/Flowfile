import type { PageHelpContent } from "../../components/common/PageHelpModal/types";

export const computeHelp: PageHelpContent = {
  title: "Compute",
  icon: "fa-solid fa-server",
  sections: [
    {
      title: "Where your flows run",
      icon: "fa-solid fa-info-circle",
      description:
        "Python Script and custom kernel nodes run in Python kernels; every other node runs on a worker. Kernels have their own tab, workers are on the Backends tab.",
      features: [
        {
          icon: "fa-brands fa-python",
          title: "Python kernels",
          description:
            "Docker containers that run Python Script and custom nodes. You create them, pick their packages, and start or stop them on the Python Kernels tab.",
        },
        {
          icon: "fa-solid fa-house-laptop",
          title: "Default worker",
          description:
            "The built-in worker on this server that runs the regular data nodes (joins, filters, formulas, …). Its warm pool keeps processes ready between nodes, skipping the per-node start-up cost; they start on demand when a flow runs and retire after sitting idle. Admins manage it at the top of the Backends tab.",
        },
        {
          icon: "fa-solid fa-network-wired",
          title: "Other workers",
          description:
            "Workers on other hosts that admins add on the Backends tab. A flow or node picks one by its id in its settings, and the Handshake button checks that a worker matches this Flowfile. Each keeps its own warm pool.",
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
            "Pick Base (Polars/PyArrow/NumPy), ML (sklearn, xgboost, lightgbm, statsmodels pre-baked), Lite (few pins, room for big installs) or Notebook (this app's flowfile, runs the canvas notebook). Use Custom for your own image.",
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
        {
          type: "success",
          title: "Warm processes come and go",
          description:
            "With the pool on, processes start when flows run and retire after sitting idle. Zero warm processes between runs is normal.",
        },
      ],
    },
  ],
};
