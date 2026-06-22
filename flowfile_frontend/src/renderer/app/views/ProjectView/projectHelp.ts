import type { PageHelpContent } from "../../components/common/PageHelpModal/types";

export const projectHelp: PageHelpContent = {
  title: "Project Tracking",
  icon: "fa-solid fa-folder-tree",
  sections: [
    {
      title: "What is a project?",
      icon: "fa-solid fa-circle-info",
      description:
        "A project turns a folder on your computer into a safe, versioned copy of your work — backed by a standard Git repository. Your flows, connections and schedules are mirrored there automatically as you work, and no passwords are ever stored in it. Because it's just Git, it's ideal for syncing to GitHub or any remote.",
    },
    {
      title: "What you can do here",
      icon: "fa-solid fa-list-check",
      features: [
        {
          icon: "fa-solid fa-camera",
          title: "Save a version",
          description:
            "Write a short message to snapshot everything as it is right now. Save as often as you like.",
        },
        {
          icon: "fa-solid fa-clock-rotate-left",
          title: "Browse history",
          description: "Every version is listed newest-first. Expand Details to see what changed.",
        },
        {
          icon: "fa-solid fa-rotate-left",
          title: "Restore a version",
          description:
            "Roll the whole project back to an earlier snapshot. An autosave is taken first, so it's reversible.",
        },
        {
          icon: "fa-solid fa-code-branch",
          title: "Sync with Git",
          description:
            "Every version is a real Git commit, so you can push the folder to GitHub or any remote to back up, share, or collaborate.",
        },
        {
          icon: "fa-solid fa-key",
          title: "Add secret values",
          description:
            "Opened on a new machine? Re-enter passwords and API keys so your flows can run.",
          link: { name: "connections", query: { tab: "secrets" } },
        },
        {
          icon: "fa-solid fa-cube",
          title: "Track data artifacts",
          description:
            "In Settings, choose whether catalog tables, dashboards and ML models are versioned too.",
        },
        {
          icon: "fa-solid fa-link-slash",
          title: "Stop tracking",
          description:
            "Disconnect the folder anytime. Your files stay on disk — Flowfile just stops mirroring them.",
        },
      ],
    },
    {
      title: "Quick tips",
      icon: "fa-solid fa-lightbulb",
      tips: [
        {
          type: "success",
          title: "Restoring is safe",
          description:
            "Flowfile saves an automatic snapshot before any restore, so you can always come back.",
        },
        {
          type: "success",
          title: "Secrets never leave your machine",
          description:
            "Passwords and keys are kept out of the project folder. On a new computer you'll be asked to re-enter them.",
        },
        {
          type: "warning",
          title: "Changed outside Flowfile?",
          description:
            "If the project files were edited by another tool, use Reload to bring those changes back in.",
        },
      ],
    },
  ],
};
