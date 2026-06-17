import type { PageHelpContent } from "../../components/common/PageHelpModal/types";
import { connectionTypes } from "./connectionTypes";

export const connectionsHelp: PageHelpContent = {
  title: "Connections",
  icon: "fa-solid fa-link",
  sections: [
    {
      title: "Overview",
      icon: "fa-solid fa-info-circle",
      description:
        "Manage all your external connections in one place. Configure databases, cloud storage, streaming, analytics, and AI providers, and securely store credentials. Select a card to jump straight to it.",
      // Derived from the shared connection-types config so icons/labels stay in
      // sync with the tab bar, overview cards, and sidebar sub-menu.
      features: connectionTypes.map((type) => ({
        icon: type.icon,
        title: type.label,
        description: type.description,
        link: { name: "connections", query: { tab: type.key } },
      })),
    },
    {
      title: "Quick Tips",
      icon: "fa-solid fa-lightbulb",
      tips: [
        {
          type: "success",
          title: "Test connections before saving",
          description:
            "Use the test button to verify credentials and network access before saving.",
        },
        {
          type: "success",
          title: "Connections are available across all flows",
          description:
            "Once saved, a connection can be used by any node that supports external data sources.",
        },
        {
          type: "warning",
          title: "Secrets are encrypted at rest",
          description:
            "Credentials are encrypted using the master key. Make sure your master key is backed up safely.",
        },
      ],
    },
  ],
};
