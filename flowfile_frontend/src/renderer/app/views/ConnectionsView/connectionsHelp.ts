import type { PageHelpContent } from "../../components/common/PageHelpModal/types";

export const connectionsHelp: PageHelpContent = {
  title: "Connections",
  icon: "fa-solid fa-link",
  sections: [
    {
      title: "Overview",
      icon: "fa-solid fa-info-circle",
      description:
        "Manage all your external connections in one place. Configure databases, cloud storage, streaming, and securely store credentials.",
      features: [
        {
          icon: "fa-solid fa-database",
          title: "Databases",
          description: "Connect to PostgreSQL, MySQL, SQL Server, and other SQL databases",
        },
        {
          icon: "fa-solid fa-cloud",
          title: "Cloud Storage",
          description: "Set up connections to S3, Google Cloud Storage, or Azure Blob Storage",
        },
        {
          icon: "fa-solid fa-bolt",
          title: "Kafka",
          description: "Configure Kafka brokers for streaming data ingestion and publishing",
        },
        {
          icon: "fa-solid fa-key",
          title: "Secrets",
          description: "Store API keys and passwords securely with encrypted storage",
        },
      ],
    },
    {
      title: "Quick Tips",
      icon: "fa-solid fa-lightbulb",
      tips: [
        {
          type: "success",
          title: "Test connections before saving",
          description: "Use the test button to verify credentials and network access before saving.",
        },
        {
          type: "success",
          title: "Connections are available across all flows",
          description: "Once saved, a connection can be used by any node that supports external data sources.",
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
