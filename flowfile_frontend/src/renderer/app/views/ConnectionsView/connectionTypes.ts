// Single source of truth for the connection types shown in the Connections area.
// Consumed by the tab bar (ConnectionsView), the overview card grid
// (ConnectionsOverview), the sidebar sub-menu (NavigationRoutes), and the page
// help modal (connectionsHelp). Keep this file dependency-free (pure data) so it
// can be imported from the sidebar without risking an import cycle.

export const CONNECTION_TYPE_KEYS = [
  "database",
  "cloud",
  "kafka",
  "google_analytics",
  "secrets",
  "ai",
] as const;

export type ConnectionTypeKey = (typeof CONNECTION_TYPE_KEYS)[number];

export interface ConnectionType {
  key: ConnectionTypeKey; // also the ?tab= query value
  label: string;
  icon: string; // FontAwesome class, the single canonical icon for this type
  description: string;
  sidebarKey: string; // i18n key for the sidebar child label
  countUnit: string; // singular noun for the "already set up" count badge
}

export const connectionTypes: ConnectionType[] = [
  {
    key: "database",
    label: "Database",
    icon: "fa-solid fa-database",
    description: "Connect to PostgreSQL, MySQL, SQL Server, and other SQL databases",
    sidebarKey: "menu.connectionsDatabase",
    countUnit: "connection",
  },
  {
    key: "cloud",
    label: "Cloud Storage",
    icon: "fa-solid fa-cloud",
    description: "Set up connections to S3, Google Cloud Storage, or Azure Blob Storage",
    sidebarKey: "menu.connectionsCloud",
    countUnit: "connection",
  },
  {
    key: "kafka",
    label: "Kafka",
    icon: "fa-solid fa-tower-broadcast",
    description: "Configure Kafka brokers for streaming data ingestion and publishing",
    sidebarKey: "menu.connectionsKafka",
    countUnit: "connection",
  },
  {
    key: "google_analytics",
    label: "Google Analytics",
    icon: "fa-solid fa-chart-line",
    description: "Read GA4 reports via a service-account key or OAuth sign-in",
    sidebarKey: "menu.connectionsGoogleAnalytics",
    countUnit: "connection",
  },
  {
    key: "secrets",
    label: "Secrets",
    icon: "fa-solid fa-key",
    description: "Store API keys and passwords securely with encrypted storage",
    sidebarKey: "menu.connectionsSecrets",
    countUnit: "secret",
  },
  {
    key: "ai",
    label: "AI Providers",
    icon: "fa-solid fa-wand-magic-sparkles",
    description: "Add API keys for Anthropic, OpenAI, Google, and other LLM providers",
    sidebarKey: "menu.connectionsAi",
    countUnit: "provider",
  },
];
