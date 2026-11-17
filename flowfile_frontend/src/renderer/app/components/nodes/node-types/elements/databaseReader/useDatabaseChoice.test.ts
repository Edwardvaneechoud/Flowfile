import { beforeEach, describe, expect, it, vi } from "vitest";
import { nextTick, ref } from "vue";

import type { DatabaseSettings } from "../../../baseNode/nodeInput";
import type { FullDatabaseConnectionInterface } from "../../../../../views/DatabaseView/databaseConnectionTypes";

const requests: string[] = [];
vi.mock("./api", () => ({
  fetchDbDatabases: vi.fn(async () => (requests.push("databases"), ["ANALYTICS"])),
  fetchDbSchemas: vi.fn(async () => (requests.push("schemas"), ["PUBLIC"])),
  fetchDbTables: vi.fn(async () => (requests.push("tables"), ["ORDERS"])),
}));
vi.mock("../../../../../composables/useDbDialects", () => ({
  useDbDialects: () => ({
    browsesDatabases: (type?: string) => type === "snowflake",
    quotedIdentifiers: (type?: string) => type === "snowflake",
  }),
}));
vi.mock("element-plus", () => ({ ElMessage: { error: vi.fn() } }));

import { useDatabaseChoice } from "./useDatabaseChoice";

interface Node {
  description: string;
  database_settings: DatabaseSettings;
}

const savedNode = (connection = "sf"): Node => ({
  description: "",
  database_settings: {
    connection_mode: "reference",
    database_connection_name: connection,
    database_name: "ANALYTICS",
    schema_name: "PUBLIC",
    table_name: "ORDERS",
    query_mode: "table",
    query: "",
  },
});

const savedConnections = [
  { connectionName: "sf", databaseType: "snowflake", database: "" },
  { connectionName: "sf2", databaseType: "snowflake", database: "" },
] as unknown as FullDatabaseConnectionInterface[];

const settle = async () => {
  await nextTick();
  await new Promise((resolve) => setTimeout(resolve, 0));
};

function drawer() {
  const node = ref<Node | null>(null);
  const connections = ref<FullDatabaseConnectionInterface[]>([]);
  const choice = useDatabaseChoice({ settings: () => node.value?.database_settings, connections });
  return { node, connections, choice };
}

async function openedDrawer() {
  const opened = drawer();
  opened.connections.value = savedConnections;
  opened.node.value = savedNode();
  await settle();
  return opened;
}

beforeEach(() => {
  requests.length = 0;
});

describe("useDatabaseChoice loading", () => {
  it("asks for no list when a node loads, whichever of node and connections arrives first", async () => {
    const nodeFirst = drawer();
    nodeFirst.node.value = savedNode();
    await settle();
    nodeFirst.connections.value = savedConnections;
    await settle();

    const connectionsFirst = drawer();
    connectionsFirst.connections.value = savedConnections;
    await settle();
    connectionsFirst.node.value = savedNode();
    await settle();

    expect(requests).toEqual([]);
  });

  it("asks for nothing when a General-tab edit replaces the node object", async () => {
    const { node } = await openedDrawer();
    node.value = { ...node.value!, description: "monthly orders" };
    await settle();
    expect(requests).toEqual([]);
  });

  it("loads a list once when its dropdown opens", async () => {
    const { choice } = await openedDrawer();
    choice.ensureDatabases();
    choice.ensureSchemas();
    choice.ensureTables();
    await settle();
    expect(requests).toEqual(["databases", "schemas", "tables"]);
    expect(choice.tables.options).toEqual(["ORDERS"]);

    choice.ensureDatabases();
    choice.ensureSchemas();
    choice.ensureTables();
    choice.ensureTables(false);
    await settle();
    expect(requests).toHaveLength(3);
  });

  it("asks nothing for a dropdown opened before the connection is known, and loads on the next open", async () => {
    const { node, connections, choice } = drawer();
    node.value = savedNode();
    await settle();
    choice.ensureSchemas();
    choice.ensureTables();
    await settle();
    expect(requests).toEqual([]);

    connections.value = savedConnections;
    await settle();
    choice.ensureTables();
    await settle();
    expect(requests).toEqual(["tables"]);
  });

  it("reloads every list and clears the database when the user picks another connection", async () => {
    const { node, choice } = await openedDrawer();
    node.value!.database_settings.database_connection_name = "sf2";
    await settle();
    expect(requests).toEqual(["databases", "schemas", "tables"]);
    expect(node.value!.database_settings.database_name).toBeNull();
    expect(choice.schemas.options).toEqual(["PUBLIC"]);
  });

  it("empties the lists when another node loads, and loads them again on demand", async () => {
    const { node, choice } = await openedDrawer();
    choice.ensureSchemas();
    await settle();
    expect(choice.schemas.options).toEqual(["PUBLIC"]);

    node.value = savedNode("sf2");
    await settle();
    expect(choice.schemas.options).toEqual([]);
    choice.ensureSchemas();
    await settle();
    expect(requests).toEqual(["schemas", "schemas"]);
  });

  it("asks again on refresh even when the list holds an answer", async () => {
    const { choice } = await openedDrawer();
    choice.ensureSchemas();
    await settle();
    await choice.loadSchemas();
    expect(requests).toEqual(["schemas", "schemas"]);
  });
});
