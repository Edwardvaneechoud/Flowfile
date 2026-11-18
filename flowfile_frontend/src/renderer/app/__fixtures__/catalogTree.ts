import type { CatalogTable, FlowRegistration, NamespaceTree } from "../types/catalog.types";

/** A namespace as the tree lists it: this machine's, and empty unless `fields` says otherwise. */
export const ns = (fields: Partial<NamespaceTree> = {}): NamespaceTree =>
  ({
    id: 1,
    name: "Sales",
    level: 0,
    parent_id: null,
    source: "local",
    children: [],
    tables: [],
    flows: [],
    artifacts: [],
    visualizations: [],
    notebooks: [],
    ...fields,
  }) as NamespaceTree;

/** A flow as the tree lists it: this machine's, never run, unless `fields` says otherwise. */
export const flow = (fields: Partial<FlowRegistration> = {}): FlowRegistration =>
  ({
    id: 4,
    name: "daily-orders",
    description: null,
    flow_path: "/flows/daily-orders.yaml",
    namespace_id: 2,
    owner_id: 1,
    created_at: "2026-11-01T12:00:00Z",
    updated_at: "2026-11-01T12:00:00Z",
    is_favorite: false,
    is_following: false,
    run_count: 0,
    last_run_at: null,
    last_run_success: null,
    file_exists: true,
    is_api_compatible: false,
    artifact_count: 0,
    tables_produced: [],
    tables_read: [],
    source: "local",
    ...fields,
  }) as FlowRegistration;

/** A table as the tree lists it: this machine's, unless `fields` says otherwise. */
export const table = (fields: Partial<CatalogTable> = {}): CatalogTable =>
  ({
    id: 3,
    name: "orders",
    namespace_id: 2,
    full_table_name: null,
    table_type: "physical",
    file_exists: true,
    source: "local",
    ...fields,
  }) as CatalogTable;
