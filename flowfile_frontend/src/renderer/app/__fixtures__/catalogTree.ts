import type { CatalogTable, NamespaceTree } from "../types/catalog.types";

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
