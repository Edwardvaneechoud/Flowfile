/**
 * Built-in flow templates for the browser editor.
 *
 * Each template is a self-contained worked example: a flow definition (YAML, in
 * the same FlowfileData format the app saves/loads) plus the sample CSV files
 * its read nodes reference. They live under `public/templates/` and are fetched
 * on demand. The set is inspired by the full desktop app's template gallery,
 * restricted to node types the WASM engine supports (no ML / fuzzy match).
 *
 * Asset paths are relative to the app base; the loader prefixes BASE_URL.
 * A read node's `file_name` must match a CSV in the template's `dataDir`.
 */

export type TemplateCategory = 'Beginner' | 'Intermediate'

export interface FlowTemplate {
  id: string
  name: string
  description: string
  category: TemplateCategory
  /** Material icon name shown on the gallery card. */
  icon: string
  /** Node types this template showcases (for the card's chip list). */
  highlights: string[]
  nodeCount: number
  /** Path (relative to BASE_URL) of the flow YAML. */
  flowPath: string
  /** Directory (relative to BASE_URL, trailing slash) holding the read CSVs. */
  dataDir: string
}

export const FLOW_TEMPLATES: FlowTemplate[] = [
  {
    id: 'regional-sales',
    name: 'Regional Sales Summary',
    description:
      'Join orders with regions, keep only completed sales, then total and rank performance by region.',
    category: 'Beginner',
    icon: 'insights',
    highlights: ['Join', 'Filter', 'Group By', 'Sort'],
    nodeCount: 8,
    // Reuses the interactive-demo assets so there is a single source of truth.
    flowPath: 'demo/sample-flow.yaml',
    dataDir: 'demo/'
  },
  {
    id: 'customer-dedup',
    name: 'Customer Deduplication',
    description:
      'Remove duplicate customer records by email address, keeping the first occurrence, sorted by name.',
    category: 'Beginner',
    icon: 'group',
    highlights: ['Unique', 'Sort'],
    nodeCount: 4,
    flowPath: 'templates/flows/customer-dedup.yaml',
    dataDir: 'templates/data/'
  },
  {
    id: 'employee-cleanup',
    name: 'Employee Directory Cleanup',
    description:
      'Keep active employees, drop sensitive columns, tidy the column order and sort by department.',
    category: 'Beginner',
    icon: 'badge',
    highlights: ['Filter', 'Select', 'Sort'],
    nodeCount: 5,
    flowPath: 'templates/flows/employee-cleanup.yaml',
    dataDir: 'templates/data/'
  },
  {
    id: 'web-traffic',
    name: 'Web Traffic by Page',
    description:
      'Aggregate page-view events into views and unique visitors per page, ranked by popularity.',
    category: 'Intermediate',
    icon: 'trending_up',
    highlights: ['Group By', 'Sort'],
    nodeCount: 4,
    flowPath: 'templates/flows/web-traffic.yaml',
    dataDir: 'templates/data/'
  },
  {
    id: 'survey-pivot',
    name: 'Survey Results Pivot',
    description:
      'Reshape long survey responses into a wide table of average rating per question, one row per respondent.',
    category: 'Intermediate',
    icon: 'pivot_table_chart',
    highlights: ['Pivot'],
    nodeCount: 3,
    flowPath: 'templates/flows/survey-pivot.yaml',
    dataDir: 'templates/data/'
  },
  {
    id: 'product-sales',
    name: 'Product Sales by Category',
    description:
      'Enrich orders with their product details, then total order count and quantity sold per category.',
    category: 'Intermediate',
    icon: 'category',
    highlights: ['Join', 'Group By', 'Sort'],
    nodeCount: 6,
    flowPath: 'templates/flows/product-sales.yaml',
    dataDir: 'templates/data/'
  }
]
