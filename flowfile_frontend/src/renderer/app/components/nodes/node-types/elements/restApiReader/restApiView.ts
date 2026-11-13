import type { RestApiSettings } from "../../../../../types/node.types";

/** What core's fetch engine writes where a secret was sent (`shared/rest_api/fetch.py::SECRET_MASK`). */
export const SECRET_MASK = "REDACTED";

export interface TraceEntry {
  page: number;
  method: string;
  url: string;
  headers: Record<string, string>;
  body: unknown | null;
  status: number;
  elapsed_ms: number;
  bytes: number;
  records: number;
  next_cursor: string | null;
  stopped: string | null;
}

export interface ResponseNode {
  key: string;
  path: string;
  kind: "object" | "list" | "value";
  length?: number;
  records?: boolean;
  preview?: string;
  children?: ResponseNode[];
  more?: number;
}

export interface SampleDebug {
  suggested_paths: { record_paths: string[]; cursor_paths: string[] } | null;
  trace: TraceEntry[] | null;
  response_tree: ResponseNode | null;
  response_text: string | null;
  response_truncated: boolean;
}

export interface TreeRow {
  key: string;
  path: string;
  depth: number;
  kind: ResponseNode["kind"];
  detail: string;
  pick: "records" | "value" | null;
  expandable: boolean;
  open: boolean;
}

const plural = (n: number, word: string) => `${n} ${word}${n === 1 ? "" : "s"}`;

const retries = (n: number) => `${n} ${n === 1 ? "retry" : "retries"}`;

const describeNode = (node: ResponseNode): string => {
  if (node.kind === "value") return node.preview ?? "";
  if (node.kind === "object") return "";
  if (!node.length) return "empty list";
  return node.records ? plural(node.length, "record") : `list of ${node.length}`;
};

const isRecordList = (node: ResponseNode) => node.kind === "list" && Boolean(node.records);

/**
 * The nodes a tree shows open at first: the response and the objects directly in it, plus the way
 * to the chosen token. A list of records starts folded, since its fields are the columns.
 */
export const defaultOpenPaths = (root: ResponseNode | null, cursorPath = ""): Set<string> => {
  const open = new Set<string>();
  if (!root) return open;
  if (!isRecordList(root)) open.add(root.path);
  for (const child of root.children ?? []) {
    if (child.kind === "object") open.add(child.path);
  }
  const parts = cursorPath.split(".");
  if (!parts.some((part) => /^-?\d+$/.test(part))) {
    for (let i = 1; i < parts.length; i++) open.add(parts.slice(0, i).join("."));
  }
  return open;
};

/**
 * The response tree as indented rows, the children of a node listed only while it is in *open*.
 * A list of objects can be picked as the records, a plain value as the next-page token; the
 * fields under a record list are that list's columns.
 */
export const treeRows = (
  root: ResponseNode | null,
  open: ReadonlySet<string>,
  maxRows = 200,
): TreeRow[] => {
  if (!root) return [];
  const rows: TreeRow[] = [];
  const walk = (node: ResponseNode, depth: number, inRecords: boolean) => {
    if (rows.length >= maxRows) return;
    const isRecords = isRecordList(node);
    const expandable = Boolean(node.children?.length);
    const isOpen = expandable && open.has(node.path);
    rows.push({
      key: node.key || "Whole response",
      path: node.path,
      depth,
      kind: node.kind,
      detail: describeNode(node),
      pick: isRecords ? "records" : node.kind === "value" && !inRecords ? "value" : null,
      expandable,
      open: isOpen,
    });
    if (!isOpen) return;
    for (const child of node.children ?? []) walk(child, depth + 1, inRecords || isRecords);
  };
  walk(root, 0, false);
  return rows;
};

export interface ColumnRow {
  key: string;
  path: string;
  depth: number;
  dataType: string;
  count: number;
  group: boolean;
  open: boolean;
}

interface ColumnGroup {
  key: string;
  path: string;
  dataType: string | null;
  children: Map<string, ColumnGroup>;
}

const countColumns = (node: ColumnGroup): number =>
  (node.dataType === null ? 0 : 1) +
  [...node.children.values()].reduce((sum, child) => sum + countColumns(child), 0);

/**
 * Flattened columns (`post.author.handle`) as a tree folded on their dotted parts, in the order the
 * columns come; a group's children are listed only while its path is in *open*. A group with a
 * single child group is shown as one step (`associated.chat`).
 */
export const columnRows = (
  fields: ReadonlyArray<{ name: string; data_type: string }>,
  open: ReadonlySet<string>,
): ColumnRow[] => {
  const root: ColumnGroup = { key: "", path: "", dataType: null, children: new Map() };
  for (const field of fields) {
    let node = root;
    const parts = field.name.split(".");
    parts.forEach((part, i) => {
      let child = node.children.get(part);
      if (!child) {
        child = {
          key: part,
          path: parts.slice(0, i + 1).join("."),
          dataType: null,
          children: new Map(),
        };
        node.children.set(part, child);
      }
      node = child;
    });
    node.dataType = field.data_type;
  }
  const rows: ColumnRow[] = [];
  const walk = (node: ColumnGroup, depth: number) => {
    let key = node.key;
    while (node.dataType === null && node.children.size === 1) {
      const only = [...node.children.values()][0];
      if (only.children.size === 0) break;
      key = `${key}.${only.key}`;
      node = only;
    }
    if (node.children.size === 0) {
      rows.push({
        key,
        path: node.path,
        depth,
        dataType: node.dataType ?? "",
        count: 1,
        group: false,
        open: false,
      });
      return;
    }
    const isOpen = open.has(node.path);
    rows.push({
      key,
      path: node.path,
      depth,
      dataType: node.dataType ?? "",
      count: countColumns(node),
      group: true,
      open: isOpen,
    });
    if (isOpen) for (const child of node.children.values()) walk(child, depth + 1);
  };
  for (const child of root.children.values()) walk(child, 0);
  return rows;
};

/** The paths of every group in *fields*, for unfolding them all. */
export const columnGroupPaths = (fields: ReadonlyArray<{ name: string }>): Set<string> => {
  const paths = new Set<string>();
  for (const field of fields) {
    const parts = field.name.split(".");
    for (let i = 1; i < parts.length; i++) paths.add(parts.slice(0, i).join("."));
  }
  return paths;
};

/** A masked value named after the secret the node uses, so a reader sees which one was sent. */
export const showSecret = (text: string, secretName: string): string =>
  text.split(SECRET_MASK).join(`‹${secretName || "secret"}›`);

/** A failed test as the engine worded it: the request, the status and the API's own answer. */
export const testErrorText = (detail: string, secretName: string): string =>
  showSecret(
    detail.replace(/^Failed to fetch sample from REST API: /, "").replace(/^[A-Za-z]+Error: /, ""),
    secretName,
  );

const formatBytes = (bytes: number): string =>
  bytes < 1024 ? `${bytes} B` : `${(bytes / 1024).toFixed(bytes < 10240 ? 1 : 0)} KB`;

export const responseLine = (entry: TraceEntry): string =>
  `${entry.status} · ${entry.elapsed_ms} ms · ${formatBytes(entry.bytes)} · ${plural(entry.records, "record")}`;

const shellQuote = (text: string): string => `'${text.split("'").join("'\\''")}'`;

/** The request as a curl command; the secret is an environment variable, never a value. */
export const toCurl = (entry: TraceEntry, secretName: string): string => {
  const variable = `$${(secretName || "secret").replace(/[^A-Za-z0-9_]/g, "_").toUpperCase()}`;
  const withVariable = (text: string) => text.split(SECRET_MASK).join(variable);
  const quote = (text: string) =>
    text.includes(SECRET_MASK)
      ? `"${withVariable(text).split('"').join('\\"')}"`
      : shellQuote(text);
  const parts = [`curl -X ${entry.method} ${quote(entry.url)}`];
  for (const [name, value] of Object.entries(entry.headers)) {
    parts.push(`-H ${quote(`${name}: ${value}`)}`);
  }
  if (entry.body !== null && entry.body !== undefined) {
    parts.push(
      `-H 'Content-Type: application/json'`,
      `-d ${shellQuote(JSON.stringify(entry.body))}`,
    );
  }
  return parts.join(" \\\n  ");
};

const CURSOR_PARAM_DEFAULT = "cursor";

/** The request parameter a response's cursor field is usually sent back as: `next_cursor` → `cursor`. */
export const guessCursorParam = (path: string): string => {
  const key = path.split(".").pop() ?? "";
  const stripped = key.replace(/^next[_-]?/i, "");
  if (!stripped || /^-?\d+$/.test(stripped)) return CURSOR_PARAM_DEFAULT;
  return stripped === key ? key : stripped.charAt(0).toLowerCase() + stripped.slice(1);
};

export const TOKEN_PLACEHOLDER = "‹token›";

/** What each request after the first carries for a token sent as *name*; a dotted body field nests, as the engine does. */
export const nextRequestExample = (
  name: string,
  inBody: boolean,
): { code: string; place: string } | null => {
  const parts = name
    .trim()
    .split(".")
    .filter((part) => part);
  if (!parts.length) return null;
  if (!inBody) return { code: `${name.trim()}=${TOKEN_PLACEHOLDER}`, place: "the URL" };
  const leaf = parts.pop();
  const nested = parts.reduceRight(
    (inner, key) => `"${key}": {${inner}}`,
    `"${leaf}": ${TOKEN_PLACEHOLDER}`,
  );
  return { code: nested, place: "the JSON body" };
};

export interface FieldHelp {
  text: string;
  example?: { label: string; sample?: string; options?: boolean; rows: [string, string][] };
}

const SAMPLE_RESPONSE = `{
  "data": {
    "items": [{ "id": 1 }, { "id": 2 }]
  },
  "next": "abc"
}`;

/** The context a field's info icon shows on hover. */
export const FIELD_HELP = {
  secret: { text: "A stored secret. The flow saves its name, never its value." },
  recordPath: {
    text: "Dot-path to the list of records in the JSON response. A number picks a list item. Empty uses the whole response. After a test request, click a list in the response to fill it in.",
    example: {
      label: "Example response",
      sample: SAMPLE_RESPONSE,
      rows: [["data.items", '[{ "id": 1 }, { "id": 2 }]']],
    },
  },
  paginationType: {
    text: "How the API splits a result over several requests. None sends one request. Cursor sends the token of each response with the next request, until a page is empty, a response has no token or a token repeats. Page number and Offset and limit count up a query parameter.",
    example: {
      label: "Second request",
      options: true,
      rows: [
        ["Cursor", "?cursor=abc"],
        ["Page number", "?page=2"],
        ["Offset and limit", "?offset=100&limit=100"],
      ],
    },
  },
  tokenLocation: {
    text: "Where a response holds the token for the next page: a field of its JSON body or a response header.",
    example: {
      label: "Example",
      options: true,
      rows: [
        ["Response body", '{ "next": "abc" }'],
        ["Response header", "X-Next-Cursor: abc"],
      ],
    },
  },
  tokenPath: {
    text: "Dot-path to the token in the response body. A negative number counts from the end of a list. After a test request, click a value in the response to fill it in.",
    example: {
      label: "Example response",
      sample: SAMPLE_RESPONSE,
      rows: [
        ["next", '"abc"'],
        ["data.items.-1.id", "2"],
      ],
    },
  },
  tokenHeader: {
    text: "Name of the response header that holds the token.",
    example: {
      label: "Example response header",
      sample: "X-Next-Cursor: abc",
      rows: [["X-Next-Cursor", "abc"]],
    },
  },
  sendAs: {
    text: "Where the next request carries the token. A JSON body field needs the method POST.",
    example: {
      label: "Next request",
      options: true,
      rows: [
        ["Query parameter", "GET /items?cursor=abc"],
        ["JSON body field", '{ "cursor": "abc" }'],
      ],
    },
  },
  tokenParameter: {
    text: "Name of the query parameter the API expects the token in. The API's documentation gives it.",
    example: { label: "Next request", rows: [["cursor", "GET /items?cursor=abc"]] },
  },
  tokenField: {
    text: "Name of the body field the API expects the token in. The API's documentation gives it. A dotted name places it in a nested object.",
    example: {
      label: "Next request body",
      rows: [
        ["cursor", '{ "cursor": "abc" }'],
        ["page.after", '{ "page": { "after": "abc" } }'],
      ],
    },
  },
  maxRetries: {
    text: "How often a request is sent again after a host that does not answer, a 5xx status or a 429. Any other 4xx fails at once.",
  },
  initialToken: {
    text: "Token sent with the first request. Empty sends the first request without one.",
  },
} satisfies Record<string, FieldHelp>;

/** The cursor path Guided offers to follow: a field of the response itself, never one of a record. */
export const detectedCursorPath = (paths: string[]): string | null =>
  paths.find((path) => !/(^|\.)-?\d+\./.test(path)) ?? null;

export const limitsSummary = (settings: RestApiSettings): string => {
  const p = settings.pagination;
  const parts: string[] = [];
  if (p.pagination_type !== "none") {
    parts.push(`${p.max_pages} pages`);
    if (p.max_records) parts.push(`${p.max_records} records`);
    if (p.page_delay_seconds > 0) parts.push(`${p.page_delay_seconds} s between pages`);
  }
  parts.push(`${settings.timeout_seconds} s timeout`, retries(settings.max_retries));
  return parts.join(" · ");
};
