// Renders the kernel's cleaned doc text (lsp/analysis.py::_clean_doc) as markup.
const SECTION_TITLES = new Set([
  "Args",
  "Arguments",
  "Attributes",
  "Examples",
  "Notes",
  "Other Parameters",
  "Parameters",
  "Raises",
  "References",
  "Returns",
  "See Also",
  "Warnings",
  "Warns",
  "Yields",
]);

export function appendInline(target: HTMLElement, text: string): void {
  for (const [i, part] of text.split("`").entries()) {
    if (!part) continue;
    if (i % 2 === 1) {
      const code = document.createElement("code");
      code.textContent = part;
      target.appendChild(code);
    } else {
      target.appendChild(document.createTextNode(part));
    }
  }
}

export function renderDocText(text: string): DocumentFragment {
  const frag = document.createDocumentFragment();
  for (const line of text.split("\n")) {
    const row = document.createElement("div");
    const title = line.trim().replace(/:$/, ""); // numpydoc "Parameters" / Google "Args:"
    if (SECTION_TITLES.has(title)) {
      row.className = "cm-lsp-doc-section";
      row.textContent = title;
    } else if (!title) {
      row.className = "cm-lsp-doc-gap";
    } else {
      appendInline(row, line);
    }
    frag.appendChild(row);
  }
  return frag;
}

const PARAM_SECTIONS = new Set([
  "Args",
  "Arguments",
  "Keyword Args",
  "Keyword Arguments",
  "Other Parameters",
  "Parameters",
]);

function indentOf(line: string): number {
  return line.length - line.trimStart().length;
}

function isSectionTitle(line: string): boolean {
  return SECTION_TITLES.has(line.trim().replace(/:$/, ""));
}

/** Bare parameter name from a Jedi param string (`*args`, `name: 'str'=None` → `args`, `name`). */
export function paramName(param: string): string {
  return /^\*{0,2}([A-Za-z_]\w*)/.exec(param.trim())?.[1] ?? "";
}

/** The docstring's opening paragraph, joined onto one line. */
export function docSummary(doc: string): string {
  const out: string[] = [];
  for (const line of doc.split("\n")) {
    if (!line.trim()) {
      if (out.length) break;
      continue;
    }
    if (isSectionTitle(line)) break;
    out.push(line.trim());
  }
  return out.join(" ");
}

/**
 * One parameter's description from a Google (`name (type): text`) or numpydoc
 * (`name : type` + indented text) parameter section, joined onto one line; "" when absent.
 */
export function paramDoc(doc: string, name: string): string {
  if (!name) return "";
  const lines = doc.split("\n");
  const google = new RegExp(`^\\*{0,2}${name}(?:\\s*\\([^)]*\\))?:(?!\\S)\\s*(.*)$`);
  const numpy = new RegExp(`^\\*{0,2}${name}(?:\\s+:.*)?$`);
  let sectionIndent = -1;
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const trimmed = line.trim();
    if (isSectionTitle(line)) {
      sectionIndent = PARAM_SECTIONS.has(trimmed.replace(/:$/, "")) ? indentOf(line) : -1;
      continue;
    }
    if (sectionIndent < 0 || !trimmed) continue;
    const g = google.exec(trimmed);
    const n = g ? null : numpy.exec(trimmed);
    if (!g && !n) continue;
    const entryIndent = indentOf(line);
    const parts = g?.[1] ? [g[1].trim()] : [];
    for (let j = i + 1; j < lines.length; j++) {
      const next = lines[j];
      if (!next.trim() || indentOf(next) <= entryIndent) break;
      parts.push(next.trim());
    }
    return parts.join(" ");
  }
  return "";
}
