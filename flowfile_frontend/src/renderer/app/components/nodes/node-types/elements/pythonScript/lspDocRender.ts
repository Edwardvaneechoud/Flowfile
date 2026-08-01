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

function appendInline(target: HTMLElement, text: string): void {
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
