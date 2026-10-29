// Vitest mounts no .vue file here, so the drawers' wiring is read from their source.

import { readFileSync } from "fs";
import { parse } from "vue/compiler-sfc";
import { describe, expect, it } from "vitest";

type TemplateProp = {
  type: number;
  name: string;
  arg?: { content?: string };
  exp?: { content?: string };
  value?: { content: string };
};
type TemplateNode = {
  type: number;
  tag?: string;
  props?: TemplateProp[];
  children?: TemplateNode[];
};

const ELEMENT = 1;
const DIRECTIVE = 7;

function elements(node: TemplateNode): TemplateNode[] {
  const own = node.type === ELEMENT ? [node] : [];
  return own.concat(...(node.children ?? []).map(elements));
}

function hasClass(node: TemplateNode, name: string): boolean {
  return (node.props ?? []).some(
    (prop) =>
      prop.type !== DIRECTIVE &&
      prop.name === "class" &&
      (prop.value?.content ?? "").split(/\s+/).includes(name),
  );
}

function directive(node: TemplateNode, name: string, arg?: string): string | undefined {
  return node.props?.find(
    (prop) => prop.type === DIRECTIVE && prop.name === name && prop.arg?.content === arg,
  )?.exp?.content;
}

function readDrawer(path: string) {
  const { descriptor, errors } = parse(readFileSync(new URL(path, import.meta.url), "utf8"));
  expect(errors).toEqual([]);
  const ast = descriptor.template?.ast;
  if (!ast) throw new Error(`${path} has no template`);
  const sections = elements(ast as TemplateNode).filter(
    (node) => node.tag === "generic-node-settings",
  );
  return { sections, script: descriptor.scriptSetup?.content ?? "" };
}

describe.each([
  { drawer: "Catalog Reader", path: "./catalogReader/CatalogReader.vue", form: "mode-toggle" },
  { drawer: "Catalog Writer", path: "./catalogWriter/CatalogWriter.vue", form: "writer-tabs" },
  { drawer: "Write Data", path: "./output/Output.vue", form: "file-type-row" },
])("the $drawer drawer", ({ path, form }) => {
  it("wraps its form in the general settings section", () => {
    const { sections } = readDrawer(path);
    expect(sections).toHaveLength(1);
    expect(elements(sections[0]).some((node) => hasClass(node, form))).toBe(true);
  });

  it("hands the section's updates and saves to the settings it saves", () => {
    const { sections, script } = readDrawer(path);
    const call = script.match(/\{([^}]*)\}\s*=\s*useNodeSettings\(\{\s*nodeRef:\s*(\w+)/);
    expect(call).not.toBeNull();
    const names = (call?.[1] ?? "").split(",").map((name) => name.trim());
    expect(directive(sections[0], "model")).toBe(call?.[2]);
    expect(directive(sections[0], "on", "update:model-value")).toBe("handleGenericSettingsUpdate");
    expect(directive(sections[0], "on", "request-save")).toBe("saveSettings");
    expect(names).toContain("handleGenericSettingsUpdate");
    expect(names).toContain("saveSettings");
  });
});
