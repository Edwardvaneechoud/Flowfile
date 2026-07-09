// componentDocs is the single source of truth for the component reference
// (help popover + modal) and the derived palette lists. These guard against a
// component being added to the designer without a matching doc entry.
import { describe, expect, it } from "vitest";
import { componentDocs, returnsChip } from "./componentDocs";
import { availableComponents } from "./constants";
import type { ComponentType } from "./designerState";

const ALL_TYPES: ComponentType[] = [
  "TextInput",
  "NumericInput",
  "SliderInput",
  "ToggleSwitch",
  "SingleSelect",
  "MultiSelect",
  "ColumnSelector",
  "SecretSelector",
  "ColumnActionInput",
];

describe("componentDocs", () => {
  it("documents every ComponentType exactly once", () => {
    const documented = componentDocs.map((c) => c.type).sort();
    expect(documented).toEqual([...ALL_TYPES].sort());
    expect(new Set(documented).size).toBe(ALL_TYPES.length);
  });

  it("every entry has the full standard block", () => {
    for (const c of componentDocs) {
      expect(c.label, `${c.type} label`).toBeTruthy();
      expect(c.icon, `${c.type} icon`).toMatch(/^fa-/);
      expect(c.whatItDoes.length, `${c.type} whatItDoes`).toBeGreaterThan(0);
      expect(c.returns.type, `${c.type} returns.type`).toBeTruthy();
      expect(c.configure.length, `${c.type} configure`).toBeGreaterThan(0);
      for (const p of c.configure) {
        expect(p.prop, `${c.type} config prop`).toBeTruthy();
        expect(p.desc, `${c.type} config desc`).toBeTruthy();
      }
      expect(c.howToUse, `${c.type} howToUse`).toContain("self.settings_schema");
      expect(c.whenToUse.length, `${c.type} whenToUse`).toBeGreaterThan(0);
    }
  });

  it("uses .secret_value only for SecretSelector", () => {
    for (const c of componentDocs) {
      const expected = c.type === "SecretSelector" ? ".secret_value" : ".value";
      expect(c.returns.accessor, `${c.type} accessor`).toBe(expected);
    }
  });

  it("only ColumnSelector and ColumnActionInput require an input", () => {
    const needInput = componentDocs
      .filter((c) => c.needsInput)
      .map((c) => c.type)
      .sort();
    expect(needInput).toEqual(["ColumnActionInput", "ColumnSelector"]);
  });

  it("returnsChip renders accessor and type", () => {
    const secret = componentDocs.find((c) => c.type === "SecretSelector")!;
    expect(returnsChip(secret.returns)).toBe(".secret_value → SecretStr | None");
  });

  it("derives availableComponents 1:1 (type/label/icon, order preserved)", () => {
    expect(availableComponents).toEqual(
      componentDocs.map((c) => ({ type: c.type, label: c.label, icon: c.icon })),
    );
  });
});
