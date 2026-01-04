/**
 * Composable for code generation utilities
 */
import { ref } from 'vue';
import type { NodeMetadata, DesignerSection } from '../types';

// Helper functions (exported for use in other composables)
export function toSnakeCase(str: string): string {
  return str
    .replace(/\s+/g, '_')
    .replace(/([a-z])([A-Z])/g, '$1_$2')
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '');
}

export function toPascalCase(str: string): string {
  return str
    .split(/[\s_-]+/)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join('');
}

export function useCodeGeneration() {
  const showPreviewModal = ref(false);
  const generatedCode = ref('');

  function generateCode(
    nodeMetadata: NodeMetadata,
    sections: DesignerSection[],
    processCode: string
  ): string {
    const nodeName = toPascalCase(nodeMetadata.node_name || 'MyCustomNode');
    const nodeSettingsName = `${nodeName}Settings`;

    // Build imports
    const imports = new Set<string>();
    imports.add('CustomNodeBase');
    imports.add('Section');
    imports.add('NodeSettings');

    sections.forEach((section) => {
      section.components.forEach((comp) => {
        imports.add(comp.component_type);
        if (comp.options_source === 'incoming_columns') {
          imports.add('IncomingColumns');
        }
      });
    });

    // Generate sections code
    let sectionsCode = '';
    sections.forEach((section) => {
      const sectionName = section.name || toSnakeCase(section.title || 'section');
      const sectionTitle = section.title || section.name || 'Section';
      sectionsCode += `\n# ${sectionTitle}\n`;
      sectionsCode += `${sectionName} = Section(\n`;
      sectionsCode += `    title="${sectionTitle}",\n`;

      section.components.forEach((comp) => {
        const fieldName = toSnakeCase(comp.field_name);
        sectionsCode += `    ${fieldName}=${comp.component_type}(\n`;
        sectionsCode += `        label="${comp.label || fieldName}",\n`;

        if (comp.component_type === 'TextInput') {
          if (comp.default) sectionsCode += `        default="${comp.default}",\n`;
          if (comp.placeholder) sectionsCode += `        placeholder="${comp.placeholder}",\n`;
        } else if (comp.component_type === 'NumericInput') {
          if (comp.default !== undefined) sectionsCode += `        default=${comp.default},\n`;
          if (comp.min_value !== undefined) sectionsCode += `        min_value=${comp.min_value},\n`;
          if (comp.max_value !== undefined) sectionsCode += `        max_value=${comp.max_value},\n`;
        } else if (comp.component_type === 'ToggleSwitch') {
          sectionsCode += `        default=${comp.default ? 'True' : 'False'},\n`;
          if (comp.description) sectionsCode += `        description="${comp.description}",\n`;
        } else if (comp.component_type === 'SingleSelect' || comp.component_type === 'MultiSelect') {
          if (comp.options_source === 'incoming_columns') {
            sectionsCode += `        options=IncomingColumns,\n`;
          } else if (comp.options_string) {
            const options = comp.options_string
              .split(',')
              .map((o) => `"${o.trim()}"`)
              .join(', ');
            sectionsCode += `        options=[${options}],\n`;
          }
        } else if (comp.component_type === 'ColumnSelector') {
          if (comp.required) sectionsCode += `        required=True,\n`;
          if (comp.multiple) sectionsCode += `        multiple=True,\n`;
          if (comp.data_types && comp.data_types !== 'ALL') {
            sectionsCode += `        data_types="${comp.data_types}",\n`;
          }
        } else if (comp.component_type === 'SliderInput') {
          sectionsCode += `        min_value=${comp.min_value ?? 0},\n`;
          sectionsCode += `        max_value=${comp.max_value ?? 100},\n`;
          if (comp.step) sectionsCode += `        step=${comp.step},\n`;
        } else if (comp.component_type === 'SecretSelector') {
          if (comp.required) sectionsCode += `        required=True,\n`;
          if (comp.description) sectionsCode += `        description="${comp.description}",\n`;
          if (comp.name_prefix) sectionsCode += `        name_prefix="${comp.name_prefix}",\n`;
        }

        sectionsCode += `    ),\n`;
      });

      sectionsCode += `)\n`;
    });

    // Generate settings class
    let settingsCode = `\nclass ${nodeSettingsName}(NodeSettings):\n`;
    sections.forEach((section) => {
      const sectionName = section.name || toSnakeCase(section.title || 'section');
      settingsCode += `    ${sectionName}: Section = ${sectionName}\n`;
    });
    if (sections.length === 0) {
      settingsCode += `    pass\n`;
    }

    // Extract process method body
    let processBody = processCode;
    const defMatch = processBody.match(/def\s+process\s*\([^)]*\)\s*->\s*[^:]+:\n?/);
    if (defMatch) {
      processBody = processBody.substring(defMatch[0].length);
    }

    // Dedent and re-indent
    const lines = processBody.split('\n');
    const nonEmptyLines = lines.filter((line) => line.trim().length > 0);
    let minIndent = 0;
    if (nonEmptyLines.length > 0) {
      minIndent = Math.min(
        ...nonEmptyLines.map((line) => {
          const match = line.match(/^(\s*)/);
          return match ? match[1].length : 0;
        })
      );
    }

    const reindentedLines = lines.map((line) => {
      if (line.trim().length === 0) {
        return '';
      }
      const dedented = line.length >= minIndent ? line.substring(minIndent) : line.trimStart();
      return '        ' + dedented;
    });
    processBody = reindentedLines.join('\n');

    // Generate node class
    const nodeCode = `

class ${nodeName}(CustomNodeBase):
    node_name: str = "${nodeMetadata.node_name}"
    node_category: str = "${nodeMetadata.node_category}"
    node_icon: str = "${nodeMetadata.node_icon || 'user-defined-icon.png'}"
    title: str = "${nodeMetadata.title || nodeMetadata.node_name}"
    intro: str = "${nodeMetadata.intro || 'A custom node for data processing'}"
    number_of_inputs: int = ${nodeMetadata.number_of_inputs}
    number_of_outputs: int = ${nodeMetadata.number_of_outputs}
    settings_schema: ${nodeSettingsName} = ${nodeSettingsName}()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
${processBody}
`;

    // Add SecretStr import if SecretStr is used in the process code
    const secretStrImport =
      processCode.includes('SecretStr') ? 'from pydantic import SecretStr\n' : '';

    // Combine all parts
    const fullCode = `# Auto-generated custom node
# Generated by Node Designer

import polars as pl
${secretStrImport}from flowfile_core.flowfile.node_designer import (
    ${Array.from(imports).join(', ')}
)
${sectionsCode}${settingsCode}${nodeCode}`;

    return fullCode;
  }

  function previewCode(
    nodeMetadata: NodeMetadata,
    sections: DesignerSection[],
    processCode: string
  ) {
    generatedCode.value = generateCode(nodeMetadata, sections, processCode);
    showPreviewModal.value = true;
  }

  function closePreview() {
    showPreviewModal.value = false;
  }

  function copyCode() {
    navigator.clipboard.writeText(generatedCode.value);
    alert('Code copied to clipboard!');
  }

  return {
    showPreviewModal,
    generatedCode,
    generateCode,
    previewCode,
    closePreview,
    copyCode,
    toSnakeCase,
    toPascalCase,
  };
}
