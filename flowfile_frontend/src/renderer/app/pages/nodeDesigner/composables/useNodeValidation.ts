/**
 * Composable for node validation
 */
import { ref } from "vue";
import type { ValidationError, NodeMetadata, DesignerSection } from "../types";
import { toSnakeCase } from "./useCodeGeneration";

export function useNodeValidation() {
  const validationErrors = ref<ValidationError[]>([]);
  const showValidationModal = ref(false);

  function validateSettings(
    nodeMetadata: NodeMetadata,
    sections: DesignerSection[],
    processCode: string,
  ): ValidationError[] {
    const errors: ValidationError[] = [];

    // Validate node metadata
    if (!nodeMetadata.node_name.trim()) {
      errors.push({ field: "node_name", message: "Node name is required" });
    } else if (!/^[a-zA-Z][a-zA-Z0-9_\s]*$/.test(nodeMetadata.node_name)) {
      errors.push({
        field: "node_name",
        message:
          "Node name must start with a letter and contain only letters, numbers, spaces, and underscores",
      });
    }

    if (!nodeMetadata.node_category.trim()) {
      errors.push({ field: "node_category", message: "Category is required" });
    }

    // Check for duplicate section names
    const sectionNames = new Set<string>();
    sections.forEach((section, index) => {
      const name = section.name || toSnakeCase(section.title || "section");
      if (sectionNames.has(name)) {
        errors.push({ field: `section_${index}`, message: `Duplicate section name: "${name}"` });
      }
      sectionNames.add(name);

      // Check for duplicate field names within section
      const fieldNames = new Set<string>();
      section.components.forEach((comp, compIndex) => {
        const fieldName = toSnakeCase(comp.field_name);
        if (!fieldName) {
          errors.push({
            field: `section_${index}_comp_${compIndex}`,
            message: `Component in "${section.title}" is missing a field name`,
          });
        } else if (fieldNames.has(fieldName)) {
          errors.push({
            field: `section_${index}_comp_${compIndex}`,
            message: `Duplicate field name "${fieldName}" in section "${section.title}"`,
          });
        }
        fieldNames.add(fieldName);
      });
    });

    // Validate Python code syntax (basic check)
    if (nodeMetadata.use_kernel) {
      if (!processCode.includes("flowfile.publish_output")) {
        errors.push({
          field: "process_code",
          message: "Kernel code must call flowfile.publish_output() to produce output",
        });
      }
    } else {
      if (!processCode.includes("def process")) {
        errors.push({ field: "process_code", message: "Process method definition is missing" });
      }
      if (!processCode.includes("return")) {
        errors.push({ field: "process_code", message: "Process method must return a value" });
      }
    }

    return errors;
  }

  function showErrors(errors: ValidationError[]) {
    validationErrors.value = errors;
    showValidationModal.value = true;
  }

  function closeValidationModal() {
    showValidationModal.value = false;
  }

  return {
    validationErrors,
    showValidationModal,
    validateSettings,
    showErrors,
    closeValidationModal,
  };
}
