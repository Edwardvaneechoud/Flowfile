/**
 * Composable for managing Node Designer state
 */
import { ref, reactive, computed } from 'vue';
import type { DesignerSection, DesignerComponent, NodeMetadata } from '../types';
import { defaultNodeMetadata, defaultProcessCode } from '../constants';

export function useNodeDesignerState() {
  // Node metadata
  const nodeMetadata = reactive<NodeMetadata>({ ...defaultNodeMetadata });

  // Sections state
  const sections = ref<DesignerSection[]>([]);
  const selectedSectionIndex = ref<number | null>(null);
  const selectedComponentIndex = ref<number | null>(null);

  // Python code for process method
  const processCode = ref(defaultProcessCode);

  // Computed: currently selected component
  const selectedComponent = computed(() => {
    if (selectedSectionIndex.value !== null && selectedComponentIndex.value !== null) {
      return sections.value[selectedSectionIndex.value]?.components[selectedComponentIndex.value] || null;
    }
    return null;
  });

  // Computed: can save (basic validation)
  const canSave = computed(() => {
    return nodeMetadata.node_name.trim() !== '' && nodeMetadata.node_category.trim() !== '';
  });

  // Section management
  function addSection() {
    const sectionNumber = sections.value.length + 1;
    sections.value.push({
      name: `section_${sectionNumber}`,
      title: `Section ${sectionNumber}`,
      components: [],
    });
    selectedSectionIndex.value = sections.value.length - 1;
    selectedComponentIndex.value = null;
  }

  function removeSection(index: number) {
    sections.value.splice(index, 1);
    if (selectedSectionIndex.value === index) {
      selectedSectionIndex.value = null;
      selectedComponentIndex.value = null;
    }
  }

  function selectSection(index: number) {
    selectedSectionIndex.value = index;
    selectedComponentIndex.value = null;
  }

  function sanitizeSectionName(index: number) {
    let name = sections.value[index].name;
    name = name.replace(/[\s-]+/g, '_');
    name = name.replace(/[^a-zA-Z0-9_]/g, '');
    if (/^[0-9]/.test(name)) {
      name = '_' + name;
    }
    name = name.toLowerCase();
    sections.value[index].name = name;
  }

  // Component management
  function selectComponent(sectionIndex: number, compIndex: number) {
    selectedSectionIndex.value = sectionIndex;
    selectedComponentIndex.value = compIndex;
  }

  function removeComponent(sectionIndex: number, compIndex: number) {
    sections.value[sectionIndex].components.splice(compIndex, 1);
    if (selectedSectionIndex.value === sectionIndex && selectedComponentIndex.value === compIndex) {
      selectedComponentIndex.value = null;
    }
  }

  function addComponentToSection(sectionIndex: number, component: DesignerComponent) {
    sections.value[sectionIndex].components.push(component);
    selectedSectionIndex.value = sectionIndex;
    selectedComponentIndex.value = sections.value[sectionIndex].components.length - 1;
  }

  // Reset state
  function resetState() {
    Object.assign(nodeMetadata, defaultNodeMetadata);
    sections.value = [];
    processCode.value = defaultProcessCode;
    selectedSectionIndex.value = null;
    selectedComponentIndex.value = null;
  }

  // Get state for serialization
  function getState() {
    return {
      nodeMetadata: { ...nodeMetadata },
      sections: sections.value,
      processCode: processCode.value,
    };
  }

  // Set state from serialized data
  function setState(state: {
    nodeMetadata?: Partial<NodeMetadata>;
    sections?: DesignerSection[];
    processCode?: string;
  }) {
    if (state.nodeMetadata) {
      Object.assign(nodeMetadata, state.nodeMetadata);
    }
    if (state.sections) {
      sections.value = state.sections;
    }
    if (state.processCode) {
      processCode.value = state.processCode;
    }
  }

  return {
    // State
    nodeMetadata,
    sections,
    selectedSectionIndex,
    selectedComponentIndex,
    processCode,

    // Computed
    selectedComponent,
    canSave,

    // Section methods
    addSection,
    removeSection,
    selectSection,
    sanitizeSectionName,

    // Component methods
    selectComponent,
    removeComponent,
    addComponentToSection,

    // State management
    resetState,
    getState,
    setState,
  };
}
