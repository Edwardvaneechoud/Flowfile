import { Position } from '@vue-flow/core';

interface NodeTemplateParams {
  id: string;
  label: string;
  numInputs: number;
  numOutputs: number;
  component: any;  // The component type
}

interface Handle {
  id: string;
  position: Position;
}

interface NodeData {
  id: string;
  label: string;
  component: any;
  inputs: Handle[];
  outputs: Handle[];
}

export function createNodeTemplate(params: NodeTemplateParams) {
  const { id, label, numInputs, numOutputs, component } = params;

  const inputs: Handle[] = Array.from({ length: numInputs }, (_, i) => ({
    id: `input-${i}`,
    position: Position.Left,
  }));

  const outputs: Handle[] = Array.from({ length: numOutputs }, (_, i) => ({
    id: `output-${i}`,
    position: Position.Right,
  }));

  return {
    id,
    type: 'custom-node',
    data: {
      id,
      label,
      component,
      inputs,
      outputs,
    },
    position: { x: Math.random() * 600, y: Math.random() * 400 },
  };
}
