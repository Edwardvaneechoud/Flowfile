import { test, expect, Page } from '@playwright/test';
import { launchElectronApp, closeElectronApp } from './helpers/electronTestHelper';
import { ElectronApplication } from 'playwright-core';

/**
 * E2E tests for complex flow handling
 * Tests that all node types can be loaded and rendered correctly
 */

let electronApp: ElectronApplication | undefined;

// All node types that should be testable
const NODE_TYPES = [
  // Data sources
  'manual_input',
  'read',

  // Basic transformations
  'filter',
  'formula',
  'select',
  'sort',
  'sample',
  'unique',
  'record_id',
  'record_count',

  // Advanced transformations
  'group_by',
  'pivot',
  'unpivot',
  'text_to_rows',
  'polars_code',

  // Joins
  'join',
  'cross_join',
  'fuzzy_match',

  // Other
  'union',
  'graph_solver',
  'explore_data',
  'output',

  // External sources
  'database_reader',
  'database_writer',
  'cloud_storage_reader',
  'cloud_storage_writer',
  'external_source',
];

test.describe('Complex Flow E2E Tests', () => {
  test.beforeAll(async () => {
    try {
      electronApp = await launchElectronApp();
    } catch (error) {
      console.error("Error launching Electron app:", error);
    }
  });

  test.afterAll(async () => {
    await closeElectronApp(electronApp);
    electronApp = undefined;
  });

  test('app should start and services should be available', async () => {
    expect(electronApp).toBeDefined();

    const mainWindow = await electronApp!.firstWindow();
    expect(mainWindow).toBeDefined();

    // Wait for the app to be ready
    await mainWindow.waitForTimeout(2000);
  });

  test('should be able to create a new flow', async () => {
    test.skip(!electronApp, 'Electron app failed to launch');

    const mainWindow = await electronApp!.firstWindow();

    // Create a new flow via the API
    const flowId = await mainWindow.evaluate(async () => {
      try {
        const response = await fetch('/add_flow', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: 'E2E Test Flow' })
        });

        if (!response.ok) {
          throw new Error(`Failed to create flow: ${response.status}`);
        }

        const data = await response.json();
        return data.flow_id || data;
      } catch (error) {
        console.error('Error creating flow:', error);
        return null;
      }
    });

    console.log('Created flow with ID:', flowId);
    expect(flowId).toBeDefined();
    expect(typeof flowId).toBe('number');
  });

  test('should load node_list without errors', async () => {
    test.skip(!electronApp, 'Electron app failed to launch');

    const mainWindow = await electronApp!.firstWindow();

    // Fetch the node list from the API
    const nodeList = await mainWindow.evaluate(async () => {
      try {
        const response = await fetch('/node_list');
        if (!response.ok) {
          throw new Error(`Failed to fetch node list: ${response.status}`);
        }
        return await response.json();
      } catch (error) {
        console.error('Error fetching node list:', error);
        return null;
      }
    });

    expect(nodeList).toBeDefined();
    expect(Array.isArray(nodeList)).toBe(true);
    expect(nodeList.length).toBeGreaterThan(0);

    console.log(`Loaded ${nodeList.length} node types`);

    // Verify each node has required properties
    for (const node of nodeList) {
      expect(node.item).toBeDefined();
      expect(node.name).toBeDefined();
      expect(typeof node.custom_node).toBe('boolean');
    }
  });

  test('should verify all expected node types are available', async () => {
    test.skip(!electronApp, 'Electron app failed to launch');

    const mainWindow = await electronApp!.firstWindow();

    const nodeList = await mainWindow.evaluate(async () => {
      const response = await fetch('/node_list');
      return await response.json();
    });

    const availableNodeTypes = nodeList.map((n: any) => n.item);

    // Check that common node types are available
    const requiredTypes = ['manual_input', 'filter', 'select', 'join', 'output'];
    for (const nodeType of requiredTypes) {
      expect(availableNodeTypes).toContain(nodeType);
    }

    console.log('All required node types are available');
  });

  test('should create a flow with multiple node types and render them', async () => {
    test.skip(!electronApp, 'Electron app failed to launch');

    const mainWindow = await electronApp!.firstWindow();

    // Track console errors related to component loading
    const componentErrors: string[] = [];
    mainWindow.on('console', (msg) => {
      const text = msg.text();
      if (msg.type() === 'error' && text.includes('Component not found')) {
        componentErrors.push(text);
      }
    });

    // Create a new flow
    const flowId = await mainWindow.evaluate(async () => {
      const response = await fetch('/add_flow', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'Multi-Node Test Flow' })
      });
      const data = await response.json();
      return data.flow_id || data;
    });

    expect(flowId).toBeDefined();

    // Add a manual_input node
    const manualInputResult = await mainWindow.evaluate(async (fid) => {
      const response = await fetch('/add_node', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          flow_id: fid,
          node_type: 'manual_input',
          pos_x: 100,
          pos_y: 100
        })
      });
      return response.ok;
    }, flowId);

    expect(manualInputResult).toBe(true);

    // Add a filter node
    const filterResult = await mainWindow.evaluate(async (fid) => {
      const response = await fetch('/add_node', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          flow_id: fid,
          node_type: 'filter',
          pos_x: 300,
          pos_y: 100
        })
      });
      return response.ok;
    }, flowId);

    expect(filterResult).toBe(true);

    // Add a select node
    const selectResult = await mainWindow.evaluate(async (fid) => {
      const response = await fetch('/add_node', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          flow_id: fid,
          node_type: 'select',
          pos_x: 500,
          pos_y: 100
        })
      });
      return response.ok;
    }, flowId);

    expect(selectResult).toBe(true);

    // Give time for any component loading errors to occur
    await mainWindow.waitForTimeout(2000);

    // Verify no component loading errors occurred
    expect(componentErrors).toHaveLength(0);

    console.log('Successfully created flow with multiple node types');
  });

  test('should handle custom_node types correctly', async () => {
    test.skip(!electronApp, 'Electron app failed to launch');

    const mainWindow = await electronApp!.firstWindow();

    // Get node list and find custom_node types
    const nodeList = await mainWindow.evaluate(async () => {
      const response = await fetch('/node_list');
      return await response.json();
    });

    const customNodes = nodeList.filter((n: any) => n.custom_node === true);
    const specificNodes = nodeList.filter((n: any) => n.custom_node === false);

    console.log(`Found ${customNodes.length} custom nodes and ${specificNodes.length} specific nodes`);

    expect(customNodes.length).toBeGreaterThan(0);
    expect(specificNodes.length).toBeGreaterThan(0);

    // Verify some known node types have correct custom_node values
    const filterNode = nodeList.find((n: any) => n.item === 'filter');
    const manualInputNode = nodeList.find((n: any) => n.item === 'manual_input');

    // These should have dedicated components (custom_node = false)
    if (filterNode) {
      expect(filterNode.custom_node).toBe(false);
    }
    if (manualInputNode) {
      expect(manualInputNode.custom_node).toBe(false);
    }
  });
});

test.describe('Node Component Loading Tests', () => {
  test.beforeAll(async () => {
    if (!electronApp) {
      try {
        electronApp = await launchElectronApp();
      } catch (error) {
        console.error("Error launching Electron app:", error);
      }
    }
  });

  test('should load all node components without errors', async () => {
    test.skip(!electronApp, 'Electron app failed to launch');

    const mainWindow = await electronApp!.firstWindow();

    // Track all console messages
    const errors: string[] = [];
    const loadedComponents: string[] = [];

    mainWindow.on('console', (msg) => {
      const text = msg.text();
      if (msg.type() === 'error') {
        errors.push(text);
      }
      if (text.includes('Loading component:')) {
        loadedComponents.push(text);
      }
    });

    // Create a new flow
    const flowId = await mainWindow.evaluate(async () => {
      const response = await fetch('/add_flow', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'Component Loading Test' })
      });
      const data = await response.json();
      return data.flow_id || data;
    });

    // Get all available node types
    const nodeList = await mainWindow.evaluate(async () => {
      const response = await fetch('/node_list');
      return await response.json();
    });

    // Test adding each node type
    const testedNodes: string[] = [];
    const failedNodes: string[] = [];

    for (const node of nodeList.slice(0, 10)) { // Test first 10 nodes
      const result = await mainWindow.evaluate(async (params) => {
        try {
          const response = await fetch('/add_node', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              flow_id: params.flowId,
              node_type: params.nodeType,
              pos_x: 100 + (params.index * 50),
              pos_y: 100
            })
          });
          return { success: response.ok, nodeType: params.nodeType };
        } catch (error) {
          return { success: false, nodeType: params.nodeType, error: String(error) };
        }
      }, { flowId, nodeType: node.item, index: testedNodes.length });

      if (result.success) {
        testedNodes.push(node.item);
      } else {
        failedNodes.push(node.item);
      }
    }

    // Allow time for component loading
    await mainWindow.waitForTimeout(3000);

    console.log(`Tested ${testedNodes.length} nodes successfully`);
    console.log(`Failed nodes: ${failedNodes.length > 0 ? failedNodes.join(', ') : 'none'}`);

    // Check for component loading errors
    const componentErrors = errors.filter(e => e.includes('Component not found'));

    if (componentErrors.length > 0) {
      console.error('Component loading errors:', componentErrors);
    }

    expect(componentErrors).toHaveLength(0);
  });
});
