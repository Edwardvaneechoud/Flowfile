import { test, expect } from '@playwright/test';
import * as path from 'path';
import * as fs from 'fs';

/**
 * Web-based E2E tests for flow handling
 * These tests run against the web dev server (no Electron build required)
 *
 * Prerequisites:
 * 1. Start backend: poetry run flowfile (from root)
 * 2. Start frontend: npm run dev:web (from flowfile_frontend)
 * 3. Run tests: npx playwright test tests/web-flow.spec.ts
 */

const BASE_URL = process.env.TEST_URL || 'http://localhost:5173';
const API_URL = process.env.API_URL || 'http://localhost:63578';

// Path to the complex flow fixture
const COMPLEX_FLOW_FIXTURE = path.resolve(__dirname, 'fixtures/complex-flow.yaml');

// All node types in the complex flow fixture
const COMPLEX_FLOW_NODE_TYPES = [
  'manual_input', 'filter', 'formula', 'select', 'sort', 'record_id',
  'sample', 'unique', 'group_by', 'pivot', 'unpivot', 'text_to_rows',
  'graph_solver', 'polars_code', 'join', 'cross_join', 'fuzzy_match',
  'record_count', 'explore_data', 'union', 'output'
];

test.describe('Web Flow E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the app
    await page.goto(BASE_URL);
  });

  test('should load the application', async ({ page }) => {
    // Wait for the page to load
    await page.waitForLoadState('networkidle');

    // Check that something rendered
    const body = await page.locator('body');
    await expect(body).toBeVisible();
  });

  test('should fetch node_list from API', async ({ page }) => {
    // Directly test the API
    const response = await page.request.get(`${API_URL}/node_list`);

    expect(response.ok()).toBe(true);

    const nodeList = await response.json();
    expect(Array.isArray(nodeList)).toBe(true);
    expect(nodeList.length).toBeGreaterThan(0);

    console.log(`Loaded ${nodeList.length} node types from API`);

    // Verify node structure
    for (const node of nodeList.slice(0, 5)) {
      expect(node).toHaveProperty('item');
      expect(node).toHaveProperty('name');
      expect(node).toHaveProperty('custom_node');
      console.log(`  - ${node.item}: custom_node=${node.custom_node}`);
    }
  });

  test('should have correct custom_node values for known nodes', async ({ page }) => {
    const response = await page.request.get(`${API_URL}/node_list`);
    const nodeList = await response.json();

    // These nodes should have dedicated components (custom_node = false)
    const dedicatedNodes = ['filter', 'manual_input', 'select', 'join', 'output', 'formula'];

    for (const nodeName of dedicatedNodes) {
      const node = nodeList.find((n: any) => n.item === nodeName);
      if (node) {
        expect(node.custom_node).toBe(false);
        console.log(`✓ ${nodeName} has custom_node=false (dedicated component)`);
      }
    }

    // Find nodes that use CustomNode
    const customNodes = nodeList.filter((n: any) => n.custom_node === true);
    console.log(`\nFound ${customNodes.length} nodes using CustomNode:`);
    for (const node of customNodes.slice(0, 5)) {
      console.log(`  - ${node.item}`);
    }
  });

  test('should create a new flow via API', async ({ page }) => {
    // Create a new flow
    const createResponse = await page.request.post(`${API_URL}/add_flow`, {
      data: { name: 'E2E Test Flow' }
    });

    expect(createResponse.ok()).toBe(true);

    const flowData = await createResponse.json();
    const flowId = flowData.flow_id || flowData;

    expect(flowId).toBeDefined();
    expect(typeof flowId).toBe('number');

    console.log(`Created flow with ID: ${flowId}`);
  });

  test('should add nodes to a flow via API', async ({ page }) => {
    // Create a new flow first
    const createResponse = await page.request.post(`${API_URL}/add_flow`, {
      data: { name: 'Node Test Flow' }
    });
    const flowData = await createResponse.json();
    const flowId = flowData.flow_id || flowData;

    // Add a manual_input node
    const addNodeResponse = await page.request.post(`${API_URL}/add_node`, {
      data: {
        flow_id: flowId,
        node_type: 'manual_input',
        pos_x: 100,
        pos_y: 100
      }
    });

    expect(addNodeResponse.ok()).toBe(true);
    console.log('✓ Added manual_input node');

    // Add a filter node
    const addFilterResponse = await page.request.post(`${API_URL}/add_node`, {
      data: {
        flow_id: flowId,
        node_type: 'filter',
        pos_x: 300,
        pos_y: 100
      }
    });

    expect(addFilterResponse.ok()).toBe(true);
    console.log('✓ Added filter node');

    // Add a custom_node type (if available)
    const nodeListResponse = await page.request.get(`${API_URL}/node_list`);
    const nodeList = await nodeListResponse.json();
    const customNode = nodeList.find((n: any) => n.custom_node === true);

    if (customNode) {
      const addCustomResponse = await page.request.post(`${API_URL}/add_node`, {
        data: {
          flow_id: flowId,
          node_type: customNode.item,
          pos_x: 500,
          pos_y: 100
        }
      });

      expect(addCustomResponse.ok()).toBe(true);
      console.log(`✓ Added custom node: ${customNode.item}`);
    }
  });

  test('should load flow in designer without component errors', async ({ page }) => {
    // Track console errors
    const errors: string[] = [];
    page.on('console', msg => {
      if (msg.type() === 'error') {
        errors.push(msg.text());
      }
    });

    // Create a flow with nodes
    const createResponse = await page.request.post(`${API_URL}/add_flow`, {
      data: { name: 'Designer Test Flow' }
    });
    const flowData = await createResponse.json();
    const flowId = flowData.flow_id || flowData;

    // Add some nodes
    await page.request.post(`${API_URL}/add_node`, {
      data: { flow_id: flowId, node_type: 'manual_input', pos_x: 100, pos_y: 100 }
    });
    await page.request.post(`${API_URL}/add_node`, {
      data: { flow_id: flowId, node_type: 'filter', pos_x: 300, pos_y: 100 }
    });
    await page.request.post(`${API_URL}/add_node`, {
      data: { flow_id: flowId, node_type: 'select', pos_x: 500, pos_y: 100 }
    });

    // Navigate to the designer
    await page.goto(`${BASE_URL}/#/designer/${flowId}`);

    // Wait for the page to load
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(3000); // Give time for components to load

    // Check for component loading errors
    const componentErrors = errors.filter(e =>
      e.includes('Component not found') ||
      e.includes('Failed to load component')
    );

    if (componentErrors.length > 0) {
      console.error('Component errors found:', componentErrors);
    }

    expect(componentErrors).toHaveLength(0);
    console.log('✓ No component loading errors');
  });
});

test.describe('API Health Checks', () => {
  test('backend API should be reachable', async ({ page }) => {
    try {
      const response = await page.request.get(`${API_URL}/health`, {
        timeout: 5000
      });
      expect(response.ok()).toBe(true);
      console.log('✓ Backend API is healthy');
    } catch (error) {
      console.error('Backend API not reachable. Make sure to start it with: poetry run flowfile');
      throw error;
    }
  });

  test('frontend dev server should be reachable', async ({ page }) => {
    try {
      const response = await page.goto(BASE_URL, { timeout: 5000 });
      expect(response?.ok()).toBe(true);
      console.log('✓ Frontend dev server is running');
    } catch (error) {
      console.error('Frontend not reachable. Make sure to start it with: npm run dev:web');
      throw error;
    }
  });
});

test.describe('Complex Flow E2E Tests', () => {
  test('should have complex flow fixture available', async () => {
    expect(fs.existsSync(COMPLEX_FLOW_FIXTURE)).toBe(true);
    console.log(`✓ Complex flow fixture found at: ${COMPLEX_FLOW_FIXTURE}`);
  });

  test('should import complex flow from YAML fixture', async ({ page }) => {
    // Import the complex flow using the API
    const importResponse = await page.request.get(
      `${API_URL}/import_flow/?flow_path=${encodeURIComponent(COMPLEX_FLOW_FIXTURE)}`
    );

    expect(importResponse.ok()).toBe(true);

    const flowId = await importResponse.json();
    expect(typeof flowId).toBe('number');
    console.log(`✓ Imported complex flow with ID: ${flowId}`);

    // Get flow data to verify nodes were imported
    const flowDataResponse = await page.request.get(`${API_URL}/flow_data?flow_id=${flowId}`);
    expect(flowDataResponse.ok()).toBe(true);

    const flowData = await flowDataResponse.json();
    expect(flowData.nodes).toBeDefined();
    expect(flowData.nodes.length).toBeGreaterThan(0);

    console.log(`✓ Flow contains ${flowData.nodes.length} nodes`);

    // Verify node types
    const nodeTypes = new Set(flowData.nodes.map((n: any) => n.node_type || n.type));
    console.log(`Node types in flow: ${Array.from(nodeTypes).join(', ')}`);
  });

  test('should load complex flow in designer without component errors', async ({ page }) => {
    // Track console errors
    const errors: string[] = [];
    const loadedComponents: string[] = [];

    page.on('console', msg => {
      const text = msg.text();
      if (msg.type() === 'error') {
        errors.push(text);
      }
      if (text.includes('Loading component:')) {
        loadedComponents.push(text);
      }
    });

    // Import the complex flow
    const importResponse = await page.request.get(
      `${API_URL}/import_flow/?flow_path=${encodeURIComponent(COMPLEX_FLOW_FIXTURE)}`
    );
    expect(importResponse.ok()).toBe(true);

    const flowId = await importResponse.json();
    console.log(`Imported flow ID: ${flowId}`);

    // Navigate to the designer
    await page.goto(`${BASE_URL}/#/designer/${flowId}`);

    // Wait for the page to load
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(5000); // Give extra time for all components to load

    // Log loaded components
    console.log(`\nLoaded ${loadedComponents.length} components:`);
    for (const comp of loadedComponents) {
      console.log(`  ${comp}`);
    }

    // Check for component loading errors
    const componentErrors = errors.filter(e =>
      e.includes('Component not found') ||
      e.includes('Failed to load component') ||
      e.includes('Invalid module name')
    );

    if (componentErrors.length > 0) {
      console.error('\nComponent errors found:');
      for (const err of componentErrors) {
        console.error(`  ${err}`);
      }
    }

    expect(componentErrors).toHaveLength(0);
    console.log('\n✓ All components loaded successfully without errors');
  });

  test('should verify all node types have correct custom_node mapping', async ({ page }) => {
    // Get node list from API
    const response = await page.request.get(`${API_URL}/node_list`);
    expect(response.ok()).toBe(true);

    const nodeList = await response.json();
    const nodeMap = new Map(nodeList.map((n: any) => [n.item, n]));

    console.log('\nNode type mappings for complex flow:');
    for (const nodeType of COMPLEX_FLOW_NODE_TYPES) {
      const node = nodeMap.get(nodeType);
      if (node) {
        const componentType = node.custom_node ? 'CustomNode' : 'Dedicated';
        console.log(`  ${nodeType}: ${componentType}`);
      } else {
        console.log(`  ${nodeType}: NOT FOUND in node_list`);
      }
    }

    // Verify some known node types
    const filterNode = nodeMap.get('filter');
    const manualInputNode = nodeMap.get('manual_input');
    const outputNode = nodeMap.get('output');

    expect(filterNode?.custom_node).toBe(false);
    expect(manualInputNode?.custom_node).toBe(false);
    expect(outputNode?.custom_node).toBe(false);

    console.log('\n✓ Core node types have correct custom_node mappings');
  });

  test('should render all nodes in complex flow without visual errors', async ({ page }) => {
    // Import the complex flow
    const importResponse = await page.request.get(
      `${API_URL}/import_flow/?flow_path=${encodeURIComponent(COMPLEX_FLOW_FIXTURE)}`
    );
    expect(importResponse.ok()).toBe(true);

    const flowId = await importResponse.json();

    // Navigate to the designer
    await page.goto(`${BASE_URL}/#/designer/${flowId}`);
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(3000);

    // Check that Vue Flow canvas is visible
    const canvas = page.locator('.vue-flow');
    await expect(canvas).toBeVisible({ timeout: 10000 });
    console.log('✓ Vue Flow canvas is visible');

    // Check that nodes are rendered
    const nodes = page.locator('.vue-flow__node');
    const nodeCount = await nodes.count();
    console.log(`✓ Rendered ${nodeCount} nodes in the designer`);

    expect(nodeCount).toBeGreaterThan(0);
  });
});
