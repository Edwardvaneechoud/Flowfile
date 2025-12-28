import { test, expect } from '@playwright/test';

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
