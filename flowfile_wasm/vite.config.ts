import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { resolve } from 'path'
import { readFileSync } from 'fs'

/**
 * Read the flowfile version from pyproject.toml
 * This ensures WASM and core use the same version
 */
function getFlowfileVersion(): string {
  try {
    const pyprojectPath = resolve(__dirname, '../pyproject.toml')
    const pyproject = readFileSync(pyprojectPath, 'utf-8')
    const match = pyproject.match(/^version\s*=\s*"([^"]+)"/m)
    return match ? match[1] : '0.0.0'
  } catch (err) {
    console.warn('Could not read version from pyproject.toml:', err)
    return '0.0.0'
  }
}

export default defineConfig({
  plugins: [vue()],
  define: {
    __FLOWFILE_VERSION__: JSON.stringify(getFlowfileVersion())
  },
  resolve: {
    alias: {
      '@': resolve(__dirname, 'src')
    }
  },
  server: {
    port: 5174,
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp'
    }
  },
  build: {
    target: 'esnext'
  },
  optimizeDeps: {
    exclude: ['pyodide']
  }
})
