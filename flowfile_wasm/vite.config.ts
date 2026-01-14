import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { resolve } from 'path'

export default defineConfig(({ mode }) => {
  const isLibrary = mode === 'library'

  return {
    plugins: [vue()],
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
    build: isLibrary ? {
      // Library build configuration
      target: 'esnext',
      lib: {
        entry: resolve(__dirname, 'src/lib.ts'),
        name: 'FlowfileWasm',
        fileName: (format) => `flowfile-wasm.${format}.js`,
        formats: ['es', 'umd']
      },
      rollupOptions: {
        // Externalize Vue and Pinia - host app provides these
        external: ['vue', 'pinia', 'vue-router'],
        output: {
          globals: {
            vue: 'Vue',
            pinia: 'Pinia',
            'vue-router': 'VueRouter'
          },
          // Preserve CSS for separate import
          assetFileNames: (assetInfo) => {
            if (assetInfo.name === 'style.css') return 'flowfile-wasm.css'
            return assetInfo.name || 'assets/[name][extname]'
          }
        }
      },
      // Generate sourcemaps for debugging
      sourcemap: true,
      // Don't minify for better debugging in development
      minify: false
    } : {
      // Standard SPA build
      target: 'esnext'
    },
    optimizeDeps: {
      exclude: ['pyodide']
    }
  }
})
