import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { resolve } from 'path'

const isLibBuild = process.env.BUILD_MODE === 'lib'

export default defineConfig({
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
  build: isLibBuild ? {
    target: 'esnext',
    lib: {
      entry: resolve(__dirname, 'src/lib/index.ts'),
      name: 'FlowfileEditor',
      formats: ['es'],
      fileName: 'flowfile-editor'
    },
    rollupOptions: {
      external: ['vue', 'pinia'],
      output: {
        globals: {
          vue: 'Vue',
          pinia: 'Pinia'
        },
        assetFileNames: 'style.[ext]'
      }
    },
    cssCodeSplit: false,
    assetsInlineLimit: 100000  // Inline icon PNGs as base64 data URIs
  } : {
    target: 'esnext'
  },
  optimizeDeps: {
    exclude: ['pyodide']
  }
})
