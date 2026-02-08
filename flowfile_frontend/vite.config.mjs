import Path from 'path';
import { fileURLToPath } from 'url';
import vuePlugin from '@vitejs/plugin-vue';
import reactPlugin from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

const __dirname = Path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
    root: Path.join(__dirname, 'src', 'renderer'),
    publicDir: 'public',
    server: {
        host: '0.0.0.0',
        port: 8080,
        proxy: {
            '/api': {
                target: 'http://localhost:63578',
                changeOrigin: true,
                rewrite: (path) => path.replace(/^\/api/, ''),
            },
        },
    },
    open: false,
    build: {
        outDir: Path.join(__dirname, 'build', 'renderer'),
        emptyOutDir: true,
        minify: false,
    },
    plugins: [
        vuePlugin(),
        reactPlugin()
    ],
    resolve: {
        alias: {
            '@': Path.resolve(__dirname, './src/renderer/app'),
            '@/api': Path.resolve(__dirname, './src/renderer/app/api'),
            '@/types': Path.resolve(__dirname, './src/renderer/app/types'),
            '@/stores': Path.resolve(__dirname, './src/renderer/app/stores'),
            '@/composables': Path.resolve(__dirname, './src/renderer/app/composables'),
        },
    },
});
