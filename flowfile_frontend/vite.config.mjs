import Path from 'path';
import { fileURLToPath } from 'url';
import vuePlugin from '@vitejs/plugin-vue';
import { defineConfig } from 'vite';
// Note: no @vitejs/plugin-react. The project has no JSX/TSX files;
// React is only used as a dynamic `import("react")` inside two Vue wrappers
// (VueGraphicWalker, VueGraphicRenderer) for the @kanaries/graphic-walker
// integration, which works natively via Vite's ESM handling.

const __dirname = Path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
    root: Path.join(__dirname, 'src', 'renderer'),
    publicDir: 'public',
    // Pin the dep-optimization cache to a stable path under the repo so it
    // doesn't get confused by parallel Vite instances (`npm run dev:web` vs
    // `tauri dev` invoking `npm run dev:web` as `beforeDevCommand`).
    cacheDir: Path.join(__dirname, 'node_modules', '.vite'),
    server: {
        host: '0.0.0.0',
        port: 8080,
        // Don't silently jump to 8082 when 8080/8081 are busy — fail fast so
        // Tauri's hard-coded devUrl doesn't end up pointing at the wrong port.
        strictPort: true,
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
    optimizeDeps: {
        // Browser sees "504 (Outdated Optimize Dep)" when Vite's pre-bundled
        // deps go stale relative to a previously loaded page. The webview
        // caches the import URL with the old hash; rebuilding the cache from
        // scratch on each `vite` start avoids the mismatch entirely. Costs
        // ~5–15s of cold-start time; cheap relative to a manual `rm -rf`.
        force: true,
    },
    plugins: [
        vuePlugin()
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
