import { defineConfig } from 'astro/config';
import sitemap from '@astrojs/sitemap';

export default defineConfig({
  site: 'https://flowfile.dev',
  integrations: [sitemap()],
  build: {
    inlineStylesheets: 'auto'
  }
});
