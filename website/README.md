# Flowfile Marketing Website

This is the marketing website for Flowfile, built with [Astro](https://astro.build/).

## Quick Start

```bash
# Install dependencies
npm install

# Start development server
npm run dev

# Build for production
npm run build

# Preview production build
npm run preview
```

## Project Structure

```
website/
├── public/
│   ├── images/         # Static images (logo, etc.)
│   └── robots.txt      # SEO robots file
├── src/
│   ├── components/     # Astro components
│   │   ├── Header.astro
│   │   ├── Hero.astro
│   │   ├── Features.astro
│   │   ├── InteractiveDemo.astro
│   │   ├── Install.astro
│   │   ├── Comparison.astro
│   │   ├── CTA.astro
│   │   └── Footer.astro
│   ├── layouts/
│   │   └── Layout.astro    # Base HTML layout
│   ├── pages/
│   │   └── index.astro     # Homepage
│   └── styles/
│       └── global.css      # Global CSS variables and styles
├── astro.config.mjs        # Astro configuration
├── package.json
└── tsconfig.json
```

## Features

- **Dark theme** with custom design system
- **Interactive demo** with AG Grid showing real data transformations
- **Responsive design** for mobile and desktop
- **SEO optimized** with sitemap, structured data, and meta tags
- **Fast loading** with Astro's static site generation

## Deployment

The site is configured for static hosting. After building:

```bash
npm run build
```

The `dist/` folder can be deployed to any static hosting service:
- Vercel
- Netlify
- GitHub Pages
- Cloudflare Pages
- Any S3/CDN setup

## Configuration

Update the site URL in `astro.config.mjs`:

```javascript
export default defineConfig({
  site: 'https://flowfile.dev',
  // ...
});
```
