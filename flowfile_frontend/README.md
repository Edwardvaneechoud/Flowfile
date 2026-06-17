# Flowfile Designer

Flowfile Designer is a modern desktop application for visual data transformation and ETL workflows, built on Tauri 2 (Rust shell) with a Vue 3 + TypeScript + Element Plus renderer. It provides an intuitive interface for building data pipelines using a drag-and-drop approach.

## 🚀 Features

- Visual Flow Designer with drag-and-drop interface
- Node-based workflow creation
- Real-time data preview
- Support for multiple data sources
- Rich set of transformation nodes
- Python code integration with Polars
- Dark/Light theme support
- Cross-platform (Windows, macOS, Linux)

## 🛠️ Technology Stack

- Vue 3 + TypeScript
- Tauri 2 (Rust shell, native WebView)
- Vue Flow for flow-chart design
- Element Plus UI components
- CodeMirror for code editing
- AG Grid for data visualization
- Graphic Walker for data exploration

## 🏗️ Development

```bash
# Install dependencies
npm install

# Start development server
npm run dev

# Start web-only development
npm run dev:web

# Lint and fix files
npm run lint
```

## 🔨 Building

```bash
# Build for the host platform (requires staged sidecars; see repo root Makefile)
npm run build

# Build web-only (no Rust/Tauri needed)
npm run build:web

# Or, from the repo root for the full pipeline:
#   make all                # python + sidecars + tauri
#   make build_tauri_mac    # platform-specific
#   make build_tauri_win
#   make build_tauri_linux
```

## 🧪 Preview Web Version
```bash
npm run preview:web
```

## 🤝 Contributing

Contributions are welcome! Please read our contributing guidelines before submitting pull requests.

## 📝 License

[MIT License](LICENSE)
