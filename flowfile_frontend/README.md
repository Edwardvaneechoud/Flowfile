# FlowFile Designer

FlowFile Designer is a modern, electron-based desktop application for visual data transformation and ETL workflows. Built with Vue 3, TypeScript, and Element Plus, it provides an intuitive interface for building data pipelines using a drag-and-drop approach.

![FlowFile Designer Screenshot](screenshot.png)

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
- Electron
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
# Build for all platforms
npm run build

# Build for specific platforms
npm run build:win    # Windows
npm run build:mac    # macOS
npm run build:linux  # Linux

# Build web version
npm run build:web
```

## 🧪 Preview Web Version
```bash
npm run preview:web
```

## 📦 Project Structure

```
src/
├── main/          # Electron main process
├── renderer/      # Vue application files
│   ├── components/
│   ├── views/
│   ├── stores/    # Pinia stores
│   └── styles/
└── assets/        # Static assets
```

## 🤝 Contributing

Contributions are welcome! Please read our contributing guidelines before submitting pull requests.

## 📝 License

[MIT License](LICENSE)
