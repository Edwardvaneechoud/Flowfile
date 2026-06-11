# Deployment

Choose how to run Flowfile based on your needs.

| Method | Best For | Multi-user |
|--------|----------|------------|
| [Flowfile Lite (Browser)](lite.md) | Trying Flowfile with zero install, embedding | No |
| [Desktop App](desktop.md) | Getting started, local development | No |
| [Python Package](python.md) | Scripting, CI/CD, automation | No |
| [Docker](docker.md) | Teams, servers, production | Yes |

## Quick Comparison

### Flowfile Lite (Browser)
No install at all — the visual editor runs entirely in your browser with Polars compiled to WebAssembly. A lightweight subset (18 nodes, no backend, no databases/cloud/scheduler/AI). Try it at [demo.flowfile.org](https://demo.flowfile.org). See the [Flowfile Lite guide](lite.md) for what's included.

### Desktop App
Download and run. No setup required. Best for exploring Flowfile or building flows locally on macOS or Windows.

### Python Package
`pip install flowfile`. Run flows programmatically or integrate into existing Python projects.

### Docker
Full deployment with authentication, secrets management, and centralized storage. Best for teams and production environments.
