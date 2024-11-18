# Quickstart Guide

This guide will help you get FlowFile up and running quickly.

## Prerequisites

- Python 3.10+
- Node.js 16+
- Poetry (Python package manager)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/edwardvaneechoud/flowfile
cd flowfile
```

2. Install Python dependencies using Poetry:
```bash
poetry install
```

3. Install frontend dependencies:
```bash
cd flowfile_frontend
npm install
```

## Running the Application

You'll need to start three components in separate terminals:

1. Start the Worker service:
```bash
# In first terminal
poetry run flowfile_worker  # Starts on port 63579
```

2. Start the Core service:
```bash
# In second terminal
poetry run flowfile_core   # Starts on port 63578
```

3. Start the frontend application:
```bash
# In third terminal
cd flowfile_frontend
npm run dev               # Starts Electron app (frontend on port 3000)
```

## Verifying Installation

Once all services are running, you should be able to:

1. Access the worker service at `http://localhost:63579`
2. Access the core service at `http://localhost:63578`
3. See the Electron application window open automatically

The application is now ready to use! You can start creating your first data flow by dragging nodes onto the canvas. 
For more information check out: [Building Flows](./flows/building.md)