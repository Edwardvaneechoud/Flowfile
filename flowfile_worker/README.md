# Flowfile Worker

Flowfile Worker is a FastAPI-based service that handles the execution and caching of data operations for Flowfile. It runs as a separate process to manage heavy computational tasks and provides a REST API for executing Polars operations efficiently.

## 🚀 Features

- Asynchronous task execution
- Process-based task isolation
- Memory-efficient data caching
- Progress tracking for long-running operations
- Graceful shutdown handling
- Support for:
  - Polars operations
  - Fuzzy joining
  - Table creation
  - Custom operations
  - Background task management

## 🛠️ API Endpoints

### Core Operations
- `POST /submit_query/` - Submit a Polars operation for execution
- `POST /write_results/` - Write processed data to disk
- `POST /create_table/{file_type}` - Create a new table from input data
- `POST /add_fuzzy_join` - Perform fuzzy join operations between datasets

### Task Management
- `GET /status/{task_id}` - Get task execution status (the response carries the serialized result once the task completes)
- `GET /memory_usage/{task_id}` - Monitor memory usage
- `POST /cancel_task/{task_id}` - Cancel running task
- `POST /shutdown` - Gracefully shutdown the worker

## 🏃‍♂️ Running the Worker

```bash
# Install dependencies
pip install flowfile-worker

# Start the worker
python -m flowfile_worker

# With custom host and port
python -m flowfile_worker --host 0.0.0.0 --port 8000
```

## 💻 Usage Example

```python
import requests

# Submit a Polars operation
task = requests.post("http://localhost:63579/submit_query/", json={
    "polars_script": script,
    "operation_type": "store"
})

# Poll task status; the response carries the result once the task completes
status = requests.get(f"http://localhost:63579/status/{task.json()['background_task_id']}")
result = status.json()["results"]
```

## ⚙️ Configuration

The worker uses environment variables for configuration:
- `WORKER_HOST` - Host address (default: 0.0.0.0)
- `WORKER_PORT` - Port number (default: 63579)
- `CACHE_DIR` - Directory for temporary files (auto-generated if not specified)

## 🔒 Security

The worker is designed to run in a trusted environment. When exposing to external networks, ensure proper security measures are in place.

## 📝 License

[MIT License](LICENSE)