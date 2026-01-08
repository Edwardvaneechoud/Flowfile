# Docker Reference

Configuration reference for running Flowfile with Docker Compose.

## Quick Start

```bash
git clone https://github.com/edwardvaneechoud/Flowfile.git
cd Flowfile
openssl rand -base64 32 > master_key.txt
docker compose up -d
```

Access at `http://localhost:8080`. Default login: `admin` / `changeme`.

For a complete deployment tutorial, see [Host Flowfile in Your Private Cloud](private-cloud-hosting.md).

## Architecture

| Service | Port | Purpose |
|---------|------|---------|
| `flowfile-frontend` | 8080 | Web UI |
| `flowfile-core` | 63578 | API, authentication |
| `flowfile-worker` | 63579 | Data processing |

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FLOWFILE_MODE` | Set to `docker` for multi-user auth | `docker` |
| `FLOWFILE_ADMIN_USER` | Initial admin username | `admin` |
| `FLOWFILE_ADMIN_PASSWORD` | Initial admin password | `changeme` |
| `JWT_SECRET_KEY` | Token signing secret (min 32 chars) | - |
| `WORKER_HOST` | Worker hostname | `flowfile-worker` |
| `CORE_HOST` | Core hostname | `flowfile-core` |

## Volumes

| Host Path | Container Path | Purpose |
|-----------|----------------|---------|
| `./flowfile_data` | `/app/user_data` | User data |
| `./saved_flows` | `/app/flowfile_core/saved_flows` | Flow definitions |
| `flowfile-internal-storage` | `/app/internal_storage` | Processing cache |

## Resource Limits

```yaml
services:
  flowfile-core:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G

  flowfile-worker:
    deploy:
      resources:
        limits:
          cpus: '4'
          memory: 8G
```

## Health Checks

| Service | Endpoint |
|---------|----------|
| Core | `http://localhost:63578/health` |
| Worker | `http://localhost:63579/health` |
| Frontend | `http://localhost:8080` |

```yaml
services:
  flowfile-core:
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:63578/health"]
      interval: 30s
      timeout: 10s
      retries: 3
```

## Commands

```bash
# Start
docker compose up -d

# Stop
docker compose down

# View logs
docker compose logs -f

# Rebuild
docker compose build --no-cache

# Upgrade
git pull && docker compose down && docker compose build && docker compose up -d
```

## Backup

```bash
cp -r saved_flows backup/
cp -r flowfile_data backup/
cp master_key.txt backup/
```
