# Docker Reference

Run Flowfile with Docker using pre-built images from Docker Hub.

## Quick Start

```bash
git clone https://github.com/edwardvaneechoud/Flowfile.git
cd Flowfile
docker compose up -d
```

Access at `http://localhost:8080`. The setup wizard will guide you through master key configuration.

![Setup Wizard](../../assets/images/guides/docker-deployment/setup_wizard.png)

## Docker Images

| Image | Description |
|-------|-------------|
| `edwardvaneechoud/flowfile-frontend` | Web UI |
| `edwardvaneechoud/flowfile-core` | API server |
| `edwardvaneechoud/flowfile-worker` | Data processing |
| `edwardvaneechoud/flowfile-kernel-base` | Python-script kernel (base) |
| `edwardvaneechoud/flowfile-kernel-ml` | Python-script kernel with sklearn / xgboost / lightgbm / statsmodels |

Application images (`flowfile-frontend`, `flowfile-core`, `flowfile-worker`) share
the project version (`latest`, `0.9.3`, `sha-...`). The kernel images carry their
own version (currently `0.3.0`) so the kernel runtime can evolve independently of
the rest of the application.

## docker-compose.yml

```yaml
services:
  flowfile-frontend:
    image: edwardvaneechoud/flowfile-frontend:latest
    ports:
      - "8080:8080"
    networks:
      - flowfile-network
    depends_on:
      - flowfile-core
      - flowfile-worker

  flowfile-core:
    image: edwardvaneechoud/flowfile-core:latest
    ports:
      - "63578:63578"
    environment:
      - FLOWFILE_MODE=docker
      - FLOWFILE_ADMIN_USER=${FLOWFILE_ADMIN_USER:-admin}
      - FLOWFILE_ADMIN_PASSWORD=${FLOWFILE_ADMIN_PASSWORD:-changeme}
      - JWT_SECRET_KEY=${JWT_SECRET_KEY}
      - FLOWFILE_MASTER_KEY=${FLOWFILE_MASTER_KEY:-}
      - FLOWFILE_SCHEDULER_ENABLED=true
      - WORKER_HOST=flowfile-worker
    volumes:
      - ./flowfile_data:/app/user_data
      - ./saved_flows:/app/flowfile_core/saved_flows
      - flowfile-storage:/app/internal_storage
    networks:
      - flowfile-network

  flowfile-worker:
    image: edwardvaneechoud/flowfile-worker:latest
    ports:
      - "63579:63579"
    environment:
      - FLOWFILE_MODE=docker
      - CORE_HOST=flowfile-core
      - FLOWFILE_MASTER_KEY=${FLOWFILE_MASTER_KEY:-}
    volumes:
      - ./flowfile_data:/app/user_data
      - flowfile-storage:/app/internal_storage
    networks:
      - flowfile-network

networks:
  flowfile-network:

volumes:
  flowfile-storage:
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FLOWFILE_MODE` | Set to `docker` for multi-user auth | `docker` |
| `FLOWFILE_ADMIN_USER` | Admin username | `admin` |
| `FLOWFILE_ADMIN_PASSWORD` | Admin password | `changeme` |
| `JWT_SECRET_KEY` | Token signing secret | Required |
| `FLOWFILE_MASTER_KEY` | Encryption key for secrets | Via setup wizard |
| `FLOWFILE_SCHEDULER_ENABLED` | Auto-start the flow scheduler | `false` |
| `FLOWFILE_ENABLE_PROJECTS` | Enable git project tracking (admin-only in docker; the `/project` router 404s when off). Accepts `true`/`1`/`yes`/`on`. | `true` in the bundled compose files |
| `WORKER_HOST` | Worker hostname | `flowfile-worker` |
| `CORE_HOST` | Core hostname | `flowfile-core` |
| `FLOWFILE_KERNEL_IMAGE` | Kernel image to launch for Python-script nodes | `edwardvaneechoud/flowfile-kernel-base:0.3.0` |

### Git project tracking

When `FLOWFILE_ENABLE_PROJECTS` is on, Flowfile can mirror your flows, connections and schedules
into a versioned, secret-free git folder. In docker it is **administrator-only**, and each user's
projects are confined to their own `/app/user_data/projects/<owner_id>` area, so tenants stay
isolated. Turn it off with `FLOWFILE_ENABLE_PROJECTS=false`.

## .env Example

```bash
FLOWFILE_ADMIN_USER=admin
FLOWFILE_ADMIN_PASSWORD=YourSecurePassword123!
JWT_SECRET_KEY=generate-with-openssl-rand-hex-32
FLOWFILE_MASTER_KEY=generated-from-setup-wizard
```

## Volumes

| Path | Purpose |
|------|---------|
| `./flowfile_data` | User data |
| `./saved_flows` | Flow definitions |
| `flowfile-storage` | Internal storage |

## Commands

```bash
docker compose up -d      # Start
docker compose down       # Stop
docker compose pull       # Update images
docker compose logs -f    # View logs
```

## Health Checks

| Service | Endpoint |
|---------|----------|
| Core | `http://localhost:63578/health` |
| Worker | `http://localhost:63579/health` |
| Frontend | `http://localhost:8080` |

## Python Script (Kernel) Nodes

Python-script nodes run inside short-lived kernel containers spawned by `flowfile-core` via the host Docker socket. To enable them, mount the Docker socket into `flowfile-core` and pull the kernel image you want to use.

### 1. Pull the kernel image

```bash
docker pull edwardvaneechoud/flowfile-kernel-base:0.3.0
# Or, for ML workloads (sklearn, xgboost, lightgbm, statsmodels pre-baked):
docker pull edwardvaneechoud/flowfile-kernel-ml:0.3.0
```

### 2. Mount the Docker socket and set the image

In your `docker-compose.yml`, on the `flowfile-core` service:

```yaml
flowfile-core:
  # ... existing config ...
  environment:
    # ... existing env vars ...
    - FLOWFILE_KERNEL_IMAGE=${FLOWFILE_KERNEL_IMAGE:-edwardvaneechoud/flowfile-kernel-base:0.3.0}
  volumes:
    - /var/run/docker.sock:/var/run/docker.sock
    # ... existing volumes ...
```

Switch flavours by setting `FLOWFILE_KERNEL_IMAGE` (no rebuild required), e.g.:

```bash
export FLOWFILE_KERNEL_IMAGE=edwardvaneechoud/flowfile-kernel-ml:0.3.0
docker compose up -d
```

### Adding Extra Packages

When you create a kernel from the UI, packages listed in the **Extra Python packages** field are baked into a per-kernel Docker image at creation time (a `FROM <flavour> + RUN pip install` layer pinned against the flavour's constraints file). Creation takes ~30 s; subsequent kernel starts reuse the image and boot in seconds. The derived image is removed automatically when you delete the kernel.

If you instead run the kernel container directly (no `flowfile-core` orchestration), the legacy `KERNEL_PACKAGES` env var still works as a runtime install — it is overridden to empty when launched by core.

---

## File Manager

*Docker mode only.*

The File Manager provides a web-based interface for uploading and downloading data files when running Flowfile in Docker (where users cannot browse the host filesystem).

![File Manager](../../assets/images/guides/docker-deployment/file-manager.png)

*The File Manager showing uploaded files*

### Supported Formats

CSV, Parquet, Excel (`.xlsx`), JSON, TSV, TXT

### File Size Limit

Maximum **500 MB** per file.

### Usage

1. Click the **File Manager** icon in the left sidebar
2. Click **Upload** to add a file
3. Uploaded files appear in the file list and can be used in **Read Data** input nodes
4. Click the delete icon to remove a file

### Access

The File Manager is only available when `FLOWFILE_MODE=docker` is set.
