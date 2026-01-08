# Docker Deployment Guide

Deploy Flowfile using Docker Compose for team and production environments.

## Overview

Docker mode provides:

- **Multi-user authentication** with admin user management
- **Encrypted secrets storage** for credentials
- **Centralized storage** shared across users

### Architecture

| Service | Port | Purpose |
|---------|------|---------|
| **flowfile-frontend** | 8080 | Web UI |
| **flowfile-core** | 63578 | REST API, authentication |
| **flowfile-worker** | 63579 | Data processing |

![Architecture Diagram](../../../assets/images/guides/docker-deployment/architecture.png)

## Prerequisites

- Docker 20.10+
- Docker Compose 2.0+

```bash
docker --version
docker compose version
```

## Quick Start

### 1. Clone and Configure

```bash
git clone https://github.com/edwardvaneechoud/Flowfile.git
cd Flowfile
```

Generate the master key (encrypts all stored secrets):

```bash
openssl rand -base64 32 > master_key.txt
```

!!! danger "Backup Required"
    Store `master_key.txt` securely. Losing it means losing access to all encrypted secrets.

### 2. Set Environment Variables

Create a `.env` file:

```bash
FLOWFILE_ADMIN_USER=admin
FLOWFILE_ADMIN_PASSWORD=YourSecurePassword123!
JWT_SECRET_KEY=your-secure-jwt-secret-key-at-least-32-chars
```

### 3. Start Services

```bash
docker compose up -d
```

### 4. Access Flowfile

Open `http://localhost:8080` and log in with your admin credentials.

![Login Screen](../../../assets/images/guides/docker-deployment/login_screen.png)

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FLOWFILE_MODE` | Must be `docker` for multi-user auth | `docker` |
| `FLOWFILE_ADMIN_USER` | Initial admin username | `admin` |
| `FLOWFILE_ADMIN_PASSWORD` | Initial admin password | `changeme` |
| `JWT_SECRET_KEY` | Token signing secret (min 32 chars) | - |
| `WORKER_HOST` | Worker service hostname | `flowfile-worker` |
| `CORE_HOST` | Core service hostname | `flowfile-core` |

## Volume Configuration

| Volume | Container Path | Purpose |
|--------|----------------|---------|
| `./flowfile_data` | `/app/user_data` | User data, connections |
| `./saved_flows` | `/app/flowfile_core/saved_flows` | Flow definitions |
| `flowfile-internal-storage` | `/app/internal_storage` | Processing cache |

### Backup

```bash
cp -r saved_flows backup/saved_flows_$(date +%Y%m%d)
cp -r flowfile_data backup/flowfile_data_$(date +%Y%m%d)
cp master_key.txt backup/master_key_$(date +%Y%m%d).txt
```

## Production Configuration

### HTTPS with Reverse Proxy

```nginx
server {
    listen 443 ssl http2;
    server_name flowfile.example.com;

    ssl_certificate /etc/ssl/certs/flowfile.crt;
    ssl_certificate_key /etc/ssl/private/flowfile.key;

    location / {
        proxy_pass http://localhost:8080;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

![Production Architecture](../../../assets/images/guides/docker-deployment/production_architecture.png)

### Resource Limits

```yaml
services:
  flowfile-core:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G
        reservations:
          cpus: '0.5'
          memory: 512M

  flowfile-worker:
    deploy:
      resources:
        limits:
          cpus: '4'
          memory: 8G
        reservations:
          cpus: '1'
          memory: 1G
```

### Health Checks

| Service | Endpoint | Expected |
|---------|----------|----------|
| Core API | `http://localhost:63578/health` | `200 OK` |
| Worker | `http://localhost:63579/health` | `200 OK` |
| Frontend | `http://localhost:8080` | `200 OK` |

Add to `docker-compose.yml`:

```yaml
services:
  flowfile-core:
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:63578/health"]
      interval: 30s
      timeout: 10s
      retries: 3
```

## Upgrading

```bash
# Backup first
cp -r saved_flows backup/
cp -r flowfile_data backup/
cp master_key.txt backup/

# Upgrade
git pull origin main
docker compose down
docker compose build --no-cache
docker compose up -d

# Verify
docker compose ps
docker compose logs | head -50
```

## Related

- [User Management](../settings.md#user-management) - Managing users and authentication
- [Secrets](../settings.md#secrets) - Storing encrypted credentials
- [Database Connectivity](database-connectivity.md) - Connecting to databases
