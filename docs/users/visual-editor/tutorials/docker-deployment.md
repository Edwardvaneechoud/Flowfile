# Docker Deployment Guide

### ![Docker Deployment](../../../assets/images/guides/docker-deployment/main_image.png) Multi-user Flowfile deployment with Docker

This guide walks you through deploying Flowfile using Docker Compose for team and production environments. Docker mode provides multi-user authentication, centralized storage, and secure secrets management.

## Overview

Running Flowfile in Docker mode transforms it from a single-user desktop application into a multi-user server application with:

- **Multi-user authentication** - JWT-based login with admin user management
- **Centralized storage** - Shared flows and data across all users
- **Secrets management** - Encrypted credential storage with master key encryption
- **Scalable architecture** - Separate services for frontend, backend, and worker

### Architecture

Flowfile's Docker deployment consists of three services communicating over an internal network:

![Architecture Diagram](../../../assets/images/guides/docker-deployment/architecture.png)

| Service | Port | Purpose |
|---------|------|---------|
| **flowfile-frontend** | 8080 | Web UI served via Node.js |
| **flowfile-core** | 63578 | REST API, authentication, flow management |
| **flowfile-worker** | 63579 | Data processing and transformations |

## Prerequisites

Before starting, ensure you have the following installed:

- **Docker** version 20.10 or higher
- **Docker Compose** version 2.0 or higher
- **Git** for cloning the repository
- **OpenSSL** for generating the master key

Verify your installation:

```bash
# Check Docker version
docker --version
# Expected: Docker version 20.10.x or higher

# Check Docker Compose version
docker compose version
# Expected: Docker Compose version v2.x.x

# Verify Docker is running
docker info
```

!!! warning "Platform Support"
    The Docker images are built for `linux/amd64`. If you're on an ARM-based system (like Apple Silicon), Docker will use emulation which may impact performance.

## Quick Start

Get Flowfile running in Docker with these steps:

### Step 1: Clone the Repository

```bash
git clone https://github.com/edwardvaneechoud/Flowfile.git
cd Flowfile
```

### Step 2: Generate the Master Key

The master key encrypts all secrets stored in Flowfile. Generate a secure random key:

```bash
openssl rand -base64 32 > master_key.txt
```

!!! danger "Protect Your Master Key"
    - Never commit `master_key.txt` to version control
    - Back up this file securely - losing it means losing access to all encrypted secrets
    - Use different keys for development and production environments

### Step 3: Configure Environment Variables

Create a `.env` file in the project root to customize your deployment:

```bash
# .env file
FLOWFILE_ADMIN_USER=admin
FLOWFILE_ADMIN_PASSWORD=YourSecurePassword123!
JWT_SECRET_KEY=your-secure-jwt-secret-key-at-least-32-chars
```

!!! note "Password Requirements"
    The admin password must meet these requirements:

    - Minimum 8 characters
    - At least one uppercase letter
    - At least one lowercase letter
    - At least one number

### Step 4: Start the Services

Launch all services with Docker Compose:

```bash
docker compose up -d
```

This command:

1. Builds all three service images
2. Creates the internal network
3. Mounts volumes for persistent storage
4. Loads the master key as a Docker secret
5. Starts all services in the background

### Step 5: Verify Deployment

Check that all services are running:

```bash
docker compose ps
```

Expected output:

```
NAME                 STATUS    PORTS
flowfile-core        running   0.0.0.0:63578->63578/tcp
flowfile-frontend    running   0.0.0.0:8080->8080/tcp
flowfile-worker      running   0.0.0.0:63579->63579/tcp
```

### Step 6: Access Flowfile

Open your browser and navigate to:

```
http://localhost:8080
```

You'll see the login screen:

![Login Screen](../../../assets/images/guides/docker-deployment/login_screen.png)

Log in with your configured admin credentials (default: `admin` / `changeme`).

!!! warning "Change Default Credentials"
    On first login with default credentials, you'll be prompted to change your password. Always use strong, unique passwords in production.

## Environment Variables Reference

Configure Flowfile's behavior through these environment variables:

### Core Configuration

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `FLOWFILE_MODE` | Deployment mode (`electron`, `package`, `docker`) | `docker` | Yes |
| `FLOWFILE_ADMIN_USER` | Initial admin username | `admin` | Yes |
| `FLOWFILE_ADMIN_PASSWORD` | Initial admin password | `changeme` | Yes |
| `JWT_SECRET_KEY` | JWT token signing secret (min 32 chars) | `flowfile-dev-secret-change-in-production` | Yes |

### Service Discovery

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `WORKER_HOST` | Worker service hostname | `flowfile-worker` | Yes |
| `CORE_HOST` | Core service hostname (used by worker) | `flowfile-core` | Yes |

### Storage Paths

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `FLOWFILE_STORAGE_DIR` | Internal storage path | `/app/internal_storage` | No |
| `FLOWFILE_USER_DATA_DIR` | User data directory | `/app/user_data` | No |

### Example `.env` File

```bash
# Deployment mode
FLOWFILE_MODE=docker

# Admin credentials (CHANGE THESE!)
FLOWFILE_ADMIN_USER=admin
FLOWFILE_ADMIN_PASSWORD=MySecureP@ssw0rd!

# JWT secret (generate with: openssl rand -base64 32)
JWT_SECRET_KEY=your-very-long-and-secure-jwt-secret-key-here

# Service hostnames (usually don't need to change)
WORKER_HOST=flowfile-worker
CORE_HOST=flowfile-core
```

## Authentication & User Management

### Understanding FLOWFILE_MODE

The `FLOWFILE_MODE` environment variable controls authentication behavior:

| Mode | Authentication | Use Case |
|------|----------------|----------|
| `electron` | Auto-login as `local_user` | Desktop application |
| `docker` | Full JWT authentication required | Multi-user server deployment |
| `package` | Similar to electron | Packaged distribution |

In Docker mode, all API requests require a valid JWT token obtained through login.

### Default Admin Account

When Flowfile starts in Docker mode, it automatically creates an admin account using the `FLOWFILE_ADMIN_USER` and `FLOWFILE_ADMIN_PASSWORD` environment variables.

The admin account has these properties:

- Full administrative privileges (`is_admin=true`)
- Must change password on first login (`must_change_password=true`)
- Can create, modify, and delete other users

### Admin Panel

Access the Admin panel by clicking your username in the top-right corner and selecting **"Admin"** or **"User Management"**.

![User Management](../../../assets/images/guides/docker-deployment/user_management.png)

From the Admin panel, administrators can:

- **View all users** - See usernames, emails, and admin status
- **Create new users** - Add team members with email and temporary password
- **Modify users** - Update user details and admin privileges
- **Delete users** - Remove users (cascades to their secrets and connections)
- **Reset passwords** - Force password change on next login

### Creating New Users

1. Navigate to the Admin panel
2. Click **"Add User"**
3. Fill in user details:
   - Username (unique identifier)
   - Email address
   - Full name
   - Temporary password
   - Admin privileges (optional)
4. Click **"Create"**

New users must change their password on first login.

### Password Requirements

All passwords must meet these requirements:

- Minimum 8 characters
- At least one uppercase letter (A-Z)
- At least one lowercase letter (a-z)
- At least one number (0-9)

## Secrets Management

Flowfile provides encrypted secrets storage for sensitive credentials like database passwords and API keys.

### How Encryption Works

1. **Master Key** - A 32-byte key stored as a Docker secret at `/run/secrets/flowfile_master_key`
2. **Fernet Encryption** - Industry-standard symmetric encryption (AES-128-CBC with HMAC)
3. **Per-User Isolation** - Each user's secrets are stored separately and only accessible to them

### Creating Secrets

1. Navigate to **Settings** → **Secrets** (or click the key icon)
2. Click **"Add Secret"**
3. Enter a unique name (e.g., `database_password`, `api_key`)
4. Enter the secret value
5. Click **"Save"**

![Secrets Management](../../../assets/images/guides/docker-deployment/secrets_management.png)

### Using Secrets in Flows

When configuring database connections or API nodes, you can reference secrets by name instead of entering credentials directly. This keeps sensitive values encrypted and out of flow definitions.

### Best Practices

!!! tip "Secrets Management Best Practices"
    - **Use descriptive names** - `prod_postgres_password` is better than `password1`
    - **Rotate regularly** - Update secrets periodically, especially after team changes
    - **Backup your master key** - Store it securely outside your Docker environment
    - **Use different secrets per environment** - Separate development and production credentials
    - **Avoid hardcoding** - Always use secrets instead of putting credentials in flows

### Master Key Recovery

If you lose your master key:

1. All encrypted secrets become unrecoverable
2. You must generate a new master key
3. Users will need to re-enter all their secrets

!!! danger "Master Key Backup"
    Back up `master_key.txt` to a secure location (password manager, encrypted storage, or secrets vault) immediately after generation.

## Volume Configuration

Docker Compose creates several volumes for persistent storage:

### Default Volumes

| Volume | Host Path | Container Path | Purpose |
|--------|-----------|----------------|---------|
| User Data | `./flowfile_data` | `/app/user_data` | User-created data, connections |
| Internal Storage | `flowfile-internal-storage` (named) | `/app/internal_storage` | Processing cache, temp files |
| Saved Flows | `./saved_flows` | `/app/flowfile_core/saved_flows` | Flow definitions (JSON) |

### Customizing Volume Paths

To use custom paths, modify `docker-compose.yml`:

```yaml
volumes:
  # Bind mount to custom location
  - /data/flowfile/user_data:/app/user_data
  - /data/flowfile/flows:/app/flowfile_core/saved_flows
```

Or use environment variables in your `.env` file:

```bash
FLOWFILE_DATA_PATH=/data/flowfile
```

Then reference in `docker-compose.yml`:

```yaml
volumes:
  - ${FLOWFILE_DATA_PATH}/user_data:/app/user_data
```

### Backup Commands

Back up your Flowfile data regularly:

```bash
# Backup user data
docker compose cp flowfile-core:/app/user_data ./backup/user_data_$(date +%Y%m%d)

# Backup saved flows
cp -r ./saved_flows ./backup/saved_flows_$(date +%Y%m%d)

# Backup internal storage volume
docker run --rm -v flowfile-internal-storage:/data -v $(pwd)/backup:/backup \
  alpine tar czf /backup/internal_storage_$(date +%Y%m%d).tar.gz -C /data .

# Backup master key (store securely!)
cp master_key.txt ./backup/master_key_$(date +%Y%m%d).txt
```

### Restore from Backup

```bash
# Restore user data
docker compose cp ./backup/user_data_20240115 flowfile-core:/app/user_data

# Restore saved flows
cp -r ./backup/saved_flows_20240115/* ./saved_flows/

# Restore internal storage
docker run --rm -v flowfile-internal-storage:/data -v $(pwd)/backup:/backup \
  alpine tar xzf /backup/internal_storage_20240115.tar.gz -C /data
```

## Production Configuration

### Security Hardening Checklist

Before deploying to production, ensure you've completed these steps:

- [ ] **Change default admin password** - Never use `changeme` in production
- [ ] **Generate strong JWT secret** - Use `openssl rand -base64 32`
- [ ] **Generate unique master key** - Use `openssl rand -base64 32`
- [ ] **Configure HTTPS** - Use a reverse proxy (nginx, Traefik, Caddy)
- [ ] **Restrict network access** - Use firewall rules to limit access
- [ ] **Set up monitoring** - Configure health checks and alerting
- [ ] **Enable logging** - Centralize logs for debugging and auditing
- [ ] **Regular backups** - Automate backup of volumes and master key

### HTTPS with Reverse Proxy

For production, always use HTTPS. Here's an example nginx configuration:

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
        proxy_cache_bypass $http_upgrade;
    }
}
```

![Production Architecture](../../../assets/images/guides/docker-deployment/production_architecture.png)

### Resource Limits

Add resource constraints to prevent runaway containers:

```yaml
# docker-compose.yml additions
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

### Health Check Endpoints

Monitor service health with these endpoints:

| Service | Endpoint | Expected Response |
|---------|----------|-------------------|
| Core API | `http://localhost:63578/health` | `200 OK` |
| Worker | `http://localhost:63579/health` | `200 OK` |
| Frontend | `http://localhost:8080` | `200 OK` |

Example health check script:

```bash
#!/bin/bash
# health-check.sh

check_service() {
    local name=$1
    local url=$2
    if curl -sf "$url" > /dev/null; then
        echo "✓ $name is healthy"
        return 0
    else
        echo "✗ $name is unhealthy"
        return 1
    fi
}

check_service "Frontend" "http://localhost:8080"
check_service "Core API" "http://localhost:63578/health"
check_service "Worker" "http://localhost:63579/health"
```

### Docker Compose Health Checks

Add health checks to `docker-compose.yml`:

```yaml
services:
  flowfile-core:
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:63578/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s
```

## Troubleshooting

<details markdown="1">
<summary><strong>Cannot connect to services</strong></summary>

**Symptoms:** Browser shows "Connection refused" or services won't start.

**Solutions:**

1. Check if containers are running:
   ```bash
   docker compose ps
   ```

2. View container logs for errors:
   ```bash
   docker compose logs flowfile-core
   docker compose logs flowfile-worker
   docker compose logs flowfile-frontend
   ```

3. Verify ports aren't already in use:
   ```bash
   lsof -i :8080
   lsof -i :63578
   lsof -i :63579
   ```

4. Check Docker network:
   ```bash
   docker network ls
   docker network inspect flowfile_flowfile-network
   ```

5. Restart services:
   ```bash
   docker compose down
   docker compose up -d
   ```

</details>

<details markdown="1">
<summary><strong>Login fails with correct credentials</strong></summary>

**Symptoms:** Login page rejects valid username/password combinations.

**Solutions:**

1. Verify environment variables are set correctly:
   ```bash
   docker compose exec flowfile-core env | grep FLOWFILE
   ```

2. Check the admin user was created:
   ```bash
   docker compose logs flowfile-core | grep -i "admin\|user"
   ```

3. Reset admin password by restarting with new credentials:
   ```bash
   # Update .env with new password
   echo "FLOWFILE_ADMIN_PASSWORD=NewSecureP@ss123" >> .env

   # Recreate containers
   docker compose down
   docker compose up -d
   ```

4. Verify JWT secret is consistent:
   ```bash
   docker compose exec flowfile-core env | grep JWT_SECRET
   ```

5. Clear browser cookies and cache, then retry.

</details>

<details markdown="1">
<summary><strong>Secrets cannot be decrypted</strong></summary>

**Symptoms:** Error messages about decryption failures or "invalid token".

**Solutions:**

1. Verify master key file exists and has content:
   ```bash
   cat master_key.txt
   # Should show a base64-encoded string
   ```

2. Check the secret is mounted correctly:
   ```bash
   docker compose exec flowfile-core cat /run/secrets/flowfile_master_key
   ```

3. Verify secret file permissions:
   ```bash
   ls -la master_key.txt
   # Should be readable
   ```

4. If master key was changed, old secrets are unrecoverable. Users must re-create their secrets.

5. Regenerate master key (WARNING: loses all existing secrets):
   ```bash
   openssl rand -base64 32 > master_key.txt
   docker compose down
   docker compose up -d
   ```

</details>

<details markdown="1">
<summary><strong>Flows not persisting after restart</strong></summary>

**Symptoms:** Saved flows disappear when containers restart.

**Solutions:**

1. Verify volumes are mounted:
   ```bash
   docker compose exec flowfile-core ls -la /app/flowfile_core/saved_flows
   ```

2. Check host directory exists and has correct permissions:
   ```bash
   ls -la ./saved_flows
   mkdir -p ./saved_flows
   chmod 755 ./saved_flows
   ```

3. Verify volume configuration in `docker-compose.yml`:
   ```yaml
   volumes:
     - ./saved_flows:/app/flowfile_core/saved_flows
   ```

4. Check for volume mount errors:
   ```bash
   docker compose logs | grep -i "volume\|mount\|permission"
   ```

5. Ensure you're using `docker compose down` (not `down -v` which removes volumes):
   ```bash
   # Correct - preserves volumes
   docker compose down

   # WRONG - removes volumes and data!
   docker compose down -v
   ```

</details>

<details markdown="1">
<summary><strong>Worker not processing flows</strong></summary>

**Symptoms:** Flows hang or timeout when running.

**Solutions:**

1. Check worker is running:
   ```bash
   docker compose ps flowfile-worker
   ```

2. View worker logs:
   ```bash
   docker compose logs -f flowfile-worker
   ```

3. Test connectivity between core and worker:
   ```bash
   docker compose exec flowfile-core curl http://flowfile-worker:63579/health
   ```

4. Check WORKER_HOST environment variable:
   ```bash
   docker compose exec flowfile-core env | grep WORKER_HOST
   ```

5. Restart worker service:
   ```bash
   docker compose restart flowfile-worker
   ```

</details>

<details markdown="1">
<summary><strong>Out of memory errors</strong></summary>

**Symptoms:** Containers crash or become unresponsive during large data operations.

**Solutions:**

1. Check container resource usage:
   ```bash
   docker stats
   ```

2. View OOM kill logs:
   ```bash
   docker compose logs | grep -i "killed\|oom\|memory"
   ```

3. Increase memory limits in `docker-compose.yml`:
   ```yaml
   deploy:
     resources:
       limits:
         memory: 8G
   ```

4. Process data in smaller batches.

5. Increase host system swap:
   ```bash
   # Check current swap
   free -h
   ```

</details>

### Debug Commands Reference

```bash
# View all logs
docker compose logs

# Follow logs in real-time
docker compose logs -f

# View logs for specific service
docker compose logs flowfile-core

# Execute command in container
docker compose exec flowfile-core bash

# Check container resource usage
docker stats

# Inspect container details
docker inspect flowfile-core

# View Docker networks
docker network ls

# Clean up unused resources
docker system prune

# Rebuild images
docker compose build --no-cache

# Full reset (WARNING: removes all data)
docker compose down -v
docker compose up -d --build
```

## Upgrading

### Before Upgrading

1. **Backup everything first:**
   ```bash
   # Create backup directory
   mkdir -p backup/$(date +%Y%m%d)

   # Backup flows
   cp -r saved_flows backup/$(date +%Y%m%d)/

   # Backup user data
   cp -r flowfile_data backup/$(date +%Y%m%d)/

   # Backup master key
   cp master_key.txt backup/$(date +%Y%m%d)/

   # Backup environment
   cp .env backup/$(date +%Y%m%d)/
   ```

2. **Note current version:**
   ```bash
   docker compose exec flowfile-core cat /app/version.txt 2>/dev/null || echo "Version file not found"
   ```

### Upgrade Steps

1. **Pull latest changes:**
   ```bash
   git pull origin main
   ```

2. **Stop running services:**
   ```bash
   docker compose down
   ```

3. **Rebuild images:**
   ```bash
   docker compose build --no-cache
   ```

4. **Start services:**
   ```bash
   docker compose up -d
   ```

5. **Verify upgrade:**
   ```bash
   # Check services are running
   docker compose ps

   # Check logs for errors
   docker compose logs | head -100

   # Test login
   curl -X POST http://localhost:63578/token \
     -d "username=admin&password=yourpassword"
   ```

### Rollback Procedure

If issues occur after upgrading:

1. Stop services:
   ```bash
   docker compose down
   ```

2. Restore from backup:
   ```bash
   cp -r backup/20240115/saved_flows ./
   cp -r backup/20240115/flowfile_data ./
   ```

3. Checkout previous version:
   ```bash
   git checkout <previous-commit-hash>
   ```

4. Rebuild and start:
   ```bash
   docker compose build
   docker compose up -d
   ```

## Related Documentation

- [Building Flows](../building-flows.md) - Learn to create data pipelines in the visual editor
- [Database Connectivity](database-connectivity.md) - Connect to PostgreSQL and other databases
- [Cloud Connections](cloud-connections.md) - Set up AWS S3 and cloud storage

---

*This guide is based on Flowfile's Docker Compose deployment, which provides multi-user authentication, encrypted secrets management, and production-ready configuration options.*
