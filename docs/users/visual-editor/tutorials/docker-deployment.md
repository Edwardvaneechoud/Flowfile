# Docker Deployment Guide

### ![Docker Deployment Architecture](../../../assets/images/guides/docker-deployment/main_image.png) Deploying Flowfile with Docker Compose

This guide covers deploying Flowfile using Docker Compose for multi-user team environments and production setups. Docker mode provides full authentication, centralized storage, and secrets management.

## Overview

When running Flowfile in Docker mode, you get:

- **Multi-user authentication** - JWT-based login with admin and user roles
- **Centralized storage** - Shared volumes for flows, user data, and internal processing
- **Secrets management** - Encrypted credential storage with a master key
- **Scalable architecture** - Three-service design separating frontend, API, and worker processes

### Architecture

Flowfile's Docker deployment consists of three services:

| Service | Port | Description |
|---------|------|-------------|
| **flowfile-frontend** | 8080 | Vue.js web interface |
| **flowfile-core** | 63578 | FastAPI ETL engine and API server |
| **flowfile-worker** | 63579 | Background job processor |

<details markdown="1">
<summary>Architecture Diagram</summary>

![Architecture](../../../assets/images/guides/docker-deployment/architecture.png)

</details>

## Prerequisites

Before starting, ensure you have:

- **Docker** version 20.10 or later
- **Docker Compose** version 2.0 or later
- **Git** for cloning the repository

Verify your installations:

```bash
# Check Docker version
docker --version
# Expected: Docker version 20.10.x or higher

# Check Docker Compose version
docker compose version
# Expected: Docker Compose version v2.x.x or higher
```

!!! warning "Platform Support"
    The Docker images are built for `linux/amd64`. If you're on Apple Silicon (M1/M2/M3), Docker will use Rosetta emulation automatically.

## Quick Start

Get Flowfile running in Docker with these steps:

### 1. Clone the Repository

```bash
git clone https://github.com/Edwardvaneechoud/Flowfile.git
cd Flowfile
```

### 2. Generate a Master Key

The master key encrypts all stored secrets. Generate a secure key:

```bash
openssl rand -base64 32 > master_key.txt
```

!!! danger "Critical: Back Up Your Master Key"
    Store `master_key.txt` securely and create backups. If lost, encrypted secrets cannot be recovered.

### 3. Configure Environment Variables

Create a `.env` file in the project root:

```bash
# .env
FLOWFILE_ADMIN_USER=admin
FLOWFILE_ADMIN_PASSWORD=YourSecurePassword123!
JWT_SECRET_KEY=your-random-jwt-secret-key-at-least-32-chars
```

Generate a secure JWT secret:

```bash
# Generate a random JWT secret
openssl rand -base64 48
```

### 4. Start the Services

```bash
docker compose up -d
```

Wait for all services to become healthy (typically 30-60 seconds for first build):

```bash
# Check service status
docker compose ps
```

### 5. Access Flowfile

Open your browser and navigate to:

```
http://localhost:8080
```

Log in with the admin credentials you configured in your `.env` file.

<details markdown="1">
<summary>Screenshot: Login Screen</summary>

![Login Screen](../../../assets/images/guides/docker-deployment/login_screen.png)

</details>

## Environment Variables Reference

### Required Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FLOWFILE_MODE` | Deployment mode (`electron`, `package`, `docker`) | `docker` |
| `FLOWFILE_ADMIN_USER` | Initial admin username | `admin` |
| `FLOWFILE_ADMIN_PASSWORD` | Initial admin password | `changeme` |
| `JWT_SECRET_KEY` | Secret key for JWT token signing | `flowfile-dev-secret-change-in-production` |
| `WORKER_HOST` | Hostname of the worker service | `flowfile-worker` |
| `CORE_HOST` | Hostname of the core service (worker config) | `flowfile-core` |

### Optional Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FLOWFILE_STORAGE_DIR` | Internal storage path for logs/cache/temp files | `/app/internal_storage` |
| `FLOWFILE_USER_DATA_DIR` | User data directory for flows and uploads | `/app/user_data` |
| `NODE_ENV` | Node.js environment for frontend | `production` |

!!! warning "Change Default Credentials"
    The default values for `FLOWFILE_ADMIN_PASSWORD` and `JWT_SECRET_KEY` are insecure. Always override them in production deployments.

## Authentication & User Management

### Understanding FLOWFILE_MODE

The `FLOWFILE_MODE` environment variable controls authentication behavior:

| Mode | Authentication | Use Case |
|------|----------------|----------|
| `electron` | Auto-login as local user | Desktop application |
| `package` | Auto-login as local user | Python package development |
| `docker` | Full JWT authentication | Multi-user deployments |

In Docker mode, users must authenticate with username and password. The system creates the initial admin account on first startup using `FLOWFILE_ADMIN_USER` and `FLOWFILE_ADMIN_PASSWORD`.

### Default Admin Account

On first startup, Flowfile creates an admin account with:

- **Username**: Value of `FLOWFILE_ADMIN_USER` (default: `admin`)
- **Password**: Value of `FLOWFILE_ADMIN_PASSWORD` (default: `changeme`)

!!! danger "Security Warning"
    Change the default admin password immediately after first login. The default credentials are well-known and insecure.

### User Management

Admins can manage users through the Admin panel:

1. Click your username in the top-right corner
2. Select **Admin Panel** (only visible to admins)
3. View, create, edit, or delete users

<details markdown="1">
<summary>Screenshot: User Management</summary>

![User Management](../../../assets/images/guides/docker-deployment/user_management.png)

</details>

### Creating New Users

When creating users, admins must set:

- **Username** - Unique identifier for login
- **Email** - User's email address
- **Full Name** - Display name
- **Password** - Initial password (user will be prompted to change on first login)
- **Is Admin** - Whether the user has admin privileges

### Password Requirements

All passwords must meet these requirements:

- Minimum **8 characters**
- At least one **uppercase letter**
- At least one **lowercase letter**
- At least one **number** (0-9)
- At least one **special character** (`!@#$%^&*()_+-=[]{}|;:,.<>?`)

## Secrets Management

Flowfile provides encrypted secrets storage for sensitive credentials like database passwords and API keys.

### How Secrets Work

1. **Master Key Encryption**: All secrets are encrypted using Fernet symmetric encryption
2. **User Isolation**: Each user's secrets are stored separately and inaccessible to other users
3. **Secure Storage**: Secrets are encrypted at rest in the database

### Creating Secrets

1. Navigate to **Settings** > **Secrets**
2. Click **Add Secret**
3. Enter a unique name and the secret value
4. Click **Save**

<details markdown="1">
<summary>Screenshot: Secrets Management</summary>

![Secrets Management](../../../assets/images/guides/docker-deployment/secrets_management.png)

</details>

### Using Secrets

Secrets can be referenced in:

- **Database connections** - Store passwords securely
- **Cloud storage connections** - Store AWS credentials and API keys
- **Workflow configurations** - Reference secrets by name

### Best Practices

| Practice | Description |
|----------|-------------|
| **Back up the master key** | Store `master_key.txt` in a secure location outside the project |
| **Rotate secrets regularly** | Update credentials periodically |
| **Use descriptive names** | Name secrets clearly (e.g., `postgres_prod_password`) |
| **Limit admin access** | Only grant admin privileges to users who need them |

!!! note "Master Key Location"
    The master key is mounted as a Docker secret at `/run/secrets/flowfile_master_key` inside containers.

## Volume Configuration

### Default Volumes

Docker Compose creates two named volumes:

| Volume | Mount Path | Purpose | Persistence |
|--------|------------|---------|-------------|
| `flowfile-user-data` | `/app/user_data` | User flows, uploads, and outputs | **Persistent** - back up regularly |
| `flowfile-internal-storage` | `/app/internal_storage` | Logs, cache, temporary files | Ephemeral - can be recreated |

Additionally, a bind mount is used for saved flows:

```yaml
volumes:
  - ./saved_flows:/app/flowfile_core/saved_flows
```

### Using Bind Mounts

To use a local directory instead of Docker volumes:

```yaml
# In docker-compose.yml, replace volume references:
volumes:
  - /path/to/local/user_data:/app/user_data
  - /path/to/local/internal_storage:/app/internal_storage
```

### Backup Commands

Back up your Docker volumes regularly:

```bash
# Backup user data volume
docker run --rm \
  -v flowfile_flowfile-user-data:/data \
  -v $(pwd)/backups:/backup \
  alpine tar czf /backup/user_data_$(date +%Y%m%d).tar.gz -C /data .

# Backup saved flows
tar czf backups/saved_flows_$(date +%Y%m%d).tar.gz saved_flows/

# Backup master key (store securely!)
cp master_key.txt backups/master_key_$(date +%Y%m%d).txt
```

!!! note "Backup Frequency"
    For production deployments, automate daily backups of `flowfile-user-data` and `saved_flows`.

## Production Configuration

### Security Hardening Checklist

Before deploying to production, ensure:

- [ ] Changed `FLOWFILE_ADMIN_PASSWORD` to a strong, unique password
- [ ] Generated a secure, random `JWT_SECRET_KEY` (at least 32 characters)
- [ ] Generated and securely stored the `master_key.txt`
- [ ] Configured a reverse proxy (nginx, Traefik) with HTTPS
- [ ] Restricted network access to internal services (ports 63578, 63579)
- [ ] Set up automated backups for volumes and master key
- [ ] Configured log rotation and monitoring

### Resource Limits

Add resource constraints to prevent runaway processes:

```yaml
# Add to each service in docker-compose.yml
services:
  flowfile-core:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 4G
        reservations:
          cpus: '0.5'
          memory: 1G
```

### Health Checks

Monitor service health with these endpoints:

| Service | Health Check URL |
|---------|-----------------|
| Frontend | `http://localhost:8080` |
| Core API | `http://localhost:63578/docs` |
| Worker | `http://localhost:63579/docs` |

Add health checks to your compose file:

```yaml
services:
  flowfile-core:
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:63578/docs"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s
```

### Reverse Proxy Configuration

For production, place Flowfile behind a reverse proxy. Example nginx configuration:

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
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

<details markdown="1">
<summary>Production Architecture Diagram</summary>

![Production Architecture](../../../assets/images/guides/docker-deployment/production_architecture.png)

</details>

## Troubleshooting

<details>
<summary><strong>Cannot connect to services</strong></summary>

**Symptoms**: Browser shows connection refused or services won't start.

**Solutions**:

1. Check if all services are running:
   ```bash
   docker compose ps
   ```

2. View service logs:
   ```bash
   docker compose logs flowfile-core
   docker compose logs flowfile-worker
   docker compose logs flowfile-frontend
   ```

3. Verify ports aren't in use:
   ```bash
   # Check if ports are available
   lsof -i :8080
   lsof -i :63578
   lsof -i :63579
   ```

4. Restart the services:
   ```bash
   docker compose down
   docker compose up -d
   ```

</details>

<details>
<summary><strong>Login fails with correct credentials</strong></summary>

**Symptoms**: Unable to log in even with correct username/password.

**Solutions**:

1. Verify environment variables are set:
   ```bash
   docker compose exec flowfile-core env | grep FLOWFILE
   ```

2. Check if the database initialized correctly:
   ```bash
   docker compose logs flowfile-core | grep -i "admin"
   ```

3. Reset the admin password by recreating containers:
   ```bash
   docker compose down -v  # Warning: removes volumes!
   docker compose up -d
   ```

4. Verify JWT_SECRET_KEY is consistent across restarts (use a `.env` file).

</details>

<details>
<summary><strong>Secrets cannot be decrypted</strong></summary>

**Symptoms**: Error messages about decryption failures or invalid secrets.

**Solutions**:

1. Verify master key file exists and is readable:
   ```bash
   ls -la master_key.txt
   cat master_key.txt
   ```

2. Check if the master key is mounted correctly:
   ```bash
   docker compose exec flowfile-core cat /run/secrets/flowfile_master_key
   ```

3. Ensure the master key hasn't changed since secrets were created. If it has, secrets encrypted with the old key cannot be recovered.

4. Regenerate secrets if the master key was lost:
   ```bash
   # Generate new master key
   openssl rand -base64 32 > master_key.txt
   # Restart services - existing secrets will be unreadable
   docker compose restart
   ```

</details>

<details>
<summary><strong>Flows not persisting after restart</strong></summary>

**Symptoms**: Saved flows disappear after `docker compose down/up`.

**Solutions**:

1. Verify volumes are created:
   ```bash
   docker volume ls | grep flowfile
   ```

2. Check volume mounts:
   ```bash
   docker compose exec flowfile-core ls -la /app/user_data
   docker compose exec flowfile-core ls -la /app/flowfile_core/saved_flows
   ```

3. Don't use `docker compose down -v` (the `-v` flag removes volumes).

4. Check for permission issues:
   ```bash
   docker compose exec flowfile-core ls -la /app/
   ```

</details>

<details>
<summary><strong>Services are slow or unresponsive</strong></summary>

**Symptoms**: Long load times, timeouts, or crashes.

**Solutions**:

1. Check resource usage:
   ```bash
   docker stats
   ```

2. View real-time logs:
   ```bash
   docker compose logs -f
   ```

3. Increase resource limits in docker-compose.yml (see Production Configuration).

4. Check disk space:
   ```bash
   df -h
   docker system df
   ```

5. Clean up unused Docker resources:
   ```bash
   docker system prune -a
   ```

</details>

## Upgrading

When upgrading Flowfile to a new version:

### 1. Backup First

```bash
# Stop services
docker compose down

# Backup volumes
docker run --rm \
  -v flowfile_flowfile-user-data:/data \
  -v $(pwd)/backups:/backup \
  alpine tar czf /backup/pre_upgrade_$(date +%Y%m%d).tar.gz -C /data .

# Backup saved flows and master key
tar czf backups/saved_flows_pre_upgrade.tar.gz saved_flows/
cp master_key.txt backups/
```

### 2. Pull Latest Changes

```bash
git pull origin main
```

### 3. Rebuild and Restart

```bash
# Rebuild images with new code
docker compose build --no-cache

# Start services
docker compose up -d
```

### 4. Verify the Upgrade

```bash
# Check all services are running
docker compose ps

# View logs for any errors
docker compose logs --tail=100

# Test login and basic functionality
curl -s http://localhost:63578/docs | head -20
```

!!! note "Database Migrations"
    Flowfile handles database migrations automatically on startup. Check logs for migration messages.

## Related Documentation

- [Building Flows](../../../quickstart.md) - Getting started with visual workflows
- [Database Connectivity](database-connectivity.md) - Connecting to PostgreSQL databases
- [Cloud Connections](cloud-connections.md) - Setting up AWS S3 and cloud storage

---

*This guide covers Flowfile Docker deployment with authentication and secrets management introduced in version 0.3.x.*
