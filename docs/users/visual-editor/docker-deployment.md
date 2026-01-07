# Docker Deployment & Authentication

Deploy Flowfile in Docker with multi-user authentication, role-based access control, and per-user session isolation.

!!! note "Docker vs Desktop"
    Authentication is only available in Docker mode. The desktop application (Electron) uses automatic single-user authentication.

## Quick Start

### 1. Clone and Configure

```bash
git clone https://github.com/edwardvaneechoud/Flowfile.git
cd Flowfile
```

Create a `master_key.txt` file in the project root (used for encrypting secrets):

```bash
echo "your-secure-master-key-here" > master_key.txt
```

### 2. Configure Environment

Set the required environment variables. You can either:

**Option A: Use a `.env` file**
```bash
# .env file
FLOWFILE_ADMIN_USER=admin
FLOWFILE_ADMIN_PASSWORD=your-secure-password
JWT_SECRET_KEY=your-secure-jwt-secret-key
```

**Option B: Export directly**
```bash
export FLOWFILE_ADMIN_USER=admin
export FLOWFILE_ADMIN_PASSWORD=your-secure-password
export JWT_SECRET_KEY=your-secure-jwt-secret-key
```

!!! warning "Production Security"
    **Always change the default values in production!**

    - `FLOWFILE_ADMIN_PASSWORD`: Use a strong password (not `changeme`)
    - `JWT_SECRET_KEY`: Use a cryptographically secure random string (not `flowfile-dev-secret-change-in-production`)

### 3. Start Flowfile

```bash
docker-compose up -d
```

Access Flowfile at: **http://localhost:8080**

---

## Accessing Your Data Files

When running Flowfile in Docker, the containers cannot access files on your host machine unless you explicitly mount them. To work with your data files (CSV, Excel, Parquet, etc.), you need to add them to a mounted directory.

### Default Data Directory

The `docker-compose.yml` includes a volume mount for user data:

```yaml
volumes:
  - flowfile-user-data:/app/user_data
```

Files placed in this volume are accessible at `/app/user_data/` inside the container.

### Mounting a Local Directory

To access files from a specific folder on your host machine, add a bind mount to the `flowfile-core` and `flowfile-worker` services in `docker-compose.yml`:

```yaml
flowfile-core:
  volumes:
    - flowfile-user-data:/app/user_data
    - flowfile-internal-storage:/app/internal_storage
    - ./saved_flows:/app/flowfile_core/saved_flows
    # Add your data directory here:
    - /path/to/your/data:/app/data

flowfile-worker:
  volumes:
    - flowfile-user-data:/app/user_data
    - flowfile-internal-storage:/app/internal_storage
    # Add the same mount to worker for processing:
    - /path/to/your/data:/app/data
```

!!! warning "Mount to Both Services"
    You must add the volume mount to **both** `flowfile-core` and `flowfile-worker` services. The core service handles the UI and file browsing, while the worker service processes the data.

### Example: Mounting Your Documents Folder

```yaml
# Mount your local data folder
- ~/Documents/flowfile-data:/app/data
```

After adding this mount and restarting (`docker-compose down && docker-compose up -d`), your files will be accessible at `/app/data/` when browsing for files in Flowfile.

### Using the Mounted Data

1. In Flowfile, add a **Read Data** node
2. Click **Browse** to open the file explorer
3. Navigate to `/app/data/` (or your mounted path)
4. Select your file

---

## Authentication System

### Login Page

When accessing Flowfile in Docker mode, users are presented with a login page.

![Login Page](__placeholder__login_page.png)

**Default admin credentials** (change in production!):

- **Username**: `admin` (or value of `FLOWFILE_ADMIN_USER`)
- **Password**: `changeme` (or value of `FLOWFILE_ADMIN_PASSWORD`)

### Password Requirements

All passwords must meet these requirements:

- Minimum 8 characters
- At least one number
- At least one special character

### Logout

In Docker mode, a logout button appears in the sidebar. Click it to end your session and return to the login page.

![Sidebar with Logout Button](__placeholder__sidebar_logout.png)

---

## User Management

Administrators can manage users through the built-in admin panel.

### Accessing User Management

1. Log in as an admin user
2. Click the **User Management** button in the sidebar (only visible to admins)

![User Management Panel](__placeholder__user_management.png)

### User Roles

| Role | Capabilities |
|------|--------------|
| **Admin** | Full access: create/edit/delete users, manage all settings |
| **User** | Standard access: create and manage own flows, use all data features |

### Creating Users

1. Open User Management
2. Click **Add User**
3. Fill in the user details:
   - Username (required)
   - Password (must meet requirements)
   - Admin checkbox (grants admin privileges)
   - Require password change (user must set new password on first login)
4. Click **Create**

### Editing Users

Admins can modify user accounts:

- Change password
- Grant/revoke admin privileges
- Enable/disable accounts
- Force password change on next login

### Admin Self-Protection

The system prevents admins from accidentally locking themselves out:

- Admins cannot disable their own account
- Admins cannot remove their own admin privileges
- At least one admin account must always exist

---

## Per-User Flow Sessions

Each user's work is isolated from other users:

- **Session Isolation**: Users only see their own active flow sessions
- **Concurrent Work**: Multiple users can work on different flows simultaneously
- **Data Privacy**: Users cannot access other users' sessions or data

This enables secure multi-user deployment where teams can work independently.

---

## Environment Variables

### Required Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FLOWFILE_MODE` | Runtime mode: `docker`, `electron`, or `package` | `docker` |
| `FLOWFILE_ADMIN_USER` | Initial admin username | `admin` |
| `FLOWFILE_ADMIN_PASSWORD` | Initial admin password | `changeme` |
| `JWT_SECRET_KEY` | Secret key for JWT token signing | `flowfile-dev-secret-change-in-production` |

### Optional Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `WORKER_HOST` | Hostname of the worker service | `flowfile-worker` |
| `FLOWFILE_STORAGE_DIR` | Internal storage directory | `/app/internal_storage` |
| `FLOWFILE_USER_DATA_DIR` | User data directory | `/app/user_data` |

---

## Docker Compose Configuration

Here's the complete `docker-compose.yml` configuration:

```yaml
version: '3.8'

services:
  flowfile-frontend:
    platform: linux/amd64
    build:
      context: .
      dockerfile: flowfile_frontend/Dockerfile
    ports:
      - "8080:8080"
    networks:
      - flowfile-network
    environment:
      - NODE_ENV=production
    depends_on:
      - flowfile-core
      - flowfile-worker

  flowfile-core:
    platform: linux/amd64
    build:
      context: .
      dockerfile: flowfile_core/Dockerfile
    ports:
      - "63578:63578"
    environment:
      - FLOWFILE_MODE=docker
      - FLOWFILE_ADMIN_USER=${FLOWFILE_ADMIN_USER:-admin}
      - FLOWFILE_ADMIN_PASSWORD=${FLOWFILE_ADMIN_PASSWORD:-changeme}
      - JWT_SECRET_KEY=${JWT_SECRET_KEY:-flowfile-dev-secret-change-in-production}
      - WORKER_HOST=flowfile-worker
      - FLOWFILE_STORAGE_DIR=/app/internal_storage
      - FLOWFILE_USER_DATA_DIR=/app/user_data
      - PYTHONDONTWRITEBYTECODE=1
      - PYTHONUNBUFFERED=1
    volumes:
      - flowfile-user-data:/app/user_data
      - flowfile-internal-storage:/app/internal_storage
      - ./saved_flows:/app/flowfile_core/saved_flows
    networks:
      - flowfile-network
    secrets:
      - flowfile_master_key

  flowfile-worker:
    build:
      context: .
      dockerfile: flowfile_worker/Dockerfile
    ports:
      - "63579:63579"
    environment:
      - FLOWFILE_MODE=docker
      - CORE_HOST=flowfile-core
      - FLOWFILE_STORAGE_DIR=/app/internal_storage
      - FLOWFILE_USER_DATA_DIR=/app/user_data
      - PYTHONDONTWRITEBYTECODE=1
      - PYTHONUNBUFFERED=1
    volumes:
      - flowfile-user-data:/app/user_data
      - flowfile-internal-storage:/app/internal_storage
    networks:
      - flowfile-network
    secrets:
      - flowfile_master_key

networks:
  flowfile-network:
    driver: bridge

volumes:
  flowfile-internal-storage:
    driver: local
  flowfile-user-data:
    driver: local

secrets:
  flowfile_master_key:
    file: ./master_key.txt
```

---

## Security Best Practices

### Production Checklist

- [ ] Change `FLOWFILE_ADMIN_PASSWORD` to a strong password
- [ ] Generate a secure random `JWT_SECRET_KEY` (e.g., using `openssl rand -hex 32`)
- [ ] Create a strong `master_key.txt` for secret encryption
- [ ] Use HTTPS/TLS with a reverse proxy (nginx, traefik, etc.)
- [ ] Restrict network access to the Docker services
- [ ] Regularly backup the `flowfile-user-data` volume
- [ ] Monitor logs for suspicious activity

### Generating Secure Keys

```bash
# Generate a secure JWT secret key
openssl rand -hex 32

# Generate a secure master key
openssl rand -base64 32 > master_key.txt
```

### Using a Reverse Proxy

For production deployments, place Flowfile behind a reverse proxy with HTTPS:

```nginx
# Example nginx configuration
server {
    listen 443 ssl;
    server_name flowfile.example.com;

    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;

    location / {
        proxy_pass http://localhost:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

---

## Database & Data Persistence

### Volumes

The Docker setup uses two volumes:

| Volume | Purpose | Persistence |
|--------|---------|-------------|
| `flowfile-user-data` | User files, flows, uploads, outputs | **Persistent** - backup regularly |
| `flowfile-internal-storage` | Logs, cache, temp files | Ephemeral - can be recreated |

### Backup

To backup user data:

```bash
# Stop the services
docker-compose down

# Backup the user data volume
docker run --rm -v flowfile-user-data:/data -v $(pwd):/backup \
  alpine tar czf /backup/flowfile-backup.tar.gz -C /data .

# Restart the services
docker-compose up -d
```

### Database Migrations

When upgrading Flowfile, database migrations run automatically. The system adds new fields (like `is_admin`, `must_change_password`) to existing user records seamlessly.

---

## Troubleshooting

### Cannot Login

1. Check that you're using the correct username and password
2. Verify the `FLOWFILE_ADMIN_USER` and `FLOWFILE_ADMIN_PASSWORD` environment variables
3. Check the container logs: `docker-compose logs flowfile-core`

### Forgot Admin Password

If you've lost the admin password:

1. Stop the services: `docker-compose down`
2. Remove the volumes: `docker-compose down -v` (warning: this deletes all data)
3. Restart with new credentials: `docker-compose up -d`

Alternatively, if you have database access, you can reset the password directly in the database.

### Session Expired

JWT tokens expire after a set period. If your session expires:

1. You'll be redirected to the login page
2. Log in again with your credentials

### Port Conflicts

If ports are already in use:

```bash
# Check what's using the ports
lsof -i :8080
lsof -i :63578
lsof -i :63579

# Modify ports in docker-compose.yml if needed
ports:
  - "9090:8080"  # Use port 9090 instead
```

---

## Comparison: Deployment Modes

| Feature | Docker | Desktop (Electron) | Python Package |
|---------|--------|-------------------|----------------|
| Multi-user support | Yes | No (single user) | No (single user) |
| Authentication | Yes | Auto-login | Auto-login |
| User management | Yes | N/A | N/A |
| Session isolation | Yes | N/A | N/A |
| Ideal for | Teams, servers | Personal use | Development, scripts |

---

## Next Steps

- [Building Flows](building-flows.md) - Learn to create data pipelines
- [Node Reference](nodes/index.md) - Explore available nodes
- [Connect to Databases](tutorials/database-connectivity.md) - Set up database connections
- [Cloud Storage Setup](tutorials/cloud-connections.md) - Configure S3 and cloud storage
