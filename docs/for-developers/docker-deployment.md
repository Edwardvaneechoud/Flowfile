# Docker Deployment Guide

This guide covers deploying Flowfile using Docker Compose for production and development environments.

## Prerequisites

- Docker and Docker Compose installed
- Basic understanding of Docker concepts

## Quick Start

### Step 1: Generate the Master Key

The master key encrypts all user secrets stored in the database. Generate it once and protect it carefully:

```bash
# Using the Makefile (recommended)
make generate_key

# Or manually with OpenSSL
openssl rand -base64 32 > master_key.txt
chmod 600 master_key.txt

# Or manually with Python
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" > master_key.txt
chmod 600 master_key.txt
```

**Security Warning:**
- Never commit `master_key.txt` to version control
- Back up this file securely - losing it means losing access to all encrypted secrets
- Use different keys for development and production environments

### Step 2: Configure Environment Variables

Copy the example environment file and customize it:

```bash
cp .env.example .env
```

Edit `.env` with your settings:

```bash
# Admin credentials for the default user
FLOWFILE_ADMIN_USER=admin
FLOWFILE_ADMIN_PASSWORD=YourSecurePassword123!

# JWT secret for session tokens (min 32 characters)
# Generate with: openssl rand -hex 32
JWT_SECRET_KEY=your-secure-jwt-secret-key-at-least-32-chars
```

### Step 3: Start the Services

```bash
docker-compose up -d
```

Access Flowfile at: http://localhost:8080

## Understanding the Security Architecture

Flowfile uses a layered security approach:

| Component | Purpose | Storage |
|-----------|---------|---------|
| **Master Key** | Encrypts all user secrets at rest | Docker secret (`/run/secrets/flowfile_master_key`) |
| **User Secrets** | API keys, passwords, tokens created by users | Encrypted in database using master key |
| **JWT Secret** | Signs authentication tokens | Environment variable |
| **User Password** | Authenticates users | Hashed in database |

### How They Work Together

1. User logs in with username/password
2. Server issues a JWT token (signed with JWT_SECRET_KEY)
3. User creates secrets (e.g., "my_api_key" = "sk-xxx")
4. Secret value is encrypted using the master key before storage
5. At runtime, secrets are decrypted with the master key for use in flows

## Production Checklist

Before deploying to production:

- [ ] Generate a unique master key (not the one from development)
- [ ] Set a strong `FLOWFILE_ADMIN_PASSWORD`
- [ ] Generate a secure `JWT_SECRET_KEY` with `openssl rand -hex 32`
- [ ] Ensure `master_key.txt` is not in version control
- [ ] Back up `master_key.txt` securely (losing it = losing all encrypted secrets)
- [ ] Consider using Docker secrets or a secrets manager for `.env` values
- [ ] Set up HTTPS (reverse proxy with nginx/traefik)
- [ ] Configure firewall rules to restrict access

## Docker Compose Configuration

The `docker-compose.yml` deploys three services:

```yaml
services:
  flowfile-frontend:  # Web UI on port 8080
  flowfile-core:      # API server on port 63578
  flowfile-worker:    # Background job processor on port 63579
```

All services share:
- The master key via Docker secrets
- User data volume (`./flowfile_data`)
- Internal network for communication

## Troubleshooting

### "Docker secret 'flowfile_master_key' is not mounted"

The master key file is missing or not properly configured:

```bash
# Check if file exists
ls -la master_key.txt

# Generate if missing
make generate_key
```

### "Invalid master key format"

The master key must be a valid Fernet key (base64-encoded, 32 bytes). Regenerate it:

```bash
make force_key
```

**Warning:** Regenerating the key will make existing encrypted secrets unreadable.

### Container fails to start

Check the logs:

```bash
docker-compose logs flowfile-core
docker-compose logs flowfile-worker
```

## Volumes and Data Persistence

| Volume | Purpose |
|--------|---------|
| `./flowfile_data` | User uploads, files |
| `./saved_flows` | Saved flow definitions |
| `flowfile-internal-storage` | Internal application data |

## Scaling Considerations

For high-availability deployments:
- Use external PostgreSQL instead of SQLite
- Deploy multiple worker instances
- Use Redis for distributed task queue
- Place services behind a load balancer
