# Docker Deployment Guide

Deploy Flowfile using Docker Compose for development and production environments.

## Prerequisites

- Docker and Docker Compose installed
- Basic understanding of Docker concepts

## Quick Start

### Option A: Interactive Setup (Recommended)

1. **Start the services:**
   ```bash
   docker compose up -d
   ```

2. **Open Flowfile:** http://localhost:8080

3. **Follow the Setup Wizard:**
   - Click "Generate Master Key"
   - Copy the generated key
   - Add to your `.env` file: `FLOWFILE_MASTER_KEY=<your-key>`
   - Restart: `docker compose restart`

4. **Log in** with default credentials (`admin` / `changeme`)

### Option B: Pre-configured Setup

For automated deployments:

#### Step 1: Generate the Master Key

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Copy the output for the next step.

#### Step 2: Configure Environment

```bash
cp .env.example .env
```

Edit `.env`:

```bash
FLOWFILE_MASTER_KEY=<your-generated-key>
FLOWFILE_ADMIN_USER=admin
FLOWFILE_ADMIN_PASSWORD=YourSecurePassword123!
JWT_SECRET_KEY=your-secure-jwt-secret-at-least-32-chars
```

#### Step 3: Start Services

```bash
docker compose up -d
```

Access at: http://localhost:8080

## Security Architecture

| Component | Purpose | Configuration |
|-----------|---------|---------------|
| **Master Key** | Encrypts user secrets at rest | `FLOWFILE_MASTER_KEY` env var |
| **User Secrets** | API keys, passwords, tokens | Encrypted in database |
| **JWT Secret** | Signs authentication tokens | `JWT_SECRET_KEY` env var |
| **User Password** | Authenticates users | Hashed in database |

### How They Work Together

1. User logs in with username/password
2. Server issues a JWT token (signed with `JWT_SECRET_KEY`)
3. User creates secrets (e.g., "my_api_key" = "sk-xxx")
4. Secret value is encrypted using the master key before storage
5. At runtime, secrets are decrypted with the master key for use in flows

## Production Checklist

- [ ] Generate a unique `FLOWFILE_MASTER_KEY`
- [ ] Set a strong `FLOWFILE_ADMIN_PASSWORD`
- [ ] Generate secure `JWT_SECRET_KEY` with `openssl rand -hex 32`
- [ ] Never commit `.env` to version control
- [ ] Back up `.env` securely (losing master key = losing all encrypted secrets)
- [ ] Set up HTTPS (reverse proxy with nginx/traefik)
- [ ] Configure firewall rules

## Docker Compose Services

```yaml
services:
  flowfile-frontend:  # Web UI on port 8080
  flowfile-core:      # API server on port 63578
  flowfile-worker:    # Background job processor on port 63579
```

All services share:
- The master key via `FLOWFILE_MASTER_KEY` environment variable
- User data volume (`./flowfile_data`)
- Internal network for communication

## Troubleshooting

### Setup wizard keeps appearing

The master key is not configured. Add `FLOWFILE_MASTER_KEY` to your `.env` file and restart.

### Invalid master key format

The master key must be a valid Fernet key. Generate a new one:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

**Warning:** Changing the key makes existing encrypted secrets unreadable.

### Container fails to start

Check the logs:

```bash
docker compose logs flowfile-core
docker compose logs flowfile-worker
```

## Volumes

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
