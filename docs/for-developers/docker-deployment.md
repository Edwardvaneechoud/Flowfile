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

> **Git project tracking** is admin-only in docker and is gated by `FLOWFILE_ENABLE_PROJECTS`
> (the `/project` router 404s when off). The bundled compose files default it to `true`; set
> `FLOWFILE_ENABLE_PROJECTS=false` to disable it. Each user's projects are confined to their own
> `/app/user_data/projects/<owner_id>` subtree, so tenants stay isolated.

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

## Group-Based Sharing (multi-user mode)

In Docker (multi-user) mode, Flowfile supports sharing resources with **user groups**.
This feature is dormant in the desktop/Electron app.

### ⚠️ Breaking change: the catalog is now private-by-default

Previously the data catalog (namespaces, tables, flows, runs, visualizations,
dashboards) was **globally visible** to every authenticated user in Docker mode.
As of the release that introduces group sharing, the catalog is
**private-by-default**: each user sees only the resources they own, the ones
explicitly shared with a group they belong to, and the seeded *public* system
namespaces (`General`, `default`, `Unnamed Flows`, `Local Flows`) as tree
containers. Global admins still see everything.

If your team relied on the old "everyone sees every flow/table" behavior, create a
group, add the relevant members, and share the namespaces/tables/flows you want
shared. No data is lost in the upgrade — only its *visibility* narrows.

### How it works

- **Groups**: only global admins create groups (`/user-groups`, or the **User
  Groups** sidebar entry). Each group has member roles (`owner` / `manager` /
  `member`) so a group owner can manage membership without being a global admin.
- **Sharing**: owners share a resource with a group at `use` (read/execute) or
  `manage` (edit + re-share) level via the **Share** action in the UI.
  - *Secrets* are **use-only** when shared: a group's flows can run with the
    credential, but members can never view its value. Sharing a secret for use is
    therefore security-equivalent to giving the group that credential.
  - *Connections* (database, cloud, Google Analytics, Kafka): a shared connection
    is usable in flows directly. A `manage`-grantee who changes the connection's
    target (host/endpoint/protocol) must re-enter the credentials.
- **Schedules**: a `use`-level member may schedule a shared flow; the run executes
  as that member, so its secret/connection references resolve against the member's
  own grants. Make sure the member also has access to everything the flow uses.
- **Not shared**: AI BYOK keys and uploaded files stay outside the sharing model
  (the Docker uploads directory is already common to all users).

### How shared secrets decrypt for non-owners

Sharing is **authorization-only**: granting a group access to a secret or
connection never copies, re-encrypts, or exposes the credential.

Every secret is stored as `$ffsec$1$<owner_id>$<ciphertext>`, encrypted with a
per-user key derived from the master key (HKDF with the owner's user id as
context). Because the owner's id is embedded in the stored value, decryption
never depends on *who is running the flow*: when a group member's flow resolves
a shared secret, core (and the worker, which derives keys independently) reads
the owner id out of the value and re-derives the **owner's** key. The member's
identity only matters for the authorization check — may this user resolve this
secret by name — which is grant-based: own secrets first, then group-shared ones.

Consequences:

- The plaintext is never returned to non-owners through the API; it only
  resolves inside flow execution.
- When a `manage`-grantee rotates a shared connection's credential, the new
  value is re-encrypted under the **owner's** key, so the stored format never
  changes hands.
- Revoking a grant takes effect immediately: the grantee never held key
  material, so there is nothing to rotate or re-encrypt.

### Operator notes

- `FLOWFILE_INTERNAL_SERVICE_USER_ID` (default `1`) selects which user the kernel
  internal-service principal maps to when no kernel id is supplied. In hardened
  deployments point it at a dedicated service account that owns no resources.
- Migration `020` adds the `user_groups`, `user_group_memberships`, and
  `resource_grants` tables and an `is_public` flag on namespaces; it runs
  automatically at core startup.
