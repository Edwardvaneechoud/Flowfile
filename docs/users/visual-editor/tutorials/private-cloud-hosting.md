# Host Flowfile in Your Private Cloud

Deploy Flowfile on your own infrastructure using pre-built Docker images.

## What You'll Have

- Flowfile running with HTTPS
- Multi-user authentication
- Encrypted secrets storage

## Prerequisites

- Linux server (Ubuntu 20.04+ recommended)
- Docker and Docker Compose installed
- Domain name with SSL certificate

## Step 1: Install Docker

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
# Log out and back in
```

## Step 2: Create Project Directory

```bash
mkdir -p /opt/flowfile && cd /opt/flowfile
```

## Step 3: Create docker-compose.yml

```bash
cat > docker-compose.yml << 'EOF'
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
      - FLOWFILE_MASTER_KEY=${FLOWFILE_MASTER_KEY}
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
      - FLOWFILE_MASTER_KEY=${FLOWFILE_MASTER_KEY}
    volumes:
      - ./flowfile_data:/app/user_data
      - flowfile-storage:/app/internal_storage
    networks:
      - flowfile-network

networks:
  flowfile-network:

volumes:
  flowfile-storage:
EOF
```

## Step 4: Configure Environment

```bash
cat > .env << 'EOF'
FLOWFILE_ADMIN_USER=admin
FLOWFILE_ADMIN_PASSWORD=YourSecurePassword123!
JWT_SECRET_KEY=your-jwt-secret-at-least-32-characters-long
EOF
```

## Step 5: Start Flowfile

```bash
docker compose up -d
```

Open `http://your-server:8080`. On first visit, you'll see a setup screen to generate your master encryption key.

## Step 6: Configure Master Key

1. Click **Generate Master Key** in the setup screen
2. Copy the generated key
3. Add to your `.env` file:
   ```bash
   echo "FLOWFILE_MASTER_KEY=your-generated-key" >> .env
   ```
4. Restart:
   ```bash
   docker compose restart
   ```

## Step 7: Set Up HTTPS

Install nginx and certbot:

```bash
sudo apt install nginx certbot python3-certbot-nginx -y
```

Create nginx config:

```bash
sudo tee /etc/nginx/sites-available/flowfile << 'EOF'
server {
    listen 80;
    server_name your-domain.com;

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
EOF

sudo ln -s /etc/nginx/sites-available/flowfile /etc/nginx/sites-enabled/
sudo certbot --nginx -d your-domain.com
```

## Updating

```bash
docker compose pull
docker compose up -d
```

## Backup

```bash
cp -r /opt/flowfile/saved_flows backup/
cp -r /opt/flowfile/flowfile_data backup/
cp /opt/flowfile/.env backup/
```

## Next Steps

- [Add team members](../settings.md#user-management)
- [Configure secrets](../secrets.md)
- [Connect to databases](database-connectivity.md)
