# Host Flowfile in Your Private Cloud

A step-by-step tutorial for deploying Flowfile on your own infrastructure.

## What You'll Build

By the end of this tutorial, you'll have:

- Flowfile running on your server with HTTPS
- Multi-user authentication enabled
- Encrypted secrets storage configured
- Health monitoring in place

## Prerequisites

- A Linux server (Ubuntu 20.04+ recommended)
- Docker and Docker Compose installed
- A domain name pointing to your server
- SSL certificate (or use Let's Encrypt)

## Step 1: Prepare Your Server

SSH into your server and install Docker:

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Docker
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER

# Install Docker Compose
sudo apt install docker-compose-plugin -y

# Verify installation
docker --version
docker compose version
```

Log out and back in for group changes to take effect.

## Step 2: Clone Flowfile

```bash
cd /opt
sudo git clone https://github.com/edwardvaneechoud/Flowfile.git
sudo chown -R $USER:$USER Flowfile
cd Flowfile
```

## Step 3: Generate Security Keys

Generate the master key for encrypting secrets:

```bash
openssl rand -base64 32 > master_key.txt
chmod 600 master_key.txt
```

Generate a JWT secret:

```bash
openssl rand -base64 32
# Copy this output for the next step
```

## Step 4: Configure Environment

Create your `.env` file:

```bash
cat > .env << 'EOF'
FLOWFILE_MODE=docker
FLOWFILE_ADMIN_USER=admin
FLOWFILE_ADMIN_PASSWORD=ChangeThisSecurePassword123!
JWT_SECRET_KEY=paste-your-generated-jwt-secret-here
EOF

chmod 600 .env
```

## Step 5: Start Flowfile

```bash
docker compose up -d
```

Verify all services are running:

```bash
docker compose ps
```

You should see three services running: `flowfile-frontend`, `flowfile-core`, `flowfile-worker`.

## Step 6: Configure HTTPS with Nginx

Install nginx:

```bash
sudo apt install nginx certbot python3-certbot-nginx -y
```

Create nginx configuration:

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
```

Enable the site and get SSL certificate:

```bash
sudo ln -s /etc/nginx/sites-available/flowfile /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx

# Get SSL certificate
sudo certbot --nginx -d your-domain.com
```

## Step 7: Verify Installation

1. Open `https://your-domain.com` in your browser
2. Log in with your admin credentials
3. Change your password when prompted
4. Create a test flow to verify everything works

## Step 8: Set Up Auto-Start

Ensure Flowfile starts on boot:

```bash
cd /opt/Flowfile
docker compose up -d
```

Docker containers with `restart: always` will auto-start. Add this to your compose file if not present.

## Step 9: Backup Configuration

Set up a backup script:

```bash
cat > /opt/Flowfile/backup.sh << 'EOF'
#!/bin/bash
BACKUP_DIR="/opt/backups/flowfile/$(date +%Y%m%d)"
mkdir -p $BACKUP_DIR
cp -r /opt/Flowfile/saved_flows $BACKUP_DIR/
cp -r /opt/Flowfile/flowfile_data $BACKUP_DIR/
cp /opt/Flowfile/master_key.txt $BACKUP_DIR/
echo "Backup completed: $BACKUP_DIR"
EOF

chmod +x /opt/Flowfile/backup.sh
```

Add to cron for daily backups:

```bash
echo "0 2 * * * /opt/Flowfile/backup.sh" | crontab -
```

## Next Steps

- [Add team members](../settings.md#user-management) via the admin panel
- [Configure secrets](../secrets.md) for database connections
- [Connect to databases](database-connectivity.md) in your flows

## Troubleshooting

**Can't access the site?**
```bash
# Check if services are running
docker compose ps

# Check logs
docker compose logs --tail=50
```

**SSL certificate issues?**
```bash
sudo certbot renew --dry-run
```

**Need to restart?**
```bash
docker compose restart
```
