# =============================================================================
# Stage 1: Builder - Build the frontend application
# =============================================================================
FROM node:20-slim AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy package files first for better layer caching
COPY flowfile_frontend/package*.json ./

# Install dependencies
RUN npm ci --no-audit --no-fund

# Copy source and build
COPY flowfile_frontend/ .
RUN npm run build:web

# =============================================================================
# Stage 2: Runtime - Serve with nginx for production
# =============================================================================
FROM nginx:alpine AS runtime

# Copy built files to nginx
COPY --from=builder /build/build/renderer /usr/share/nginx/html

# Copy custom nginx config for SPA routing
RUN echo 'server { \
    listen 8080; \
    root /usr/share/nginx/html; \
    index index.html; \
    location / { \
        try_files $uri $uri/ /index.html; \
    } \
    location ~* \.(js|css|png|jpg|jpeg|gif|ico|svg|woff|woff2|ttf|eot)$ { \
        expires 1y; \
        add_header Cache-Control "public, immutable"; \
    } \
    gzip on; \
    gzip_types text/plain text/css application/json application/javascript text/xml application/xml application/xml+rss text/javascript; \
}' > /etc/nginx/conf.d/default.conf

EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/ || exit 1

CMD ["nginx", "-g", "daemon off;"]
