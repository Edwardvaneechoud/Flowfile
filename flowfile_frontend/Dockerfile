# Build stage
FROM node:20-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY flowfile_frontend/package*.json ./
RUN npm ci --no-audit --no-fund

COPY flowfile_frontend/ .
RUN npm run build:web

# Runtime stage
FROM nginx:alpine

COPY --from=builder /build/build/renderer /usr/share/nginx/html
COPY --from=builder /build/nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/ || exit 1

CMD ["nginx", "-g", "daemon off;"]
