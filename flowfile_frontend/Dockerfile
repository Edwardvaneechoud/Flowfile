# Use Node.js LTS as base image
FROM node:20-slim

# Set working directory
WORKDIR /app

RUN apt-get update && apt-get install -y \
    git \
    curl \
    wget \
    gnupg \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY flowfile_frontend/package*.json ./

RUN npm install -g npm@latest

RUN npm install --platform=linux --arch=arm64 --no-optional

COPY flowfile_frontend/ .

RUN npm run build:web

RUN npm install -g serve

# Expose the port the app runs on
EXPOSE 8080

# Command to serve the built application
CMD ["serve", "-s", "build/renderer", "-l", "8080"]
