FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy only the requirements.txt first to leverage Docker cache
COPY flowfile_core/requirements.txt /app/requirements.txt

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    curl \
    build-essential \
    && curl https://sh.rustup.rs -sSf | sh -s -- -y \
    && rm -rf /var/lib/apt/lists/*

# Add cargo to PATH
ENV PATH="/root/.cargo/bin:${PATH}"

# Install Python dependencies
RUN pip install -r requirements.txt

# Copy the flowfile_core package contents into the container
COPY flowfile_core/flowfile_core /app/flowfile_core

# Create directories for saved flows and shared files
RUN mkdir -p /app/flowfile_core/saved_flows \
    && mkdir -p /app/shared

# Expose the port the app runs on
EXPOSE 63578

# Set Python path to recognize the package
ENV PYTHONPATH=/app

ENV RUNNING_IN_DOCKER=true

# Change working directory to where the module is located
WORKDIR /app

# Command to run the application
CMD ["python", "-m", "flowfile_core.main"]