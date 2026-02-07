#!/bin/bash
set -e

if [ -n "$KERNEL_PACKAGES" ]; then
    echo "Installing packages: $KERNEL_PACKAGES"
    pip install --no-cache-dir $KERNEL_PACKAGES
fi

exec uvicorn kernel_runtime.main:app --host 0.0.0.0 --port 9999
