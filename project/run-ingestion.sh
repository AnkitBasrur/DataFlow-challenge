#!/bin/sh
set -e

echo "Starting DataSync ingestion..."
docker compose up --build
