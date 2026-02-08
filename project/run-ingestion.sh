#!/bin/sh
set -e

MODE="${1:-ingest}"   # ingest | export

echo "Starting DataSync ingestion... mode=$MODE"

if [ "$MODE" = "export" ]; then
  API_KEY="${API_KEY}" EXPORT_ONLY=true \
    docker compose up --build --abort-on-container-exit --exit-code-from ingestion
else
  API_KEY="${API_KEY}" EXPORT_ONLY=false \
    docker compose up --build --abort-on-container-exit --exit-code-from ingestion
fi