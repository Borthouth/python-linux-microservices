#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Get the directory where this script is actually located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.test.yml"

echo "🚀 Starting ReportHub test databases on isolated ports..."
docker compose -f "$COMPOSE_FILE" up -d

echo ""
echo "✅ Test databases are up and running!"
echo "--------------------------------------------------"
echo "PostgreSQL (Chinook):    localhost:5433 (User: test_user / DB: chinook)"
echo "MySQL (Northwind):       localhost:3307 (User: test_user / DB: northwind)"
echo "Microsoft SQL Server:    localhost:1434 (User: sa / DB: master)"
echo "--------------------------------------------------"
echo "To stop them later, run: docker compose -f $COMPOSE_FILE down"