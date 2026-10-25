#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.test.yml"
PG_INIT_DIR="$SCRIPT_DIR/init-db/postgres"
MYSQL_INIT_DIR="$SCRIPT_DIR/init-db/mysql"
MSSQL_INIT_DIR="$SCRIPT_DIR/init-db/mssql"

usage() {
    echo "Usage: ./start_test_dbs.sh [OPTIONS]"
    echo "  --reset     Tears down old volumes and restarts clean."
    exit 0
}

RESET_VOLUMES=false
for arg in "$@"; do
    case "$arg" in
        --reset|--rebuild) RESET_VOLUMES=true ;;
        --help|-h) usage ;;
        *) echo "Unknown option: $arg"; exit 1 ;;
    esac
done

echo "🛠️ Ensuring initialization directories exist..."
mkdir -p "$PG_INIT_DIR"
mkdir -p "$MYSQL_INIT_DIR"
mkdir -p "$MSSQL_INIT_DIR"

# 1. Automatically download datasets if missing & robustly sanitize Postgres script
if [ ! -f "$PG_INIT_DIR/chinook.sql" ]; then
    echo "📥 Automatically downloading Chinook for PostgreSQL..."
    curl -sL "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_PostgreSql.sql" -o "$PG_INIT_DIR/chinook.sql"
fi

echo "🛡️ Ensuring PostgreSQL Chinook script is sanitized against database-level conflicts..."
sed -i '/^\s*DROP DATABASE/Id' "$PG_INIT_DIR/chinook.sql"
sed -i '/^\s*CREATE DATABASE/Id' "$PG_INIT_DIR/chinook.sql"
sed -i '/^\s*\\c /d' "$PG_INIT_DIR/chinook.sql"

if [ ! -f "$MYSQL_INIT_DIR/northwind.sql" ]; then
    echo "📥 Automatically downloading Northwind for MySQL..."
    curl -sL "https://raw.githubusercontent.com/dalers/mywind/master/northwind.sql" -o "$MYSQL_INIT_DIR/northwind.sql"
fi

if [ ! -f "$MSSQL_INIT_DIR/instnwnd.sql" ]; then
    echo "📥 Automatically downloading official Microsoft Northwind for MSSQL..."
    curl -sL "https://raw.githubusercontent.com/microsoft/sql-server-samples/master/samples/databases/northwind-pubs/instnwnd.sql" -o "$MSSQL_INIT_DIR/instnwnd.sql"
fi

# 2. Sanitize and constraint-wrap the MSSQL script if the final version doesn't exist
if [ ! -f "$MSSQL_INIT_DIR/instnwnd_final.sql" ]; then
    echo "⚙️ Sanitizing MSSQL Northwind script (removing photo blobs and wrapping constraints)..."
    python3 -c "
import re, os
path = '$MSSQL_INIT_DIR/instnwnd.sql'
with open(path, 'r', encoding='latin1') as f:
    content = f.read()
cleaned = re.sub(r'0x[0-9A-Fa-f]{100,}', 'NULL', content)
wrapped = 'EXEC sp_msforeachtable \\'ALTER TABLE ? NOCHECK CONSTRAINT ALL\\';\nGO\n' + cleaned + '\nGO\nEXEC sp_msforeachtable \\'ALTER TABLE ? WITH CHECK CHECK CONSTRAINT ALL\\';\nGO\n'
with open('$MSSQL_INIT_DIR/instnwnd_final.sql', 'w', encoding='utf-8') as f:
    f.write(wrapped)
"
fi

if [ "$RESET_VOLUMES" = true ]; then
    echo "🧹 Resetting volumes (--reset flag detected)..."
    docker compose -f "$COMPOSE_FILE" down -v
fi

echo "🚀 Starting ReportHub test databases..."
docker compose -f "$COMPOSE_FILE" up -d

# 3. Fully automated wait & import loop
echo "⏳ Waiting for databases to accept connections and auto-importing schemas..."

# Wait for PostgreSQL
echo "   - Checking PostgreSQL (Chinook)..."
until docker exec reporthub-postgres-test pg_isready -U test_user -d chinook >/dev/null 2>&1; do
    sleep 2
done
echo "   ✅ PostgreSQL is ready."

# Wait for MySQL
echo "   - Checking MySQL (Northwind)..."
until docker exec reporthub-mysql-test mysqladmin ping -h localhost -u test_user -p'test_password' --silent >/dev/null 2>&1; do
    sleep 2
done
echo "   ✅ MySQL is ready."

# Wait for MSSQL & Import Northwind using the constraint-safe script
echo "   - Checking Microsoft SQL Server (this may take a minute)..."
until docker exec reporthub-mssql-test /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStrongPassword123!' -Q "SELECT 1" -C >/dev/null 2>&1; do
    sleep 2
done
echo "   ✅ MSSQL is ready. Creating database and importing Northwind schema..."

docker exec -i reporthub-mssql-test /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStrongPassword123!' -C -Q "DROP DATABASE IF EXISTS northwind; CREATE DATABASE northwind;" >/dev/null 2>&1
docker exec -i reporthub-mssql-test /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStrongPassword123!' -C -d northwind < "$MSSQL_INIT_DIR/instnwnd_final.sql" >/dev/null 2>&1
echo "   ✅ MSSQL Northwind imported."

echo ""
echo "✅ All test databases are up, running, and fully populated automatically!"
echo "--------------------------------------------------"
echo "PostgreSQL (Chinook):    localhost:5433"
echo "MySQL (Northwind):       localhost:3307"
echo "Microsoft SQL Server:    localhost:1434"
echo "--------------------------------------------------"