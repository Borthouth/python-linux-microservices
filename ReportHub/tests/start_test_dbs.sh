#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.test.yml"
PG_INIT_DIR="$SCRIPT_DIR/init-db/postgres"
MYSQL_INIT_DIR="$SCRIPT_DIR/init-db/mysql"
MSSQL_INIT_DIR="$SCRIPT_DIR/init-db/mssql"
MONGO_INIT_DIR="$SCRIPT_DIR/init-db/mongo"
ORACLE_INIT_DIR="$SCRIPT_DIR/init-db/oracle"

usage() {
    echo "Usage: ./start_test_dbs.sh [--start|--stop] [--reset]"
    echo "  --start     Start and initialize the test databases (default)."
    echo "  --stop      Stop the test database containers without removing them or their volumes."
    echo "  --reset     Remove old containers and volumes, then start clean."
    exit 0
}

wait_for_service() {
    local name="$1"
    local attempts="$2"
    local delay="$3"
    shift 3

    echo "   - Waiting for $name..."
    for ((attempt = 1; attempt <= attempts; attempt++)); do
        if "$@" >/dev/null 2>&1; then
            echo "   ✅ $name is ready."
            return 0
        fi
        sleep "$delay"
    done

    echo "   ❌ Timed out waiting for $name." >&2
    return 1
}

ACTION=start
ACTION_SELECTED=false
RESET_VOLUMES=false
for arg in "$@"; do
    case "$arg" in
        --start)
            if [ "$ACTION_SELECTED" = true ] && [ "$ACTION" != "start" ]; then
                echo "Error: --start and --stop cannot be used together." >&2
                exit 1
            fi
            ACTION=start
            ACTION_SELECTED=true
            ;;
        --stop)
            if [ "$ACTION_SELECTED" = true ] && [ "$ACTION" != "stop" ]; then
                echo "Error: --start and --stop cannot be used together." >&2
                exit 1
            fi
            ACTION=stop
            ACTION_SELECTED=true
            ;;
        --reset|--rebuild) RESET_VOLUMES=true ;;
        --help|-h) usage ;;
        *) echo "Unknown option: $arg"; exit 1 ;;
    esac
done

if [ "$ACTION" = stop ]; then
    if [ "$RESET_VOLUMES" = true ]; then
        echo "Error: --reset cannot be used with --stop." >&2
        exit 1
    fi
    echo "🛑 Stopping ReportHub test databases..."
    docker compose -f "$COMPOSE_FILE" stop
    echo "✅ Test database containers stopped; their containers and volumes are preserved."
    exit 0
fi

echo "🛠️ Ensuring initialization directories exist..."
mkdir -p "$PG_INIT_DIR"
mkdir -p "$MYSQL_INIT_DIR"
mkdir -p "$MSSQL_INIT_DIR"
mkdir -p "$MONGO_INIT_DIR"
mkdir -p "$ORACLE_INIT_DIR"

# 1. Automatically download datasets if missing & robustly sanitize Postgres script
if [ ! -f "$PG_INIT_DIR/chinook.sql" ]; then
    echo "📥 Automatically downloading Chinook for PostgreSQL..."
    curl -fsSL "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_PostgreSql.sql" -o "$PG_INIT_DIR/chinook.sql"
fi

echo "🛡️ Ensuring PostgreSQL Chinook script is sanitized against database-level conflicts..."
sed -i '/^\s*DROP DATABASE/Id' "$PG_INIT_DIR/chinook.sql"
sed -i '/^\s*CREATE DATABASE/Id' "$PG_INIT_DIR/chinook.sql"
sed -i '/^\s*\\c /d' "$PG_INIT_DIR/chinook.sql"

if [ ! -f "$MSSQL_INIT_DIR/instnwnd.sql" ]; then
    echo "📥 Automatically downloading official Microsoft Northwind for MSSQL..."
    curl -fsSL "https://raw.githubusercontent.com/microsoft/sql-server-samples/master/samples/databases/northwind-pubs/instnwnd.sql" -o "$MSSQL_INIT_DIR/instnwnd.sql"
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

# 3. Create MongoDB initialization script if missing (app_analytics + event_logs)
if [ ! -f "$MONGO_INIT_DIR/01-init.js" ]; then
    echo "⚙️ Creating MongoDB initialization seed script..."
    cat << 'EOF' > "$MONGO_INIT_DIR/01-init.js"
db = db.getSiblingDB('app_analytics');
const events = [
  {
    event_id: "evt_1001",
    user_email: "alice@example.com",
    customer_id: "CUST-001",
    event_type: "login",
    platform: "web",
    metadata: { browser: "Chrome", ip: "192.168.1.10" },
    timestamp: new Date("2026-03-01T10:15:00Z")
  },
  {
    event_id: "evt_1002",
    user_email: "alice@example.com",
    customer_id: "CUST-001",
    event_type: "feature_used",
    platform: "web",
    metadata: { feature_name: "export_report" },
    timestamp: new Date("2026-03-01T10:18:30Z")
  },
  {
    event_id: "evt_1003",
    user_email: "bob@example.com",
    customer_id: "CUST-002",
    event_type: "login",
    platform: "mobile",
    metadata: { device: "Google Pixel 10a" },
    timestamp: new Date("2026-03-02T14:20:00Z")
  }
];
db.event_logs.bulkWrite(events.map(event => ({
  updateOne: {
    filter: { event_id: event.event_id },
    update: { $setOnInsert: event },
    upsert: true
  }
})));
db.event_logs.createIndex({ user_email: 1 });
db.event_logs.createIndex({ customer_id: 1 });
EOF
fi

# 4. Create Oracle initialization script if missing (app_user schema + invoices table)
if [ ! -f "$ORACLE_INIT_DIR/01-init.sql" ]; then
    echo "⚙️ Creating Oracle initialization seed script..."
    cat << 'EOF' > "$ORACLE_INIT_DIR/01-init.sql"
    WHENEVER SQLERROR EXIT SQL.SQLCODE

    ALTER SESSION SET CONTAINER = FREEPDB1;

    DECLARE
        user_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO user_count FROM dba_users WHERE username = 'APP_USER';
        IF user_count = 0 THEN
            EXECUTE IMMEDIATE 'CREATE USER app_user IDENTIFIED BY "YourStrongPassword123!"';
        ELSE
            EXECUTE IMMEDIATE 'ALTER USER app_user IDENTIFIED BY "YourStrongPassword123!"';
        END IF;
    END;
    /

    GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO app_user;

    DECLARE
        table_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO table_count FROM all_tables
        WHERE owner = 'APP_USER' AND table_name = 'INVOICES';
        IF table_count = 0 THEN
            EXECUTE IMMEDIATE '
                CREATE TABLE app_user.invoices (
                    invoice_id VARCHAR2(50) PRIMARY KEY,
                    customer_email VARCHAR2(255) NOT NULL,
                    customer_id VARCHAR2(50) NOT NULL,
                    total_amount NUMBER(12, 2) NOT NULL,
                    status VARCHAR2(20) NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )';
        END IF;
    END;
    /

    MERGE INTO app_user.invoices target
    USING (
        SELECT 'INV-9001' invoice_id, 'alice@example.com' customer_email,
               'CUST-001' customer_id, 1250.00 total_amount, 'PAID' status,
               TIMESTAMP '2026-01-15 09:00:00 +00:00' created_at FROM dual
        UNION ALL
        SELECT 'INV-9002', 'bob@example.com', 'CUST-002', 450.50, 'PENDING',
               TIMESTAMP '2026-02-20 11:30:00 +00:00' FROM dual
        UNION ALL
        SELECT 'INV-9003', 'alice@example.com', 'CUST-001', 89.99, 'PAID',
               TIMESTAMP '2026-03-01 16:45:00 +00:00' FROM dual
    ) seed
    ON (target.invoice_id = seed.invoice_id)
    WHEN NOT MATCHED THEN
        INSERT (invoice_id, customer_email, customer_id, total_amount, status, created_at)
        VALUES (seed.invoice_id, seed.customer_email, seed.customer_id, seed.total_amount,
                seed.status, seed.created_at);

    COMMIT;
EOF
fi

if [ "$RESET_VOLUMES" = true ]; then
    echo "🧹 Resetting volumes (--reset flag detected)..."
    docker compose -f "$COMPOSE_FILE" down -v
fi

if ! docker network inspect reporthub_shared_network >/dev/null 2>&1; then
    echo "🔌 Creating shared Docker network..."
    docker network create reporthub_shared_network >/dev/null
fi

echo "🚀 Starting ReportHub test databases..."
"$SCRIPT_DIR/start_test_sftp.sh" --prepare-only
docker compose -f "$COMPOSE_FILE" up -d

# 5. Fully automated wait & import loop
echo "⏳ Waiting for databases to accept connections and auto-importing schemas..."

# Wait for PostgreSQL
wait_for_service "PostgreSQL" 90 2 \
    docker exec reporthub-postgres-test pg_isready -U test_user -d chinook
postgres_wal_level="$(
    docker exec reporthub-postgres-test psql -U test_user -d chinook -tAc "SHOW wal_level"
)"
if [ "$postgres_wal_level" != "logical" ]; then
    echo "   - Enabling PostgreSQL logical replication..."
    docker exec reporthub-postgres-test psql -U test_user -d chinook \
        -c "ALTER SYSTEM SET wal_level = 'logical';" >/dev/null
    docker compose -f "$COMPOSE_FILE" restart postgres-test >/dev/null
    wait_for_service "PostgreSQL restart" 90 2 \
        docker exec reporthub-postgres-test pg_isready -U test_user -d chinook
    postgres_wal_level="$(
        docker exec reporthub-postgres-test psql -U test_user -d chinook -tAc "SHOW wal_level"
    )"
fi
if [ "$postgres_wal_level" != "logical" ]; then
    echo "   ❌ PostgreSQL wal_level is '$postgres_wal_level', expected 'logical'." >&2
    exit 1
fi
echo "   ✅ PostgreSQL logical replication is enabled."

# Wait for MySQL
wait_for_service "MySQL" 90 2 \
    docker exec reporthub-mysql-test mysqladmin ping -h localhost -u test_user -p'test_password' --silent
echo "   - Ensuring MySQL CDC snapshot and replication privileges..."
docker exec reporthub-mysql-test mysql -uroot -proot_password \
    -e "GRANT RELOAD, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'test_user'@'%';"
echo "   ✅ MySQL CDC snapshot and replication privileges are ready."
mysql_customers_table="$(
    docker exec reporthub-mysql-test mysql -u test_user -ptest_password -N -s \
        -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='northwind' AND table_name='customers'" \
        2>/dev/null
)"
mysql_customer_count=0
if [ "$mysql_customers_table" = "1" ]; then
    mysql_customer_count="$(
        docker exec reporthub-mysql-test mysql -u test_user -ptest_password -N -s \
            -e "SELECT COUNT(*) FROM northwind.customers" 2>/dev/null
    )"
fi
if [ "$mysql_customer_count" = "0" ]; then
    echo "   - Importing MySQL Northwind schema and data..."
    docker exec -i reporthub-mysql-test mysql -u test_user -ptest_password \
        < "$MYSQL_INIT_DIR/1_schema.sql"
    docker exec -i reporthub-mysql-test mysql -u test_user -ptest_password \
        < "$MYSQL_INIT_DIR/2_data.sql"
    echo "   ✅ MySQL Northwind imported."
else
    echo "   ✅ MySQL Northwind already contains data; keeping it."
fi

# Wait for MSSQL & Import Northwind using the constraint-safe script
wait_for_service "Microsoft SQL Server" 180 2 \
    docker exec reporthub-mssql-test /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P 'YourStrongPassword123!' -Q "SELECT 1" -C

northwind_exists="$(
    docker exec reporthub-mssql-test /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P 'YourStrongPassword123!' -C -b -h -1 -W \
        -Q "SET NOCOUNT ON; SELECT CASE WHEN DB_ID('northwind') IS NULL THEN 0 ELSE 1 END" \
        | tr -d '[:space:]'
)"
if [ "$northwind_exists" = "0" ]; then
    echo "   - Creating and importing MSSQL Northwind..."
    docker exec reporthub-mssql-test /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P 'YourStrongPassword123!' -C \
        -Q "CREATE DATABASE northwind;"
    docker exec -i reporthub-mssql-test /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P 'YourStrongPassword123!' -C -d northwind \
        < "$MSSQL_INIT_DIR/instnwnd_final.sql" >/dev/null
    echo "   ✅ MSSQL Northwind imported."
elif [ "$northwind_exists" = "1" ]; then
    echo "   ✅ MSSQL Northwind already exists; keeping its data."
else
    echo "   ❌ Unexpected SQL Server database existence result: '$northwind_exists'." >&2
    exit 1
fi

if ! wait_for_service "SQL Server Northwind database" 180 2 \
    docker exec reporthub-mssql-test /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P 'YourStrongPassword123!' -C -b -l 5 \
        -d northwind -Q "SELECT 1"; then
    echo "   ❌ Northwind is not accepting connections; last connection attempt:" >&2
    docker exec reporthub-mssql-test /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P 'YourStrongPassword123!' -C -b -l 5 \
        -d northwind -Q "SELECT 1" >&2 || true
    exit 1
fi

wait_for_service "Microsoft SQL Server Agent" 90 2 \
    docker exec reporthub-mssql-test /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P 'YourStrongPassword123!' -C -b \
        -Q "IF NOT EXISTS (SELECT 1 FROM sys.dm_server_services WHERE servicename LIKE '%SQL Server Agent%' AND status_desc = 'Running') THROW 51000, 'SQL Server Agent is not running', 1;"
echo "   - Enabling SQL Server CDC for Northwind and its primary-key tables..."
docker exec -i reporthub-mssql-test /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrongPassword123!' -C -b -d northwind <<'SQL'
SET NOCOUNT ON;
IF NOT EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE name = DB_NAME() AND is_cdc_enabled = 1
)
    EXEC sys.sp_cdc_enable_db;

DECLARE @schema_name sysname;
DECLARE @table_name sysname;
DECLARE cdc_tables CURSOR LOCAL FAST_FORWARD FOR
    SELECT schemas.name, tables.name
    FROM sys.tables AS tables
    JOIN sys.schemas AS schemas ON schemas.schema_id = tables.schema_id
    WHERE tables.is_ms_shipped = 0
      AND tables.is_tracked_by_cdc = 0
      AND EXISTS (
          SELECT 1
          FROM sys.indexes AS indexes
          WHERE indexes.object_id = tables.object_id
            AND indexes.is_primary_key = 1
      );

OPEN cdc_tables;
FETCH NEXT FROM cdc_tables INTO @schema_name, @table_name;
WHILE @@FETCH_STATUS = 0
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = @schema_name,
        @source_name = @table_name,
        @role_name = NULL,
        @supports_net_changes = 0;
    FETCH NEXT FROM cdc_tables INTO @schema_name, @table_name;
END;
CLOSE cdc_tables;
DEALLOCATE cdc_tables;
SQL
echo "   ✅ SQL Server CDC is enabled for Northwind primary-key tables."

# Wait for MongoDB
wait_for_service "MongoDB" 90 2 \
    docker exec reporthub-mongo-test mongosh -u "test_user" -p "test_password" \
        --authenticationDatabase "admin" --eval "db.adminCommand('ping')"
echo "   - Applying MongoDB seed data..."
docker exec reporthub-mongo-test mongosh -u "test_user" -p "test_password" \
    --authenticationDatabase "admin" --file /docker-entrypoint-initdb.d/01-init.js >/dev/null

# Wait for Oracle (Oracle takes a couple of minutes to boot its internal services fully)
wait_for_service "Oracle Database Free" 90 5 \
    docker exec reporthub-oracle-test bash -lc \
        "echo 'SELECT 1 FROM DUAL;' | sqlplus -L -S 'sys/YourStrongPassword123!@localhost:1521/FREEPDB1 as sysdba'"
echo "   - Applying Oracle seed data and verifying app_user..."
docker exec reporthub-oracle-test sqlplus -L -S \
    'sys/YourStrongPassword123!@localhost:1521/FREEPDB1 as sysdba' \
    @/opt/oracle/scripts/startup/01-init.sql >/dev/null
docker exec -i reporthub-oracle-test sqlplus -L -S \
    'app_user/YourStrongPassword123!@localhost:1521/FREEPDB1' >/dev/null <<'SQL'
WHENEVER SQLERROR EXIT SQL.SQLCODE
SELECT COUNT(*) FROM invoices;
EXIT
SQL
echo "   ✅ Oracle is ready with app_user and seed data."

echo ""
echo "✅ All test databases are up, running, and fully populated automatically!"
echo "--------------------------------------------------"
echo "PostgreSQL (Chinook):    localhost:5433"
echo "MySQL (Northwind):       localhost:3307"
echo "Microsoft SQL Server:    localhost:1434"
echo "MongoDB (app_analytics): localhost:27018"
echo "Oracle Free (app_user):  localhost:1522"
echo "From ReportHub containers use postgres-test:5432, mysql-test:3306, mssql-test:1433,"
echo "  mongo-test:27017, and oracle-test:1521."
echo "--------------------------------------------------"