# ReportHub 🚀
> Enterprise-grade, dynamic multi-source analytical warehouse and reporting control plane.

`ReportHub` is an analytics platform designed to ingest, unify, and report on data from multiple distributed source databases (PostgreSQL, MySQL, Microsoft SQL Server, MongoDB, and Oracle) into a centralized analytical warehouse—**without hardcoding source-data models**. Built with a Django control plane, a **ClickHouse** analytical engine, **MinIO** S3-compatible object storage, and Celery-driven background tasks, ReportHub provides schema reflection, source-access configuration, scheduled task governance, asynchronous email notifications, and report generation.

---

## 🏗️ Core Architecture & Design

- **Decoupled Data Architecture:** The Django database stores control-plane configuration and source metadata, while ClickHouse stores ingested analytical data. Ingestion provisions ClickHouse tables from source metadata. A separate Alembic configuration and an empty initial revision exist, but they do not manage those ClickHouse tables.
- **ClickHouse Analytical Warehouse:** Columnar database engine for analytical queries and aggregations. CDC-managed tables use versioned `ReplacingMergeTree` engines with delete tombstones; batch-mode sources and CDC tables without a discovered primary key use bounded full refreshes. Automated TTL data retention is not configured.
- **MinIO S3-Compatible Object Storage:** MinIO persists data in the host-mounted `./docker/minio_data` directory. Celery uploads report artifacts to MinIO; authenticated administrator downloads are served through Django. The Compose file publishes MinIO's console port and should be restricted appropriately outside development.
- **Session Security & Idle Timeout:** Hardened session management utilizing browser-close expiration (`SESSION_EXPIRE_AT_BROWSER_CLOSE = True`) and `django-auto-logout` with dynamic environment-driven idle timeout control (`SESSION_IDLE_MINUTES`).
- **Asynchronous Reporting:** Celery report tasks query ClickHouse and currently assemble export data in memory. Chunked/streaming report exports remain planned. Other background work uses dedicated Celery queues.
- **Django Control Plane & Standardized Shell Layout:** Centralized `base.html`, role-based access configuration, self-service profile management, source database profiles, scheduled ETL task management, and user widget preferences. Collaborative workspaces and report sharing links are not currently implemented.
- **Audit History:** Selected control-plane models use historical records, pipeline automation changes have a dedicated audit log, and `/control-plane/audit/` displays available configuration history. Separate email-delivery and failed-task (DLQ) audit/management pages are also available; a consolidated multi-domain audit suite is not implemented.
- **Dead Letter Queue (DLQ) & Background Task Governance:** Redis-backed storage (`dlq:task:*`) requiring JSON decoding, attribute normalization, and robust timestamp parsing via `django.utils.dateparse`. Fully searchable and paginated management UI (`/control-plane/dlq/`) with matched container widths (`max-w-7xl`) and modular dashboard grid widgets for tracking, inspecting stack traces, and re-queuing failed background tasks across the 7-tier Celery infrastructure.
- **Active pipeline telemetry:** The dashboard shows the five most recent tasks last reported as running. The full searchable, paginated list is under **Administration > Operations & History > Active Pipeline Executions**, with a fixed current-step filter; table and collection names can be found using search. Redis progress records are not a live worker census: interrupted or completed batch tasks may remain marked running until their records expire.
- **Active report generations:** The dashboard shows the five newest report executions recorded as running. The full list is searchable by report name, filterable by current step, and paginated under **Administration > Operations & History > Active Report Generations**. Steps reflect last-saved progress; an unrecognized progress value is shown as Unknown. A recorded running status does not guarantee that its worker is still running.
- **Asynchronous Email Delivery & Status Auditing:** Celery-driven background email dispatches backed by Django's email backend and a local development test email server. Includes an **Email Delivery Audit & Governance Module** tracking delivery lifecycles (`sent`, `delivered`, `failed`, `bounced`), retry counts, error tracebacks, inspection modals, and re-queue/delete workflows.
- **Asynchronous UI Status & Toast Notification Architecture:** Real-time database connection testing driven by HTMX polling (`every 2s`), backed by CSP-compliant event-driven (`htmx:afterSwap`) floating toast notifications and robust fallback UI states ("Ready", "Running...", "Failed").
- **Dynamic Warehouse Provisioning:** Rather than hardcoding models, ReportHub inspects source databases (`information_schema` / catalog views), allows selective field mapping via an interactive master-detail UI, and provisions analytical tables with source namespacing and logical foreign-key metadata.
- **Multi-Source Ingestion:** ClickHouse table names include the source ID for source identification. Reports can join directly related tables within one source; cross-source linking and reporting are planned, not implemented.
- **Security & Access Configuration:**
  1. *Source Access Assignments:* `UserSourceAccess` stores per-user source assignments and roles. Consistent enforcement across all operations, workspace access, and tenant-level row filtering still require verification or implementation.
  2. *Role-Based Access Control (RBAC):* Viewer, Analyst/Builder, and System Admin roles (`is_admin`).
  3. *Tenant Row-Level Security:* Company-scoped `company_id` query injection is not currently implemented.
  4. *Security Hardening:* Includes `django-csp`, HttpOnly session/CSRF cookies, and configured Django security headers. Compose publishes database, Redis, ClickHouse, and MinIO-console ports to the host; restrict them with firewall/network settings for deployments.
- **Operational Audit Views:** Available views include configuration history, DLQ task management, email delivery audit, and pipeline/report execution histories. Dedicated file-transfer, consolidated ETL metrics, and login-security audit views remain future work.
- **Report Executions & Artifact Downloads:** Track report execution status and history (`ReportExecution`), store completed exports in MinIO, and provide authenticated downloads to administrators.
- **Visual Report Exports:** Configure bar, line, or pie charts from report columns and include them in PDF and Excel exports; Excel workbooks also include a filtered data sheet and a separate chart.
- **Tabular and Grouped Reports:** Tabular Detail returns selected source records. Grouped Summary groups by selected columns without an aggregate and applies `COUNT`, `SUM`, `AVG`, `MIN`, or `MAX` to aggregated columns; selecting aggregates for every column produces one overall summary row.
- **Readable PDF Exports:** Wide reports use landscape layout, wrapped cells, and repeated table headers. PDF output is capped to a configured row limit.

---

## 🧪 Polyglot Test Database & Test Email Server Environment

ReportHub includes an isolated multi-database test harness in `tests/docker-compose.test.yml` and a one-command setup script at `tests/start_test_dbs.sh`. The script starts the test databases, waits for them to accept connections, enables PostgreSQL logical replication, grants MySQL's Debezium snapshot and replication privileges, imports Northwind into MSSQL, and reapplies the MongoDB and Oracle seeds safely on every run. MongoDB runs as a standalone server. PostgreSQL and MySQL run their initialization scripts when their volumes are first created. Test volumes persist between runs.

### Test Database Connection Matrix

| Database Engine | Host on this machine | Port | Credentials (User / Password) | Target Database / Service Name |
| :--- | :--- | :--- | :--- | :--- |
| **PostgreSQL** | `localhost` | `5433` | `test_user` / `test_password` | `chinook` |
| **MySQL** | `localhost` | `3307` | `test_user` / `test_password` | `northwind` |
| **Microsoft SQL Server** | `localhost` | `1434` | `sa` / `YourStrongPassword123!` | `northwind` |
| **MongoDB** | `localhost` | `27018` | `test_user` / `test_password` | `app_analytics` (auth DB: `admin`) |
| **Oracle** | `localhost` | `1522` | `app_user` / `YourStrongPassword123!` | `FREEPDB1` |
| **Test Email Server** | `localhost` | `1025` / `8025` | *None (Development SMTP/Web UI)* | `mailpit` / local SMTP sink |

When ReportHub itself runs in Docker, use the Compose service hostnames and internal ports instead: `postgres-test:5432`, `mysql-test:3306`, `mssql-test:1433`, `mongo-test:27017`, and `oracle-test:1521`.

If the test MongoDB container previously ran with replica-set options, recreate it with `docker compose -f tests/docker-compose.test.yml up -d mongo-test` to apply the standalone configuration; this preserves the data volume. For existing MongoDB Debezium connectors, stop or remove those connectors in Kafka Connect to avoid leaving them running. Restart ingestion workers after updating the application, and allow the next MongoDB refresh to replace existing CDC tables.

### Starting and stopping test databases
```bash
# From the ReportHub project directory: start and populate the test databases
./tests/start_test_dbs.sh

# Stop containers but preserve their database volumes
docker compose -f tests/docker-compose.test.yml down

# Optional: remove test containers and erase all their data, then start fresh
./tests/start_test_dbs.sh --reset
```

## ClickHouse ingestion and backfills

Each new data source has an **Ingestion mode**: **Batch refresh** (the default, no Kafka required) or **CDC** (requires Kafka, Debezium, and supported source configuration). Existing relational sources retain their previous CDC behavior when migrating; existing MongoDB sources retain batch refresh. The mode cannot yet be changed after a source is created: safe transitions between existing batch and CDC tables require a coordinated snapshot and cutover. MongoDB CDC is not currently supported.

Scheduled ingestion consumes at most `DEFAULT_ETL_BATCH_SIZE` CDC events per Celery task (default `10000`) and then returns so other queued work can run. CDC batches are serialized per source with a Redis lock; concurrent requests are requeued. In CDC mode relational tables with discovered primary keys use Debezium change events; tables without a primary key use bounded full-refresh batches. Batch-mode sources use bounded full refreshes for all tables. Backfills use the same row limit and the dedicated `queue_backfill` queue, and are skipped for CDC sources with primary-key tables because a safe rebuild requires a coordinated CDC snapshot. Celery prefetch is set to 1.

Start the CDC services alongside ReportHub:

```bash
docker compose -f docker-compose.yml -f docker-compose.cdc.yml up -d
```

The Compose overlay gives the app and its Celery ingestion worker these defaults:

```dotenv
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_CONNECT_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_CONNECT_URL=http://debezium-connect:8083
```

Compose reads those values from the project `.env` file and passes them into the app and ingestion worker. `KAFKA_BOOTSTRAP_SERVERS` is where ReportHub's Kafka consumer connects. `KAFKA_CONNECT_BOOTSTRAP_SERVERS` is the broker address Debezium Connect uses, including for relational connector schema history; it must be reachable from the Connect container. `KAFKA_CONNECT_URL` is the Connect REST API address ReportHub uses to create and check connectors. The defaults above work when using the bundled services on the Compose network.

The CDC overlay uses Debezium Connect 3.5.2.Final. Before upgrading an existing deployment, review Debezium's migration notes and validate each connector and its offsets; a running connector alone does not establish that warehouse ingestion is working. Oracle CDC additionally requires compatible CDB/PDB connection settings, a suitably privileged LogMiner account, and supplemental logging.

Oracle source profiles keep **Database / service name**, **Username**, and **Password** for ReportHub's direct source connection and schema sync. For Oracle CDC, optionally configure both **Oracle CDB name** and **Oracle PDB name**; these map to Debezium's `database.dbname` and `database.pdb.name`. Leaving both blank preserves the existing service-name connector configuration. Optionally configure both **LogMiner username** and **LogMiner password** to give Debezium a separate account; these map to `database.user` and `database.password`. Leaving both blank makes Debezium continue using the source username/password. The LogMiner password is encrypted in ReportHub and is kept when its edit field is left blank. The CDB/PDB values and LogMiner credentials must each be configured as a pair. SQLAlchemy-normalized lowercase Oracle schema/table identifiers are converted to uppercase for Debezium topic/filter matching; mixed-case quoted identifiers are preserved. Oracle event fields and keys match metadata casing exactly first, then use an uppercase fallback only for lowercase metadata names. If that fallback would match identifiers with ambiguous casing, CDC fails explicitly rather than selecting one.

To deploy this configuration, deploy the application code and apply the new control-plane migration in the application's environment (this does not modify the Oracle database):

```bash
python manage.py migrate control_plane 0025_oracle_cdb_pdb_logminer
```

Then edit the Oracle source profile and save the CDB/PDB values and, if needed, the separate LogMiner account. Restart the Django application and Celery ingestion worker so they load the new code, then run the normal ingestion schedule/task to update the existing Debezium connector. Confirm the connector and its task are `RUNNING` in Kafka Connect and verify that changes arrive in the warehouse. Configure Oracle archive logging, supplemental logging, and required user privileges separately with the Oracle administrator; the application does not grant those privileges or perform an initial snapshot/rebuild.

If running Django or Celery directly on the host while Kafka and Connect remain in Compose, use `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` and `KAFKA_CONNECT_URL=http://localhost:8083` for that host process; keep `KAFKA_CONNECT_BOOTSTRAP_SERVERS=kafka:9092` because Debezium Connect itself still uses the Docker-network broker address. Do not put the host-only `localhost` values in a shared `.env` file used to configure Compose containers: inside a container, `localhost` points back to that container. The bundled single-node Kafka configuration is for local development; production deployments should use a secured, replicated Kafka cluster and Connect workers, then set the broker and REST addresses to the endpoints reachable from each service.

After source schema sync discovers a relational primary key, scheduled ingestion registers or updates one Debezium connector per source. Debezium performs an initial snapshot, then tails native database change streams/logs. Events are inserted into ClickHouse `ReplacingMergeTree` tables; each event's Kafka offset is its replacement version, and delete events create tombstones that ReportHub filters from report queries. A key remains on one Kafka partition, so its offsets preserve row-change order without relying on source clock precision. Kafka offsets are committed only after the ClickHouse batch succeeds, so a retry may replay records; ClickHouse replacement semantics make replays idempotent. Debezium and the source database must retain enough log data to cover connector outages.

An existing non-CDC warehouse table is not automatically converted to CDC: ingestion fails instead of dropping the table. If a CDC-managed warehouse table is missing, ingestion also fails rather than recreating an empty table and resuming from old offsets. Both situations require a coordinated initial snapshot and cutover, which is not yet implemented.

In the local test environment, the PostgreSQL and Oracle warehouse tables were rebuilt from retained snapshot events and source-to-warehouse inserts, updates, and deletes were verified. This was a one-off test-data procedure, not a supported automatic migration or a production recovery procedure. Historical CDC failure entries can remain visible after recovery.

MongoDB ingestion does not use change streams or require replication. Each scheduled run reads every collection in bounded pages into a staging table, then swaps it into place. Inserts, updates, and deletes become visible after a successful refresh. This is not a point-in-time snapshot: concurrent source changes can affect a multi-page scan, and repeated scans increase source load.

CDC is not zero-overhead: it uses source log/stream capacity, Kafka storage, connector CPU, and source-side privileges/configuration. It avoids repeated whole-table scans and does not require a modified-at column, but tables need a primary key. The initial snapshot reads source rows once. Required source configuration includes:

| Source | CDC prerequisites |
|---|---|
| MySQL | Row-based binary logging with full row images, a unique server ID, and a CDC account with replication and snapshot-read privileges. |
| PostgreSQL | `wal_level=logical`, an available logical replication slot, and a CDC account with replication and table-read privileges. |
| SQL Server | SQL Server CDC enabled on the database and captured tables, plus a CDC-capable account. |
| Oracle | Archive logging, supplemental logging, LogMiner access, and the Debezium-required catalog/LogMiner privileges. |

Administrators can view recorded CDC execution failures on the dashboard and in **Pipeline Execution History**. The dashboard widget can be hidden under **Customize Widgets**. Entries record the source, time, task ID, and exception type; use the task ID to locate details in the ETL worker logs. Entries represent failures recorded after this feature is deployed, not the current health of a connector or older executions.

The warehouse field selections saved in **Warehouse Mapping** control the ClickHouse columns. Batch-mode sources and CDC-mode relational tables without a discovered primary key use scheduled bounded full refreshes. Full-refresh backfills are rejected for CDC sources containing a primary-key table: replacing a CDC table while changes continue would require a coordinated Debezium snapshot and offset barrier to avoid losing updates. New columns are added to CDC-managed warehouse tables, but existing rows are not repopulated; use a coordinated Debezium snapshot before relying on newly selected fields for historical data.

Potential future alternatives to full refresh include explicitly configured append-only ID watermarks and database-specific row-version tracking. These need safeguards for transaction ordering, retries, updates, and deletes; choosing a numeric column alone is not sufficient. See [ROADMAP.md](./ROADMAP.md) for the planned strategy and cross-source reporting work.

## 🤖 Local AI Report Design

The report form can use Ollama to draft a report configuration from a natural-language request and the selected source's introspected table, field, and direct foreign-key metadata. A prompt can request Tabular Detail or Grouped Summary, choose fields and labels, assign supported aggregates, and configure summary totals. The model returns a constrained JSON design; ReportHub validates it and its existing query builder generates ClickHouse SQL. Source row data and arbitrary SQL are not sent to or accepted from the model.

The AI draft currently does not configure charts, export format, sorting, email distribution, or AI-summary inclusion; configure those separately in the form. A requested sort order in the AI prompt is not applied.

PDF, Excel, and JSON reports can optionally include an AI-generated summary based on aggregate statistics calculated from the report result rows; individual source row values are not sent to Ollama. For Grouped Summary reports, those statistics describe the grouped output, not the underlying source records. Excel places the summary near the top of the Report sheet with a blank row above and below it, and JSON includes it as a top-level `ai_summary` property. If a PDF is truncated, the summary is limited to the rows included in that PDF. Enable **Include AI summary** while creating or editing a report configured for PDF, Excel, or JSON export.

Note: grouped-query aggregates are currently displayed under the selected field label (for example, `Track ID` for `COUNT(track_id)`). Choose a descriptive label such as `Track Count` when configuring the field.

Ollama is included in the main Compose stack. It is not published to the host, and model files persist in the `ollama_data` Docker volume.

```bash
# Start the full ReportHub and CDC stack, including Ollama
docker compose -f docker-compose.yml -f docker-compose.cdc.yml up -d

# Download the default model into the persistent Ollama volume (initial setup only)
docker exec reporthub-ollama ollama pull qwen2.5:3b
```

Set these variables in the project-root `.env` file to override the defaults:

```dotenv
OLLAMA_BASE_URL=http://ollama:11434
OLLAMA_MODEL=qwen2.5:3b
OLLAMA_TIMEOUT=120
```

`OLLAMA_BASE_URL` can point to an existing Ollama server reachable from the app container; the bundled Ollama service remains part of the default stack. If using a remote endpoint, secure its network access and use TLS where available. Prompts and schema metadata are sent to the configured endpoint, along with report-result aggregate statistics when AI summaries are enabled for PDF, Excel, or JSON; individual source row values are not. Model downloads require network access during setup; avoid `docker compose down -v` if you want to retain the bundled model files.

---

## 🛠️ Tech Stack

- **Backend:** Python, Django (Synchronous Control Plane), Hypercorn / Gunicorn ASGI/WSGI
- **Async & ETL:** Celery, Redis (Configured with AOF persistence enabled), Kafka, and Debezium Connect
- **Analytical & Object Storage:** ClickHouse (Columnar Data Warehouse), MinIO (S3-Compatible Object Storage via `django-storages` and `boto3`), PostgreSQL 15 (Django control-plane database)
- **Source Integration & Drivers:** PostgreSQL (`psycopg2`), MySQL (`pymysql`), Microsoft SQL Server (`pyodbc` with Microsoft ODBC Driver 18 for Debian/Linux), MongoDB (`pymongo`), and Oracle (`python-oracledb`)
- **Testing & Quality:** Pytest, Playwright (for automated web application testing & debugging)
- **Frontend:** HTMX, Tailwind CSS, Alpine.js, Custom Components, Modern Vanilla JavaScript (Event-Driven DOM handlers)
- **Infrastructure:** Docker, Docker Compose, RHEL 9 compatible deployment targets

---

## 📂 Project Structure

```text
reporthub/
├── app/                  # Core application configuration & database sessions
├── config/               # Django project settings, URLs, WSGI/ASGI, and Celery setup
├── control_plane/        # Django app: Users, RBAC, Source Profiles, Audit Logs, DLQ, Email Audit, Preferences
├── warehouse/            # Dynamic schema reflection & table provisioner utilities
├── etl/                  # Celery tasks (Batch ingestion, roll-ups, backfilling, cleanup)
├── reports/              # Report generation engine, templates, execution logs, and MinIO storage pipelines
├── alembic/              # Standalone Alembic configuration and initial empty revision
├── tests/                # Test database Compose file, setup script, and seed data
├── Dockerfile            # Container build instructions (including MS ODBC drivers)
├── docker-compose.yml    # Service orchestration (Web, Postgres, Redis, ClickHouse, MinIO, Celery)
├── docker-compose.cdc.yml # Kafka and Debezium Connect overlay
├── requirements.txt      # Python dependencies
├── entrypoint.sh         # Container startup & auto-migration script
├── ROADMAP.md            # Project implementation tracker & audit cleanup checklist
└── README.md             # Project documentation
```