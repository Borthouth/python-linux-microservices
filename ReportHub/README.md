# ReportHub 🚀
> Enterprise-grade, dynamic multi-source analytical warehouse and reporting control plane.

`ReportHub` is an analytics platform designed to ingest, unify, and report on data from multiple distributed source databases (PostgreSQL, MySQL, Microsoft SQL Server, MongoDB, and Oracle) into a centralized analytical warehouse—**without hardcoding source-data models**. Built with a Django control plane, a **ClickHouse** analytical engine, **MinIO** S3-compatible object storage, and Celery-driven background tasks, ReportHub provides schema reflection, source-access configuration, scheduled task governance, asynchronous email notifications, and report generation.

---

## 🏗️ Core Architecture & Design

- **Decoupled Data Architecture:** The Django database stores control-plane configuration and source metadata, while ClickHouse stores ingested analytical data. Alembic is used for warehouse schema migrations.
- **ClickHouse Analytical Warehouse:** Columnar database engine for analytical queries and aggregations. Tables configured for incremental ingestion use `ReplacingMergeTree`; tables without a safe incremental key use `MergeTree`. Automated TTL data retention is not configured.
- **MinIO S3-Compatible Object Storage:** MinIO persists data in the host-mounted `./docker/minio_data` directory. Celery uploads report artifacts to MinIO; authenticated administrator downloads are served through Django. The Compose file publishes MinIO's console port and should be restricted appropriately outside development.
- **Session Security & Idle Timeout:** Hardened session management utilizing browser-close expiration (`SESSION_EXPIRE_AT_BROWSER_CLOSE = True`) and `django-auto-logout` with dynamic environment-driven idle timeout control (`SESSION_IDLE_MINUTES`).
- **Asynchronous Reporting:** Celery report tasks query ClickHouse and currently assemble export data in memory. Chunked/streaming report exports remain planned. Other background work uses dedicated Celery queues.
- **Django Control Plane & Standardized Shell Layout:** Centralized `base.html`, role-based access configuration, self-service profile management, source database profiles, scheduled ETL task management, and user widget preferences. Collaborative workspaces and report sharing links are not currently implemented.
- **Audit History:** Selected control-plane models use historical records, pipeline automation changes have a dedicated audit log, and `/control-plane/audit/` displays available configuration history. Separate email-delivery and failed-task (DLQ) audit/management pages are also available; a consolidated multi-domain audit suite is not implemented.
- **Dead Letter Queue (DLQ) & Background Task Governance:** Redis-backed storage (`dlq:task:*`) requiring JSON decoding, attribute normalization, and robust timestamp parsing via `django.utils.dateparse`. Fully searchable and paginated management UI (`/control-plane/dlq/`) with matched container widths (`max-w-7xl`) and modular dashboard grid widgets for tracking, inspecting stack traces, and re-queuing failed background tasks across the 7-tier Celery infrastructure.
- **Asynchronous Email Delivery & Status Auditing:** Celery-driven background email dispatches backed by Django's email backend and a local development test email server. Includes an **Email Delivery Audit & Governance Module** tracking delivery lifecycles (`sent`, `delivered`, `failed`, `bounced`), retry counts, error tracebacks, inspection modals, and re-queue/delete workflows.
- **Asynchronous UI Status & Toast Notification Architecture:** Real-time database connection testing driven by HTMX polling (`every 2s`), backed by CSP-compliant event-driven (`htmx:afterSwap`) floating toast notifications and robust fallback UI states ("Ready", "Running...", "Failed").
- **Dynamic Warehouse Provisioning:** Rather than hardcoding models, ReportHub inspects source databases (`information_schema` / catalog views), allows selective field mapping via an interactive master-detail UI, and provisions analytical tables with source namespacing and logical foreign-key metadata.
- **Multi-Source Unification:** Ingested data is namespaced by source, with `source_id` included in the warehouse schema for source identification.
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

ReportHub includes an isolated multi-database test harness in `tests/docker-compose.test.yml` and a one-command setup script at `tests/start_test_dbs.sh`. The script starts the test databases, waits for them to accept connections, imports Northwind into MSSQL, and reapplies the MongoDB and Oracle seeds safely on every run. PostgreSQL and MySQL run their initialization scripts when their volumes are first created. Test volumes persist between runs.

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

### Starting and stopping test databases
```bash
# From the repository root: start and populate the test databases
./tests/start_test_dbs.sh

# Stop containers but preserve their database volumes
docker compose -f tests/docker-compose.test.yml down

# Optional: remove test containers and erase all their data, then start fresh
./tests/start_test_dbs.sh --reset
```

## ClickHouse ingestion and backfills

Scheduled ingestion processes at most `DEFAULT_ETL_BATCH_SIZE` rows per Celery task (already set to `10000` in the project `.env`), then queues a continuation so other sources can run between batches. The same row limit applies to backfills. To change it, update `.env` and restart the app and Celery workers so they reload the setting. Celery prefetch is set to 1 to reduce workers reserving batches ahead of time.

For routine incremental ingestion, first sync the source schema, then open **Warehouse Mapping** and choose a date/time **Modified-at field** for each table. A single or composite primary key must also be discovered by introspection. The first load (and any load after the warehouse field selection changes) is a bounded full refresh. Later scheduled loads query rows at or after the saved per-table modification-time watermark, through a fixed upper bound, and upsert them into a ClickHouse `ReplacingMergeTree` keyed by the primary key. Reports read these tables with `FINAL` so they see deduplicated current rows. The chosen modified-at field must be updated whenever a source row changes; tables without a suitable timestamp or primary key use bounded full refreshes instead.

The warehouse field selections saved in **Warehouse Mapping** control the ClickHouse columns. When selections change, run **Backfill All Active Sources to ClickHouse** from Pipeline Automations to rebuild the selected ClickHouse columns and reload their historical values for every active source. Backfill batches use the dedicated `queue_backfill` queue; routine ingestion uses `queue_hot`. Both yield between bounded tasks so other queued work can run.

## 🤖 Local AI Report Design

The report form can use Ollama to draft a report configuration from a natural-language request and the selected source's introspected table, field, and direct foreign-key metadata. A prompt can request Tabular Detail or Grouped Summary, choose fields and labels, assign supported aggregates, and configure summary totals. The model returns a constrained JSON design; ReportHub validates it and its existing query builder generates ClickHouse SQL. Source row data and arbitrary SQL are not sent to or accepted from the model.

The AI draft currently does not configure charts, export format, sorting, email distribution, or AI-summary inclusion; configure those separately in the form. A requested sort order in the AI prompt is not applied.

PDF, Excel, and JSON reports can optionally include an AI-generated summary based on aggregate statistics calculated from the report result rows; individual source row values are not sent to Ollama. For Grouped Summary reports, those statistics describe the grouped output, not the underlying source records. Excel places the summary near the top of the Report sheet with a blank row above and below it, and JSON includes it as a top-level `ai_summary` property. If a PDF is truncated, the summary is limited to the rows included in that PDF. Enable **Include AI summary** while creating or editing a report configured for PDF, Excel, or JSON export.

Note: grouped-query aggregates are currently displayed under the selected field label (for example, `Track ID` for `COUNT(track_id)`). Choose a descriptive label such as `Track Count` when configuring the field.

Ollama is included in the main Compose stack. It is not published to the host, and model files persist in the `ollama_data` Docker volume.

```bash
# Start the full ReportHub stack, including Ollama
docker compose up -d

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
- **Async & ETL:** Celery, Redis (Configured with AOF persistence enabled)
- **Analytical & Object Storage:** ClickHouse (Columnar Data Warehouse), MinIO (S3-Compatible Object Storage via `django-storages` and `boto3`), PostgreSQL 15 (Operational & Warm Storage)
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
├── alembic/              # Standalone Alembic migrations for analytical database state
├── tests/                # Polyglot test database environments & sandbox configs
├── Dockerfile            # Container build instructions (including MS ODBC drivers)
├── docker-compose.yml    # Service orchestration (Web, Postgres, Redis, ClickHouse, MinIO, Celery)
├── docker-compose.test.yml # Polyglot test databases & local test email server (Mailpit)
├── test.sh               # Automated test execution script harness
├── requirements.txt      # Python dependencies
├── entrypoint.sh         # Container startup & auto-migration script
├── ROADMAP.md            # Project implementation tracker & audit cleanup checklist
└── README.md             # Project documentation
```