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
  Staff users can view the five latest audit records on the dashboard and toggle the widget under **Customize Widgets**; the widget links to the full audit page.
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

ReportHub includes an isolated multi-database test harness in `tests/docker-compose.test.yml` and a one-command setup script at `tests/start_test_dbs.sh`. The script starts the test databases, waits for them to accept connections, enables PostgreSQL logical replication, grants MySQL's Debezium snapshot and replication privileges, imports Northwind into MSSQL, enables SQL Server Agent and CDC on Northwind's primary-key tables, and reapplies the MongoDB and Oracle seeds safely on every run. MongoDB runs as a standalone server. PostgreSQL and MySQL run their initialization scripts when their volumes are first created. Test volumes persist between runs.

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

Scheduled CDC ingestion, its continuations, and CDC rebuilds use `queue_cdc` on the dedicated `celery_cdc_worker`. Scheduled batch refreshes, keyless CDC refreshes, backfills, and the internal CDC-to-batch staging task use `queue_backfill` on `celery_batch_worker`. Interactive and legacy `queue_hot` tasks remain on `celery_ux_worker`; `celery_warm_worker` consumes `queue_warm`. Set `CDC_WORKER_CONCURRENCY` and `BATCH_WORKER_CONCURRENCY` in the Compose environment to tune worker slots (default 2 each). Per-source CDC locking still applies; extra slots primarily help different sources.

CDC-to-batch staging is internal and does not expose a switch or start cutover. It stages one whole table per task, then revalidates the table list and staged UUIDs and schemas. Checkpoint statuses record staging progress; they are not proof that a future cutover is safe. A failed or interrupted task needs an explicit retry of the staging task for the existing transition; there is no automatic crash recovery yet. Interrupted attempts can leave separate ClickHouse tables for manual cleanup. Long tables hold a checkpoint transaction while staging, and connector streaming and large-table performance still need validation.

Staging validation compares the actual ClickHouse engine, sorting, partition, primary and sampling keys, column names, order, types and default expressions with the expected table after staging, during retries and when recording the active table UUID. A mismatch fails validation and requires investigation; it is not automatically repaired. These checks do not prove row completeness or protect against changes after validation, so the future cutover executor must revalidate under a complete fence.

CDC ingestion remains enabled during staging. Each ingestion task now holds the control-plane source row lock while it acquires the Redis lock, checks source mode and cutover status, and processes its batch. This serializes healthy database sessions for the same source even if the Redis lease expires; a future cutover executor must acquire the same source row lock before marking cutover pending. Queued CDC batches skip while cutover is pending. Holding the row lock across connector and ClickHouse work can delay other source-row updates. A database connection loss, external side effects, and Redis lease ownership still require a complete fencing and recovery design. No existing operation marks a transition cutover pending or performs that switch.

CDC-mode batch refresh tasks, including keyless-table fallback, hold the same control-plane source row lock for each bounded task and recheck source mode and cutover status under that lock. A cutover executor must acquire that lock before marking cutover pending so it waits for an in-flight refresh. Queued refreshes then skip while cutover is pending; a stale keyless continuation is rejected after the source switches to batch. Each refresh holds a database transaction while accessing the source and ClickHouse, so long-running tasks can delay other source-row updates. Database connection loss still requires a complete fencing design; these guards do not implement cutover.

Scheduled ingestion, backfill dispatch, retries and continuations carry their originating source mode; a task from another mode is rejected. Legacy unmarked tasks still run for sources without a completed transition, but are rejected for batch sources with a successful CDC-to-batch transition because their origin is ambiguous. A future transition executor must commit the mode change and successful transition status together under the source lock, retain the successful transition record, and ensure all producers and workers use this version before switching. Any external callers submitting ingestion or backfill tasks for a transitioned source must supply the current `origin_mode`. These checks do not protect against lost database connections, external side effects or future mode-switch cycles.

The internal cutover checkpoint distinguishes an unchecked active table from an observed missing one. An internal preparation operation can record the active table UUID after checking the staged UUID and configuration; it rejects a missing CDC-managed table. Re-observation rejects changed identities rather than replacing the original checkpoint. UUID decision logic can identify a pending exchange or rename, recognize one completed before a crash, and reject unexpected table identities. These preparation observations may become stale while CDC or keyless refreshes continue: a future executor must revalidate them under a complete fence. No operation performs the exchange, establishes that fence, or switches the source yet.

Local staging tests compared source rows with staging for PostgreSQL, MySQL, SQL Server, and Oracle. A PostgreSQL two-table run was staged through the batch worker, and an isolated worker exit after a PostgreSQL staging insert was recovered by manually retrying into a fresh table. In a separate PostgreSQL test, updates, deletes, and inserts committed during a table read were absent from that staged snapshot and appeared in a fresh staging run; the existing live CDC table UUID and source mode remained unchanged. This is a result for that driver and configuration, not a cross-database or source-wide snapshot guarantee. Other concurrent-write cases, crash boundaries, production-scale tables, and the unfinished cutover remain to be validated.

An automated concurrency regression pauses a staging insert while a CDC ingestion task processes simulated insert, update, and delete events. It checks that CDC completes against a separate live table and staging remains independent. This uses a real control-plane database transaction but mocked Kafka and ClickHouse clients and a SQLite source; it does not replace end-to-end testing with live CDC infrastructure.

In a local PostgreSQL test, staging was paused after an actual ClickHouse insert while test-row updates, deletes, and inserts flowed through Debezium, Kafka, and the CDC worker to the live table. The live table reflected the changes before staging resumed; the staged table retained the earlier snapshot, and the live table UUID and source mode were unchanged. Test rows and temporary transition records were removed; the staging table was retained for manual cleanup. Staging ran in a separate application process, not through the batch worker. This validates one local overlap scenario, not cutover safety or other drivers.

Deploy queue changes to all producers and workers together. Let active tasks finish before replacing workers, start the new CDC and batch workers, and keep `queue_hot` and `queue_backfill` consumed while older queued tasks drain. Older CDC jobs on `queue_hot` can run once on the UX worker; their CDC continuations move to `queue_cdc`. Keep Kafka configuration on that worker during migration. Monitor queue depth, CDC failures and Kafka lag; a stopped worker leaves its queue unprocessed. The base Compose stack includes both new workers even without the CDC overlay, so batch-only deployments still process refreshes. CDC sources also require the overlay or equivalent Kafka/Connect configuration.

### Rebuilding one CDC-managed table

For a CDC-managed relational table with a discovered primary key, use the Django admin **Source tables** action **Safely rebuild selected CDC warehouse tables**. The rebuild creates a dedicated Debezium connector and Kafka topic generation, loads a staging ClickHouse table, waits for Debezium's successful snapshot notification and drains the snapshot offsets, then cuts over under the normal source-ingestion lock. The durable rebuild record and its status are available in the read-only **CDC table rebuilds** admin view; records cannot be created, edited, or deleted there. Do not drop or recreate the active ClickHouse table manually.

If a rebuild remains **Cutover pending**, inspect its `last_error` and resolve the underlying problem, then run the same action again on that source table to resume it. If ClickHouse already exchanged the table, normal CDC ingestion for the source fails safely until the rebuild is resumed; it does not automatically retry a failed rebuild. A source lock left by an interrupted worker may take up to 15 minutes to expire. After a worker crash, an immediate retry may fail to acquire Kafka partitions until the previous consumer's group session expires; retry the rebuild once the group has rebalanced.

After a successful exchange, the prior ClickHouse table remains under the rebuild record's `staging_table` name; it is not automatically removed. Superseded per-table connectors and connectors created by failed rebuilds are also retained, potentially still running and producing Kafka events. This can consume connector capacity and Kafka storage; PostgreSQL replication slots may retain WAL even if a connector stops. Monitor connector health, Kafka retention and disk usage, and have an operator verify that a connector, slot, publication, or retained table is no longer needed before cleaning it up. Do not remove resources belonging to an active connector or an unfinished rebuild. An administrator-initiated cleanup workflow has not yet been implemented.

The ClickHouse database must use the `Atomic` engine. Apply Django migrations and restart Celery workers when deploying this feature so workers load the rebuild model and task. Local single-table rebuilds and post-cutover insert/update/delete flows have been verified with SQL Server `northwind.dbo.Categories`, PostgreSQL `chinook.public.artist`, MySQL `northwind.customers`, and Oracle `APP_USER.invoices`. MySQL tests also paused a rebuild with a snapshot backlog and source writes, and terminated isolated test workers after ClickHouse exchange and after saving the snapshot checkpoint but before committing Kafka offsets; both rebuilds were manually resumed. The post-exchange crash test shortened the source lock to 45 seconds; production uses a 15-minute lock. Other crash boundaries, broader concurrency cases, and deployment-specific source permissions still require further validation. Source-wide batch/CDC mode transitions are not implemented.

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

If a connector or task has failed, the next scheduled ingestion attempts one restart without resetting its offsets. If it does not recover within 30 seconds, ingestion fails and the Kafka Connect and source database logs should be checked. Connector and task states alone do not prove that source changes have reached ClickHouse.

The local test environment has verified insert, update, and delete propagation to ClickHouse for PostgreSQL, MySQL, Oracle, and SQL Server. A temporary Celery Beat schedule also exercised the same dispatcher and ingestion task used by the hourly schedule; the test did not wait for the next regular hourly tick. These checks cover the local test databases, not production deployments.

An existing non-CDC warehouse table is not automatically converted to CDC: ingestion fails instead of dropping the table. If a CDC-managed warehouse table is missing, ingestion also fails rather than recreating an empty table and resuming from old offsets. Both situations require a coordinated initial snapshot and cutover, which is not yet implemented.

The PostgreSQL and Oracle warehouse tables were previously rebuilt from retained snapshot events as a one-off test-data procedure. This is not a supported automatic migration or production recovery procedure. Historical CDC failure entries can remain visible after recovery.

MongoDB ingestion does not use change streams or require replication. Each scheduled run reads every collection in bounded pages into a staging table, then swaps it into place. Inserts, updates, and deletes become visible after a successful refresh. This is not a point-in-time snapshot: concurrent source changes can affect a multi-page scan, and repeated scans increase source load.

CDC is not zero-overhead: it uses source log/stream capacity, Kafka storage, connector CPU, and source-side privileges/configuration. It avoids repeated whole-table scans and does not require a modified-at column, but tables need a primary key. The initial snapshot reads source rows once. Required source configuration includes:

| Source | CDC prerequisites |
|---|---|
| MySQL | Row-based binary logging with full row images, a unique server ID, and a CDC account with replication and snapshot-read privileges. |
| PostgreSQL | `wal_level=logical`, an available logical replication slot, and a CDC account with replication and table-read privileges. |
| SQL Server | SQL Server CDC enabled on the database and captured tables, plus a CDC-capable account. |
| Oracle | Archive logging, supplemental logging, LogMiner access, and the Debezium-required catalog/LogMiner privileges. |

Administrators can view recent CDC execution failures on the dashboard and open **CDC Execution Failures** for a searchable, paginated history. Search covers source name, database type, task ID, and recorded error. The dashboard widget can be hidden under **Customize Widgets**. Entries record the source, time, task ID, and exception type; use the task ID to locate details in the ETL worker logs. Entries represent failures recorded after this feature is deployed, not the current health of a connector or older executions.

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