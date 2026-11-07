# ReportHub Project Roadmap 🗺️

Track the development phases, architectural milestones, and codebase optimization tasks for `ReportHub`.

## Phase 1: Infrastructure, Networking & Decoupled Architecture
- [x] Initialize repository structure (`README.md`, `ROADMAP.md`, `.gitignore`).
- [x] Configure `Dockerfile`, `requirements.txt` (including `pyodbc` and Microsoft ODBC Driver 18 dependencies), and entrypoint scripts.
- [x] Set up `docker-compose.yml` orchestrating PostgreSQL (tuned for analytics), Redis (with AOF persistence), Django Web, and Celery workers.
- [x] Set up **Polyglot Test Database Environment (`docker-compose.test.yml`)** supporting PostgreSQL (`5433`), MySQL (`3307`), and Microsoft SQL Server (`1434`) simultaneously on non-conflicting local ports with isolated persistent volumes.
- [x] Configure environment variables (`.env.example`) and verify local container startup.
- [ ] **Hyper-V LAN Network Bridging:** Confirm the external Hyper-V bridge (`HomeLAN_Bridge`) and access to LAN targets; this machine-level setup cannot be verified from the repository.
- [x] **Decoupled Architecture & Warehouse Engine:** Established dual-database design:
  - **Control Plane (Operational DB):** Django database for configuration, user-source access assignments, and target metadata.
  - **Analytical Warehouse (Warehouse DB):** Standalone PostgreSQL / ClickHouse database setup using dynamic schema reflection.
- [x] **Alembic Warehouse Migration Engine:** Fully configured standalone Alembic setup in virtual environment; validated transactional DDL execution (`alembic upgrade head`).
- [x] **MinIO S3-Compatible Object Storage Architecture:**
  - [x] Deploy MinIO container in `docker-compose.yml` with data persisted to the host-mounted `./docker/minio_data` directory.
  - [x] Configure `django-storages` and `boto3` pointing to internal Docker S3 endpoints (`AWS_S3_ENDPOINT_URL = 'http://minio:9000'`).
  - [x] Support environment-provided MinIO credentials (development defaults remain in Compose).
  - [ ] Restrict host-published service ports and replace development credential defaults for production deployments.

---

## Phase 2: Django Control Plane, User Management & Audit Engine
- [x] Initialize Django project and core apps (`control_plane`, `warehouse`, `etl`, `reports`).
- [x] Configure automatic database migrations via container startup scripts (`entrypoint.sh`).
- [x] **Control Plane Shell & Layout Standardization**:
  - [x] Centralized layout shell in `base.html` and converted `dashboard/dashboard.html` into a pure content fragment to eliminate recursive navbar rendering bugs.
  - [x] Enforced strict container width alignment (`max-w-7xl mx-auto px-6 py-8`) across the Dashboard, Audit Logs, and Dead Letter Queue (DLQ) sub-pages for visual consistency.
- [x] **Security-First Hardening & Session Security**: 
  - [x] Content Security Policy (`django-csp`), strict `ALLOWED_HOSTS`, clickjacking/MIME defenses, HttpOnly session/CSRF cookies, and global 64-bit `BigAutoField`.
  - [x] Enabled browser-close session expiration (`SESSION_EXPIRE_AT_BROWSER_CLOSE = True`).
  - [x] Installed and configured `django-auto-logout` with environment-driven idle timeout management (`SESSION_IDLE_MINUTES` via `.env`) and flash notification integration.
  - [x] Avoided combining auto-logout with `SESSION_SAVE_EVERY_REQUEST = True` to eliminate unnecessary session table writes.
- [x] Set up user authentication and RBAC roles (*Viewer, Analyst/Builder, System Admin*).
- [x] Build control plane models for **Target Connections (`SourceProfile` / `DbChoice`)** supporting MySQL, PostgreSQL, and Microsoft SQL Server, alongside the **User-Source Access Matrix** (Layer 1 security).
- [x] Implement **Self-Service User Profile Management** (nav-bar linked account view for users to update their details, first/last name, email, and password).
- [x] **Unified Audit Trail Engine & Scheduled Task Governance:**
  - Integrated history auditing across selected control plane models, `TrackedUser`, and the standalone `PipelineAutomationAuditLog` model (with `changes_data` JSON diff tracking).
  - Built unified audit log viewer (`/control-plane/audit/`) with global timestamp sorting, action code filtering (`+`, `~`, `-`), and full-text search across sources, names, and modifier users.
  - Developed field-level diff engine displaying precise *Old → New* value comparisons inside a responsive modal.
  - Hardened frontend against Stored XSS by removing `|safe` rendering, utilizing DOM `data-*` attributes, and escaping dynamic values.
  - Implemented searchable, paginated management UI for `PeriodicTask` scheduled ETL jobs with robust uniqueness validation (`ValidationError` / `IntegrityError` handling for duplicate names).
- [x] **Dead Letter Queue (DLQ) & Background Task Governance:**
  - [x] Implemented Redis-backed storage scanning (`dlq:task:*`) with JSON decoding and attribute normalization (`id`, `display_name`, `error_message`, `failed_timestamp`).
  - [x] Integrated robust timestamp parsing via `django.utils.dateparse.parse_datetime` to eliminate template rendering crashes and support formatted date displays.
  - [x] Refactored the dashboard DLQ widget into a grid-based layout matching the "Recent Pipeline" component, complete with failure counts, stack trace previews, and search/pagination controls on `/control-plane/dlq/`.
- [x] **Asynchronous UI Status & Toast Notification Architecture:**
  - Configured HTMX polling integration (`hx-get`, `hx-trigger="every 2s"`, `hx-swap="outerHTML"`) for asynchronous database connection tests across MySQL, PostgreSQL, and MSSQL.
  - Developed CSP-compliant global toast event listeners (`htmx:afterSwap` decoupled handling) avoiding inline script violations.
  - Built idempotent state guards and clean fallback UI states ("Ready", "Running...", "Failed") for background connection testing.

---

## Phase 3: Schema Reflection & Configuration UI
- [x] Build the multi-source connection profile manager UI and control plane web views.
- [x] **Celery Schema Introspection Task:**
  - Implemented background `introspect_source_schema_task` using SQLAlchemy inspector (`information_schema`, MSSQL catalog views) to asynchronously discover tables, columns, and primary keys across PostgreSQL, MySQL, Microsoft SQL Server, and Oracle; MongoDB collections and document fields are also introspected.
  - Automatically populates and updates relational control plane metadata models (`SourceTable` and `SourceField`), including key metadata for incremental loads.
  - Added periodic Celery Beat wrapper (`trigger_all_active_source_introspection_task`) for automated recurring schema discovery.
- [x] **High-Performance Master-Detail Schema Mapping UI:**
  - Developed `WarehouseDatasetMapping` domain model for fine-grained table and field synchronization control.
  - Built master-detail mapping interface (`/control-plane/warehouse/`) featuring a searchable, scrollable table navigator on the left and a column selection panel on the right.
  - Implemented live table/column search filtering, quick "Select All / Deselect All" batch toggles, and data type badges optimized for high-density database schemas.
  - Persist warehouse field selections as the ingestion source of truth and configure an optional modified-at field per table.
- [x] Validate schema discovery and persistent field selection against target databases over local LAN and test containers.

---

## Phase 4: Analytical Data Warehouse (ClickHouse Integration & Ingestion Engine)
- [x] **ClickHouse Container Deployment & Security:**
  - [x] Deploy ClickHouse alongside PostgreSQL/Redis in Docker and configure Django/Celery connections.
  - [ ] Restrict host-published ports `9000`/`8123` and replace development credential defaults for production deployments.
- [x] **Data Model & Performance Optimization:**
  - [x] Establish isolated ClickHouse table namespacing (`source_{pk}__{schema}_{table}`), using `ReplacingMergeTree` for primary-key incremental tables and `MergeTree` otherwise.
  - [x] Implement robust type mapping (`map_to_clickhouse_type`) and ISO string casting for temporal/complex types to prevent driver struct-packing crashes.
- [x] **Ingestion Engine & High-Volume Flushing:**
  - [x] Build bounded Celery ingestion tasks (`ingest_source_to_clickhouse_task`) using keyset pagination where primary keys exist; enqueue continuations after at most `DEFAULT_ETL_BATCH_SIZE` rows so other queued work can run.
  - [x] Incrementally capture inserts and updates for tables with an introspected primary key and configured modified-at field; persist per-table watermarks and use ClickHouse `ReplacingMergeTree` deduplication for reports.
  - [x] Fall back to bounded full refreshes for tables without safe incremental metadata; rebuild full selected history after warehouse-field changes.
  - [x] Add a scheduled/manual Pipeline Automation backfill dispatcher (`trigger_all_active_clickhouse_backfill_task`) on the dedicated `queue_backfill` queue.
  - [x] Implement active-source guard checks (`is_active=True`), set the default batch limit to 10,000 rows via `DEFAULT_ETL_BATCH_SIZE`, and set Celery worker prefetch to 1.
  - [x] Configure periodic dispatch tasks (`trigger_all_active_clickhouse_ingestion_task`) and dedicated worker queue routing (`queue_hot`, `queue_backfill`, `queue_periodic`, and other application queues).

---

## Phase 5: User Management & Source Access Control
- [x] **Built-in Auth Integration:** Use Django's standard `auth.User` model with profile and password management views.
- [x] **User-Source Access Configuration:** Configure source access assignments and roles through `UserSourceAccess`.
- [ ] **Collaborative Workspaces:** Add workspace and membership models for sharing data sources, pipelines, and connections across users.
- [ ] **Permission Enforcement:** Apply source-access and workspace authorization consistently across report, pipeline, and data-management views.
- [ ] **Tenant Row-Level Security:** Add and enforce company/tenant-scoped row filtering; `company_id` query injection is not currently implemented.

---

## Phase 6: Asynchronous Email Notifications & Delivery Audit
- [x] **Celery Integration:** Moving email dispatches to background Celery tasks using Django's email backend so UI and HTMX workflows never block.
- [x] **Report Email Lists:** Attaching recipient lists or subscription structures directly to report configurations.
- [x] **Email Delivery Audit & Governance Module:**
  - [x] Build `EmailAuditLog` data model storing delivery lifecycle states (`sent`, `delivered`, `failed`, `bounced`), `retry_count`, and error tracebacks.
  - [x] Implement backend views for record retrieval, webhook status transitions, and administrative actions (`Re-queue` via sky-blue action buttons, `Delete` via confirmation modals).
  - [x] Design fully responsive UI matching the Dead Letter Queue (DLQ) layout patterns, complete with error inspection modals, search filters, and status-coded badge pills.

---

## Phase 7: SFTP & File Transfer Protocol Integration
- [ ] **Transfer Destinations Model:** Create configuration models to securely store SFTP, FTP, or cloud storage target credentials (hosts, ports, authentication keys).
- [ ] **Automated Export Pipelines:** Hook generated report files into a downstream task that handles automated transfers using appropriate Python libraries.

---

## Phase 8: UI Framework, Navigation & Global Polish
- [x] **Pure Tailwind CSS (Slate Palette):** Styling components (like action dropdowns, tables, and modals) using clean slate utilities (`slate-900`, `slate-800`, `slate-700`) and Alpine.js for lightweight interactivity.
- [x] **Admin Navigation Dropdown:** Grouping administrative tools into clear categories:
  - Users & Access Management (Profiles, Source Access, Connection Permissions)
  - System Operations & Monitoring (DLQ, Task/Worker Status, Global Pipeline History)
  - Integrations & Infrastructure (Email Settings)
- [x] **Global Cursor/Selection Optimization:** Applied a clean app-wide `select-none` approach on structural/UI chrome while targeted `select-text` preserves copy-paste usability on essential audit logs, error traces, and recipient strings.

---

## Phase 9: Audit & Operational History
- [x] Provide a unified view of available model-history records and pipeline-automation changes.
- [x] Provide a searchable Dead Letter Queue interface for failed task inspection and requeue actions.
- [x] Track report-email delivery states and provide email audit history and administrative actions.
- [x] Provide pipeline and report execution history views.
- [ ] Add dedicated file-transfer audit tracking for SFTP/FTP/object-storage deliveries.
- [ ] Add consolidated ETL ingestion metrics and dedicated security/login audit views.

---

## Phase 10: Codebase Audit, Cleanup & Optimization (Active Phase 🎯)
- [ ] **Dependency & Configuration Pruning:**
  - [ ] Consolidate duplicate package declarations in `requirements.txt` (`celery`, `pymysql`, `psycopg2-binary`).
  - [ ] Evaluate unused stack dependencies (FastAPI, Uvicorn, AsyncPG) to trim container build overhead if not servicing separate microservice endpoints.
  - [ ] Refactor `docker-compose.yml` to extract the repeated 12-line environment variable block across `app` and Celery worker services into an `env_file: .env` configuration.
- [ ] **Audit Trail & Encryption Hardening:**
  - [ ] Audit `changes_data` JSON serialization in `control_plane/models.py` / `admin.py` to ensure encrypted credential fields are explicitly masked before historical snapshots are persisted.
- [ ] **Task & Memory Safety Safeguards:**
  - [ ] Audit all Celery tasks in `etl/tasks.py` and `reports/tasks.py` to verify strict primary-key cursor pagination (`WHERE id > last_seen_id`) over raw `OFFSET` queries.
  - [ ] Ensure dynamic SQLAlchemy engine calls strictly utilize context managers to prevent connection pool leaks against polyglot target databases.
- [ ] **Frontend & HTMX Polish:**
  - [ ] Audit template event bindings to guarantee 100% CSP compliance via `htmx:afterSwap` delegated listeners.
  - [ ] Verify clean termination on HTMX polling triggers (`hx-trigger="every 2s"`) upon reaching terminal execution states.

---

## Phase 11: Asynchronous Reporting, Query Generation & Export Pipelines
- [x] **Multi-Tiered Storage & Lifecycle Indexing:**
  - [x] Track report execution lifecycle metadata in PostgreSQL (`pending`, `running`, `completed`, `failed`, `cancelled`).
  - [x] Upload completed report artifacts to MinIO and persist their object paths with execution records.
  - [ ] Add atomic/recoverable consistency handling between MinIO uploads and PostgreSQL execution updates.
- [ ] **Dynamic Query Builder & Join Engine:**
  - [x] Build the `ReportDefinition` model and report configuration interface.
  - [x] Generate validated direct `LEFT JOIN` queries against ClickHouse analytical tables using stored `SourceRelationship` foreign keys.
  - [x] Implement Tabular Detail and Grouped Summary queries with `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`.
  - [ ] Implement report filters and configurable ordering, including applying requested ordering from AI drafts.
  - [ ] Extend report joins to multi-hop relationship paths and configurable join types.
- [ ] **Exports, Charts & Share Control:**
  - [x] Generate CSV, JSON, Excel, and PDF report artifacts; store completed artifacts in MinIO and support authenticated admin downloads.
  - [x] Support configurable bar, line, and pie charts in PDF and Excel exports.
  - [x] Improve PDF readability with adaptive page layout, wrapped cells, and repeating headers.
  - [x] Add an optional AI report-design draft workflow using validated schema metadata and natural-language requests.
  - [x] Add optional aggregate-only AI summaries to PDF, Excel, and JSON exports.
  - [ ] Implement chunked streaming exports (CSV, Excel/pandas, ReportLab PDF) to keep Celery memory utilization low.
  - [ ] Add tokenized, time-expiring report-sharing URLs with tenant-scoped authorization.
  - [ ] Improve grouped-output labeling and calculate AI summary metrics with awareness of aggregate semantics.