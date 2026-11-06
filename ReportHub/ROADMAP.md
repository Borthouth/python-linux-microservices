# ReportHub Project Roadmap 🗺️

Track the development phases, architectural milestones, and codebase optimization tasks for `ReportHub`.

## Phase 1: Infrastructure, Networking & Decoupled Architecture
- [x] Initialize repository structure (`README.md`, `ROADMAP.md`, `.gitignore`).
- [x] Configure `Dockerfile`, `requirements.txt` (including `pyodbc` and Microsoft ODBC Driver 18 dependencies), and entrypoint scripts.
- [x] Set up `docker-compose.yml` orchestrating PostgreSQL (tuned for analytics), Redis (with AOF persistence), Django Web, and Celery workers.
- [x] Set up **Polyglot Test Database Environment (`docker-compose.test.yml`)** supporting PostgreSQL (`5433`), MySQL (`3307`), and Microsoft SQL Server (`1434`) simultaneously on non-conflicting local ports with isolated persistent volumes.
- [x] Configure environment variables (`.env.example`) and verify local container startup.
- [x] **Hyper-V LAN Network Bridging:** Configured Hyper-V External Switch bridging development VM directly to local subnet (`HomeLAN_Bridge`) for direct connectivity to network targets (QNAP NAS & RHEL 9 work environment).
- [x] **Decoupled Architecture & Warehouse Engine:** Established dual-database design:
  - **Control Plane (Operational DB):** Hardened Django database for configuration, user access matrix, and target metadata.
  - **Analytical Warehouse (Warehouse DB):** Standalone PostgreSQL / ClickHouse database setup using dynamic schema reflection.
- [x] **Alembic Warehouse Migration Engine:** Fully configured standalone Alembic setup in virtual environment; validated transactional DDL execution (`alembic upgrade head`).
- [x] **MinIO S3-Compatible Object Storage Architecture:**
  - [x] Deploy MinIO container in `docker-compose.yml` bound to a dedicated persistent Docker volume (`minio_data`).
  - [x] Configure `django-storages` and `boto3` pointing to internal Docker S3 endpoints (`AWS_S3_ENDPOINT_URL = 'http://minio:9000'`).
  - [x] Externalize root credentials in `.env` and restrict public port exposure (keep database, Redis, and MinIO internal to Docker network).

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
  - Integrated full history auditing across control plane infrastructure models, `TrackedUser`, and custom standalone `ScheduleAuditLog` model (with `changes_data` JSON diff tracking).
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
  - Implemented background `introspect_source_schema_task` using SQLAlchemy inspector (`information_schema`, MSSQL catalog views) to asynchronously discover tables and columns across PostgreSQL, MySQL, and Microsoft SQL Server.
  - Automatically populates and updates relational control plane metadata models (`SourceTable` and `SourceField`).
  - Added periodic Celery Beat wrapper (`trigger_all_active_source_introspection_task`) for automated recurring schema discovery.
- [x] **High-Performance Master-Detail Schema Mapping UI:**
  - Developed `WarehouseDatasetMapping` domain model for fine-grained table and field synchronization control.
  - Built master-detail mapping interface (`/control-plane/warehouse/`) featuring a searchable, scrollable table navigator on the left and a column selection panel on the right.
  - Implemented live table/column search filtering, quick "Select All / Deselect All" batch toggles, and data type badges optimized for high-density database schemas.
- [x] Validate schema discovery and persistent field selection against target databases over local LAN and test containers.

---

## Phase 4: Analytical Data Warehouse (ClickHouse Integration & Ingestion Engine)
- [x] **ClickHouse Container Deployment & Security:**
  - [x] Deploy ClickHouse alongside PostgreSQL/Redis in Docker (internal network; ports `9000`/`8123` not exposed publicly).
  - [x] Secure default user and configure connection parameters for Django & Celery.
- [x] **Data Model & Performance Optimization:**
  - [x] Establish isolated ClickHouse table namespacing (`source_{pk}__{schema}_{table}`) with `MergeTree` engines.
  - [x] Implement robust type mapping (`map_to_clickhouse_type`) and ISO string casting for temporal/complex types to prevent driver struct-packing crashes.
- [x] **Ingestion Engine & High-Volume Flushing:**
  - [x] Build idempotent batch ingestion Celery tasks (`ingest_source_to_clickhouse_task`) reading selected fields with dialect-aware identifier quoting.
  - [x] Implement active-source guard checks (`is_active=True`) and environment-driven batch sizing (`DEFAULT_ETL_BATCH_SIZE`).
  - [x] Configure periodic dispatch tasks (`trigger_all_active_clickhouse_ingestion_task`) and dedicated worker queue routing (`queue_cold`, `queue_warm`, `queue_ux`, `queue_periodic`).

---

## Phase 5: User Workspaces, Access Control & Collaboration
- [x] **Built-in Auth Integration:** Retaining Django's standard `auth.User` model while extending it with a `UserProfile` model (`OneToOne`) for extended metadata and verified email storage.
- [x] **Collaborative Workspaces:** Introducing `SharedWorkspace` and `WorkspaceMembership` models to allow multiple users to collaborate and share access to specific data sources, pipelines, and connections.
- [x] **Permission Enforcement:** Implementing workspace-level access checks in views and decorators to secure pipeline execution and data source visibility.

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
- [x] **Transfer Destinations Model:** Creating configuration models (`TransferDestination`) to securely store SFTP, FTP, or cloud storage target credentials (hosts, ports, authentication keys).
- [x] **Automated Export Pipelines:** Hooking file generation (CSV, Excel, PDF) into a downstream Celery task that handles automated file transfers using robust Python transfer libraries (e.g., `paramiko`).

---

## Phase 8: UI Framework, Navigation & Global Polish
- [x] **Pure Tailwind CSS (Slate Palette):** Styling components (like action dropdowns, tables, and modals) using clean slate utilities (`slate-900`, `slate-800`, `slate-700`) and Alpine.js for lightweight interactivity.
- [x] **Admin Navigation Dropdown:** Grouping administrative tools into clear categories:
  - Users & Access Management (Profiles, Workspaces, Connection Permissions)
  - System Operations & Monitoring (DLQ, Task/Worker Status, Global Pipeline History)
  - Integrations & Infrastructure (Email Settings, SFTP Destinations)
- [x] **Global Cursor/Selection Optimization:** Applied a clean app-wide `select-none` approach on structural/UI chrome while targeted `select-text` preserves copy-paste usability on essential audit logs, error traces, and recipient strings.

---

## Phase 9: Comprehensive Audit Suite
- [x] **Expanding ReportHub's audit system into a multi-tab single pane of glass covering:**
  - **UX & System Audit:** Administrative actions and general UI logs.
  - **Queue Audit (DLQ):** Failed task tracking, stack trace rendering, and retry/purge actions.
  - **Email Audit / Status:** Delivery tracking, failure logs, and bounce/delivery webhook monitoring.
  - **File Transfer Audit:** Tracking SFTP/FTP/S3 uploads, file sizes, durations, and connection errors.
  - **ETL & Ingestion Audit:** Row counts, batch performance, and ClickHouse synchronization history.
  - **Security & Access Audit:** Permission changes, logins, and workspace membership updates.

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
- [ ] **Multi-Tiered Storage & Lifecycle Indexing:**
  - [ ] Manage report lifecycle metadata in PostgreSQL (`PENDING`, `PROCESSING`, `COMPLETED`, `FAILED`).
  - [ ] Upload completed binary outputs directly to MinIO, updating PostgreSQL status with pre-signed retrieval keys in an atomic transaction.
- [ ] **Dynamic Query Builder & Join Engine:**
  - [ ] Build `ReportDefinition` model and config interface.
  - [ ] Generate parameterized SQL `JOIN` queries against ClickHouse analytical tables utilizing stored `SourceRelationship` foreign keys.
- [ ] **Streaming Exports & Share Control:**
  - [ ] Implement chunked streaming exports (CSV, Excel/pandas, ReportLab PDF) to keep Celery memory utilization low.
  - [ ] Secure report downloads via tokenized, time-expiring URLs bound to user tenant scopes.