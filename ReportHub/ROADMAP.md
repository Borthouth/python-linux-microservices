# ReportHub Project Roadmap 🗺️

Track the development and implementation phases of `ReportHub`.

## Phase 1: Infrastructure, Networking & Decoupled Architecture
- [x] Initialize repository structure (`README.md`, `ROADMAP.md`, `.gitignore`).
- [x] Configure `Dockerfile`, `requirements.txt` (including `pyodbc` and Microsoft ODBC Driver 18 dependencies), and entrypoint scripts.
- [x] Set up `docker-compose.yml` orchestrating PostgreSQL (tuned for analytics), Redis (with AOF persistence), Django Web, and Celery workers.
- [x] Set up **Polyglot Test Database Environment (`docker-compose.test.yml`)** supporting PostgreSQL (`5433`), MySQL (`3307`), and Microsoft SQL Server (`1434`) simultaneously on non-conflicting local ports with isolated persistent volumes.
- [x] Configure environment variables (`.env.example`) and verify local container startup.
- [x] **Hyper-V LAN Network Bridging:** Configured Hyper-V External Switch bridging development VM directly to local subnet (`HomeLAN_Bridge`) for direct connectivity to network targets (QNAP NAS & RHEL 9 work environment).
- [x] **Decoupled Architecture & Warehouse Engine:** Established dual-database design:
  - **Control Plane (Operational DB):** Hardened Django database for configuration, user access matrix, and target metadata.
  - **Analytical Warehouse (Warehouse DB):** Standalone PostgreSQL database using SQLAlchemy models with `JSONB` flex-payload columns to handle dynamic payloads without rigid migrations.
- [x] **Alembic Warehouse Migration Engine:** Fully configured standalone Alembic setup in virtual environment; validated transactional DDL execution (`alembic upgrade head`).
- [x] **MinIO S3-Compatible Object Storage Architecture:**
  - [x] Deploy MinIO container in `docker-compose.yml` bound to a dedicated persistent Docker volume (`minio_data`).
  - [x] Configure `django-storages` and `boto3` pointing to internal Docker S3 endpoints (`AWS_S3_ENDPOINT_URL = 'http://minio:9000'`).
  - [x] Externalize root credentials in `.env` and restrict public port exposure (keep database, Redis, and MinIO internal to Docker network).
  - [ ] Configure pre-signed secure URLs for streaming report downloads directly from MinIO, bypassing Django web workers.
  - [ ] Mount a dedicated Docker "scratch" volume for temporary local worker writes to eliminate RAM OOM errors and container disk bloat.

---

## Phase 2: Django Control Plane, User Management & Audit Engine
- [x] Initialize Django project and core apps (`control_plane`, `warehouse`, `etl`).
- [x] Configure automatic database migrations via container startup scripts (`entrypoint.sh`).
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

## Phase 4: Analytical Data Warehouse (ClickHouse Integration & Optimization)
- [x] **ClickHouse Container Deployment & Security:**
  - [x] Deploy ClickHouse alongside PostgreSQL/Redis in Docker (internal network; ports `9000`/`8123` not exposed publicly).
  - [x] Secure default user and configure connection parameters for Django & Celery.
- [x] **Data Model & Performance Optimization:**
  - [x] Establish isolated ClickHouse table namespacing (`source_{pk}__{schema}_{table}`) with `MergeTree` engines.
  - [x] Implement robust type mapping (`map_to_clickhouse_type`) and ISO string casting for temporal/complex types to prevent driver struct-packing crashes.
- [x] **Ingestion Engine & High-Volume Flushing:**
  - [x] Build idempotent batch ingestion Celery tasks (`ingest_source_to_clickhouse_task`) reading selected fields with dialect-aware identifier quoting.
  - [x] Implement active-source guard checks (`is_active=True`) and environment-driven batch sizing (`DEFAULT_ETL_BATCH_SIZE`).
  - [x] Configure periodic dispatch tasks (`trigger_all_active_clickhouse_ingestion_task`) and dedicated worker queue routing (`queue_hot`, `queue_ux`, `queue_periodic`).

---

## Phase 5: Asynchronous Reporting & Export Pipeline
- [ ] **Multi-Tiered Storage & Lifecycle Indexing:**
  - [ ] PostgreSQL handles lightweight report lifecycle metadata state tracking (`PENDING`, `PROCESSING`, `COMPLETED`, `FAILED`).
  - [ ] Completed binary report outputs uploaded directly to MinIO, updating PostgreSQL status with location keys in a single atomic update.
- [ ] **Chunked Conveyor-Belt Execution:**
  - [ ] Break long-running report jobs into sequential Celery task chains (e.g., 50,000 rows per chunk).
  - [ ] Use primary-key/timestamp cursor pagination (`WHERE id > last_seen_id`) instead of `OFFSET` to prevent shifting-window data drift mid-report.
- [ ] **Queue Isolation & Queue Hygiene:**
  - [ ] Segregate Celery workers into dedicated queues (`default` for fast web application operations vs `reports` for heavy analytical extractions).
- [ ] **Schema Evolution Safety Guards:**
  - [ ] Implement pre-migration checks against active PostgreSQL report states (`PROCESSING`) to pause destructive ClickHouse DDL schema migrations until active jobs complete.

---

## Phase 6: Report Generation & Query Engine
- [ ] **Dynamic Query Builder & Join Engine:**
  - [ ] Design `ReportDefinition` model and report configuration interface.
  - [ ] Implement automated query generation leveraging stored `SourceRelationship` foreign key metadata to construct multi-table SQL `JOIN` statements dynamically.
- [ ] **Execution & Visualization:**
  - [ ] Execute parameterized queries against ClickHouse analytical tables.
  - [ ] Develop report templates, data visualization components, and export formats (CSV/Excel/PDF).
- [ ] **Authenticated Sharing & RBAC:**
  - [ ] Implement role-based visibility restricting "View Only" users strictly to authorized dashboards and reports.