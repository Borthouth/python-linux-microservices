# ReportHub Project Roadmap 🗺️

Track the development phases, architectural milestones, and codebase optimization tasks for `ReportHub`.

## Suggested next priorities

1. Implement safe source-wide CDC-to-batch switching, then add control-plane rebuild monitoring and administrator-initiated resource cleanup. Continue validating CDC permissions, concurrent snapshot writes, and interruption recovery.
2. Add the failed/bounced email dashboard widget using the existing delivery audit.
3. Enforce source and tenant access before introducing cross-source linking references and linked reports.
4. Evaluate explicit incremental strategies and S3/Parquet sources where their source guarantees and operational costs justify them.
5. Extend AI layouts or diagnostics and dashboard navigation where they improve the existing workflows.

The unchecked items below are proposals, not implemented capabilities. Existing CDC and AI features have the limitations described in [README.md](./README.md).

## Phase 1: Infrastructure, Networking & Decoupled Architecture
- [x] Initialize repository structure (`README.md`, `ROADMAP.md`, `.gitignore`).
- [x] Configure `Dockerfile`, `requirements.txt` (including `pyodbc` and Microsoft ODBC Driver 18 dependencies), and entrypoint scripts.
- [x] Set up `docker-compose.yml` orchestrating PostgreSQL, Redis (with AOF persistence), Django Web, and Celery workers.
- [x] Set up **Polyglot Test Database Environment (`tests/docker-compose.test.yml`)** supporting PostgreSQL (`5433`), MySQL (`3307`), Microsoft SQL Server (`1434`), MongoDB (`27018`), and Oracle (`1522`) on local ports with persistent volumes.
- [x] Configure environment-driven settings for the local Compose stack.
- [ ] Provide and maintain a safe `.env.example` with documented non-production defaults; no example file is currently present.
- [ ] **Hyper-V LAN Network Bridging:** Confirm the external Hyper-V bridge (`HomeLAN_Bridge`) and access to LAN targets; this machine-level setup cannot be verified from the repository.
- [x] **Decoupled Architecture & Warehouse Engine:** Established dual-database design:
  - **Control Plane (Operational DB):** Django database for configuration, user-source access assignments, and target metadata.
  - **Analytical Warehouse:** ClickHouse tables provisioned from reflected source metadata.
- [x] **Alembic scaffolding:** A standalone Alembic configuration and initial empty revision exist; ClickHouse ingestion currently provisions and alters its own tables rather than using these revisions.
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
- [x] Build control plane models for **Target Connections (`SourceProfile` / `DbChoice`)** supporting MySQL, PostgreSQL, Microsoft SQL Server, MongoDB, and Oracle, alongside user-source access assignments. Enforcement across operations remains incomplete (Phase 5).
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
  - Persist warehouse field selections as the ingestion source of truth; CDC-managed tables require a discovered primary key.
- [x] Validate schema discovery and persistent field selection against target databases over local LAN and test containers.

---

## Phase 4: Analytical Data Warehouse (ClickHouse Integration & Ingestion Engine)
- [x] **ClickHouse Container Deployment & Security:**
  - [x] Deploy ClickHouse alongside PostgreSQL/Redis in Docker and configure Django/Celery connections.
  - [ ] Restrict host-published ports `9000`/`8123` and replace development credential defaults for production deployments.
- [x] **Data Model & Performance Optimization:**
  - [x] Establish ClickHouse table namespacing (`source_{pk}__{schema}_{table}`), using versioned `ReplacingMergeTree` tables for CDC-managed tables and `MergeTree` for normal batch refreshes.
  - [x] Implement robust type mapping (`map_to_clickhouse_type`) and ISO string casting for temporal/complex types to prevent driver struct-packing crashes.
- [x] **Ingestion Engine & High-Volume Flushing:**
  - [x] Route keyed relational tables in CDC mode through Debezium and Kafka Connect for PostgreSQL, MySQL, SQL Server, and Oracle; consume bounded event batches and enqueue continuations so other queued work can run.
  - [x] Apply CDC inserts, updates, and delete tombstones to versioned ClickHouse `ReplacingMergeTree` tables; commit Kafka offsets only after successful warehouse writes.
  - [x] Use bounded full-refresh batches for MongoDB and tables without a discovered primary key; reject unsafe full-refresh backfills for sources containing keyed CDC tables.
  - [x] Add a scheduled/manual Pipeline Automation backfill dispatcher (`trigger_all_active_clickhouse_backfill_task`) on the dedicated `queue_backfill` queue.
  - [x] Implement active-source guard checks (`is_active=True`), set the default batch limit to 10,000 rows via `DEFAULT_ETL_BATCH_SIZE`, and set Celery worker prefetch to 1.
  - [x] Configure periodic dispatch tasks (`trigger_all_active_clickhouse_ingestion_task`) and dedicated worker queue routing (`queue_hot`, `queue_backfill`, `queue_periodic`, and other application queues).
- [x] Configure optional Oracle CDB/PDB and separate LogMiner credentials; verify insert, update, and delete CDC flows for the local PostgreSQL, MySQL, Oracle, and SQL Server test sources.
- [x] Exercise the scheduled-ingestion dispatcher and task through a temporary Celery Beat schedule; the actual hourly tick was not observed directly.
- [x] **Safe single-table CDC rebuild:** Create a dedicated connector/topic generation, snapshot into a staging table, checkpoint snapshot completion and offsets, and cut over under the source-ingestion lock using ClickHouse `Atomic` table exchange/rename. Expose durable status and an admin action.
- [x] Test the admin rebuild action and read-only status view; prevent administrators from creating, editing, or deleting rebuild checkpoints through the admin.
- [ ] Add source-authorized control-plane rebuild monitoring showing source, table, status, last update, and required attention; let authorized administrators resume pending rebuilds while keeping checkpoint internals in Django admin.
- [x] Route post-cutover changes through a dedicated consumer group initialized at the snapshot end offsets; verify local SQL Server `northwind.dbo.Categories`, PostgreSQL `chinook.public.artist`, MySQL `northwind.customers`, and Oracle `APP_USER.invoices` rebuilds plus insert/update/delete propagation.
- [x] Test a paused MySQL rebuild with a snapshot backlog and source writes before cutover; verify continuation drains the checkpoint before replacement.
- [x] Inject a MySQL cutover failure after table exchange; block normal ingestion until manual resume finishes metadata updates without repeating the exchange.
- [x] Inject a MySQL cutover failure after connector routing is saved but before rebuild status is saved; verify ingestion is blocked and manual resume does not repeat the exchange.
- [x] Terminate an isolated MySQL rebuild worker after table exchange, wait for a shortened test lock to expire, and verify manual resume and subsequent CDC ingestion.
- [x] Terminate an isolated MySQL rebuild worker after snapshot checkpoint persistence but before Kafka offset commit; manually resume and verify replay, subsequent changes, and source/warehouse values.
- [ ] Add manual cleanup to the control plane: authorized administrators review retained connector generations, PostgreSQL slots/publications, Kafka resources, and ClickHouse tables, then confirm cleanup. Recheck that resources are obsolete and not needed by active or unfinished rebuilds; record outcomes and support retries. Do not automatically delete retained resources.
  - [x] Stop automatically deleting superseded and failed rebuild connectors; retain them for operator review. The administrator-initiated cleanup workflow remains to be implemented.
  - [ ] After administrator confirmation, run cleanup in a Celery task on a dedicated maintenance queue, separate from the UX and CDC workers.
  - [ ] Recheck each resource's eligibility at execution time, including active connectors and unfinished rebuilds or transitions; reject resources that cannot be safely verified.
  - [ ] Persist per-resource progress, outcomes, and errors; expose queued, running, completed, and failed states with results in the control plane.
  - [ ] Make retries and interruption recovery safe and idempotent, without repeating completed deletions; require explicit administrator initiation rather than automatic cleanup.
- [ ] Validate source-specific snapshot behavior and permissions, including schema changes, concurrent source writes, retention, retries, and recovery after interruption.
- [ ] Add end-to-end multi-database tests and operational guidance for lag, restart/replay, and failed cutovers.
- [ ] Implement a manually initiated source-wide CDC-to-batch transition with coordinated refresh and cutover, safe handling of active rebuilds and queued work, failure recovery, and tests for ongoing writes and deletes.
  - [x] Isolate scheduled CDC ingestion and rebuilds on a CDC worker, and batch/keyless refreshes on a batch worker, with separate configurable concurrency.
  - [x] Preserve staging tables across keyset-paginated batch refresh continuations and reject continuations whose staging table is missing.
  - [x] Recheck source mode and active state under the CDC source lock before ingestion or rebuild cutover; reject stale work if either changed.
  - [x] Skip new CDC ingestion batches when a CDC-to-batch transition is cutover pending, after acquiring the source lock; continue normal CDC during staging. This is not sufficient fencing for in-flight work, lock expiry, or keyless refreshes.
  - [x] Hold the control-plane source row lock across each CDC ingestion batch and recheck cutover state while locked. Tests verify a future cutover waits for an in-flight batch and a second ingestion waits even when Redis permits another lock acquisition. Database connection loss and external side effects still need fencing and recovery; this does not implement cutover.
  - [x] Serialize CDC-mode batch refresh tasks using a control-plane source row lock and recheck cutover status and source mode under the lock. A future executor must acquire this same lock before marking cutover pending; in-flight work completes before it can proceed, and queued keyless refreshes are skipped. The lock holds a database transaction over each bounded refresh and does not by itself fence CDC lock expiry.
  - [x] Carry source-mode provenance through ingestion and backfill dispatch, retries and continuations; reject mismatched work and ambiguous unmarked work after a successful CDC-to-batch transition. The future executor must atomically commit source mode and transition success under the source lock, retain successful transition records, and require upgraded producers and workers before switching. Future mode-switch cycles need a generation fence; these checks do not complete cutover fencing.
  - [x] Add durable transition and per-table checkpoint models with uniqueness constraints; these records are not yet used to execute a transition.
  - [x] Add an internal preparation operation that creates table checkpoints atomically and serializes transition preparation against CDC rebuild creation. It is not exposed as a user-facing switch.
  - [x] Add internal per-table batch staging into separate ClickHouse tables, with chunked reads, UUID and schema checkpoints, and fresh-table retries after interruption. Interrupted staging tables are retained for manual cleanup. Staging alone does not switch the source.
  - [x] Queue internal staging one table at a time on the batch worker; validate all checkpoints and table membership after staging. Failures require explicit retry; staging does not trigger cutover.
  - [x] Add an automated concurrency regression that pauses staging while CDC ingestion processes simulated insert, update, and delete events against a separate live table. Kafka and ClickHouse are mocked and the source is SQLite; live infrastructure still needs validation.
  - [x] Add a durable active-table-checked checkpoint field and tested UUID decision logic for pending and already-completed table exchanges or renames.
  - [x] Add an internal operation to record and revalidate the initial active-table UUID against a staged checkpoint, including missing-table and changed-identity checks. Observations can become stale during normal operation and must be revalidated under a complete cutover fence; execution and recovery orchestration remain unimplemented.
  - [x] Validate actual ClickHouse staged column names, order, types and defaults after staging, on retry and during active-table observation; reject mismatches without automatically changing tables. Cutover must revalidate and separately address row completeness and concurrent changes.
  - [x] Validate that the staged ClickHouse table retains its expected MergeTree engine and empty sorting, partition, primary and sampling keys at those same checkpoints. This does not protect against subsequent changes.
  - [ ] Validate staging and interruption recovery across all relational connectors. Local isolated staging matched source rows for PostgreSQL, MySQL, SQL Server, and Oracle. A PostgreSQL two-table run completed through the batch worker; an isolated worker exited after an insert and manual retry staged a fresh table with matching rows. PostgreSQL writes during staging produced an earlier staged snapshot; a further test paused staging after an actual insert and confirmed the live CDC worker applied insert, update, and delete events before staging resumed, without changing the live UUID or source mode. Staging in the overlap test ran in a separate application process. Other connectors, batch-worker overlap, other interruption boundaries, and source-specific worker recovery remain to be tested.
  - [ ] Validate source-driver streaming and large-table performance; the current staging primitive holds a checkpoint database transaction for the whole table and restarts that table after interruption.
  - [ ] Coordinate all refresh and CDC work, persist and validate checkpoint progress, and implement resumable transition execution before exposing a switch.
- [ ] Design safe source-wide batch-to-CDC transitions; the single-table rebuild does not perform mode transitions or new-column backfills.
- [ ] Decide whether and how to support automatic batch fallback after CDC failures; it is not currently implemented.
- [ ] **Alternative incremental loading strategies:**
  - [ ] Design explicit per-table strategies and validation; do not infer safety from a numeric type or automatically switch strategies. Retain full refresh where guarantees are absent.
  - [ ] Evaluate monotonic ID/sequence watermarks for explicitly append-only sources, with initial-load checkpoints, stable paging, retry safety, concurrent transaction ordering, and a documented policy for late or lower IDs. Updates and deletes require another mechanism or reconciliation.
  - [ ] Evaluate SQL Server `rowversion` and other source-specific change tokens with correct binary/type handling, transaction visibility and safe high-water marks; updates and inserts can be captured, but deletes require separate tracking or reconciliation.
  - [ ] Provide strategy-aware backend validation and a warehouse configuration selector only after source semantics are supported and tested; document timestamp ties, precision, nulls, backfills, and delete behavior.
- [ ] **S3/Parquet sources:** Design source configuration, access controls, file/schema discovery, incremental file/version tracking, and ClickHouse ingestion or query integration; test failure recovery and credential handling before adding this source type.

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
- [ ] Add an administrator dashboard widget summarizing failed and bounced deliveries, with links to appropriately filtered email audit views and coverage for empty/error states.

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
- [ ] Evaluate optional accessible widget navigation menus for filtered destinations where they add value; retain clear direct links for simple destinations and test keyboard, mobile, and empty-state behavior.

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
  - [ ] Evaluate reducing duplicated Compose environment configuration without changing variable precedence, service-specific settings, or secret exposure.
- [ ] **Audit Trail & Encryption Hardening:**
  - [ ] Audit `changes_data` JSON serialization in `control_plane/models.py` / `admin.py` to ensure encrypted credential fields are explicitly masked before historical snapshots are persisted.
- [ ] **Task & Memory Safety Safeguards:**
  - [ ] Audit ingestion and report pagination for bounded memory, stable ordering, and correct results under concurrent writes. Use keyset pagination only where a suitable stable key exists; account for composite keys and keyless sources.
  - [ ] Ensure dynamic SQLAlchemy engine calls strictly utilize context managers to prevent connection pool leaks against polyglot target databases.
- [ ] **Frontend & HTMX Polish:**
  - [ ] Audit templates and event bindings against the configured CSP, using delegated listeners where appropriate, and test the resulting behavior.
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
  - [ ] **Cross-source linking references:** Design named business-key metadata mapping authorized source/table/field combinations, including composite keys, compatible types, normalization, null handling, duplicate keys, and join cardinality.
  - [ ] Build a searchable linking-reference management page with dependent source, table, and field selectors; validate mappings, permissions, and audit changes.
  - [ ] Extend the report builder with an optional linked-report flow and HTMX-driven key, related-table, and column selection. Reuse existing foreign-key joins where appropriate; validate configuration server-side.
  - [ ] Generate constrained ClickHouse joins over linked source tables, with explicit join behavior, duplicate/row-explosion controls, and enforced source and tenant authorization. Cross-source mappings and joins are not yet implemented.
- [ ] **Exports, Charts & Share Control:**
  - [x] Generate CSV, JSON, Excel, and PDF report artifacts; store completed artifacts in MinIO and support authenticated admin downloads.
  - [x] Support configurable bar, line, and pie charts in PDF and Excel exports.
  - [x] Improve PDF readability with adaptive page layout, wrapped cells, and repeating headers.
  - [x] Add an optional AI report-design draft workflow using validated schema metadata and natural-language requests.
  - [x] Add optional aggregate-only AI summaries to PDF, Excel, and JSON exports.
  - [ ] Implement chunked streaming exports (CSV, Excel/pandas, ReportLab PDF) to keep Celery memory utilization low.
  - [ ] Add tokenized, time-expiring report-sharing URLs with tenant-scoped authorization.
  - [ ] Improve grouped-output labeling and calculate AI summary metrics with awareness of aggregate semantics.
  - [ ] Evaluate richer AI-assisted report layout drafts and metadata retrieval while retaining validated structured output and the existing safe SQL builder; define permissions, prompt-injection defenses, model/network privacy boundaries, and human review before execution.
  - [ ] Document and test backup/restore for Django PostgreSQL, ClickHouse, MinIO, Kafka state, and Ollama model storage; local deployment alone does not guarantee privacy or recovery.