# ReportHub Project Roadmap 🗺️

Track the development phases, architectural milestones, and codebase optimization tasks for `ReportHub`.

## Suggested next priorities

1. Complete safe CDC-to-batch switching in the control plane: resolve uncertain external requests, migrate existing-object ownership and privileges, coordinate restricted-identity rollout and worker/client fencing, then expose confirmed switch and resume actions. Real-worker cancellation behavior is tested, but delayed requests are not fenced or safely recovered. Candidate roles and query-ID propagation are verified in isolation; staging and status are available, but switching remains disabled. Use [the continuation handoff](./CDC_TO_BATCH_HANDOFF.md) as the detailed CDC work tracker and next-task source.
2. Validate the CDC cleanup workflow against disposable connector/PostgreSQL/Kafka/ClickHouse services. Continue validating CDC permissions, concurrent snapshot writes, and interruption recovery.
3. Enforce source and tenant access before introducing cross-source linking references and linked reports.
4. Evaluate explicit incremental strategies and S3/Parquet sources where their source guarantees and operational costs justify them.
5. Extend AI layouts or diagnostics and dashboard navigation where they improve the existing workflows.

The unchecked items below are proposals, not implemented capabilities. Existing CDC and AI features have the limitations described in [README.md](./README.md).

## Current checkpoint (2026-11-14)

- The agreed SFTP scope is complete: reusable destinations, multi-destination
  Celery delivery, connection tests, Save & Test, Activate/Deactivate, delivery
  audit/resend and the optional dashboard widget. Enterprise cloud destinations
  remain future work.
- The staff-only CDC rebuild-resource cleanup workflow is implemented in the
  current worktree. Migration `0048` reconciles historical cleanup defaults;
  migration consistency, 12 focused tests and an upgrade preserving historical
  records pass in isolated SQLite. Migrations `0047` and `0048` are applied to
  the running database; validate against disposable external services before
  use. Do not run cleanup against production or shared resources.
- Page-level control-plane templates now use thin wrappers and separate content
  templates; the standalone login shell and shared component partials remain
  intentionally purpose-specific.
- CDC-to-batch work is parked, not complete. Restricted-role, migration-gate,
  ownership-transfer candidate and real-worker cancellation probes are recorded
  in [CDC_TO_BATCH_HANDOFF.md](./CDC_TO_BATCH_HANDOFF.md), along with the next
  steps. The switch remains fail-closed until fencing and recovery are proven.
- The running app uses Django 5.2.19; a 6.1 upgrade is deferred because the
  released `django-celery-beat` requires Django <6.1 and Python must also be
  upgraded. The requirements file does not pin Django to the 5.2 series.
- Browser verification of polling and the unused-dependency review are small
  follow-up candidates, deferred until a later session.

## Phase 1: Infrastructure, Networking & Decoupled Architecture
- [x] Initialize repository structure (`README.md`, `ROADMAP.md`, `.gitignore`).
- [x] Configure `Dockerfile`, `requirements.txt` (including `pyodbc` and Microsoft ODBC Driver 18 dependencies), and entrypoint scripts.
- [x] Set up `docker-compose.yml` orchestrating PostgreSQL, Redis (with AOF persistence), Django Web, and Celery workers.
- [x] Set up **Polyglot Test Database Environment (`tests/docker-compose.test.yml`)** supporting PostgreSQL (`5433`), MySQL (`3307`), Microsoft SQL Server (`1434`), MongoDB (`27018`), and Oracle (`1522`) on local ports with persistent volumes.
- [x] Configure environment-driven settings for the local Compose stack.
- [x] Provide a local-development `.env.example` with credential placeholders, key-generation instructions, existing-volume warnings, and Docker build-context exclusion for environment files.
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
- [ ] **Restore MongoDB CDC support:** MongoDB currently uses batch refresh; CDC remains disabled until implemented and validated. Debezium requires MongoDB change streams from a replica set or sharded cluster, not a standalone server.
  - [ ] Add a separate, persistent single-node replica-set fixture to the test database setup without replacing or resetting the existing standalone MongoDB data. Configure authentication, initialization, readiness and addresses reachable by ReportHub and Kafka Connect; a second member is unnecessary for basic CDC tests.
  - [ ] Review the existing MongoDB connector configuration and implement validated snapshot and change-event handling, including document identity, BSON/nested values, updates, deletes, ordering, replay, offsets and oplog-retention recovery.
  - [ ] Exercise snapshot plus insert/update/delete propagation end-to-end through Debezium, Kafka, real workers and ClickHouse using disposable resources. Test restart, retries, concurrent changes and failure recovery before enabling the control-plane CDC option.
  - [ ] Preserve existing MongoDB batch sources and the mode-change safety gate. Separately design and test any future MongoDB batch/CDC transitions, rebuilds and cleanup; a single-node fixture does not test replication or failover.
- [x] Configure optional Oracle CDB/PDB and separate LogMiner credentials; verify insert, update, and delete CDC flows for the local PostgreSQL, MySQL, Oracle, and SQL Server test sources.
- [x] Exercise the scheduled-ingestion dispatcher and task through a temporary Celery Beat schedule; the actual hourly tick was not observed directly.
- [x] **Safe single-table CDC rebuild:** Create a dedicated connector/topic generation, snapshot into a staging table, checkpoint snapshot completion and offsets, and cut over under the source-ingestion lock using ClickHouse `Atomic` table exchange/rename. Expose durable status.
- [x] Move CDC rebuild initiation from Django admin into the control plane with a confirmation step, source authorization, eligibility rechecks, and `queue_cdc` dispatch.
- [x] Test control-plane rebuild initiation and verify the Django admin no longer exposes a rebuild action; prevent administrators from creating, editing, or deleting rebuild checkpoints through the admin.
- [x] Add source-authorized control-plane rebuild monitoring showing source, table, status, last update, and required attention; let authorized administrators resume cutover-pending rebuilds while keeping checkpoint internals in Django admin.
- [x] Route post-cutover changes through a dedicated consumer group initialized at the snapshot end offsets; verify local SQL Server `northwind.dbo.Categories`, PostgreSQL `chinook.public.artist`, MySQL `northwind.customers`, and Oracle `APP_USER.invoices` rebuilds plus insert/update/delete propagation.
- [x] Test a paused MySQL rebuild with a snapshot backlog and source writes before cutover; verify continuation drains the checkpoint before replacement.
- [x] Inject a MySQL cutover failure after table exchange; block normal ingestion until manual resume finishes metadata updates without repeating the exchange.
- [x] Inject a MySQL cutover failure after connector routing is saved but before rebuild status is saved; verify ingestion is blocked and manual resume does not repeat the exchange.
- [x] Terminate an isolated MySQL rebuild worker after table exchange, wait for a shortened test lock to expire, and verify manual resume and subsequent CDC ingestion.
- [x] Terminate an isolated MySQL rebuild worker after snapshot checkpoint persistence but before Kafka offset commit; manually resume and verify replay, subsequent changes, and source/warehouse values.
- [ ] Validate and complete manual cleanup in the control plane: administrators review retained rebuild-generation resources and explicitly confirm cleanup. Do not automatically delete retained resources.
  - [x] Stop automatically deleting superseded and failed rebuild connectors; retain them for operator review.
  - [x] Implement staff-only read-only inventory and explicit confirmation, with cleanup dispatched to a dedicated maintenance queue.
  - [x] Recheck resource ownership and eligibility before each deletion; block active references, unfinished rebuilds/transitions, and unverified resources.
  - [x] Persist per-resource progress, outcomes, and errors, and expose job states and recovery controls in the control plane.
  - [x] Run focused cleanup view and service tests; all 12 tests pass in the isolated Django test database.
  - [x] Reconcile historical cleanup-field defaults with migration `0048`; verify migration consistency, focused tests and an upgrade preserving historical records in isolated SQLite.
  - [ ] Validate permissions and interruption behavior against disposable external services before using the workflow on shared resources.
- [ ] Validate source-specific snapshot behavior and permissions, including schema changes, concurrent source writes, retention, retries, and recovery after interruption.
- [ ] Add end-to-end multi-database tests and operational guidance for lag, restart/replay, and failed cutovers.
- [ ] Implement a manually initiated source-wide CDC-to-batch transition with coordinated refresh and cutover, safe handling of active rebuilds and queued work, failure recovery, and tests for ongoing writes and deletes.
  - [ ] Establish restricted service identities and a coordinated rollout that retires old credentials and workers and proves outstanding external operations have terminated; shared privileged accounts cannot enforce a cutover fence.
  - [x] Support optional distinct PostgreSQL and ClickHouse application credentials, with legacy defaults and fail-closed validation of partial pairs. Candidate role grants are tested in isolation; production provisioning, deployment of migration separation, rollout and request termination proof remain outstanding. Separate credentials alone do not fence writers.
  - [x] Add an opt-in one-shot migration service with explicit migration credentials and successful-completion dependencies for all application services. Runtime startup migrations are disabled only in this mode; default startup remains unchanged and migration failures stop startup. Standalone startup and Compose tests pass; production deployment, existing-object ownership and live deployment orchestration remain outstanding. This does not fence writers.
  - [x] Validate candidate non-superuser PostgreSQL migration/runtime and database-scoped ClickHouse runtime identities in disposable isolated containers. The full migration chain and positive/negative runtime probes pass, including actual staging and own-query cancellation. Production provisioning, existing-object ownership and full worker/source coverage remain outstanding; database-wide runtime grants are not a source-scoped fence.
  - [x] Fix report and bounded ETL query-ID propagation using driver settings for queries, commands and inserts; share parameterized cancellation and log ETL cancellation failures. Fourteen isolated workflow/permission probes and 15 host tests pass, including active-query cancellation, actual report/batch refresh execution and retained uncertain operations after timeouts/simulated interruptions. Immediate UI cancellation and delayed-request recovery remain unimplemented; query IDs are not an external-write fence.
  - [x] Exercise report and ingestion cancellation through real Celery workers, control-plane flags, process termination, delayed requests and dropped responses. Report cancellation remains cooperative until a blocking query returns. Source deactivation during a delayed staging insert is applied after the insert returns, with staging removed and the active table retained; in-flight requests are not preempted. Eight isolated worker scenarios pass, including the case where a delayed request executes after termination and a successful cancellation found no active query. Source cancellation is recorded with status `CANCELLED` through migration `0041`.
  - [ ] Design and validate recovery/fencing for uncertain or delayed external requests. A successful cancellation or absent query does not prove a delayed request cannot execute; keep CDC-to-batch switching disabled until outstanding operations can be proven stopped.
  - [ ] Define and review a production PostgreSQL existing-object ownership/grant transition. A repeatable single-legacy-owner candidate is tested on a disposable populated database, including existing tables, serial and identity sequences, migration history, future grants and denied runtime DDL. It is not a production procedure; inventory multiple owners, schemas, extensions, existing grants and shared objects before rollout. Default privileges alone are not retroactive.
  - [ ] Expand restricted-runtime validation to CDC ingestion/rebuilds and all supported source databases. Isolated real workers now run report cancellation, batch ingestion and cancellation scenarios with restricted PostgreSQL/ClickHouse identities, checking worker identity and denied administration. A separate table-scoped read-only PostgreSQL source also passes successful initial/replacement full refreshes through keyset-paginated continuations and a completed CSV export with exact row checks; source writes/DDL and ungranted table reads are denied. Cancellation scenarios still use privileged source connections.
  - [ ] Complete coordinated deployment validation for the dedicated migration service and prove each deployment runs a fresh migration job before upgraded application processes start. An isolated Compose probe now verifies real non-superuser roles, authentication and migration DDL failures blocking a runtime probe, and two successful deployments with a forced fresh migration. Existing processes, actual production web/worker commands and all deployment paths remain untested.
  - [ ] Remove administrator credentials from runtime environment variables and mounted configuration through an approved coordinated rollout; selecting `APP_*` credentials does not remove access to legacy credentials. Retire old workers, clients, identities and sessions without changing shared credentials or restarting services without approval.
  - [x] Fail closed in the queued experimental executor even when its configuration flags are enabled; tests may replace the gate for isolated prototype validation. This guard is not a deployment fence and does not protect direct calls to private primitives or old workers.
  - [ ] Expose confirmed switch and resume actions on the source page only after fencing and recovery are enforceable.
  - [ ] Validate the complete workflow with real workers and CDC streams across supported relational databases, including interruptions and concurrent source writes.
  - [x] Exercise PostgreSQL Debezium changes during staging, private generation publication, report routing, late writes to the retained CDC table and a subsequent batch refresh in an opt-in live test. Tasks execute in the test process; this does not establish a deployment fence.
  - [x] Isolate scheduled CDC ingestion and rebuilds on a CDC worker, and batch/keyless refreshes on a batch worker, with separate configurable concurrency.
  - [x] Preserve staging tables across keyset-paginated batch refresh continuations and reject continuations whose staging table is missing.
  - [x] Recheck source mode and active state under the CDC source lock before ingestion or rebuild cutover; reject stale work if either changed.
  - [x] Skip new CDC ingestion batches when a CDC-to-batch transition is cutover pending, after acquiring the source lock; continue normal CDC during staging. This is not sufficient fencing for in-flight work, lock expiry, or keyless refreshes.
  - [x] Hold the control-plane source row lock across each CDC ingestion batch and recheck cutover state while locked. Tests verify a future cutover waits for an in-flight batch and a second ingestion waits even when Redis permits another lock acquisition. Database connection loss and external side effects still need fencing and recovery; this does not implement cutover.
  - [x] Serialize CDC-mode batch refresh tasks using a control-plane source row lock and recheck cutover status and source mode under the lock. A future executor must acquire this same lock before marking cutover pending; in-flight work completes before it can proceed, and queued keyless refreshes are skipped. The lock holds a database transaction over each bounded refresh and does not by itself fence CDC lock expiry.
  - [x] Carry source-mode provenance through ingestion and backfill dispatch, retries and continuations; reject mismatched work and ambiguous unmarked work after a successful CDC-to-batch transition. The future executor must atomically commit source mode and transition success under the source lock, retain successful transition records, and require upgraded producers and workers before switching. Future mode-switch cycles need a generation fence; these checks do not complete cutover fencing.
  - [x] Add private durable source-scoped external-operation registration and a source-lock barrier, with PostgreSQL concurrency tests for both registration/cutover orderings. Unresolved records block the barrier and source deletion.
  - [x] Add disabled-by-default experimental tracking around CDC ingestion, CDC-mode refresh and rebuild tasks, with synchronous ClickHouse inserts, failure retention, and tests for lost database connections. The experimental cutover checks the barrier, but it is not a production-ready fence: request termination, recovery, upgrade coordination and complete writer coverage remain to be established.
  - [x] Add durable transition and per-table checkpoint models with uniqueness constraints, used by staging and the private experimental executor.
  - [x] Add an internal preparation operation that creates table checkpoints atomically and serializes transition preparation against CDC rebuild creation. It is not exposed as a user-facing switch.
  - [x] Reject colliding ClickHouse table names across a source during transition preparation, staging and active-table observation, including case-folding and underscore collisions. The future executor must revalidate under a complete fence; this guard does not change the existing naming scheme.
  - [x] Add internal per-table batch staging into separate ClickHouse tables, with chunked reads, UUID and schema checkpoints, and fresh-table retries after interruption. Interrupted staging tables are retained for manual cleanup. Staging alone does not switch the source.
  - [x] Queue internal staging and legacy restaging one table at a time on the batch worker; validate all checkpoints and table membership afterward without restaging during verification. Failures require explicit retry; staging does not trigger cutover.
  - [x] Add an automated concurrency regression that pauses staging while CDC ingestion processes simulated insert, update, and delete events against a separate live table. Kafka and ClickHouse are mocked and the source is SQLite; live infrastructure still needs validation.
  - [x] Add a durable active-table-checked checkpoint field and tested UUID decision logic for pending and already-completed table exchanges or renames.
  - [x] Add an internal operation to record and revalidate the initial active-table UUID against a staged checkpoint, including missing-table and changed-identity checks. Observations can become stale during normal operation and must be revalidated under a complete cutover fence; execution and recovery orchestration remain unimplemented.
  - [x] Add a private single-table exchange/rename primitive with a durable command-in-flight marker committed before ClickHouse execution. Lost responses, worker exits and checkpoint failures leave an uncertain state that blocks automatic retry and finalization; UUID observations alone cannot rule out a delayed command. It does not establish a fence and must not be invoked without one.
  - [x] Exercise a private exchange and metadata finalization against isolated synthetic ClickHouse tables, including a simulated lost response after a real exchange. This does not test a live CDC stream, delayed execution, recovery or a complete fence.
  - [x] Prototype physical-generation routing and atomic metadata publication as an alternative to exchanging live names. Each copy attempt uses a new candidate name; tests cover lost responses, superseded delayed copies, retained-table writes and multi-table atomic publication. The experimental executor can select it behind a disabled-by-default flag, but it is not exposed in the UI and does not establish deployment coordination or cover untracked metadata writers and direct clients.
  - [x] Coordinate source-page and source/table Django admin mutations, warehouse field selection, report configuration writes and schema introspection with pending cutover and generation publication using a source-scoped advisory transaction lock. Existing source mode is read-only in Django admin, and bulk admin actions require explicit selection. Introspection holds the source row lock for its scan, which may delay CDC processing. Old workers and direct clients still require coordination.
  - [x] Add a private resumable cutover step that exchanges one staged table per call and finalizes on a subsequent call. Uncertain table commands and unresolved external-operation records block further work; tests cover interrupted responses, retries, concurrent calls and report blocking.
  - [x] Add a disabled-by-default experimental queued executor that advances staging, pending cutover, table exchanges and finalization, with explicit resume after task or publish failures and tests for uncertain exchanges. It is not exposed to users and must not be enabled until rollout coordination, recovery, and live interruption testing establish a complete fence.
  - [x] Show administrators transition status, per-table progress and errors on the source edit page, with confirmed staging start, retry and cancellation. Stale retry and cancel confirmations are tied to the original transition. An additional pending-cutover abandonment action retains resources and refuses uncertain or completed exchanges and changed active UUIDs; it does not fence untracked writers. The switch and cutover remain unavailable regardless of experimental settings.
  - [x] Record and display completion of whole-transition staging verification separately from table staging. Verification locks checkpoints before the source and records the timestamp only after all checks succeed; a past timestamp is not proof of cutover readiness.
  - [x] Recheck the checkpoint, source and transition eligibility, table membership, active rebuilds, staged schema and row count after the ClickHouse command before marking a table cut over. Changes or validation failures retain the in-flight marker. These checks do not replace the required external-write fence.
  - [ ] Implement and validate safe recovery of uncertain ClickHouse cutover commands, including proving any prior request has terminated before allowing retries or completion. The exchange primitive cannot run inside an enclosing database transaction.
  - [x] Add a private metadata-finalization primitive that verifies completed checkpoints and current physical tables, then commits table metadata, source mode and transition success atomically. It retains connector references and is not exposed; a future executor must establish and maintain a complete source-scoped external-write fence before invoking it.
  - [x] Acquire checkpoint locks in a consistent order before the source lock during finalization, and reject checkpoint membership changes while waiting. The future caller must not already hold the source row lock; this avoids a database deadlock but is not an external-write fence.
  - [x] Add source-scoped shared report-query locks and a pending-cutover guard, with an exclusive-lock primitive acquired before committing cutover pending. Tests cover concurrent reports, waiting cutover, pending errors and fresh post-cutover metadata. PostgreSQL connection loss and external ClickHouse readers remain a concern; the executor is disabled by default.
  - [x] Extend the disabled-by-default experimental external-operation tracking to report queries, retaining the record on failures or PostgreSQL connection loss; unresolved report records block pending cutover. Tests cover report-task success and failure and lost database connections. This does not implement query termination proof or recovery, ensure all report workers are upgraded, or cover direct ClickHouse readers.
  - [x] Add a private pending-cutover primitive that acquires the exclusive report lock, locks checkpoints before the source, blocks unresolved external-operation records, revalidates staged tables and atomically records live UUIDs and pending status. Tests cover rollback and report/writer coordination. It requires the experimental tracking flag but cannot prove writer rollout or external-request termination; it must not be exposed until those guarantees and recovery exist.
  - [x] Validate actual ClickHouse staged column names, order, types and defaults after staging, on retry and during active-table observation; reject mismatches without automatically changing tables. Cutover must revalidate and separately address row completeness and concurrent changes.
  - [x] Validate that the staged ClickHouse table retains its expected MergeTree engine and empty sorting, partition, primary and sampling keys at those same checkpoints. This does not protect against subsequent changes.
  - [x] Persist the number of rows read and inserted during staging and compare it with the ClickHouse row count before marking the table staged, on retry and during active-table observation. Older checkpoints without a count are restaged into a new table; old tables remain for manual cleanup. Equal-count data changes, source scan completeness, snapshot semantics and post-validation changes remain unverified.
  - [x] Include the source username in the staging signature and reject changes during staging or later verification. Checkpoints created without it are restaged into a new table, retaining the old one; permission or row-level-policy changes for the same user remain undetected.
  - [ ] Validate staging and interruption recovery across all relational connectors. Local isolated staging matched source rows for PostgreSQL, MySQL, SQL Server, and Oracle. A PostgreSQL two-table run completed through the batch worker; an isolated worker exited after an insert and manual retry staged a fresh table with matching rows. PostgreSQL writes during staging produced an earlier staged snapshot; a further test paused staging after an actual insert and confirmed the live CDC worker applied insert, update, and delete events before staging resumed, without changing the live UUID or source mode. Staging in the overlap test ran in a separate application process. Other connectors, batch-worker overlap, other interruption boundaries, and source-specific worker recovery remain to be tested.
  - [ ] Validate source-driver streaming and large-table performance; the current staging primitive holds a checkpoint database transaction for the whole table and restarts that table after interruption.
  - [ ] Implement an automated source-scoped external-write fence and recovery orchestration; coordinate all refresh and CDC work and report reads before exposing a switch. A database row lock alone is insufficient if its connection is lost while ClickHouse work continues. During sequential exchanges, existing CDC report queries cannot safely read a batch table before metadata changes.
- [ ] Design safe source-wide batch-to-CDC transitions; the single-table rebuild does not perform mode transitions or new-column backfills.
- [ ] Decide whether and how to support automatic batch fallback after CDC failures; it is not currently implemented.
- [ ] **Alternative incremental loading strategies:**
  - [ ] Design explicit per-table strategies and validation; do not infer safety from a numeric type or automatically switch strategies. Retain full refresh where guarantees are absent.
  - [ ] Evaluate monotonic ID/sequence watermarks for explicitly append-only sources, with initial-load checkpoints, stable paging, retry safety, concurrent transaction ordering, and a documented policy for late or lower IDs. Updates and deletes require another mechanism or reconciliation.
  - [ ] Evaluate SQL Server `rowversion` and other source-specific change tokens with correct binary/type handling, transaction visibility and safe high-water marks; updates and inserts can be captured, but deletes require separate tracking or reconciliation.
  - [ ] Provide strategy-aware backend validation and a warehouse configuration selector only after source semantics are supported and tested; document timestamp ties, precision, nulls, backfills, and delete behavior.
- [ ] **Inbound file ingestion (proposal; not implemented):** Build a governed onboarding pipeline for files, separate from the existing outbound SFTP report-delivery workflow. Keep this proposal behind the Phase 5 source/tenant authorization work.
  - [ ] **Design the trust boundary and lifecycle:** Define authorized owners, retention, limits, audit data, and explicit states from receipt through scan, schema approval, load, and failure. Do not treat a directory path or MinIO prefix alone as authorization or isolation.
  - [ ] **Integrate the user-owned ScanHub API (deployment details pending):** Complete ScanHub's security hardening in its own project before exposing it; its owner plans to externalize secrets through protected environment configuration such as an uncommitted `.env`. After ScanHub is installed and its owner provides deployment and interface details, verify network reachability, authentication, TLS, upload/stream behavior, size limits, timeout/error semantics, health endpoints, and available engine/signature metadata. Call it from Celery workers rather than synchronous web requests; configure credentials as secrets and restrict service access. Bind each clean verdict to the exact immutable object version/content hash and record scan time, engine/signature version, and correlation ID. Treat unavailable, errored, timed-out, or ambiguous scans as not clean; never parse or load them.
  - [ ] **Add durable file registration and quarantine:** Store originals in a private untrusted MinIO area under generated, non-overwriting keys; record source, object version, SHA-256, size, uploader/partner, state, scan result, attempts, and errors. Promote only the exact scanned bytes to trusted staging. Enforce file and archive-expansion limits, deduplicate by content/source policy rather than filename alone, and retain rejected files according to an explicit quarantine policy.
  - [ ] **Prove multi-format ingestion in the first release:** Map one authorized customer inbox/folder to one logical dataset and ClickHouse target. Support a deliberately small set of record formats (initially CSV and a selected JSON record shape), with explicit mappings into the same typed target and append-only loading. Apply size quotas and stream large files; defer arbitrary formats, upserts, and snapshot replacement.
  - [ ] **Add versioned schema profiles and review:** Scope profiles to an authorized customer and logical dataset, using detected format plus a schema fingerprint rather than filename extension or path alone. Show bounded metadata previews (headers by default; redact or require explicit authorization for row samples). Reuse approved profiles; pause unknown or breaking schemas for review and a new mapping, classify additive changes separately, preserve the original file, and never silently reinterpret types or meanings.
  - [ ] **Map input schemas into logical datasets:** Treat each configured customer folder/inbox as a logical dataset, not as the security boundary or necessarily a literal ClickHouse table. Define a typed canonical schema and explicit, versioned mappings from each accepted input schema; multiple formats may append to the same target only when row grain, key meaning, units, timestamps, and null semantics agree. Preserve file and mapping provenance; avoid one ClickHouse table per header variant unless isolation requires it, and keep volatile extras in JSON/Map only when query needs justify it.
  - [ ] **Separate ingestion from cross-dataset linking:** Files with different schemas mapped to one logical dataset are normalized into its common rows; files in other folders create separate datasets. Link those datasets later through explicit, authorized reference mappings with compatible keys, defined cardinality, and safeguards against duplicate or row-exploding joins. Do not treat row normalization and relational joins as the same operation.
  - [ ] **Define inbound transport and expand sources:** Plan for both pulling files from a partner's SFTP server and receiving pushes to a ReportHub-managed SFTP endpoint; choose implementation order after assessing deployment needs. Route both through the same file-registration, scanning, schema-review, and loading pipeline. For pull, use read-only credentials, pinned host keys, allowed paths, and durable discovery checkpoints. For push, isolate customer accounts and folders, restrict permissions, and define a reliable upload-completion protocol so partial files are not processed. Define duplicate and retry behavior for both, and keep inbound credentials and workflows separate from outbound delivery destinations. Evaluate portal uploads and external S3/Parquet as additional transports/formats.
  - [ ] **Add correction and snapshot modes only with guarantees:** For keyed updates, require stable keys and deterministic versions/tie-breaking; account for `ReplacingMergeTree` deduplication being eventual. For full snapshots, allow partition replacement only when the file is a complete, validated replacement for the exact target partition. Make retries idempotent and document delete/late-arrival behavior.
  - [ ] **Validate end to end before production:** Test ScanHub outages, unknown verdicts, changed object versions, infected and oversized/archive-bomb samples, schema approval/drift, duplicates, partial uploads, worker restarts, concurrent arrivals, retention, and large-file memory/disk bounds against disposable MinIO, ScanHub, SFTP, Celery, and ClickHouse services.

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
- [x] Use the existing email delivery dashboard widget and audit page for delivery monitoring; the audit page filters failed and bounced emails, avoiding a duplicate dashboard widget.

---

## Phase 7: SFTP & Enterprise Cloud Delivery
- FTP and FTPS are outside the supported and planned destination scope.
- [x] **SFTP Transfer Destinations:** Reusable SFTP configuration with protocol-specific authentication and validation. Future cloud destinations are tracked separately below.
  - [x] Add SFTP destination configuration and staff-only control-plane create/edit/delete screens with search and pagination. Passwords, private keys and key passphrases use encrypted model fields, are never rendered back to the browser and are excluded from configuration history. Validate server fingerprints, ports, directories and supported private keys; destinations default to inactive. Saving a destination alone does not connect to its server.
  - [x] Add an SFTP fixture alongside Mailpit in the test Compose stack, with Docker-managed upload and SSH host-key volumes, loopback-only host access and a private client key generated outside Git. A disposable real-SFTP probe verifies upload/download, persistence and host-key stability after container recreation, rejection of a wrong pinned key and denied writes outside the upload directory.
  - [x] Match the Data Sources list/edit styling, breadcrumbs and aligned action buttons; use Active/Inactive status and Activate/Deactivate controls. Add Save & Test Connection to create/edit, preserve saved settings if queuing fails, and only show/submit credential fields for the selected authentication method.
- [x] **SFTP Automated Export Pipelines:** Deliver stored report artifacts through independent downstream Celery tasks.
  - [x] Add a dedicated transfer queue/worker, strict SHA256 host-key pinning, password/key authentication, bounded connection/I/O/task timeouts and staging/non-overwriting publication. Expose a searchable multi-select in Automated File Delivery on report create/edit, independently of email.
  - [x] Generate once and track each execution/destination independently, including failure messages, attempt/retry counts and confirmed manual resend of failed deliveries only. Existing matching remote files are recognized on retry; different content is not overwritten. Delivery remains at-least-once if recipients remove files before acknowledgement. Interrupted running tasks require operator reconciliation rather than automatic resend.
  - [x] Verify actual CSV generation, multi-destination delivery, authentication-failure isolation, host-key rejection and lost-acknowledgement retry through a real prefork Celery worker against disposable PostgreSQL, Redis, ClickHouse, MinIO and SFTP.
  - [x] Add a standalone background SFTP connection-test action without generating a report. Verify the pinned host key, authentication and remote directory without writing files, show polled results and safe errors, and support staff-only Activate/Deactivate actions with audited configuration changes.
  - [x] Verify the final forms and actions with 36 focused destination/delivery tests, live local SFTP checks, and desktop/mobile browser checks. Connection tests do not verify write permission; delivery remains at-least-once, and interrupted running transfers need operator reconciliation.
- [ ] **Enterprise Cloud Destinations (evaluation):**
  - [ ] Evaluate S3-compatible delivery first, reusing existing SDK experience and MinIO for isolated tests. Keep customer export destinations and their credentials separate from ReportHub's internal report storage.
  - [ ] Evaluate Azure Blob Storage and Google Cloud Storage according to customer requirements, preferring workload/managed identities and short-lived credentials over stored long-lived keys.
  - [ ] Define destination authorization and tenant isolation, least-privilege bucket/container-prefix access, TLS verification, encryption requirements, safe object naming, retry/idempotency behavior and delivery audit records before exposing cloud destination choices. Reuse the report destination selector and delivery history rather than creating a separate workflow.

---

## Phase 8: UI Framework, Navigation & Global Polish
- [x] **Pure Tailwind CSS (Slate Palette):** Styling components (like action dropdowns, tables, and modals) using clean slate utilities (`slate-900`, `slate-800`, `slate-700`) and Alpine.js for lightweight interactivity.
- [x] **Admin Navigation:** Split administrative links into Data & Pipelines, Operations, History, Governance, and Integrations menus; page breadcrumbs link back to relevant lists.
- [x] **Global Cursor/Selection Optimization:** Applied a clean app-wide `select-none` approach on structural/UI chrome while targeted `select-text` preserves copy-paste usability on essential audit logs, error traces, and recipient strings.
- [x] Evaluate optional accessible widget navigation menus: the email-delivery widget offers a native disclosure with sent/delivered/failed/bounced audit links, while retaining its direct "View all" link. Simple pipeline, report, CDC failure and DLQ destinations retain direct links. Keyboard activation, Escape/focus return, outside-click dismissal, mobile sizing and empty-state links are verified.
- [ ] **Contextual operations widget (proposal):** Add an optional compact dashboard card highlighting relevant recorded operational issues and linking to existing control-plane workflows. Existing operational records and dashboard components make this feasible; evaluate the implementation against the application's needs rather than assuming a specific architecture. Start with deterministic relevance rules; an LLM is not required.
  - [ ] Start with a staff-only, read-only card using a small set of existing signals, such as a CDC rebuild awaiting cutover, a recently recorded CDC execution failure, or a failed report file delivery. Link to the authorized rebuild or audit page; actions remain in their existing workflows. Define severity, event windows and stable tie-breaking, and distinguish an unresolved state from a historical failure that may already have been addressed. Avoid repeatedly presenting old failures as current incidents.
  - [ ] Scope every candidate and destination to the user's permissions before ranking; don't expose names, counts or details from inaccessible records. Staff access is the initial boundary for these signals, not tenant isolation. Non-staff or tenant-specific cards depend on Phase 5's consistent source/workspace authorization and tenant filtering, plus corresponding authorization on the linked pages. Do not assume an organization model or user-profile organization exists.
  - [ ] Show the record's timestamp and what it actually establishes; a recorded failure does not prove a connector is currently unhealthy, and a recorded running state does not prove a worker is alive. Define an honest no-recent-notices state and an unavailable/stale state for failed or incomplete lookups; never infer that all pipelines, CDC streams or ClickHouse are healthy from the absence of records.
  - [ ] Integrate with dashboard widget preferences and accessible responsive styling. Choose a refresh mechanism consistent with the dashboard and bound its frequency and queries; avoid expensive external checks on each request, surface refresh failures, and evaluate background aggregation only if measurement justifies it.
  - [ ] Add inbound file/schema-review notices only after the Phase 4 inbound pipeline exists; the current SFTP feature is outbound report delivery. Add lag or volume-anomaly notices only after reliable metrics, thresholds, freshness and recovery semantics are implemented and validated. Optional AI wording can be evaluated later alongside the operations assistant, with restricted data, privacy review and deterministic facts; it must not determine severity or invent health claims.
  - [ ] Test ranking and tie-breaking, stale and resolved/historical notices, empty and unavailable states, query bounds, role/source isolation and linked-page authorization. Verify polling, failure feedback, keyboard access and mobile layout in a browser.
- [ ] **AI operations assistant:** Add a floating **Ask ReportHub** button across the control plane, opening an accessible, dismissible chat-style panel rather than a permanent breadcrumb-area panel. Support mobile and keyboard use. Offer suggested questions and read-only answers grounded in current operational records, such as emails sent today, recorded report delivery statuses and recent failures; link answers to the relevant histories.
  - [ ] Use a bounded set of validated query types, with no arbitrary model-generated SQL or operational actions. Enforce existing permissions and source/tenant visibility, restrict the data sent to the model, and surface unavailable, incomplete or stale information rather than inventing answers.
  - [ ] Define time zones and counting semantics, distinguish sent from delivered, and handle missing audit records and stalled tasks. Existing records alone do not prove every scheduled report ran or was delivered.
  - [ ] Start without persistent question history or personalized prediction. Review application and AI-service logs, retention and privacy boundaries before deployment; provide suggested questions without recording individual usage.

---

## Phase 9: Audit & Operational History
- [x] Provide a unified view of available model-history records and pipeline-automation changes.
- [x] Provide a searchable Dead Letter Queue interface for failed task inspection and requeue actions.
- [x] Track report-email delivery states and provide email audit history and administrative actions.
- [x] Provide pipeline and report execution history views.
- [x] Add SFTP delivery audit tracking with search/status filters, per-destination errors and retry counts, confirmed failed-only resend and a staff-only optional dashboard widget.
- [ ] Extend delivery audit tracking to future cloud object-storage deliveries.
- [ ] Add consolidated ETL ingestion metrics and dedicated security/login audit views.

---

## Phase 10: Codebase Audit, Cleanup & Optimization (Active Phase 🎯)
- [ ] **Codebase simplification:** Identify and remove verified unused code, obsolete paths and unnecessary duplication. Simplify complex flows and clarify naming where useful, preserving behavior and public interfaces; validate affected functionality with focused tests and update related documentation.
- [ ] **Dependency & Configuration Pruning:**
  - [x] Consolidate duplicate package declarations in `requirements.txt` (`celery`, `pymysql`, `psycopg2-binary`); verified all declared package names are unique.
  - [ ] Evaluate unused stack dependencies (FastAPI, Uvicorn, AsyncPG) to trim container build overhead if not servicing separate microservice endpoints.
  - [ ] Evaluate reducing duplicated Compose environment configuration without changing variable precedence, service-specific settings, or secret exposure.
- [ ] **Upgrade to Django 6.1:** Stay on Django 5.2 for now. The released `django-celery-beat` 2.9.0 requires Django <6.1; track [upstream support](https://github.com/celery/django-celery-beat/issues/1079) before upgrading.
  - [ ] Upgrade the application image and development environment to a supported Python version (3.12 or newer), then review Django and third-party dependency compatibility, including CSP, encrypted fields, sessions, history and Celery.
  - [ ] Review Django 6.0 and 6.1 settings and breaking changes; test migrations, scheduling, authentication, report delivery and existing application behavior in an isolated environment before deployment.
- [ ] **Audit Trail & Encryption Hardening:**
  - [ ] Audit `changes_data` JSON serialization in `control_plane/models.py` / `admin.py` to ensure encrypted credential fields are explicitly masked before historical snapshots are persisted.
- [ ] **Task & Memory Safety Safeguards:**
  - [ ] Audit ingestion and report pagination for bounded memory, stable ordering, and correct results under concurrent writes. Use keyset pagination only where a suitable stable key exists; account for composite keys and keyless sources.
  - [ ] Ensure dynamic SQLAlchemy engine calls strictly utilize context managers to prevent connection pool leaks against polyglot target databases.
- [ ] **Frontend & HTMX Polish:**
  - [ ] Audit templates and event bindings against the configured CSP, using delegated listeners where appropriate, and test the resulting behavior.
  - [ ] Verify clean termination on HTMX polling triggers (`hx-trigger="every 2s"`) upon reaching terminal execution states.
    - [x] Check data-source, SFTP connection-test and report status responses: they poll only while queued or running and stop emitting polling attributes on terminal states. Remove duplicate polling from the source and report lists.
    - [ ] Verify polling behavior in a browser. Dashboard active-work widgets deliberately continue polling when empty so newly started work can appear.

---

## Phase 11: Asynchronous Reporting, Query Generation & Export Pipelines
- [x] **Multi-Tiered Storage & Lifecycle Indexing:**
  - [x] Track report execution lifecycle metadata in PostgreSQL (`pending`, `running`, `completed`, `failed`, `cancelled`).
  - [x] Upload completed report artifacts to MinIO and persist their object paths with execution records.
  - [x] Add recoverable consistency handling between MinIO uploads and PostgreSQL execution updates. Each upload attempt has a unique object path and records its expected size and SHA-256 before upload; completion verifies the stored size and checksum metadata. Staff can reconcile the current uncertain attempt from report history. Missing or mismatched objects are retained for review and never automatically deleted. Reconciliation finalizes the artifact only; it does not replay email or file-delivery fan-out.
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
  - [ ] **Evaluate AI-assisted ClickHouse analytics and observability (proposal; not implemented):**
    - [ ] Extend natural-language report drafting to answer analytical questions through a constrained, validated report/query specification and the existing safe SQL builder; do not execute arbitrary model-generated SQL. Require source/tenant authorization, read-only ClickHouse access, and query/result limits before execution.
    - [ ] Curate business metric definitions and table/column descriptions in PostgreSQL, retrieve relevant metadata for ambiguous terms, and evaluate `pgvector` only if simpler metadata search proves insufficient. Do not send raw event rows to the model.
    - [ ] Offer opt-in, staff-visible query diagnostics using sanitized ClickHouse query-log and `EXPLAIN` data; generate reviewed optimization suggestions only, never automatic schema or query changes.
    - [ ] Explore scheduled anomaly checks over bounded aggregate metrics with explicit baselines and thresholds; use AI only to summarize detected changes, and require configured human-reviewed alerting.
  - [ ] Implement chunked streaming exports (CSV, Excel/pandas, ReportLab PDF) to keep Celery memory utilization low.
  - [ ] Add tokenized, time-expiring report-sharing URLs with tenant-scoped authorization.
  - [x] Include aggregate functions in Grouped Summary output labels while preserving configured display labels.
  - [ ] Calculate AI summary metrics with awareness of aggregate semantics.
  - [ ] Evaluate richer AI-assisted report layout drafts and metadata retrieval while retaining validated structured output and the existing safe SQL builder; define permissions, prompt-injection defenses, model/network privacy boundaries, and human review before execution.
  - [ ] Document and test backup/restore for Django PostgreSQL, ClickHouse, MinIO, Kafka state, and Ollama model storage; local deployment alone does not guarantee privacy or recovery.