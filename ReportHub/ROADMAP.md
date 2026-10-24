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

---

## Phase 2: Django Control Plane, User Management & Audit Engine
- [x] Initialize Django project and core apps (`control_plane`, `warehouse`, `etl`).
- [x] Configure automatic database migrations via container startup scripts (`entrypoint.sh`).
- [x] Implement **Security-First Hardening**: Content Security Policy (`django-csp`), strict `ALLOWED_HOSTS`, clickjacking/MIME defenses, HttpOnly session/CSRF cookies, and global 64-bit `BigAutoField`.
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
- [ ] Implement **Celery Schema Introspection Task** (`information_schema` / MSSQL catalog views / SQLAlchemy inspector) to asynchronously discover tables/columns from target databases (PostgreSQL, MySQL, and Microsoft SQL Server) and populate Django control plane metadata.
- [ ] Validate schema discovery against real-world target databases over local LAN and test containers.
- [ ] Develop the configuration web page (interactive field toggling, blob exclusion, and multi-metric selection).

---

## Phase 4: Multi-Source Ingestion & Dynamic ETL Pipeline (Celery + Redis)
- [ ] Define SQLAlchemy analytical models featuring dynamic `JSONB` payload fields for variable record schemas.
- [ ] Build idempotent batch ingestion Celery tasks with `source_id` stamping and JSON exclusion filtering.
- [ ] Implement relational roll-up workers (Hot-to-Warm daily summaries leveraging preserved foreign key IDs).
- [ ] Implement automated **Row-Level Security (RLS)** query filtering (`company_id` enforcement).
- [ ] Build date-range chunking and `UPSERT` historical backfill engine.

---

## Phase 5: Report Generation & Query Engine
- [ ] Build the dynamic query builder and execution engine over ingested database tables (`JSONB` query extraction).
- [ ] Develop report templates, data visualization components, and export tools (CSV/Excel/PDF).
- [ ] Implement **Authenticated Report & Dashboard Sharing** (role-based visibility restricting "View Only" users strictly to authorized dashboards and reports).