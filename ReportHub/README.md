# ReportHub 🚀
> Enterprise-grade, dynamic multi-source analytical warehouse and reporting control plane.

`ReportHub` is a high-performance analytics platform designed to dynamically ingest, unify, and report on data from multiple distributed source databases (PostgreSQL, MySQL, and Microsoft SQL Server) into a centralized analytical warehouse—**without hardcoding database models**. Built with a Django control plane, a high-throughput **ClickHouse** analytical engine, **MinIO** S3-compatible object storage, and a Celery-driven asynchronous ETL pipeline, ReportHub empowers organizations to manage schema reflection, multi-tenant row-level security, scheduled task governance, and complex metric aggregations through a sleek modern web interface.

---

## 🏗️ Core Architecture & Design

- **Decoupled Dual-Database Architecture:** Strict separation between the operational **Control Plane DB** (hardened Django database for configuration, user access matrix, and metadata) and the high-throughput **Analytical Warehouse (ClickHouse + PostgreSQL)** driven via standalone Alembic migrations and columnar storage.
- **ClickHouse Analytical Warehouse:** Columnar database engine optimized for real-time analytical queries, high-performance aggregations, primary key optimization, and automated TTL data retention.
- **MinIO S3-Compatible Object Storage:** Dedicated sidecar container bound to persistent volumes (`minio_data`) for bulky binary report storage. Utilizes `django-storages` and `boto3` with pre-signed secure URLs to stream report downloads directly to users while bypassing Django web workers.
- **Session Security & Idle Timeout:** Hardened session management utilizing browser-close expiration (`SESSION_EXPIRE_AT_BROWSER_CLOSE = True`) and `django-auto-logout` with dynamic environment-driven idle timeout control (`SESSION_IDLE_MINUTES`).
- **Asynchronous Reporting & Conveyor-Belt Pipelines:** Long-running reports are broken down into sequential Celery task chains (e.g., 50,000 rows per chunk) using primary-key/timestamp cursor pagination (`WHERE id > last_seen_id`) to prevent shifting-window data drift. Features queue isolation (`default` for fast app tasks vs. `reports` for heavy extractions) and schema migration safety guards.
- **Django Control Plane:** Synchronous control plane managing user authentication, role-based access control (RBAC), self-service user profile management, source database connection profiles, scheduled ETL task management, and report sharing rules.
- **Unified Audit Governance & Scheduled Task Tracking:** Full history tracking across control plane infrastructure models, `TrackedUser`, and standalone scheduled tasks via `ScheduleAuditLog` (utilizing custom JSON diff tracking). Features an XSS-hardened unified audit log viewer (`/control-plane/audit/`) with field-level Old-to-New diff comparison modal cards and full-text search.
- **Scheduled ETL Task Governance:** Fully searchable and paginated management UI for `PeriodicTask` background jobs with integrated error-handling and uniqueness validation (`ValidationError` / `IntegrityError` safeguards for duplicate task names).
- **Asynchronous UI Status & Toast Notification Architecture:** Real-time database connection testing driven by HTMX polling (`every 2s`), backed by CSP-compliant event-driven (`htmx:afterSwap`) floating toast notifications and robust fallback UI states ("Ready", "Running...", "Failed").
- **Dynamic Warehouse Provisioning:** Rather than hardcoding models, ReportHub inspects source databases (`information_schema` / catalog views), allows selective field mapping via an interactive master-detail UI, and dynamically provisions analytical tables complete with composite indexes, `source_id`, `company_id`, and logical foreign key references.
- **Multi-Source Unification:** Every ingested row is stamped with a `source_id`, allowing independent ETL schedules per server while enabling seamless cross-server global reporting through simple query filters.
- **Enterprise Security Matrix:** 
  1. *Source-Level Access:* Restrict users to specific source database instances.
  2. *Role-Based Access Control (RBAC):* Viewer, Analyst/Builder, and System Admin roles.
  3. *Row-Level Security (RLS):* Automated tenant isolation via `company_id` injection on all queries.
  4. *Security Hardening:* Enforced via `django-csp` (Content Security Policy), HttpOnly session/CSRF cookies, strict host configurations, and internal-only Docker networking for databases, Redis, and MinIO.
- **Secure Report Sharing:** Save custom reports and generate time-expiring, tokenized public or team-shared URLs.

---

## 🧪 Polyglot Test Database Environment

ReportHub includes an isolated multi-database test harness (`docker-compose.test.yml`) that spins up all supported target database engines simultaneously alongside persistent volumes, allowing you to test schema introspection and ETL synchronization against real instances locally.

### Test Database Connection Matrix

| Database Engine | Default Host | Port | Credentials (User / Password) | Target Database / Service Name |
| :--- | :--- | :--- | :--- | :--- |
| **PostgreSQL** | `localhost` | `5433` | `postgres` / `postgres_test_pwd` | `reporthub_test_pg` |
| **MySQL** | `localhost` | `3307` | `root` / `mysql_test_pwd` | `reporthub_test_mysql` |
| **Microsoft SQL Server** | `localhost` | `1434` | `sa` / `MssqlTestPassword123!` | `master` (or custom test db) |

### Launching the Test Environment
```bash
# Spin up the polyglot test containers in the background
docker compose -f docker-compose.test.yml up -d

# Shut down and clean up test containers/volumes when finished
docker compose -f docker-compose.test.yml down -v
```

---

## 🛠️ Tech Stack

- **Backend:** Python, Django (Synchronous Control Plane), Hypercorn / Gunicorn ASGI/WSGI
- **Async & ETL:** Celery, Redis (Configured with AOF persistence enabled)
- **Analytical & Object Storage:** ClickHouse (Columnar Data Warehouse), MinIO (S3-Compatible Object Storage via `django-storages` and `boto3`), PostgreSQL 15 (Operational & Warm Storage)
- **Source Integration & Drivers:** PostgreSQL (`psycopg2`), MySQL (`pymysql`), Microsoft SQL Server (`pyodbc` with Microsoft ODBC Driver 18 for Debian/Linux)
- **Testing & Quality:** Pytest, Playwright (for automated web application testing & debugging)
- **Frontend:** HTMX, Tailwind CSS, Custom Components, Modern Vanilla JavaScript (Event-Driven DOM handlers)
- **Infrastructure:** Docker, Docker Compose, RHEL 9 compatible deployment targets

---

## 📂 Project Structure

```text
reporthub/
├── app/                  # Django project configuration (settings, wsgi, celery)
├── control_plane/          # Django app: Users, RBAC, Source Profiles, Audit Logs, Schedules
├── warehouse/              # Dynamic schema reflection & table provisioner utilities
├── etl/                    # Celery tasks (Batch ingestion, roll-ups, backfilling, cleanup)
├── test/                   # Polyglot test database environments & sandbox configs
├── Dockerfile              # Container build instructions (including MS ODBC drivers)
├── docker-compose.yml      # Service orchestration (Web, Postgres, Redis, ClickHouse, MinIO, Celery)
├── docker-compose.test.yml # Polyglot test databases (Postgres, MySQL, MSSQL)
├── requirements.txt        # Python dependencies (includes pyodbc, boto3, django-storages)
├── entrypoint.sh           # Container startup & auto-migration script
├── ROADMAP.md              # Project implementation tracker
└── README.md               # Project documentation