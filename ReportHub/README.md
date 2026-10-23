# ReportHub 🚀
> Enterprise-grade, dynamic multi-source analytical warehouse and reporting control plane.

`ReportHub` is a high-performance analytics platform designed to dynamically ingest, unify, and report on data from multiple distributed MySQL source databases into a centralized PostgreSQL warehouse—**without hardcoding database models**. Built with a Django control plane and a Celery-driven asynchronous ETL pipeline, ReportHub empowers organizations to manage schema reflection, multi-tenant row-level security, and complex metric aggregations through a sleek modern web interface.

---

## 🏗️ Core Architecture & Design

- **Decoupled Dual-Database Architecture:** Strict separation between the operational **Control Plane DB** (hardened Django database for configuration, user access matrix, and metadata) and the standalone **Analytical Warehouse DB** (driven via standalone Alembic migrations with `JSONB` flex-payload columns).
- **Django Control Plane:** Synchronous control plane managing user authentication, role-based access control (RBAC), self-service user profile management, source database connection profiles, and report sharing rules.
- **Unified Audit Governance:** Full history tracking across control plane infrastructure models and user accounts using `django-simple-history`, featuring an XSS-hardened unified audit log viewer (`/control-plane/audit/`) with field-level Old-to-New diff comparisons and full-text search.
- **Dynamic Warehouse Provisioning:** Rather than hardcoding models, ReportHub inspects source databases (`information_schema`), allows selective field mapping (excluding heavy files like binary blobs), and dynamically provisions PostgreSQL analytical tables complete with composite indexes, `source_id`, `company_id`, and logical foreign key references.
- **Multi-Source Unification:** Every ingested row is stamped with a `source_id`, allowing independent ETL schedules per server while enabling seamless cross-server global reporting through simple query filters.
- **Multi-Metric Engine:** Apply multiple aggregate calculations (`SUM`, `AVG`, `MAX`, conditional counts like `> £1,000`) to a single source field during nightly rollups.
- **Enterprise Security Matrix:** 
  1. *Source-Level Access:* Restrict users to specific source database instances.
  2. *Role-Based Access Control (RBAC):* Viewer, Analyst/Builder, and System Admin roles.
  3. *Row-Level Security (RLS):* Automated tenant isolation via `company_id` injection on all queries.
  4. *Security Hardening:* Enforced via `django-csp` (Content Security Policy), HttpOnly session/CSRF cookies, and strict host configurations.
- **Secure Report Sharing:** Save custom reports and generate time-expiring, tokenized public or team-shared URLs.

---

## 🛠️ Tech Stack

- **Backend:** Python, Django (Synchronous Control Plane), Hypercorn / Gunicorn ASGI/WSGI
- **Async & ETL:** Celery, Redis (Configured with AOF - Append-Only File persistence enabled)
- **Warehouse & Storage:** PostgreSQL 15 (Optimized for analytical queries & warm storage)
- **Source Integration:** MySQL (`information_schema` reflection)
- **Testing & Quality:** Pytest, Playwright (for automated web application testing & debugging)
- **Frontend:** Bootstrap 5 / Tailwind-inspired components, Custom CSS, Modern Vanilla JavaScript (AJAX/Fetch)
- **Infrastructure:** Docker, Docker Compose, RHEL 9 compatible deployment targets

---

## 📂 Project Structure


reporthub/
├── app/                  # Django project configuration (settings, wsgi, celery)
├── control_plane/        # Django app: Users, RBAC, Source Profiles, Audit Logs, Profile Management
├── warehouse/            # Dynamic schema reflection & table provisioner utilities
├── etl/                  # Celery tasks (Batch ingestion, roll-ups, backfilling)
├── Dockerfile            # Container build instructions
├── docker-compose.yml    # Service orchestration (Web, Postgres, Redis, Celery)
├── requirements.txt      # Python dependencies
├── entrypoint.sh         # Container startup & auto-migration script
├── ROADMAP.md            # Project implementation tracker
└── README.md             # Project documentation