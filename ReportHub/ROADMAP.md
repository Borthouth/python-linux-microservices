# ReportHub Project Roadmap 🗺️

Track the development and implementation phases of `ReportHub`.

## Phase 1: Infrastructure & Containerization
- [x] Initialize repository structure (`README.md`, `ROADMAP.md`, `.gitignore`).
- [x] Configure `Dockerfile`, `requirements.txt`, and entrypoint scripts.
- [x] Set up `docker-compose.yml` orchestrating PostgreSQL (tuned for analytics), Redis (with AOF persistence), Django Web, and Celery workers.
- [x] Configure environment variables (`.env.example`) and verify local container startup.

## Phase 2: Django Control Plane & Security Engine
- [x] Initialize Django project and core apps (`control_plane`, `warehouse`, `etl`).
- [x] Configure automatic database migrations via container startup scripts (`entrypoint.sh`).
- [x] Implement **Security-First Hardening**: Content Security Policy (`django-csp`), strict `ALLOWED_HOSTS`, clickjacking/MIME defenses, HttpOnly session/CSRF cookies, and global 64-bit `BigAutoField`.
- [x] Set up user authentication and RBAC roles (*Viewer, Analyst/Builder, System Admin*).
- [x] Build models for **Source Profiles** and the **User-Source Access Matrix** (Layer 1 security).
- [ ] Implement **Self-Service User Profile Management** (nav-bar linked account view for users to update their email and change their password).

## Phase 3: Schema Reflection & Configuration UI
- [ ] Build the multi-source connection profile manager and control plane web views.
- [ ] Write the **Information Schema Reader** to introspect remote MySQL/Postgres databases (`information_schema.columns`).
- [ ] Develop the configuration web page (interactive field toggling, blob exclusion, and multi-metric selection).

## Phase 4: Multi-Source Ingestion & Dynamic ETL Pipeline (Celery + Redis)
- [ ] Build idempotent batch ingestion Celery tasks with `source_id` stamping and JSON exclusion filtering.
- [ ] Implement relational roll-up workers (Hot-to-Warm daily summaries leveraging preserved foreign key IDs).
- [ ] Implement automated **Row-Level Security (RLS)** query filtering (`company_id` enforcement).
- [ ] Build date-range chunking and `UPSERT` historical backfill engine.

## Phase 5: Report Generation & Query Engine
- [ ] Build the dynamic query builder and execution engine over ingested database tables.
- [ ] Develop report templates, data visualization components, and export tools.
- [ ] Implement **Authenticated Report & Dashboard Sharing** (role-based visibility restricting "View Only" users strictly to authorized dashboards and reports).