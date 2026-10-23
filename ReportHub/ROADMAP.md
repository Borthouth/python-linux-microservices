# ReportHub Project Roadmap 🗺️

Track the development and implementation phases of `ReportHub`.

## Phase 1: Infrastructure & Containerization
- [ ] Initialize repository structure (`README.md`, `ROADMAP.md`, `.gitignore`).
- [ ] Configure `Dockerfile`, `requirements.txt`, and entrypoint scripts.
- [ ] Set up `docker-compose.yml` orchestrating PostgreSQL (tuned for analytics), Redis (with AOF persistence), Django Web, and Celery workers.
- [ ] Configure environment variables (`.env.example`) and verify local container startup.

## Phase 2: Django Control Plane & Security Engine
- [ ] Initialize Django project and core apps (`control_plane`, `warehouse`, `etl`).
- [ ] Set up user authentication and RBAC roles (*Viewer, Analyst/Builder, System Admin*).
- [ ] Build models for **Source Profiles** and the **User-Source Access Matrix** (Layer 1 security).
- [ ] Build models for **Report Sharing & Tokenized URLs**.
- [ ] Configure automatic database migrations via container startup scripts.

## Phase 3: Schema Reflection & Configuration UI
- [ ] Build the multi-source connection profile manager.
- [ ] Write the **Information Schema Reader** to introspect remote MySQL databases (`information_schema.columns`).
- [ ] Develop the sleek Bootstrap 5 **Configuration Web Page** (interactive field toggling, blob exclusion, and multi-metric selection).

## Phase 4: Multi-Source Ingestion & Dynamic ETL Pipeline (Celery + Redis)
- [ ] Build idempotent batch ingestion Celery tasks with `source_id` stamping and JSON exclusion filtering.
- [ ] Implement relational roll-up workers (Hot-to-Warm daily summaries leveraging preserved foreign key IDs).
- [ ] Implement automated **Row-Level Security (RLS)** query filtering (`company_id` enforcement).
- [ ] Build date-range chunking and `UPSERT` historical backfill engine.
