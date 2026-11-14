# ReportHub 🚀
> Enterprise-grade, dynamic multi-source analytical warehouse and reporting control plane.

`ReportHub` is an analytics platform designed to ingest, unify, and report on data from multiple distributed source databases (PostgreSQL, MySQL, Microsoft SQL Server, MongoDB, and Oracle) into a centralized analytical warehouse—**without hardcoding source-data models**. Built with a Django control plane, a **ClickHouse** analytical engine, **MinIO** S3-compatible object storage, and Celery-driven background tasks, ReportHub provides schema reflection, source-access configuration, scheduled task governance, asynchronous email notifications, and report generation.

For overall progress and future work, see [ROADMAP.md](./ROADMAP.md). The [CDC-to-batch handoff](./CDC_TO_BATCH_HANDOFF.md) is the detailed tracker and next-task guide for CDC work.

---

## 🏗️ Core Architecture & Design

- **Decoupled Data Architecture:** The Django database stores control-plane configuration and source metadata, while ClickHouse stores ingested analytical data. Ingestion provisions ClickHouse tables from source metadata. A separate Alembic configuration and an empty initial revision exist, but they do not manage those ClickHouse tables.
- **ClickHouse Analytical Warehouse:** Columnar database engine for analytical queries and aggregations. CDC-managed tables use versioned `ReplacingMergeTree` engines with delete tombstones; batch-mode sources and CDC tables without a discovered primary key use bounded full refreshes. Automated TTL data retention is not configured.
- **MinIO S3-Compatible Object Storage:** MinIO persists data in the host-mounted `./docker/minio_data` directory. Celery uploads report artifacts to MinIO; authenticated administrator downloads are served through Django. The Compose file publishes MinIO's console port and should be restricted appropriately outside development.
- **Session Security & Idle Timeout:** Hardened session management utilizing browser-close expiration (`SESSION_EXPIRE_AT_BROWSER_CLOSE = True`) and `django-auto-logout` with dynamic environment-driven idle timeout control (`SESSION_IDLE_MINUTES`).
- **Asynchronous Reporting:** Celery report tasks query ClickHouse and currently assemble export data in memory. Chunked/streaming report exports remain planned. Other background work uses dedicated Celery queues.
- **Django Control Plane & Standardized Shell Layout:** Centralized `base.html`, role-based access configuration, self-service profile management, source database profiles, scheduled ETL task management, and user widget preferences. Collaborative workspaces and report sharing links are not currently implemented.
- **Audit History:** Selected control-plane models use historical records, pipeline automation changes have a dedicated audit log, and `/control-plane/audit/` displays available configuration history. Separate email-delivery and failed-task (DLQ) audit/management pages are also available; a consolidated multi-domain audit suite is not implemented.
- **Dead Letter Queue (DLQ) & Background Task Governance:** Redis-backed storage (`dlq:task:*`) requiring JSON decoding, attribute normalization, and robust timestamp parsing via `django.utils.dateparse`. Fully searchable and paginated management UI (`/control-plane/dlq/`) with matched container widths (`max-w-7xl`) and modular dashboard grid widgets for tracking, inspecting stack traces, and re-queuing failed background tasks across the 7-tier Celery infrastructure.
- **Active pipeline telemetry:** The dashboard refreshes the five most recent tasks last reported as running every three seconds. The full searchable, paginated list is under **Operations > Active Pipeline Executions**, with a fixed current-step filter; table and collection names can be found using search. Redis progress records are not a live worker census: interrupted or completed batch tasks may remain marked running until their records expire.
- **Active report generations:** The dashboard refreshes the five newest report executions recorded as running every three seconds. The full list is searchable by report name, filterable by current step, and paginated under **Operations > Active Report Generations**. Steps reflect last-saved progress; an unrecognized progress value is shown as Unknown. A recorded running status does not guarantee that its worker is still running.
- **Asynchronous Email Delivery & Status Auditing:** Celery-driven background email dispatches backed by Django's email backend and a local development test email server. Includes an **Email Delivery Audit & Governance Module** tracking delivery lifecycles (`sent`, `delivered`, `failed`, `bounced`), retry counts, error tracebacks, inspection modals, and re-queue/delete workflows.
  Staff users can view the five latest audit records on the dashboard and toggle the widget under **Customize Widgets**; the widget links to the full audit page.
- **Asynchronous UI Status & Toast Notification Architecture:** Database connection testing uses HTMX polling (`every 2s` while running), backed by CSP-compliant event-driven (`htmx:afterSwap`) floating toast notifications and robust fallback UI states ("Ready", "Running...", "Failed"). Polling stops when the test succeeds or fails.
- **Dynamic Warehouse Provisioning:** Rather than hardcoding models, ReportHub inspects source databases (`information_schema` / catalog views), allows selective field mapping via an interactive master-detail UI, and provisions analytical tables with source namespacing and logical foreign-key metadata.
- **Multi-Source Ingestion:** ClickHouse table names include the source ID for source identification. Reports can join directly related tables within one source; cross-source linking and reporting are planned, not implemented.
- **Security & Access Configuration:**
  1. *Source Access Assignments:* `UserSourceAccess` stores per-user source assignments and roles. Consistent enforcement across all operations, workspace access, and tenant-level row filtering still require verification or implementation.
  2. *Role-Based Access Control (RBAC):* Viewer, Analyst/Builder, and System Admin roles (`is_admin`).
  3. *Tenant Row-Level Security:* Company-scoped `company_id` query injection is not currently implemented.
  4. *Security Hardening:* Includes `django-csp`, HttpOnly session/CSRF cookies, and configured Django security headers. Compose publishes database, Redis, ClickHouse, and MinIO-console ports to the host; restrict them with firewall/network settings for deployments.
- **Operational Audit Views:** Available views include configuration history, DLQ task management, email and SFTP delivery audit, and pipeline/report execution histories. Consolidated ETL metrics and login-security audit views remain future work.
- **Report Executions & Artifact Downloads:** Track report execution status and history (`ReportExecution`), store completed exports in MinIO, and provide authenticated downloads to administrators. The report list polls while an execution is queued or running and stops when it finishes. Each upload attempt uses a unique object path and records expected size and SHA-256 before upload; completion verifies the stored size and checksum metadata. Staff can reconcile the current uncertain attempt from **All Report Runs**. Missing or mismatched objects are retained for review and never automatically deleted. Reconciliation finalizes artifact state only; it does not replay email or file-delivery fan-out.
- **Visual Report Exports:** Configure bar, line, or pie charts from report columns and include them in PDF and Excel exports; Excel workbooks also include a filtered data sheet and a separate chart.
- **Tabular and Grouped Reports:** Tabular Detail returns selected source records. Grouped Summary groups by selected columns without an aggregate and applies `COUNT`, `SUM`, `AVG`, `MIN`, or `MAX` to aggregated columns; selecting aggregates for every column produces one overall summary row.
- **Readable PDF Exports:** Wide reports use landscape layout, wrapped cells, and repeated table headers. PDF output is capped to a configured row limit.

---

## Local configuration

For a **new local development deployment**, copy `.env.example` to `.env`
without overwriting an existing file:

```bash
cp -n .env.example .env
```

Edit `.env` and replace every `CHANGE_ME` value. Generate a Django secret and a
Fernet field-encryption key with Python 3:

```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
python3 -c "import base64, secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())"
```

Use the first output for `SECRET_KEY` and the second for
`FIELD_ENCRYPTION_KEY`. Generate separate strong passwords for PostgreSQL,
ClickHouse and MinIO; use the same MinIO username and password in both the
`MINIO_ROOT_*` and `MINIO_STORAGE_*` variables. Keep the two bucket names
consistent. Add the actual browser hostname to `ALLOWED_HOSTS` if accessing
ReportHub from another machine.

Keep `.env` private and backed up securely. **Do not replace an existing
`FIELD_ENCRYPTION_KEY`**: saved source credentials depend on it. Database
password variables initialize new persistent volumes; changing them in `.env`
does not rotate existing PostgreSQL or ClickHouse credentials. MinIO changes
also require coordinated credential configuration. Follow an appropriate
rotation procedure instead of overwriting an existing configuration.

The template is for local development only: it enables debug mode, and the
bundled Compose stack exposes infrastructure ports and uses privileged service
accounts. It is not a production security configuration. The experimental
CDC-to-batch flags must remain disabled until the documented fencing and
recovery work is complete. Docker Compose reads `.env` for interpolation;
Django also reads the mounted project file, while explicitly supplied process
environment variables take precedence. The Docker build excludes `.env` files.

### Optional separate application database identities

Django, Celery workers and Beat can use separately provisioned credentials by
setting **both** `APP_POSTGRES_USER` and `APP_POSTGRES_PASSWORD`, and/or both
`APP_CLICKHOUSE_USER` and `APP_CLICKHOUSE_PASSWORD`. These are optional; when
both values for a database are absent, ReportHub retains the existing
`POSTGRES_USER` / `POSTGRES_PASSWORD` and `CLICKHOUSE_USER` /
`CLICKHOUSE_PASSWORD` behavior. A partial or empty application credential pair
stops Django startup rather than silently using an administrator account.
The bundled PostgreSQL and ClickHouse servers continue to use their existing
server credentials; changing the application values does not create users,
grant privileges, rotate credentials or restart services.
With the bundled Compose configuration, place these values in the project
`.env` mounted into each application container, or configure the container
environment explicitly. Merely exporting them in the host shell does not pass
them into the containers unless you also include the optional
`docker-compose.application-credentials.yml` override, for example:

```bash
docker compose -f docker-compose.yml -f docker-compose.cdc.yml \
  -f docker-compose.application-credentials.yml config --quiet
```

Include the same files in Compose commands when you intentionally deploy the
override. Process environment values take precedence over the mounted file per
variable, so set or override each pair together.

Provision and test any new identities separately before a coordinated rollout
to **all** web and worker processes. By default, every container runs Django
migrations at startup, so a PostgreSQL role without the required migration and
application permissions can cause migrations to fail. If migrations fail, the
entrypoint logs a startup error and exits with the migration command's status
without launching the main process. An opt-in migration process is described
below; the required least-privilege roles and rollout are not yet implemented.
Compose still passes the legacy PostgreSQL credentials to application
containers and mounts the project `.env`, which can contain administrator
credentials. Distinct credentials alone do **not** fence old workers, direct
clients or in-flight requests and must not be used to enable CDC-to-batch cutover.

### Opt-in dedicated migration process

`docker-compose.migrations.yml` adds a one-shot `migrations` service. It runs
`python manage.py migrate --noinput` with the separately provisioned PostgreSQL
identity supplied by `MIGRATION_POSTGRES_USER` and `MIGRATION_POSTGRES_PASSWORD`.
Both variables must be nonempty and available to Compose (export them or put
them in its environment file); missing values reject the configuration.
The migration role must have all permissions needed for existing and future
Django migrations, including ownership of objects it must alter. This override
does not create roles, transfer ownership or grant privileges.

All web, worker and beat services disable their own startup migrations and
depend on successful completion of the migration service. A failed migration
therefore blocks dependent services from starting. Existing running processes
are not stopped by this dependency. Before adopting this configuration, coordinate
draining and stopping existing application processes and test the identities in
isolation; do not apply it to a running shared stack without approval.

For an approved deployment, use the same Compose files for both steps:

```bash
compose_with_migrations() {
  docker compose -f docker-compose.yml -f docker-compose.cdc.yml \
    -f docker-compose.application-credentials.yml \
    -f docker-compose.migrations.yml "$@"
}

# Force a fresh migration run for each deployment, not a stale successful job.
compose_with_migrations up -d --build --force-recreate migrations
compose_with_migrations up -d --build
```

Omit the CDC overlay for batch-only deployments. Do not use `--no-deps` for
application startup: it bypasses the migration dependency. Ordinary container
restarts do not rerun this deployment gate; the two-step procedure is required
for each new deployment. The existing `reporthub-stack.sh` helper does not
include these optional overrides and must not be used to deploy this mode.

An opt-in disposable Compose probe applies the actual migration override to an
isolated test stack. It checks that incorrect credentials and a PostgreSQL DDL
failure prevent runtime startup, then verifies a restricted runtime after a
successful migration and after a second, forced fresh migration run. The test
uses a runtime probe in place of the production web and worker commands; it
does not validate stopping existing production processes or every deployment
path. Run it from `ReportHub/`:

```bash
REPORTHUB_MIGRATION_GATE_TEST=1 python3 -m unittest discover \
  -s tests/infrastructure -t . -p test_migration_gate.py -v
```

Outside this override, `MIGRATIONS_ON_STARTUP` defaults to `1`; explicit `0`
skips startup migrations, and any other value fails startup. Disabling it
outside an equivalent migration-gated deployment is unsafe. This separation
does not remove privileged credentials from the shared `.env` mount or legacy
container environment, and is not a CDC-to-batch fence.

### Test layout and discovery

- `tests/infrastructure/`: standalone startup, Compose, query-cancellation and
  opt-in disposable service-identity tests. No Django installation is needed
  on the host for discovery.
- `tests/integration/`: opt-in Django/ClickHouse and PostgreSQL CDC stream
  tests, plus the identity probe executed inside disposable containers.
- `tests/docker-compose*.yml`, `tests/init-db/` and test startup scripts:
  supporting infrastructure.
- Application `tests.py` modules remain alongside their Django apps, including
  `config/tests.py`, `control_plane/tests.py`, `etl/tests.py` and `reports/tests.py`.
  Django's normal application-test discovery remains supported.

Run standalone tests from `ReportHub/`:

```bash
python3 -m unittest discover -s tests/infrastructure -t . -v
```

The identity integration runner is skipped unless `REPORTHUB_IDENTITY_TEST=1`
is set. Live Django integration tests retain their separate opt-in flags and
must run through `manage.py test tests.integration` in a configured Django
environment; do not use host-only unittest discovery for that directory.

### Isolated service-identity permission tests

The candidate permission model is exercised by
`tests/integration/service_identity_probe.py`, not installed into the running stack:

- PostgreSQL migration identity: owns the isolated database and `public` schema
  and objects it creates; no superuser, role/database creation, replication or
  RLS-bypass attributes. It applies the complete Django migration chain.
- A separate populated PostgreSQL database tests a candidate transfer from a
  legacy owner: the isolated administrator runs `REASSIGN OWNED`, sets schema
  ownership and grants for existing and future objects, and repeats the
  operation. Existing data, serial and identity sequences, migration history,
  a subsequent migration, and positive and negative runtime permissions are
  checked. This is a probe, not a production migration procedure.
- PostgreSQL runtime identity: database `CONNECT`, schema `USAGE`, table
  `SELECT/INSERT/UPDATE/DELETE`, and sequence `USAGE/SELECT`. Default privileges
  belonging to the migration role cover future tables and sequences. It has no
  schema creation, table alteration/drop/truncate, temporary-table, role/database
  creation or migration-role membership privileges. ORM history, row locking,
  database sessions, schedules and report metadata are exercised.
- ClickHouse runtime identity: `SELECT`, `INSERT`, `CREATE TABLE`, `DROP TABLE`
  and `ALTER ADD COLUMN` on the isolated `default` database, plus `SELECT` on
  `system.tables`, `system.columns` and `system.databases`. Tests exercise
  `MergeTree`, CDC `ReplacingMergeTree` evolution and report reads, staging
  through the application workflow, copy/exchange/rename/drop, and cancellation
  of its own queries without a global `KILL QUERY` grant. Another identity's
  query must finish normally despite an attempted cancellation. Access to an
  unrelated database and user/role/database administration are denied.

Run from `ReportHub/`:

```bash
REPORTHUB_IDENTITY_TEST=1 python3 -m unittest discover \
  -s tests/infrastructure -t . -p test_service_identities.py -v
```

Docker must already have the `postgres:15-alpine`,
`clickhouse/clickhouse-server:latest` and `reporthub-app` images. Build the
application image separately if absent, or select an existing compatible image
with `REPORTHUB_IDENTITY_APP_IMAGE`. The runner never pulls images, publishes
ports, joins the shared network, mounts existing database data, or modifies
existing services. It generates throwaway credentials, uses a unique internal
Compose network and disposable tmpfs database storage, mounts project code
read-only, and masks the project `.env` with `/dev/null`. Its own project is
removed after success or failure; do not run the probe script directly against
an existing stack.

These are candidate grants verified on PostgreSQL 15.19 and ClickHouse
26.10.3.7, not a production provisioning or upgrade procedure. The isolated
existing-object probe assumes one legacy owner and a disposable database;
production needs an inventory and review of all owners, schemas, extensions,
grants, default privileges, connections and shared objects before any transfer.
`REASSIGN OWNED` affects all objects owned by the role in the current database
and shared objects; it is not a source-scoped operation. Default privileges
are not retroactive and only apply to objects created by the specified role.
The runtime can change all application metadata and
create/drop/rename/exchange every table in its ClickHouse database. It cannot
fence different sources or old workers sharing that identity. System-table
reads may also reveal server metadata; this is not tenant isolation.

Live Kafka/Debezium, all supported source databases, production role rollout
and interrupted-operation recovery are not covered by the identity probe.
Report and bounded ETL requests assign their IDs through driver
`settings={"query_id": ...}`; `transport_settings` carries HTTP headers and does
not assign the query ID. Isolated tests verify exact IDs for queries, commands
and inserts, active-query cancellation through the driver, and actual report
execution and batch refresh/publication. Cancellation binds the ID as a query
parameter; ETL cancellation failures are logged and the client is closed.
Timeouts and simulated interruptions retain uncertain external-operation records,
even after a cancellation command finds no active query.

UI cancellation remains cooperative, not immediate. Report cancellation flags
are checked after blocking queries return. Ingestion rechecks whether its source
is active after source reads and ClickHouse writes and before publishing a
staging table; deactivation during a read or a request delayed before a staging
insert reaches ClickHouse cancels the task after the request returns, removes
staging and preserves the existing ClickHouse table. The source sync status is recorded as
`CANCELLED`. These checks do not preempt a database or ClickHouse request already
in progress.

Eight opt-in, disposable real-worker scenarios cover report and ingestion
control-plane flags, worker termination, delayed ClickHouse requests and dropped
cancellation responses. They also demonstrate that a delayed request can still
execute after worker termination and after a cancellation found no active
query. This behavior is tested, but uncertain-request recovery and fencing are
not implemented. Keep CDC-to-batch switching disabled.

The same real-worker scenarios can additionally run with candidate restricted
PostgreSQL and ClickHouse runtime identities. The worker's actual identities
and denied administration are checked, along with report and batch-ingestion
cancellation. Another scenario uses a separate PostgreSQL source database and
a table-scoped read-only account, verifies denied writes/DDL and ungranted
table reads, then runs initial and replacement full refreshes with two-row
keyset pagination through real worker continuations. It verifies exact rows,
physical-table replacement and staging cleanup, and downloads a completed
CSV export from MinIO to check its contents. The cancellation scenarios still
use a privileged source test account. CDC worker operations, other source engines and production role
rollout are not covered. Enable both flags from `ReportHub/`:

```bash
REPORTHUB_CELERY_CANCELLATION_TEST=1 REPORTHUB_CELERY_RESTRICTED_TEST=1 \
  python3 -m unittest discover -s tests/infrastructure -t . \
  -p test_celery_cancellation.py -v
```

## Dashboard navigation

Global administrators now have separate **Data & Pipelines**, **Operations**,
**History**, **Governance**, and **Integrations** menus. Breadcrumbs return to
the relevant list or section, with the current page shown as the final item.

The Recent Email Deliveries widget retains a direct link to the full audit
page and offers an optional **Filter email deliveries** disclosure for sent,
delivered, failed and bounced records. Open it with Enter or Space, tab to a
filter, or press Escape to close it and return focus to the disclosure.
Clicking outside closes it too. Filters remain available when the widget is
empty. Widgets with a single destination retain their direct links.

## Automated SFTP report delivery

Staff users can manage destinations under **Integrations > Transfer
Destinations**. Apply the current control-plane and reports migrations
using the deployment's migration identity before deploying updated workers.
Only SFTP is currently supported.
FTP and FTPS are outside the product scope. Cloud object-storage destinations
are planned for enterprise evaluation, not available protocol choices.

The destination pages follow the Data Sources layout, with standard action
buttons and breadcrumbs that return to the destination list. Search by name or
host and filter by Active/Inactive status; pagination preserves both filters.
Use **Test** to queue a Celery connection check, including the pinned host key,
authentication and remote directory. **Last Tested** updates automatically.
The create/edit form also offers **Save & Test Connection**, using the same
button styling as Data Sources. It saves valid settings before queuing the
test; if queuing fails, the saved settings are retained and an error is shown.
The separate **Save Destination** button does not queue a connection test.
Tests also work on inactive destinations and do not write files or verify write
permission. Editing connection settings clears an earlier test result.
Connection tests have a 60-second soft and 90-second hard task limit; queued
tests expire after 120 seconds. If the worker never reports back, polling or
another Test request marks the old attempt failed after five minutes, allowing
a new attempt.
**Activate/Deactivate** changes report delivery availability immediately; deactivating
does not cancel transfers already in progress. Both actions require a staff
account and a CSRF-protected POST.

Configure the hostname or IP (without a scheme or port), port (default 22),
username, absolute remote directory and server's `SHA256:` host-key fingerprint.
Obtain the fingerprint from the server administrator through a trusted channel;
do not treat an unverified first connection as proof of the server's identity.
Choose password authentication or an OpenSSH/PEM RSA, Ed25519 or NIST ECDSA
private key, supplying a passphrase when the key is encrypted.
The form only shows fields for the selected authentication method; hidden
credential fields are excluded from submission when switching methods.

Credentials use the existing `FIELD_ENCRYPTION_KEY` encryption mechanism. Keep
that key available and outside version control; losing it makes saved
credentials unreadable. Passwords, private keys and passphrases are never
rendered back to the browser and are excluded from configuration history.
Leave credential fields blank when editing to preserve the saved values for the
same authentication method. Changing methods removes obsolete credentials.
When replacing an encrypted key with an unencrypted one, explicitly select
**Clear the saved key passphrase**. Deleting a destination removes its saved
credentials, retaining only non-secret configuration history.

Destinations default to inactive. Saving or activating one does **not** itself
connect to the server. In report create/edit, enable **Automated File Delivery**
and use its searchable multi-select to choose one or more active destinations.
Email distribution is independent and can be enabled alongside SFTP.

After generation stores the artifact, one Celery task per destination reads
that same artifact. The dedicated `celery_transfer_worker` consumes
`queue_transfers`; report generation continues on the existing report queue.
Upgrade workers that execute reports as well as starting the transfer worker.
Rebuild application/worker images with `docker compose build` after dependency
changes; stopping and starting an old image does not install new requirements.
No transfer runs in the web request. SSH host keys must match the configured
SHA256 fingerprint; SSH agents, automatically discovered client keys and
trust-on-first-use are disabled. Connection/authentication timeouts are 10
seconds, channel I/O is limited to 30 seconds, and tasks have a 300-second soft
and 330-second hard limit.

Remote filenames include the execution and delivery UUIDs. Files are uploaded
to a `.part` path and published with a non-overwriting SFTP rename. A retry
recognizes an already-published file only if its size and SHA256 match the
stored artifact; different existing content is never overwritten. Partial
uploads can remain after a failure and be replaced by a later retry.
Delivery is **at-least-once**, not exactly-once: if a recipient consumes or
moves a file before acknowledgement is recorded, a resend can produce a
downstream duplicate. Recipients should deduplicate using the filename IDs.

The optional **Recent File Deliveries** dashboard widget follows the existing
widget customization controls. Its full audit page shows each destination's
status, errors, attempts and retry count, with a confirmed **Resend** action
for failed deliveries only. Retries are manual, independently queued and
counted; they do not regenerate the report or resend successful destinations.
Inactive/removed destinations, changed connection targets and unavailable
artifacts block resend. Credential rotation for an otherwise unchanged target
is allowed. An abruptly terminated task can remain `running`; it is not
automatically resent and requires operator reconciliation after the old
worker/request is known to have ended. Connection tests are separate from
report deliveries and do not create delivery audit records.

Run focused configuration, delivery and monitoring tests with a disposable
test database:

```bash
python manage.py test reports.test_file_delivery control_plane.test_file_deliveries \
  control_plane.tests.TransferDestinationTests
```

Verification on 2026-11-12 included 36 focused destination/delivery tests after
the final form changes, real Celery/SFTP report fanout and retry integration,
and live **Test** and **Save & Test Connection** checks against the local SFTP
fixture. Browser checks verified authentication-specific field visibility,
exclusion of hidden credentials from submission, matching Data Sources form
styles, and layouts at 375, 768 and 1280 pixels without page overflow.
The agreed SFTP scope is complete; cloud destinations and automatic recovery
of interrupted running deliveries remain outside that completed scope.

The enterprise cloud direction is S3-compatible delivery first, with MinIO for
local testing; Azure Blob Storage and Google Cloud Storage can follow customer
requirements. These would be customer-selected export targets, separate from
ReportHub's internal report storage, using the same report destination selector
and delivery history as SFTP. Destination authorization, tenant isolation,
least-privilege access, workload identities or short-lived credentials, verified
TLS, encryption and safe retry behavior must be designed and tested before
cloud delivery is exposed.

## Starting and stopping ReportHub with CDC

From the ReportHub directory, start the main stack and CDC overlay with:

```bash
./reporthub-stack.sh start
```

Stop them without removing containers or data:

```bash
./reporthub-stack.sh stop
```

The stop command stops the scheduler and web app first, then gives workers up to
10 minutes to finish before stopping the remaining services. A task still
running after the timeout may be forcibly stopped and require recovery. Queued
tasks can remain for the next start. Stopping CDC also pauses change capture:
source logs must remain available until the connector catches up after restart.
This script does not start or stop the separate test database stack; start it
with `./tests/start_test_dbs.sh` first if your sources depend on it.

---

## 🧪 Polyglot Test Database & Test Email Server Environment

ReportHub includes an isolated multi-database test harness in `tests/docker-compose.test.yml` and a one-command setup script at `tests/start_test_dbs.sh`. The script starts the test databases, waits for them to accept connections, enables PostgreSQL logical replication, grants MySQL's Debezium snapshot and replication privileges, imports Northwind into MSSQL, enables SQL Server Agent and CDC on Northwind's primary-key tables, and reapplies the MongoDB and Oracle seeds safely on every run. MongoDB runs as a standalone server. PostgreSQL and MySQL run their initialization scripts when their volumes are first created. Test volumes persist between runs.

SQL Server can accept connections to its system database before Northwind has
finished recovery. The setup script waits for Northwind itself to accept a
connection before enabling CDC; if it times out, inspect the SQL Server
container logs rather than resetting its persistent volume.

### Test Database Connection Matrix

| Database Engine | Host on this machine | Port | Credentials (User / Password) | Target Database / Service Name |
| :--- | :--- | :--- | :--- | :--- |
| **PostgreSQL** | `localhost` | `5433` | `test_user` / `test_password` | `chinook` |
| **MySQL** | `localhost` | `3307` | `test_user` / `test_password` | `northwind` |
| **Microsoft SQL Server** | `localhost` | `1434` | `sa` / `YourStrongPassword123!` | `northwind` |
| **MongoDB** | `localhost` | `27018` | `test_user` / `test_password` | `app_analytics` (auth DB: `admin`) |
| **Oracle** | `localhost` | `1522` | `app_user` / `YourStrongPassword123!` | `FREEPDB1` |
| **Test Email Server** | `localhost` | `1025` / `8025` | *None (Development SMTP/Web UI)* | `mailpit` / local SMTP sink |
| **Test SFTP Server** | `127.0.0.1` | `2222` | `reporthub` / generated SSH private key | `/upload` |

When ReportHub itself runs in Docker, use the Compose service hostnames and internal ports instead: `postgres-test:5432`, `mysql-test:3306`, `mssql-test:1433`, `mongo-test:27017`, and `oracle-test:1521`.

If the test MongoDB container previously ran with replica-set options, recreate it with `docker compose -f tests/docker-compose.test.yml up -d mongo-test` to apply the standalone configuration; this preserves the data volume. For existing MongoDB Debezium connectors, stop or remove those connectors in Kafka Connect to avoid leaving them running. Restart ingestion workers after updating the application, and allow the next MongoDB refresh to replace existing CDC tables.

### Starting and stopping test databases
```bash
# From the ReportHub project directory: start and populate the test databases
./tests/start_test_dbs.sh
# Or use the explicit start option
./tests/start_test_dbs.sh --start

# Stop containers but preserve their database volumes
./tests/start_test_dbs.sh --stop

# Optional: remove test containers and erase all their data, then start fresh
./tests/start_test_dbs.sh --reset
```

The reset option also deletes test SFTP uploads and host keys. Previously saved
SFTP destination fingerprints will need to be updated from the new trusted
server fingerprint after a reset.

### Persistent test SFTP server

The `sftp-test` service lives beside Mailpit in
`tests/docker-compose.test.yml`. Its uploaded files are stored in the
`sftp_test_data` named volume, not the container's writable layer or the Git
checkout. A second named volume, `sftp_test_host_keys`, persists `/etc/ssh`
(including server configuration and host keys), keeping the server fingerprint
stable when its container is recreated. Do not remove these volumes to restart
the server.

Start **only SFTP**, without restarting the databases, email or ReportHub:

```bash
./tests/start_test_sftp.sh
```

The helper requires Docker Compose, Python 3 and `ssh-keygen`. It creates an
unencrypted development-only Ed25519 client key in
`~/.local/share/reporthub/sftp-test/client_ed25519`, outside the repository,
with restricted directory/file permissions. Only its public key is mounted into
the server. Existing keys are reused. The main test-stack startup script
prepares this key automatically as well.

The helper prints the server's SHA256 fingerprint obtained directly from its
container. Configure a destination in **Transfer Destinations** with:

- Host: `sftp-test` and port: `22` when ReportHub runs in Docker.
- Username: `reporthub`; authentication: **SSH private key**.
- Private key: the generated client key's contents; leave the passphrase blank.
  Do not commit the key or paste it into chat, issues or logs.
- Server host-key fingerprint: the helper's printed `SHA256:` value.
- Remote directory: `/upload` (the account is chrooted; its root is not writable).

Saving the destination does not send anything by itself. Enable it, select it
under **Automated File Delivery** on a report, save the report, then run it.
Monitor the dashboard widget or its full file-delivery audit page. Use a clear
name such as **Local Test SFTP** to distinguish this fixture from production
destinations.

For manual SFTP from the Ubuntu host, use:

```bash
sftp -i "$HOME/.local/share/reporthub/sftp-test/client_ed25519" \
  -P 2222 reporthub@127.0.0.1
```

Verify the fingerprint against the helper's output before accepting the host
key. Host port `2222` is bound to loopback only; it is not exposed to the LAN.
ReportHub containers reach it over the existing shared Docker network.
Optionally set `SFTP_TEST_PORT` or an absolute `SFTP_TEST_KEY_DIR` in the
environment or local `.env`; the key directory must remain outside Git.

Inspect or copy uploaded files without changing their volume:

```bash
docker compose -f tests/docker-compose.test.yml exec sftp-test ls -l /home/reporthub/upload
mkdir -p "$HOME/ReportHub-SFTP-downloads"
docker cp reporthub-sftp-test:/home/reporthub/upload/. "$HOME/ReportHub-SFTP-downloads/"

# Stop just SFTP; preserve files and host keys
./tests/start_test_sftp.sh --stop
```

Run the disposable integration probe without touching this persistent service
or its volumes:

```bash
REPORTHUB_SFTP_TEST=1 python3 -m unittest discover \
  -s tests/infrastructure -t . -p test_sftp_server.py -v
```

For complete report generation and delivery through a real prefork Celery
worker, isolated PostgreSQL, Redis, ClickHouse, MinIO and SFTP:

```bash
docker compose build celery_transfer_worker
REPORTHUB_REPORT_SFTP_TEST=1 python3 -m unittest discover \
  -s tests/infrastructure -t . -p test_report_sftp.py -v
```

This probe checks multi-destination CSV delivery, authentication-failure
isolation, host-key rejection and an idempotent retry after a simulated lost
acknowledgement. It uses synthetic data and removes its own containers,
networks, volumes and client keys afterward.

## ClickHouse ingestion and backfills

Each new data source has an **Ingestion mode**: **Batch refresh** (the default, no Kafka required) or **CDC** (requires Kafka, Debezium, and supported source configuration). Existing relational sources retain their previous CDC behavior when migrating; existing MongoDB sources retain batch refresh. The mode cannot yet be changed after a source is created: safe transitions between existing batch and CDC tables require a coordinated snapshot and cutover. MongoDB CDC is not currently supported.

Scheduled ingestion consumes at most `DEFAULT_ETL_BATCH_SIZE` CDC events per Celery task (default `10000`) and then returns so other queued work can run. CDC batches are serialized per source with a Redis lock; concurrent requests are requeued. In CDC mode relational tables with discovered primary keys use Debezium change events; tables without a primary key use bounded full-refresh batches. Batch-mode sources use bounded full refreshes for all tables. Backfills use the same row limit and the dedicated `queue_backfill` queue, and are skipped for CDC sources with primary-key tables because a safe rebuild requires a coordinated CDC snapshot. Celery prefetch is set to 1.

Scheduled CDC ingestion, its continuations, and CDC rebuilds use `queue_cdc` on the dedicated `celery_cdc_worker`. Scheduled batch refreshes, keyless CDC refreshes, backfills, and the CDC-to-batch staging task use `queue_backfill` on `celery_batch_worker`. Interactive and legacy `queue_hot` tasks remain on `celery_ux_worker`; `celery_warm_worker` consumes `queue_warm`. Set `CDC_WORKER_CONCURRENCY` and `BATCH_WORKER_CONCURRENCY` in the Compose environment to tune worker slots (default 2 each). Per-source CDC locking still applies; extra slots primarily help different sources.

Administrators can prepare CDC-to-batch staging from the source edit page. This does not switch modes or start cutover, and CDC continues while staging runs. It stages or restages one whole table per task, then revalidates the table list and staged UUIDs and schemas without restaging in the verification task. Checkpoint statuses record staging progress; they are not proof that verification has finished or that a future cutover is safe. A failed or interrupted task needs an explicit retry; there is no automatic crash recovery yet. If dispatch could not be confirmed, check whether a worker is still running before retrying. Administrators can cancel preparation; cancellation is rejected while a staging checkpoint is locked. Cancelling leaves staged tables for manual cleanup and keeps the source in CDC mode. Pending cutover has a separate, restricted abandonment action described below. Active preparation also prevents starting CDC table rebuilds until preparation is cancelled or resolved. Interrupted attempts can leave separate ClickHouse tables for manual cleanup. Long tables hold a checkpoint transaction while staging, and connector streaming and large-table performance still need validation.

Staging actions use a separate confirmation page. The source page renders status without JavaScript; if automatic polling is unavailable, reload the page to see current progress.

The transition status shows the last completed whole-transition verification separately from the table count. This timestamp is cleared on staging, cancellation or a preparation error. It records a past check, not current cutover readiness: source data and configuration can change afterward, and cutover must revalidate under a complete fence. Checkpoints created before timestamp tracking will show verification as incomplete until the staging task is retried.

Transition preparation and staging reject source tables that map to the same ClickHouse name, including collisions caused by case folding or underscores. The existing naming scheme is unchanged; resolve the underlying mapping conflict before retrying. This check is not a cutover fence and does not prevent later metadata changes.

Staging validation compares the actual ClickHouse engine, sorting, partition, primary and sampling keys, column names, order, types and default expressions with the expected table after staging, during retries and when recording the active table UUID. Staging explicitly disables ClickHouse asynchronous inserts and checks the physical row count against the number of rows read and inserted, including empty tables. The staging signature includes the source username because different database users can see different rows. A mismatch fails validation and requires investigation; it is not automatically repaired. Staged checkpoints created before row-count, username or synchronous-insert tracking are restaged into a fresh table on retry, retaining the old table for manual cleanup; they cannot be used for active-table observation until restaged. Counting adds an extra ClickHouse query for each validation. These checks cannot detect changed values or equal-count substitutions, changes in permissions or row-level policies for the same username, prove that the source scan captured every intended row, or protect against changes after validation, so the future cutover executor must revalidate under a complete fence.

CDC ingestion remains enabled during staging. Each ingestion task now holds the control-plane source row lock while it acquires the Redis lock, checks source mode and cutover status, and processes its batch. This serializes healthy database sessions for the same source even if the Redis lease expires; a future cutover executor must acquire the same source row lock before marking cutover pending. Queued CDC batches skip while cutover is pending. Holding the row lock across connector and ClickHouse work can delay other source-row updates. A database connection loss, external side effects, and Redis lease ownership still require a complete fencing and recovery design. No user-facing operation performs that switch.

CDC-mode batch refresh tasks, including keyless-table fallback, hold the same control-plane source row lock for each bounded task and recheck source mode and cutover status under that lock. A cutover executor must acquire that lock before marking cutover pending so it waits for an in-flight refresh. Queued refreshes then skip while cutover is pending; a stale keyless continuation is rejected after the source switches to batch. Each refresh holds a database transaction while accessing the source and ClickHouse, so long-running tasks can delay other source-row updates. Database connection loss still requires a complete fencing design; these guards do not implement cutover.

Private source-scoped external-operation records and a row-lock barrier are available as groundwork for a future fence. An experimental, disabled-by-default `CDC_EXTERNAL_OPERATION_TRACKING` setting registers CDC ingestion, CDC-mode refresh, rebuild and report-query tasks before their work and keeps records on failure; tracked ClickHouse inserts disable asynchronous insertion. Report queries are registered for batch and inactive sources too, so tracking also affects those sources when enabled. Unresolved records block the barrier and protect their source from deletion; only a caller that has proved all external work ended may remove one. **Do not enable this setting in production yet:** recovery is not implemented, errors can leave permanent records, and coverage and external request completion still need validation. The barrier is not wired to a cutover executor, and neither the setting nor the records account for old workers or already-running work. A future executor must commit cutover-pending while holding the source lock, coordinate report reads, and address worker upgrades and uncertain requests before switching modes.

An unexposed pending-cutover primitive now coordinates the report lock, checkpoint locks and source lock, rejects unresolved operation records, and rechecks staged tables before committing active UUID observations and pending status together. It requires the experimental tracking setting, but that setting does **not** prove that every writer is upgraded or that an earlier request has terminated. The primitive must not be used until an executor establishes those guarantees; report connection loss and direct ClickHouse readers are still outside the report guard.

Scheduled ingestion, backfill dispatch, retries and continuations carry their originating source mode; a task from another mode is rejected. Legacy unmarked tasks still run for sources without a completed transition, but are rejected for batch sources with a successful CDC-to-batch transition because their origin is ambiguous. A future transition executor must commit the mode change and successful transition status together under the source lock, retain the successful transition record, and ensure all producers and workers use this version before switching. Any external callers submitting ingestion or backfill tasks for a transitioned source must supply the current `origin_mode`. These checks do not protect against lost database connections, external side effects or future mode-switch cycles.

The internal cutover checkpoint distinguishes an unchecked active table from an observed missing one. An internal preparation operation can record the active table UUID after checking the staged UUID and configuration; it rejects a missing CDC-managed table. Re-observation rejects changed identities rather than replacing the original checkpoint. UUID decision logic can identify a pending exchange or rename, recognize one completed before a crash, and reject unexpected table identities. These preparation observations may become stale while CDC or keyless refreshes continue: a future executor must revalidate them under a complete fence.

Private primitives can exchange or rename one table and finalize metadata after all checkpoints are complete. Before issuing a ClickHouse command, the exchange primitive commits a durable in-flight marker; it clears the marker only after a successful response and verified checkpoint update. After the command, it rechecks cutover eligibility, checkpoint data, table membership, configuration, physical schema and row count before completing the checkpoint. A changed state or failed validation leaves the marker in place. If a response is lost, the worker exits, or checkpointing fails, retries and finalization are blocked because the command may still be running. Recovery for this uncertain state is not implemented; checking UUIDs alone is insufficient to prove that a delayed command cannot still execute. The exchange primitive cannot run inside an enclosing database transaction. Finalization checks physical table identities and configurations, then commits table metadata, source mode and successful transition status in one control-plane transaction. Connector references and old tables are retained. **These primitives and the experimental executor do not establish a complete external-write fence and must not be invoked until that fence exists.** Report reads also require coordination: between table exchange and metadata finalization, the existing CDC query logic would expect CDC columns on the new batch table. No switch is available yet.

A separate experimental generation-publication path copies each staged table to a fresh, uniquely named ClickHouse table and atomically publishes all physical names with the source mode. A retry reserves a different name, so a delayed copy from an earlier attempt cannot become the published generation; old CDC and staging tables are retained. Reports, batch ingestion and CDC paths resolve the stored physical name, falling back to the existing naming convention for unchanged sources. The private executor can select this path with `CDC_TO_BATCH_GENERATION_EXECUTOR_ENABLED`, which defaults to false. It is **not exposed in the UI and must not be enabled for real sources**: it still needs deployment and worker-version coordination, protection against untracked metadata writers and external clients, recovery and end-to-end CDC validation. Do not manually set physical names or call these private functions to switch a real source.

The source edit, toggle and delete routes, source and table Django admin mutations, schema introspection, warehouse field selection, and report configuration writes coordinate with the pending and generation-publication operations using a source-scoped PostgreSQL advisory transaction lock. They reject changes while cutover is pending. Existing source ingestion mode is read-only in Django admin; bulk source and table admin actions require explicit selection rather than "select all" across pages. Introspection holds the advisory lock and source row lock throughout the source scan, so CDC processing for that source can wait for a long scan; source database writes remain online. Unupgraded workers and direct database or ClickHouse clients do not participate and can still cause unsafe effects. These gaps must be resolved before the generation path is safe to expose.

An experimental queued executor contains staging, pending cutover, per-table cutover and finalization orchestration. It is not exposed in the UI and is disabled by default. Even if its flags are enabled, a hard fail-closed deployment-fence check now blocks the executor because worker/client fencing and external-request recovery are not implemented. This check is a safety gate, **not an implementation of fencing**; private primitives must not be invoked for real sources. Tests replace the gate only for isolated prototype validation. Worker rollout, safe recovery and live interruption tests remain required before the source-page switch can be enabled.

Administrators can view the latest transition on the source edit page, including per-table progress, errors and uncertain command markers. Active transitions refresh automatically. The switch button is deliberately disabled even if experimental executor settings are enabled. The page can start, retry or cancel staging. It can also abandon a pending cutover after confirming that no exchange is recorded or uncertain and the active table UUIDs still match their checkpoints; it retains all staged and copied tables and leaves the source in CDC mode. Uncertain or completed exchanges cannot be abandoned this way. These checks do not fence untracked external writers. Retry and cancel confirmations identify a specific transition, so an old confirmation cannot act on a newer one.

The same control-plane panel shows generation-copy progress and any recorded external operations. An operation record can represent running work or an uncertain outcome; neither its age nor its presence alone proves whether the external request has stopped. No control-plane action currently clears these records or resolves uncertain exchange commands safely.

Finalization locks checkpoints in primary-key order before the source row, matching the checkpoint-first staging and exchange operations. A caller must not already hold the source row lock when invoking it. It rejects checkpoint membership changes detected while acquiring locks. This avoids one lock-order deadlock but does not fence external writes.

Report tasks use a shared, source-scoped PostgreSQL advisory transaction lock while building and executing their ClickHouse query; concurrent reports can still run together. Reports fail with a visible error if a CDC-to-batch cutover is pending. A future executor must acquire the corresponding exclusive lock **before the source row lock and before** marking cutover pending, then commit that status while holding it; it must also prevent new reads until cutover finishes. A report reloads table metadata after obtaining the lock. The lock holds a PostgreSQL transaction over the ClickHouse query, and loss of that connection can release the lock while the external query continues. With the experimental tracking setting enabled on the report worker, the durable report record remains after such a failure and blocks the pending-cutover primitive. This still does not prove that the external query has stopped, provide recovery, cover older workers, or fence direct ClickHouse clients.

Local staging tests compared source rows with staging for PostgreSQL, MySQL, SQL Server, and Oracle. A PostgreSQL two-table run was staged through the batch worker, and an isolated worker exit after a PostgreSQL staging insert was recovered by manually retrying into a fresh table. In a separate PostgreSQL test, updates, deletes, and inserts committed during a table read were absent from that staged snapshot and appeared in a fresh staging run; the existing live CDC table UUID and source mode remained unchanged. This is a result for that driver and configuration, not a cross-database or source-wide snapshot guarantee. Other concurrent-write cases, crash boundaries, production-scale tables, and the unfinished cutover remain to be validated.

An automated concurrency regression pauses a staging insert while a CDC ingestion task processes simulated insert, update, and delete events. It checks that CDC completes against a separate live table and staging remains independent. This uses a real control-plane database transaction but mocked Kafka and ClickHouse clients and a SQLite source; it does not replace end-to-end testing with live CDC infrastructure.

An opt-in live test exercises the private exchange, generation copy and publication, lost responses, late writes to retained tables, and a delayed superseded copy against ClickHouse. It uses a Django test database and uniquely named synthetic ClickHouse tables, which it removes afterward; it does not establish a fence or test a live CDC stream. With the Compose stack running, execute `docker compose -f docker-compose.yml -f docker-compose.cdc.yml exec -T -e REPORTHUB_LIVE_CUTOVER_TEST=1 app python manage.py test tests.integration.test_cdc_to_batch_live --noinput`. Do not run this against a ClickHouse instance where creating and dropping test tables is inappropriate.

An additional opt-in test exercises a PostgreSQL Debezium snapshot and insert, update and delete events while staging is paused, private generation publication, report queries, a late write to the retained CDC table and a subsequent batch refresh. The tasks run in the test process, not on workers or through the control-plane switch; this is not a deployment fence or validation of other databases. It creates a uniquely named table, connector, replication slot, publication, Kafka topic and ClickHouse tables, and attempts to remove those exact test resources afterward. Use it only with disposable test infrastructure; an interrupted run or unconfirmed external request may leave resources requiring inspection. With the CDC stack and `tests/docker-compose.test.yml` running, execute `docker compose -f docker-compose.yml -f docker-compose.cdc.yml exec -T -e REPORTHUB_LIVE_CDC_STREAM_TEST=1 app python manage.py test tests.integration.test_cdc_to_batch_stream_live --noinput`. The test defaults to the PostgreSQL test Compose service and accepts `REPORTHUB_TEST_PG_HOST`, `REPORTHUB_TEST_PG_PORT`, `REPORTHUB_TEST_PG_DATABASE`, `REPORTHUB_TEST_PG_USER` and `REPORTHUB_TEST_PG_PASSWORD` overrides.

In a local PostgreSQL test, staging was paused after an actual ClickHouse insert while test-row updates, deletes, and inserts flowed through Debezium, Kafka, and the CDC worker to the live table. The live table reflected the changes before staging resumed; the staged table retained the earlier snapshot, and the live table UUID and source mode were unchanged. Test rows and temporary transition records were removed; the staging table was retained for manual cleanup. Staging ran in a separate application process, not through the batch worker. This validates one local overlap scenario, not cutover safety or other drivers.

Deploy queue changes to all producers and workers together. Let active tasks finish before replacing workers, start the new CDC and batch workers, and keep `queue_hot` and `queue_backfill` consumed while older queued tasks drain. Older CDC jobs on `queue_hot` can run once on the UX worker; their CDC continuations move to `queue_cdc`. Keep Kafka configuration on that worker during migration. Monitor queue depth, CDC failures and Kafka lag; a stopped worker leaves its queue unprocessed. The base Compose stack includes both new workers even without the CDC overlay, so batch-only deployments still process refreshes. CDC sources also require the overlay or equivalent Kafka/Connect configuration.

### Rebuilding one CDC-managed table

For a CDC-managed relational table with a discovered primary key, global administrators can start a rebuild from the table's **Warehouse Field Selection** panel; the **CDC Table Rebuilds** page also lists eligible tables for source-scoped administrators. A confirmation page explains the operation, and the control plane rechecks source access, CDC eligibility, active rebuilds, and active mode transitions before reserving a generation and queueing work on `queue_cdc`. The Django admin no longer has a rebuild action. The task creates a dedicated Debezium connector and Kafka topic generation, loads a staging ClickHouse table, waits for Debezium's successful snapshot notification and drains the snapshot offsets, then cuts over under the normal source-ingestion lock. The **CDC Table Rebuilds** page monitors status and allows authorized users to resume cutover-pending rebuilds. Do not drop or recreate the active ClickHouse table manually.

If a rebuild remains **Cutover pending**, inspect its `last_error`, resolve the underlying problem, and use **Resume cutover** on the **CDC Table Rebuilds** page. Starting a new rebuild is blocked while an active generation exists. If ClickHouse already exchanged the table, normal CDC ingestion for the source fails safely until the rebuild is resumed; it does not automatically retry a failed rebuild. A source lock left by an interrupted worker may take up to 15 minutes to expire. After a worker crash, an immediate retry may fail to acquire Kafka partitions until the previous consumer's group session expires; retry once the group has rebalanced.

After a successful exchange, the prior ClickHouse table remains under the rebuild record's `staging_table` name; it is not automatically removed. Superseded per-table connectors and connectors created by failed rebuilds are also retained, potentially still running and producing Kafka events. This can consume connector capacity and Kafka storage; PostgreSQL replication slots may retain WAL even if a connector stops. Monitor connector health, Kafka retention and disk usage.

The control plane's **CDC Resource Cleanup** page provides a staff-only, two-stage workflow for resources belonging to recorded rebuild generations: it first performs a read-only inventory, then requires an administrator to review and explicitly confirm cleanup. It runs on the dedicated `queue_maintenance` worker, rechecks generation ownership and eligibility before each deletion, records per-resource outcomes, and stops on errors. Interrupted attempts require a fresh inventory; the workflow never retries or deletes resources automatically. Legacy resources without a durable rebuild-generation record are not inferred as candidates. Cleanup uses the configured runtime identities and will fail visibly if they lack the required privileges; it does not elevate permissions.

Migration `0048` reconciles historical cleanup-field defaults. Migration consistency, the 12 focused cleanup view and service tests, and an upgrade preserving existing audit records pass with isolated SQLite. The workflow has not yet been validated against disposable external services. Do not use it against production or shared resources until isolated connector/PostgreSQL/Kafka/ClickHouse validation is complete. Never remove resources belonging to an active connector or an unfinished rebuild.

The ClickHouse database must use the `Atomic` engine. Apply Django migrations and restart Celery workers when deploying this feature so workers load the rebuild model and task. Local single-table rebuilds and post-cutover insert/update/delete flows have been verified with SQL Server `northwind.dbo.Categories`, PostgreSQL `chinook.public.artist`, MySQL `northwind.customers`, and Oracle `APP_USER.invoices`. MySQL tests also paused a rebuild with a snapshot backlog and source writes, and terminated isolated test workers after ClickHouse exchange and after saving the snapshot checkpoint but before committing Kafka offsets; both rebuilds were manually resumed. The post-exchange crash test shortened the source lock to 45 seconds; production uses a 15-minute lock. Other crash boundaries, broader concurrency cases, and deployment-specific source permissions still require further validation. Source-wide batch/CDC mode transitions are not implemented.

Start the CDC services alongside ReportHub:

```bash
docker compose -f docker-compose.yml -f docker-compose.cdc.yml up -d
```

The Compose overlay gives the app and its Celery ingestion worker these defaults:

```dotenv
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_CONNECT_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_CONNECT_URL=http://debezium-connect:8083
```

Compose reads those values from the project `.env` file and passes them into the app and ingestion worker. `KAFKA_BOOTSTRAP_SERVERS` is where ReportHub's Kafka consumer connects. `KAFKA_CONNECT_BOOTSTRAP_SERVERS` is the broker address Debezium Connect uses, including for relational connector schema history; it must be reachable from the Connect container. `KAFKA_CONNECT_URL` is the Connect REST API address ReportHub uses to create and check connectors. The defaults above work when using the bundled services on the Compose network.

The CDC overlay uses Debezium Connect 3.5.2.Final. Before upgrading an existing deployment, review Debezium's migration notes and validate each connector and its offsets; a running connector alone does not establish that warehouse ingestion is working. Oracle CDC additionally requires compatible CDB/PDB connection settings, a suitably privileged LogMiner account, and supplemental logging.

Oracle source profiles keep **Database / service name**, **Username**, and **Password** for ReportHub's direct source connection and schema sync. For Oracle CDC, optionally configure both **Oracle CDB name** and **Oracle PDB name**; these map to Debezium's `database.dbname` and `database.pdb.name`. Leaving both blank preserves the existing service-name connector configuration. Optionally configure both **LogMiner username** and **LogMiner password** to give Debezium a separate account; these map to `database.user` and `database.password`. Leaving both blank makes Debezium continue using the source username/password. The LogMiner password is encrypted in ReportHub and is kept when its edit field is left blank. The CDB/PDB values and LogMiner credentials must each be configured as a pair. SQLAlchemy-normalized lowercase Oracle schema/table identifiers are converted to uppercase for Debezium topic/filter matching; mixed-case quoted identifiers are preserved. Oracle event fields and keys match metadata casing exactly first, then use an uppercase fallback only for lowercase metadata names. If that fallback would match identifiers with ambiguous casing, CDC fails explicitly rather than selecting one.

To deploy this configuration, deploy the application code and apply the new control-plane migration in the application's environment (this does not modify the Oracle database):

```bash
python manage.py migrate control_plane 0025_oracle_cdb_pdb_logminer
```

Then edit the Oracle source profile and save the CDB/PDB values and, if needed, the separate LogMiner account. Restart the Django application and Celery ingestion worker so they load the new code, then run the normal ingestion schedule/task to update the existing Debezium connector. Confirm the connector and its task are `RUNNING` in Kafka Connect and verify that changes arrive in the warehouse. Configure Oracle archive logging, supplemental logging, and required user privileges separately with the Oracle administrator; the application does not grant those privileges or perform an initial snapshot/rebuild.

If running Django or Celery directly on the host while Kafka and Connect remain in Compose, use `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` and `KAFKA_CONNECT_URL=http://localhost:8083` for that host process; keep `KAFKA_CONNECT_BOOTSTRAP_SERVERS=kafka:9092` because Debezium Connect itself still uses the Docker-network broker address. Do not put the host-only `localhost` values in a shared `.env` file used to configure Compose containers: inside a container, `localhost` points back to that container. The bundled single-node Kafka configuration is for local development; production deployments should use a secured, replicated Kafka cluster and Connect workers, then set the broker and REST addresses to the endpoints reachable from each service.

After source schema sync discovers a relational primary key, scheduled ingestion registers or updates one Debezium connector per source. Debezium performs an initial snapshot, then tails native database change streams/logs. Events are inserted into ClickHouse `ReplacingMergeTree` tables; each event's Kafka offset is its replacement version, and delete events create tombstones that ReportHub filters from report queries. A key remains on one Kafka partition, so its offsets preserve row-change order without relying on source clock precision. Kafka offsets are committed only after the ClickHouse batch succeeds, so a retry may replay records; ClickHouse replacement semantics make replays idempotent. Debezium and the source database must retain enough log data to cover connector outages.

If a connector or task has failed, the next scheduled ingestion attempts one restart without resetting its offsets. If it does not recover within 30 seconds, ingestion fails and the Kafka Connect and source database logs should be checked. Connector and task states alone do not prove that source changes have reached ClickHouse.

The local test environment has verified insert, update, and delete propagation to ClickHouse for PostgreSQL, MySQL, Oracle, and SQL Server. A temporary Celery Beat schedule also exercised the same dispatcher and ingestion task used by the hourly schedule; the test did not wait for the next regular hourly tick. These checks cover the local test databases, not production deployments.

An existing non-CDC warehouse table is not automatically converted to CDC: ingestion fails instead of dropping the table. If a CDC-managed warehouse table is missing, ingestion also fails rather than recreating an empty table and resuming from old offsets. Both situations require a coordinated initial snapshot and cutover, which is not yet implemented.

The PostgreSQL and Oracle warehouse tables were previously rebuilt from retained snapshot events as a one-off test-data procedure. This is not a supported automatic migration or production recovery procedure. Historical CDC failure entries can remain visible after recovery.

MongoDB ingestion does not use change streams or require replication. Each scheduled run reads every collection in bounded pages into a staging table, then swaps it into place. Inserts, updates, and deletes become visible after a successful refresh. This is not a point-in-time snapshot: concurrent source changes can affect a multi-page scan, and repeated scans increase source load.

CDC is not zero-overhead: it uses source log/stream capacity, Kafka storage, connector CPU, and source-side privileges/configuration. It avoids repeated whole-table scans and does not require a modified-at column, but tables need a primary key. The initial snapshot reads source rows once. Required source configuration includes:

| Source | CDC prerequisites |
|---|---|
| MySQL | Row-based binary logging with full row images, a unique server ID, and a CDC account with replication and snapshot-read privileges. |
| PostgreSQL | `wal_level=logical`, an available logical replication slot, and a CDC account with replication and table-read privileges. |
| SQL Server | SQL Server CDC enabled on the database and captured tables, plus a CDC-capable account. |
| Oracle | Archive logging, supplemental logging, LogMiner access, and the Debezium-required catalog/LogMiner privileges. |

Administrators can view recent CDC execution failures on the dashboard and open **CDC Execution Failures** for a searchable, paginated history. Search covers source name, database type, task ID, and recorded error. The dashboard widget can be hidden under **Customize Widgets**. Entries record the source, time, task ID, and exception type; use the task ID to locate details in the ETL worker logs. Entries represent failures recorded after this feature is deployed, not the current health of a connector or older executions.

The **CDC Table Rebuilds** page in the Control Plane shows each authorized source's table, rebuild status, last update, and required attention. Staff and superusers can monitor all sources; other users see only sources assigned to them, and only source-scoped **System Admin** users or global administrators can resume a rebuild in **Cutover pending**. Other statuses cannot be resumed from this page. Rebuild checkpoints remain read-only in Django admin.

The warehouse field selections saved in **Warehouse Mapping** control the ClickHouse columns. Batch-mode sources and CDC-mode relational tables without a discovered primary key use scheduled bounded full refreshes. Full-refresh backfills are rejected for CDC sources containing a primary-key table: replacing a CDC table while changes continue would require a coordinated Debezium snapshot and offset barrier to avoid losing updates. New columns are added to CDC-managed warehouse tables, but existing rows are not repopulated; use a coordinated Debezium snapshot before relying on newly selected fields for historical data.

Potential future alternatives to full refresh include explicitly configured append-only ID watermarks and database-specific row-version tracking. These need safeguards for transaction ordering, retries, updates, and deletes; choosing a numeric column alone is not sufficient. See [ROADMAP.md](./ROADMAP.md) for the planned strategy and cross-source reporting work.

## 🤖 Local AI Report Design

The report form can use Ollama to draft a report configuration from a natural-language request and the selected source's introspected table, field, and direct foreign-key metadata. A prompt can request Tabular Detail or Grouped Summary, choose fields and labels, assign supported aggregates, and configure summary totals. The model returns a constrained JSON design; ReportHub validates it and its existing query builder generates ClickHouse SQL. Source row data and arbitrary SQL are not sent to or accepted from the model.

The AI draft currently does not configure charts, export format, sorting, email distribution, or AI-summary inclusion; configure those separately in the form. A requested sort order in the AI prompt is not applied.

PDF, Excel, and JSON reports can optionally include an AI-generated summary based on aggregate statistics calculated from the report result rows; individual source row values are not sent to Ollama. For Grouped Summary reports, those statistics describe the grouped output, not the underlying source records. Excel places the summary near the top of the Report sheet with a blank row above and below it, and JSON includes it as a top-level `ai_summary` property. If a PDF is truncated, the summary is limited to the rows included in that PDF. Enable **Include AI summary** while creating or editing a report configured for PDF, Excel, or JSON export.

Grouped Summary output labels identify the aggregate function as well as the configured display label (for example, `COUNT(Track Count)`).

Ollama is included in the main Compose stack. It is not published to the host, and model files persist in the `ollama_data` Docker volume.

```bash
# Start the full ReportHub and CDC stack, including Ollama
docker compose -f docker-compose.yml -f docker-compose.cdc.yml up -d

# Download the default model into the persistent Ollama volume (initial setup only)
docker exec reporthub-ollama ollama pull qwen2.5:3b
```

Set these variables in the project-root `.env` file to override the defaults:

```dotenv
OLLAMA_BASE_URL=http://ollama:11434
OLLAMA_MODEL=qwen2.5:3b
OLLAMA_TIMEOUT=120
```

`OLLAMA_BASE_URL` can point to an existing Ollama server reachable from the app container; the bundled Ollama service remains part of the default stack. If using a remote endpoint, secure its network access and use TLS where available. Prompts and schema metadata are sent to the configured endpoint, along with report-result aggregate statistics when AI summaries are enabled for PDF, Excel, or JSON; individual source row values are not. Model downloads require network access during setup; avoid `docker compose down -v` if you want to retain the bundled model files.

---

## 🛠️ Tech Stack

- **Backend:** Python, Django (Synchronous Control Plane), Hypercorn / Gunicorn ASGI/WSGI
- **Async & ETL:** Celery, Redis (Configured with AOF persistence enabled), Kafka, and Debezium Connect
- **Analytical & Object Storage:** ClickHouse (Columnar Data Warehouse), MinIO (S3-Compatible Object Storage via `django-storages` and `boto3`), PostgreSQL 15 (Django control-plane database)
- **Source Integration & Drivers:** PostgreSQL (`psycopg2`), MySQL (`pymysql`), Microsoft SQL Server (`pyodbc` with Microsoft ODBC Driver 18 for Debian/Linux), MongoDB (`pymongo`), and Oracle (`python-oracledb`)
- **Testing & Quality:** Pytest, Playwright (for automated web application testing & debugging)
- **Frontend:** HTMX, Tailwind CSS, Alpine.js, Custom Components, Modern Vanilla JavaScript (Event-Driven DOM handlers)
- **Infrastructure:** Docker, Docker Compose, RHEL 9 compatible deployment targets

---

## 📂 Project Structure

```text
reporthub/
├── app/                  # Core application configuration & database sessions
├── config/               # Django project settings, URLs, WSGI/ASGI, and Celery setup
├── control_plane/        # Django app: Users, RBAC, Source Profiles, Audit Logs, DLQ, Email Audit, Preferences
├── warehouse/            # Dynamic schema reflection & table provisioner utilities
├── etl/                  # Celery tasks (Batch ingestion, roll-ups, backfilling, cleanup)
├── reports/              # Report generation engine, templates, execution logs, and MinIO storage pipelines
├── alembic/              # Standalone Alembic configuration and initial empty revision
├── tests/                # Test database Compose file, setup script, and seed data
├── Dockerfile            # Container build instructions (including MS ODBC drivers)
├── docker-compose.yml    # Service orchestration (Web, Postgres, Redis, ClickHouse, MinIO, Celery)
├── docker-compose.cdc.yml # Kafka and Debezium Connect overlay
├── requirements.txt      # Python dependencies
├── reporthub-stack.sh    # Start/stop the main stack with the CDC overlay
├── entrypoint.sh         # Container startup & auto-migration script
├── ROADMAP.md            # Project implementation tracker & audit cleanup checklist
└── README.md             # Project documentation
```