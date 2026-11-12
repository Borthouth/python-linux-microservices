# CDC-to-batch continuation handoff

Last updated: 2026-11-10

## Goal and current status

Let an administrator safely switch an existing CDC source to batch from the
control-plane source page. Staging and progress are available there, but the
actual switch is deliberately **not available**. Safety work is incomplete:
the queued experimental executor has an unconditional fail-closed deployment
fence check, even if its feature flags are enabled. That check does not fence
old workers, direct clients or private primitives.

The user wants operational actions in the control plane, not Django admin.
Keep old CDC, staging and generation resources until a separately approved
cleanup workflow exists.

## Work completed

- [x] Added per-table batch staging, validation, retries, status and restricted
  abandonment in the source page. Staging does not switch the source.
- [x] Built internal cutover prototypes, including durable external-operation
  records and generation-based publication. These are experimental and are
  not a complete fence or safe recovery mechanism.
- [x] Added synchronous ClickHouse inserts for staging; old checkpoints without
  the synchronous-insert marker must be restaged.
- [x] Added an opt-in PostgreSQL Debezium stream test and synthetic ClickHouse
  tests. These do not replace end-to-end testing with real workers.
- [x] Added optional `APP_POSTGRES_USER` / `APP_POSTGRES_PASSWORD` and
  `APP_CLICKHOUSE_USER` / `APP_CLICKHOUSE_PASSWORD` pairs. If a pair is
  absent, existing credentials remain in use; incomplete or empty pairs fail
  Django startup. See `config/service_credentials.py`, `config/settings.py`
  and the local configuration section of `README.md`.
- [x] Added `docker-compose.application-credentials.yml` to optionally pass these
  values into all web, worker and beat containers. The existing Compose file
  is root-owned and was not modified. The mounted project `.env` is another
  way for Django to read these values.
- [x] Stop container startup if Django migrations fail, log an explicit error
  and preserve the migration command's exit status. Successful startup and
  main-command arguments remain unchanged.
- [x] Added opt-in `docker-compose.migrations.yml`: a one-shot migration service
  uses explicitly required migration credentials, and all application services
  skip startup migrations and wait for its successful completion. Legacy
  startup remains the default. Migration failures stop startup. See README.md
  for the coordinated two-step deployment procedure, including forcing a fresh
  migration run each deployment. This configuration has not been deployed.
- [x] Verify startup and migration separation with 12 standalone entrypoint
  and Compose configuration tests, shell syntax and whitespace checks. These
  checks do not validate live database permissions or deployment orchestration.
- [x] Define and test candidate PostgreSQL migration/runtime and ClickHouse
  runtime permissions in fresh, isolated databases. All Django migrations run
  as the non-superuser migration role. Nine runtime probes cover ORM/history,
  locks, sessions, schedules, future-object grants, CDC table evolution/report
  reads, application staging, batch table publication primitives, own-query
  cancellation and denied administrative/cross-database operations.
  `tests/infrastructure/test_service_identities.py` creates and removes a unique internal
  Compose project; credentials are generated, data is disposable, and the
  project `.env` is masked. No live identities or services were changed.
- [x] Fix ClickHouse query-ID propagation in report and bounded ETL queries,
  commands and inserts: use `settings={"query_id": ...}`, not
  `transport_settings` (HTTP headers). Use shared parameterized cancellation;
  ETL cancellation failures are logged and its client is closed. Isolated
  driver tests observe exact active IDs, cancel an active runtime request and
  verify another user's request completes unaffected. Actual report execution,
  initial batch publication and refresh/exchange paths pass. Timeout and
  simulated worker-interruption tests retain uncertain external-operation
  records even after a successful cancellation with no matching active query.
  This does not implement immediate UI cancellation or an external-write fence.

Separate credential *configuration* does not create restricted roles or
remove access to privileged credentials: application containers still receive
the legacy PostgreSQL credentials and mount the project `.env`. No credentials
or running services were changed for this step.

## Next logical work

When resuming, inspect the current worktree and service state first, then start
with the first unchecked task below. Use isolated infrastructure and preserve
all existing changes; do not enable cutover or alter the shared stack.

- [ ] Validate cancellation end-to-end with real Celery workers, process
   termination, delayed requests and dropped network responses. Query-ID
   propagation is fixed, but current UI cancellation remains cooperative:
   reports check the flag after the blocking query returns, and pipeline
   deactivation is checked at task boundaries. An immediate cancellation flow
   needs an explicitly designed control-plane action and race handling; do not
   imply that the current UI terminates an active query. A successful KILL
   command or absent query is not proof that a delayed request cannot execute.
   - [x] Add an opt-in, disposable Compose probe with a real prefork Celery
     worker and ClickHouse fault proxy. Terminating a task using
     `CancellableETLTask` stops its active query.
   - [x] Verify a delayed KILL leaves the query active until the command reaches
     ClickHouse; drop the KILL response and verify the query stops while the
     worker logs cancellation uncertainty.
   - [x] Demonstrate that a query request delayed before reaching ClickHouse can
     execute after task termination and a successful KILL found no active
     query. This confirms that cancellation is not an external-write fence.
   - [x] Exercise the production report and ingestion workflows and their
     control-plane cancellation flags; UI cancellation is still not immediate.
     - [x] Run `execute_report_task` in a real worker, invoke the control-plane
       report-cancel view while its ClickHouse query request is delayed, and
       verify that the flag is applied and the execution becomes cancelled
       after the request returns. This confirms report cancellation is
       cooperative, not immediate.
     - [x] Deactivate a source through the control-plane pipeline action before
       enqueueing ingestion; verify the real worker skips the task and retains
       the existing ClickHouse table.
     - [x] Deactivate a source while a real worker is blocked reading its
       PostgreSQL source table; verify cancellation after the read, staging
       cleanup, and preservation of the existing ClickHouse table. Ingestion
       now rechecks source activity after reads and writes and before
       publication. This remains cooperative; it does not fence an external
       write already in progress. Added `CANCELLED` to current and historical
       source status choices in migration `0041`.
- [ ] Design and test restricted PostgreSQL and ClickHouse service identities,
   including all required application operations and ownership/permissions for
   the optional dedicated migration identity. The separate migration process
   is implemented but not deployed; production role provisioning, existing-object
   ownership changes and live migration/rollout validation remain unimplemented.
   Candidate grants and representative isolated workflows are now verified.
   Remaining work includes existing-object ownership/grants and complete
   worker/source-database coverage. Query-ID propagation, worker termination,
   and cooperative report/source cancellation are verified in isolation;
   in-flight source cancellation and immediate UI cancellation are not.
   - [ ] Define a reviewed, repeatable ownership/grant transition for an
     existing PostgreSQL database, including existing tables/sequences and
     future objects created by the migration identity. Test it on disposable
     populated data; default privileges alone do not repair existing ownership.
   - [ ] Expand runtime-role tests to real workers, report execution and
     cancellation, batch refreshes, CDC ingestion/rebuilds and supported source
     databases. Verify required operations succeed and prohibited operations
     remain denied. Candidate database-wide grants are not source-scoped fencing.
- [ ] Plan a coordinated rollout that removes administrator credentials from
   application containers, upgrades and retires old workers and clients, and
   prevents old identities or sessions from writing to newly published tables
   and metadata. Use isolated test infrastructure; do not rotate shared
   credentials or restart the running stack without coordination.
   - [ ] Validate the dedicated migration service against real database roles
     and exercise failed migration startup orchestration in isolation, not
     just Compose configuration. Verify every deployment runs a fresh migration
     job before starting upgraded application processes.
   - [ ] Remove privileged credentials from runtime environment variables and
     mounted configuration through an approved deployment design; merely
     selecting `APP_*` credentials does not remove legacy credential access.
- [ ] Establish how to prove outstanding external requests have terminated and
   implement safe recovery for interrupted operations. An old operation record
   or expired lease alone is not proof that its external request has stopped.
- [ ] Only after fencing and recovery are enforceable, expose a confirmed switch
   and resume flow in the control plane. Validate real worker interruptions
   and concurrent CDC changes across supported source databases.

See the CDC-to-batch section of `ROADMAP.md` for detailed progress. Source
writes should remain possible during staging and cutover; temporary
inconsistency until a later batch refresh is acceptable.

## Important safeguards

- Do not remove the fail-closed executor gate or enable the switch merely
  because credentials can be configured separately.
- Generation publication isolates old physical table names, but does not stop
  an old worker from loading new routing or a privileged client from writing.
- Respect existing lock ordering when working on cutover. Uncertain external
  operations and exchanges must not be cleared just because they are old.
- The repository has substantial **uncommitted** work from this and earlier
  sessions, including unrelated changes. Inspect `git status` and preserve it;
  do not reset, commit, rotate credentials or remove volumes without approval.
- Migrations `0039` and `0040` represent an added then removed email widget
  and have been applied locally; preserve both.

## Most recent verification

Test organization: standalone tests now live in `tests/infrastructure/`;
opt-in live Django tests and the container identity probe live in
`tests/integration/`. Conventional app-level `tests.py` modules remain in
their Django apps. Run host-only discovery with
`python3 -m unittest discover -s tests/infrastructure -t . -v`.
After relocation, all 15 host tests and 14 isolated runtime probes passed;
all 12 relocated Django integration tests imported and were discovered in an
isolated container, without running those live tests. Imports, patch targets,
Compose probe paths and documented commands were updated.

After the identity-configuration changes, 11 credential/executor Django tests
passed, along with two Compose credential-propagation tests. Compose config
validation, migration consistency and `git diff --check` also passed. An
isolated Django settings check confirmed both separate credential pairs load.
No new role provisioning or live-worker fencing was tested.
Subsequent startup/migration separation checks passed 12 standalone entrypoint
and Compose tests, plus shell syntax validation. These verify failure exit
codes, skipped runtime migrations, required dedicated credentials and dependency
gates with and without the CDC overlay. No containers were started or restarted;
live database permissions and Docker failure orchestration remain untested.
The subsequent isolated identity suite passed against PostgreSQL 15.19 and
ClickHouse 26.10.3.7: 9 in-container permission/workflow tests and 13 host
tests (including the integration runner and existing startup/Compose tests).
The migration identity applied every Django migration. Own-query cancellation
was verified against an active request; another user's request was not killed.
All isolated containers and their network were removed. The production
permission/ownership transition and real-worker fencing remain untested.
After the query-ID fix, 14 in-container permission/workflow probes and 15 host
tests passed, including driver queries/commands/inserts, actual report and ETL
refresh execution, parameterized cancellation and retained uncertainty after
timeouts/simulated interruptions. Isolated resources were removed. No shared
services were restarted or credentials changed.

The real-worker cancellation probes run in a separate, uniquely named Compose
project with isolated PostgreSQL, Redis, ClickHouse and MinIO services. Six
scenarios passed: active-query cancellation after worker termination, delayed
KILL, dropped KILL response with explicit uncertainty logging, execution of a
delayed query after termination despite a prior KILL finding no active query,
production report cancellation through the control-plane view, and skipping a
queued ingestion after control-plane source deactivation while retaining the
existing ClickHouse table. The report test confirms that cancellation takes
effect only after the delayed query returns. In-flight ingestion cancellation
and immediate UI cancellation remain unverified. The task-revocation signal
invokes the existing ETL query-cancellation helper for terminated
`CancellableETLTask` jobs. All temporary services were removed; the shared
stack was not changed.

Useful focused commands from `ReportHub/`:

```bash
python3 -m unittest discover -s tests/infrastructure -t . -p test_entrypoint.py -v
python3 -m unittest discover -s tests/infrastructure -t . -p test_compose_credentials.py -v
python3 -m unittest discover -s tests/infrastructure -t . -p test_compose_migrations.py -v
REPORTHUB_CELERY_CANCELLATION_TEST=1 python3 -m unittest discover \
  -s tests/infrastructure -t . -p test_celery_cancellation.py -v
REPORTHUB_IDENTITY_TEST=1 python3 -m unittest discover \
  -s tests/infrastructure -t . -p test_service_identities.py -v
docker compose -f docker-compose.yml -f docker-compose.cdc.yml exec -T app \
  python manage.py test config.tests etl.tests.CdcToBatchExecutorTests --noinput
docker compose --env-file .env.example -f docker-compose.yml \
  -f docker-compose.cdc.yml -f docker-compose.application-credentials.yml \
  config --quiet
```

The Django test command requires the existing app and test database services
to be available. Check current service state before running it; do not assume
the stack is still running.
