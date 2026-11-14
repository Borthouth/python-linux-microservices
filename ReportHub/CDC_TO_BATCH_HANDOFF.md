# CDC-to-batch continuation handoff

Last updated: 2026-11-14

## Goal and current status

This is the working checklist and cross-session source of truth for CDC-to-batch
progress and the next task. Update its checkboxes, evidence, and next-task
section as work is completed; reflect overall milestones in
[ROADMAP.md](./ROADMAP.md). Operational details are in [README.md](./README.md).

The goal is a safe, confirmed control-plane operation to transition a
CDC-managed source to batch ingestion. Source writes should remain possible
during staging and cutover; temporary inconsistency until a later batch refresh
is acceptable. Staging and progress reporting exist, but no user-facing
source-wide switch or resume operation is available. The experimental executor
remains fail-closed.

The blocking issue is not staging alone: old workers, direct clients, delayed
external requests, and in-flight writes must be fenced and safely recovered
before publication. Cancellation and an absent ClickHouse query do not prove a
delayed request cannot still execute.

The staff-only CDC resource cleanup workflow is implemented. Migration `0048`
reconciles historical cleanup-field defaults; migration consistency and all 12
focused view/service tests pass with an isolated SQLite test database. An
isolated `0047` to `0048` upgrade preserved existing audit records. The migration
pair `0047` and `0048` was applied to the running ReportHub database on
2026-11-14; migration and model consistency checks pass there. External-service
behavior remains unverified. Do not use cleanup against production or shared
resources.

## Next task

**Begin disposable cleanup validation.** Inspect the existing cleanup probes
and design a bounded isolated test for read-only inventory and resource
eligibility using disposable services and uniquely owned rebuild-generation
resources. Record what is actually verified and the remaining deletion,
permission-failure and interruption cases here and in the roadmap. Do not
inventory or delete shared resources. CDC work is parked for a later session;
check the worktree and service state before starting.

## Completed groundwork

- [x] Add source-page preparation, per-table staging, status, explicit retry
  and cancellation, and restricted pending-cutover abandonment. Staging leaves
  the source in CDC mode; interrupted tables are retained for manual cleanup.
- [x] Add staging schema, engine, row-count, username and physical UUID checks,
  plus whole-transition verification status. These checks do not prove source
  scan completeness, equal-count data changes, or safety after validation.
- [x] Add private checkpointed exchange/finalization and generation-publication
  prototypes, including uncertain-command markers and atomic publication.
  Neither path establishes a fence or safe recovery.
- [x] Add source-row and report locks, task mode provenance, and experimental
  external-operation records. They do not cover old workers, direct clients,
  connection loss, or prove external requests have stopped.
- [x] Add optional application credentials and an opt-in migration service.
  Test candidate PostgreSQL and ClickHouse roles, a single-legacy-owner
  transfer, and migration gating in disposable environments; production
  provisioning and deployment remain unfinished.
- [x] Fix query-ID propagation and exercise cancellation with real workers.
  The tests show a delayed request can execute after worker termination even
  after cancellation finds no active query. UI cancellation remains cooperative.
- [x] Exercise PostgreSQL CDC changes during staging and private generation
  publication in an opt-in live test. Tasks ran in the test process, not through
  a production switch; this is not an end-to-end rollout or fencing proof.
- [x] Move single-table rebuild initiation and monitoring into the control
  plane. This is separate from source-wide switching.
- [x] Implement staff-only rebuild-resource cleanup with read-only inventory,
  explicit confirmation, a dedicated maintenance queue, eligibility rechecks,
  recorded outcomes, and interruption controls. Twelve focused tests passed in
  an isolated Django test database; external services were not exercised.
- [x] Reconcile cleanup history defaults with migration `0048`; migration
  consistency and the focused tests pass, and an isolated SQLite upgrade from
  `0047` preserves existing historical values. The running database now has
  `0047` and `0048` applied.

## Remaining work checklist

### Rebuild-resource cleanup

- [x] Reconcile the historical-field migration drift and pass the migration
  consistency check and focused tests in isolation.
- [ ] Validate inventory, confirmed deletion, permission failures, interrupted
  operations and recovery against disposable connector, PostgreSQL,
  Kafka/Connect and ClickHouse resources. Verify active, unfinished and
  unverified resources remain protected and per-resource outcomes are visible.
- [ ] Document verified operating and recovery procedures before using cleanup
  on shared or production resources; never automatically delete retained
  resources.

### Fencing and recovery

- [ ] Design and validate an automated source-scoped fence covering CDC
  ingestion and rebuilds, keyless refreshes, report reads, queued work, old
  workers, direct clients and delayed external requests. A database lock or
  separate credentials alone cannot establish this fence.
- [ ] Prove outstanding requests have terminated and implement safe recovery
  for uncertain external-operation records and interrupted work. An expired
  lease, old record, successful cancellation or missing active query is not
  proof of termination.
- [ ] Implement and test recovery for uncertain ClickHouse exchange/rename
  commands, including lost responses and delayed execution, before retry or
  finalization. UUID checks alone are insufficient; the exchange primitive
  cannot run inside an enclosing database transaction.
- [ ] Coordinate report reads and metadata during publication. Sequential
  exchanges must not expose batch tables to reports still using CDC metadata;
  cover lost PostgreSQL connections and direct ClickHouse readers.
- [ ] Revalidate staging, source/table membership and physical identities
  under the complete fence. Account for untracked metadata writers and future
  mode-switch cycles; mode provenance is not a generation fence.

### Identities and deployment

- [ ] Review an existing PostgreSQL ownership/grant transition: inventory
  multiple owners, schemas, extensions, grants, sessions, shared objects,
  tables, sequences and future-object privileges. The tested single-owner
  candidate is not a production procedure; default privileges are not
  retroactive.
- [ ] Validate the migration gate with actual application web/worker commands
  and deployment paths, including failure, existing processes, and a fresh
  migration job on every deployment.
- [ ] Design and validate coordinated rollout that removes privileged
  credentials from runtime environments and mounted configuration, upgrades
  producers/workers, and retires old clients, identities and sessions. Merely
  selecting `APP_*` credentials does not remove access to legacy credentials.
- [ ] Extend restricted-identity real-worker tests to CDC ingestion/rebuilds
  and all supported source databases. Restricted report/batch worker and
  read-only PostgreSQL source probes already pass; cancellation scenarios still
  use privileged source connections.

### Staging, switch and end-to-end validation

- [ ] Validate staging and interruption recovery for PostgreSQL, MySQL,
  SQL Server and Oracle, including concurrent writes/deletes, other crash
  boundaries, batch-worker overlap and source-specific permissions. Local
  staging matched rows for all four; the verified live CDC overlap and worker
  retry scenarios are PostgreSQL-specific and do not prove full cutover safety.
- [ ] Validate source-driver streaming and large-table performance. Staging
  currently holds a checkpoint transaction for a whole table and restarts that
  table after interruption.
- [ ] Once fencing, recovery and rollout guarantees are enforceable, expose
  confirmed switch and resume actions in the control plane, with visible
  progress, errors and safe recovery. Keep the executor fail-closed until then.
- [ ] Validate the complete workflow with real workers and live CDC streams
  across supported relational databases, including concurrent source writes,
  inserts and deletes, worker interruption, retries and report continuity.
- [ ] Provide operational guidance for lag, restart/replay, failed cutovers and
  retained resources based on the verified workflow.

### Separate optional improvement

- [ ] Design immediate control-plane query cancellation and race handling if
  needed. Existing report/source cancellation is cooperative; immediate
  cancellation would not replace the required external-write fence.
- [ ] Plan MongoDB CDC separately as described in [ROADMAP.md](./ROADMAP.md):
  MongoDB currently remains batch-only. A disposable single-node replica set
  enables change-stream testing but does not implement CDC or prove failover.
  Define and validate MongoDB-specific transitions, rebuilds and cleanup before
  including MongoDB in source-wide switching; retain the existing mode gate.

## Safety and continuation notes

- Do not enable the experimental transition setting, remove the executor gate,
  or expose a switch based only on candidate role or cancellation tests.
- Do not run inventory/deletion against shared resources or use production
  credentials for probes. Cleanup requires a fresh reviewed inventory and
  explicit confirmation.
- The running stack had no maintenance worker when migrations were applied;
  no services were restarted. Do not start it to use cleanup on shared resources
  before disposable validation.
- Preserve the existing uncommitted work across sessions. Inspect `git status`;
  do not reset, commit, rotate credentials, or remove volumes without approval.

## Useful references

- [CDC transition implementation](./control_plane/views/cdc_transition.py)
- [Staging and cutover primitives](./etl/cdc_to_batch.py)
- [CDC task and writer guards](./etl/tasks.py)
- [External-operation tracking](./etl/cdc_external_operations.py)
- [Report/cutover coordination](./control_plane/metadata_guard.py)
- [Cleanup view](./control_plane/views/cdc_cleanup.py),
  [cleanup service](./etl/cdc_cleanup.py), and
  [cleanup migration 0047](./control_plane/migrations/0047_cdc_resource_cleanup.py)
- [Cleanup view tests](./control_plane/test_cdc_cleanup.py) and
  [cleanup service tests](./etl/test_cdc_cleanup.py)
- [Restricted-identity probes](./tests/infrastructure/test_service_identities.py)
  and [real-worker cancellation tests](./tests/infrastructure/test_celery_cancellation.py)
- [Migration-gate tests](./tests/infrastructure/test_migration_gate.py),
  [ClickHouse cutover tests](./tests/integration/test_cdc_to_batch_live.py), and
  [live PostgreSQL CDC stream test](./tests/integration/test_cdc_to_batch_stream_live.py)
