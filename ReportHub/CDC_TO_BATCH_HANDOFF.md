# CDC and CDC-to-batch continuation handoff

Last updated: 2026-11-14

## Goal and current status

This is the working checklist and cross-session source of truth for CDC
progress, CDC-to-batch work and the next task. MongoDB CDC parity with the
relational sources is the current priority. Update its checkboxes, evidence,
and next-task section as work is completed; reflect overall milestones in
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
2026-11-14; migration and model consistency checks pass there. Read-only
inventory, selected deletion and permission-failure scenarios, and one
real-worker interruption boundary now pass against isolated external services.
Migration `0049` and the current worktree add a durable uncertainty marker:
failed or interrupted deletion attempts block subsequent cleanup of that source.
An administrator may explicitly accept the risk for each failed uncertain
attempt; the action and reason are audited, old inventories are invalidated,
and a new inventory and confirmation are required. The uncertainty remains
recorded: an override is not proof that the old request stopped and may still
result in deletion or data loss. There is no automatic termination proof yet.
Existing failed and running attempts are conservatively marked uncertain during
migration. Migration `0049` was applied to the running database on 2026-11-14
after draining and stopping the maintenance worker; the worker was restarted
and responded to a Celery ping. Future deployments must also stop and drain old
cleanup workers before applying it, since older code ignores the marker.
Control-plane source deletion and Django admin source/table deletion
are blocked while cleanup is queued, running, or unresolved without a risk
override. The override also permits deletion through those paths. Direct record
deletion, source reconfiguration, and duplicate or recreated source profiles
are not fenced; cascading deletion can still remove the marker. Do not bypass
it. The marker does not fence other clients. Recovery from uncertain deletion requests remains
unverified. Do not use cleanup against production or shared resources.

## Next task

**Prioritize MongoDB CDC parity.** Keep MongoDB CDC disabled until the
end-to-end tests pass; preserve existing batch sources and the mode-change gate.
Check the worktree and service state before starting.

Agreed replica-set approach: the customer enters the full MongoDB connection
string they already use, including `replicaSet` for replica sets. ReportHub must
not hardcode `rs0` and must not silently rewrite the URI. Steps, in order:

1. Done: optional encrypted `mongodb_connection_string` on the MongoDB source
   profile, migration `0050_mongodb_connection_string` (untracked). It was applied
   to the live database after stopping the maintenance worker container
   (`reporthub_celery_maintenance_worker`); the worker was restarted and
   responded to a Celery ping. Its last task had finished before the stop.
2. Done: `mongodb_connection_string()` in [cdc.py](./etl/cdc.py) builds the
   Debezium URI from the stored string, or from the old host/port with
   `authSource=admin` when none is stored. Credential encoding is preserved and
   `replicaSet` is no longer dropped.
3. Done: read-only `check_mongodb_replica_set()` in [cdc.py](./etl/cdc.py) calls
   `hello` with `replicaSet` overridden to `None`, so a wrong name is reported
   as `replica_set_name_mismatch` rather than as unreachable. Outcomes are
   `ok`, `sharded`, `not_replica_set`, `missing_replica_set_name`,
   `replica_set_name_mismatch`, `unreachable` and `invalid_connection_string`.
   Live check against the fixture: `rs0` gives `ok`, a missing name gives
   `missing_replica_set_name`, and `rsX` gives `replica_set_name_mismatch`.
   It is not yet wired into the control plane UI.
4. Done (unit level): tests in `DebeziumConnectorConfigTests`
   ([tests.py](./etl/tests.py)) cover the stored-string path and each readiness
   outcome. The form test in `SourceProfileFormTests` checks that `mongodb://`
   and `mongodb+srv://` are accepted and other schemes are rejected. Still to do:
   wire the decoder into ingestion, following the MongoDB checklist below.

Verification: in isolated SQLite, `etl.tests.DebeziumConnectorConfigTests`,
`SourceProfileFormTests` and `MongoIngestionTests` pass (39 tests), and
`control_plane.test_cdc_cleanup` and `etl.test_cdc_cleanup` pass (39 tests),
so the cleanup rerun listed below is done.

Open decisions: whether the customer string should also drive batch ingestion,
whether readiness should check change-stream permissions, and whether
`mongodb+srv` needs live validation.

Status of the cleanup UI: the source page and attempt page split is implemented
([cdc_cleanup_attempt.html](./control_plane/templates/control_plane/cdc_cleanup_attempt.html)).
The focused cleanup tests pass in isolated SQLite (39 tests). The attempt page
has not been checked in a browser.

Shared-task status: no database type has reached the shared readiness and
switching stage. PostgreSQL, MySQL, SQL Server and Oracle have basic CDC and
single-table rebuilds tested locally, but readiness checks and safe
source-wide switching are incomplete for all four.

CDC-to-batch switching and cleanup recovery remain parked, not completed.
Outstanding delayed requests still need service-specific termination proof or
an explicit audited risk override; a fresh inventory is not proof. Do not
inventory or delete shared resources.

## Database CDC baseline

- PostgreSQL, MySQL, SQL Server and Oracle support relational CDC. Local tests
  have verified insert, update and delete propagation to ClickHouse and
  single-table rebuilds followed by further changes for all four.
- They are not equally validated in every failure scenario: the detailed
  rebuild crash/cutover exercises are MySQL-specific, while the live
  staging-overlap and worker-retry tests are PostgreSQL-specific. Further
  source-specific snapshot, permission, recovery and operational validation
  remains. Shared readiness checks and safe source-wide switching are not
  complete for any of them.
- MongoDB currently uses batch refresh only. A MongoDB connector configuration
  exists, but the control plane and ingestion worker explicitly reject MongoDB
  CDC, and the relational decoder expects row-shaped events. MongoDB rebuilds
  are also blocked. Do not treat the connector configuration as working CDC.
- A separate persistent single-node MongoDB replica-set fixture now starts and
  stops with the test database script. Its change stream has been tested for
  inserts, updates and deletes; restart preserved data and authentication.
  ReportHub verified authenticated replica-set discovery, and Kafka Connect
  verified TCP reachability. This fixture test does not test Debezium, Kafka,
  ClickHouse, replication or failover.

## MongoDB CDC parity checklist

- [x] Add an authenticated, persistent single-node replica-set fixture without
  changing the existing standalone MongoDB data. Its normal start and stop are
  included in [start_test_dbs.sh](./tests/start_test_dbs.sh); the
  `--mongo-cdc-only` option starts only the fixture.
- [x] Run a disposable MongoDB/Kafka/Connect probe with the installed Debezium
  3.5.2 connector. After supplying `replicaSet=rs0` in the probe, it captured and
  checked snapshot, insert, full update and delete events, BSON/nested values,
  and distinct ObjectId and string IDs with the same text. It uses no shared
  services and removes its uniquely named containers.
- [ ] Complete connector configuration and snapshot validation: the connector
  URI now comes from the customer connection string (steps 1–3 in "Next task"),
  and the read-only `setName` check is implemented and tested. Still to
  validate: the Debezium run with the stored string end to end, collection
  selection, snapshot completion, `mongodb+srv`, and supported deployment types.
- [ ] Decode MongoDB's observed Debezium format: snapshot/insert/update `after`
  values and keys are Extended JSON strings, while deletes have no before/after
  image and must use the key. Preserve BSON identity across types; an ObjectId
  and a string can contain the same text but identify different documents. The
  user chose type-aware IDs in reports. A decoder now produces canonical Extended
  JSON IDs and checks the key against the document. Unit tests cover malformed
  and mismatched keys; the isolated Debezium probe exercises the decoder on real
  snapshot, insert, update and delete events with distinct ObjectId and string
  IDs sharing the same text. It is not yet wired into ingestion or rebuilds;
  MongoDB CDC remains disabled.
- [ ] Implement and test snapshot and streaming ingestion for MongoDB
  collections. Define stable `_id` identity and BSON/nested-value mapping,
  including non-ObjectId IDs, schema changes, missing/null fields, updates and
  deletes. Preserve ordering, replay/idempotence and commit offsets only after
  successful warehouse writes. Fail visibly when a value or event cannot be
  represented safely.
- [ ] Add read-only MongoDB CDC readiness checks to the control plane for
  supported topology/version, change-stream access, permissions and
  connector-network reachability where verifiable. Show failed, unknown and
  stale results and actionable customer guidance; never silently change the
  database or enable CDC. Test standalone, healthy, insufficient-permission and
  unreachable cases.
- [ ] Run isolated end-to-end snapshot and insert/update/delete tests through
  Debezium, Kafka, actual workers and ClickHouse. Include concurrent writes,
  restart/replay, failed warehouse writes, source outages and oplog-retention
  recovery. A one-member fixture does not test replica failover.
- [ ] Design, implement and validate MongoDB single-collection rebuilds,
  monitoring, resume and explicit retained-resource cleanup to match the
  relational workflows. Cover MongoDB-specific snapshot and connector
  semantics, permissions and failure recovery; do not assume relational
  rebuild/cleanup behavior is sufficient.
- [ ] Preserve existing MongoDB batch sources and fail-closed mode-change
  behavior. Design and test MongoDB-specific batch/CDC transitions separately
  before including it in source-wide switching; the shared fencing and
  recovery blockers remain unresolved for all databases.
- [ ] Only expose MongoDB CDC after its implementation and end-to-end checks
  establish the intended behavior. Document supported topologies, prerequisites,
  operational limits and remaining parity gaps.

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
- [x] Run a read-only inventory against disposable PostgreSQL, Kafka,
  Debezium Connect and ClickHouse with unique resources and no published ports.
  The probe checked present and absent resources, preserved a paused connector,
  replication slot, publication, consumer group, topic and two table UUIDs,
  and rejected an active reference, unfinished rebuild and mismatched staging
  UUID. It did not exercise deletion, permission failures or worker recovery.
- [x] Fix Kafka inventory compatibility for the installed kafka-python 3.0.13
  and retain compatibility with the 2.x group-listing API. The isolated live
  probe passed with 3.0.13; mocked tests cover both group-listing shapes and
  15 focused cleanup tests passed with isolated SQLite and local cache.
- [x] Fix Kafka group/topic deletion response handling for kafka-python 2.x
  and 3.x. Mocked tests cover both response formats; a disposable live 3.0.13
  probe confirms deletion of a paused connector, inactive group, slot,
  publication, topic and retained ClickHouse table after control-plane
  confirmation. The active table remains unchanged. Celery dispatch is mocked
  and the cleanup service runs in the probe, not a maintenance worker.
- [x] Exercise a real active Kafka group failure after confirmed cleanup:
  completed earlier operations remain recorded, the group failure is visible,
  and subsequent Kafka and ClickHouse resources remain untouched.
- [x] Exercise a real PostgreSQL replication-slot permission denial after
  inventory and confirmation. Cleanup records the failure, redacts the source
  password and stops before the publication, topic and retained table.
- [x] Exercise a real ClickHouse `DROP TABLE` permission denial with a
  separate restricted runtime identity and distinct source and warehouse
  passwords. Cleanup records failure and uncertainty while retaining the
  active and staging tables.
- [x] Terminate a real solo Celery maintenance worker while a read-only
  connector configuration request is held before forwarding. With deletion
  explicitly blocked by the test proxy and the request allowed to finish, the
  control plane requires interruption acknowledgement, blocks a fresh
  inventory until explicit risk acceptance, and permits a fresh inventory
  afterwards. No deletion request was in flight; this does not establish safe
  recovery from a delayed or uncertain deletion.
- [x] Hold a real connector DELETE at a test proxy, kill the maintenance
  worker, verify source deletion and new inventory are blocked, and accept the
  risk explicitly. A fresh inventory reports the connector present; releasing
  the old DELETE afterwards removes it. This demonstrates that worker
  termination and fresh inventory do not prove safety.
- [x] Block control-plane source deletion and admin source/table deletion while
  a cleanup is queued, running or unresolved without an override. This does
  not protect direct database changes, duplicate sources or all metadata paths.
- [x] Persist uncertainty before potentially mutating cleanup operations, retain
  it on failure or interruption, and block further cleanup for the same source.
  Focused tests cover late success after interruption and read-only inventory
  recovery. An isolated SQLite `0048` to `0049` upgrade marks existing failed
  and running attempts uncertain; migration consistency and the updated
  real-worker and successful-deletion probes pass. An audited administrator
  override preserves the uncertainty and requires a fresh inventory. This does not prove an
  uncertain deletion has terminated or implement recovery.

## Remaining work checklist

### Rebuild-resource cleanup

- [x] Reconcile the historical-field migration drift and pass the migration
  consistency check and focused tests in isolation.
- [x] Validate read-only inventory and selected eligibility checks against
  disposable connector, PostgreSQL, Kafka and ClickHouse services. Repeat all
  inventory and deletion scenarios with
  `REPORTHUB_CDC_CLEANUP_INVENTORY_TEST=1 venv/bin/python -m unittest -v tests.infrastructure.test_cdc_cleanup_inventory`
  from the ReportHub directory, with the local `reporthub-app` image available.
- [x] Correct Kafka group/topic deletion client APIs and validate confirmed
  deletion and active-group failure against disposable services with the
  installed kafka-python 3.0.13. The 2.x API shapes are covered by mocks, not
  a live integration test.
- [x] Validate a PostgreSQL slot permission failure and a real solo-worker
  interruption at a controlled read-only boundary, using disposable services.
- [x] Validate a ClickHouse drop permission failure and delayed connector
  deletion following a real worker interruption, using disposable services.
- [ ] Validate remaining service permission failures and interrupted operations.
  Establish automatic proof where possible; a risk override is not proof.
- [ ] Extend uncertainty protection beyond the guarded deletion UI paths to
  direct record deletion, metadata changes and duplicate or recreated source
  profiles; the current source-scoped guard depends on those records.
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

## Safety and continuation notes

- Do not enable the experimental transition setting, remove the executor gate,
  or expose a switch based only on candidate role or cancellation tests.
- Do not run inventory/deletion against shared resources or use production
  credentials for probes. Cleanup requires a fresh reviewed inventory and
  explicit confirmation.
- The maintenance worker runs as the `reporthub_celery_maintenance_worker`
  container. Its response to a ping does not validate cleanup on shared
  resources. Do not use cleanup on shared resources before the remaining safety
  work and validation.
- Schema migrations: before applying one, confirm the maintenance worker has no
  running task (`docker logs --since <time>` should show no received tasks
  without a matching success or failure), stop the container, run
  `venv/bin/python manage.py migrate control_plane <name>` from the host,
  restart the container, and check it with
  `docker exec reporthub_celery_maintenance_worker celery -A config inspect ping`.
  Migrations `0049` and `0050` followed this procedure. Other workers keep old
  code until restarted; `0050` only adds a nullable column, while `0049` needs
  old cleanup workers drained first.
- Preserve the existing uncommitted work across sessions. Inspect `git status`;
  do not reset, commit, rotate credentials, or remove volumes without approval.

## Useful references

- [CDC transition implementation](./control_plane/views/cdc_transition.py)
- [Staging and cutover primitives](./etl/cdc_to_batch.py)
- [CDC task and writer guards](./etl/tasks.py)
- [CDC connector and event processing](./etl/cdc.py)
- [MongoDB CDC fixture test](./tests/infrastructure/test_mongo_cdc_fixture.py)
- [Isolated MongoDB Debezium probe](./tests/infrastructure/test_mongo_cdc_debezium.py)
  and [probe implementation](./tests/integration/mongo_cdc_probe.py)
- [External-operation tracking](./etl/cdc_external_operations.py)
- [Report/cutover coordination](./control_plane/metadata_guard.py)
- [Cleanup view](./control_plane/views/cdc_cleanup.py),
  [cleanup service](./etl/cdc_cleanup.py), and
  [cleanup migration 0047](./control_plane/migrations/0047_cdc_resource_cleanup.py)
- [Cleanup view tests](./control_plane/test_cdc_cleanup.py) and
  [cleanup service tests](./etl/test_cdc_cleanup.py)
- [Restricted-identity probes](./tests/infrastructure/test_service_identities.py)
  and [real-worker cancellation tests](./tests/infrastructure/test_celery_cancellation.py)
- [Disposable cleanup inventory test](./tests/infrastructure/test_cdc_cleanup_inventory.py),
  [Compose stack](./tests/docker-compose.cdc-cleanup.yml) and
  [inventory probe](./tests/integration/cdc_cleanup_inventory_probe.py)
- [Migration-gate tests](./tests/infrastructure/test_migration_gate.py),
  [ClickHouse cutover tests](./tests/integration/test_cdc_to_batch_live.py), and
  [live PostgreSQL CDC stream test](./tests/integration/test_cdc_to_batch_stream_live.py)
