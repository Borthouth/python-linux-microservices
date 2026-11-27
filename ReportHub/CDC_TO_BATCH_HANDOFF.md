# CDC and CDC-to-batch continuation handoff

Last updated: 2026-11-27

## Goal and current status

This is the working checklist and cross-session source of truth for CDC progress,
CDC-to-batch work and the next task. MongoDB CDC parity with the relational
sources is the current priority. Update its checkboxes and next-task section as
work is completed; reflect overall milestones in [ROADMAP.md](./ROADMAP.md).
Operational details are in [README.md](./README.md). Logging progress and its
separate next steps are tracked in
[OPERATIONS_LOGGING_HANDOFF.md](./OPERATIONS_LOGGING_HANDOFF.md).

The goal is a safe, confirmed control-plane operation to transition a
CDC-managed source to batch ingestion. The experimental executor remains
fail-closed.

The blocking issue is that old workers, direct clients, delayed external
requests and in-flight writes must be fenced and safely recovered before
publication. Cancellation and an absent ClickHouse query do not prove a delayed
request cannot still execute.

Cleanup safety: failed or interrupted deletion attempts mark the source uncertain
(migration `0049`). An administrator may accept that risk per attempt, which is
audited and requires a fresh inventory. The override is not proof that the old
request stopped. Do not use cleanup against production or shared resources.

## Next task

**Prioritize MongoDB CDC parity.** Keep MongoDB CDC disabled until the
end-to-end tests pass; preserve existing batch sources and the mode-change gate.
Check the worktree and service state before starting.

Agreed replica-set approach: the customer enters the full MongoDB connection
string they already use, including `replicaSet` for replica sets. ReportHub must
not hardcode `rs0` and must not silently rewrite the URI.

Completed so far: the encrypted connection-string field and migration `0050`
(applied), the Debezium URI builder, and MongoDB prerequisite checks on the
source edit page. These check topology, compare server version to the Debezium
3.5 tested versions, open and close a deployment-wide change stream, and check
listing and a one-ID read for up to 10 configured keyed collections. Larger,
missing or incomplete checks report unknown. This is not a full readiness
verdict: full snapshot behavior, connector-network reachability and recovery are
unverified. The disposable probe verified a replica set, standalone, restricted
and unauthorized users, missing collection and unreachable address; it does not
validate connector-network reachability for customer deployments.

In progress: the ingestion consumer routes MongoDB events through the decoder
and maps supported values conservatively. Focused consumer tests cover typed
IDs, unsupported values and write-before-commit behavior. A disposable probe
passed using a URI retrieved from an encrypted database record and real
Debezium, Kafka, the consumer and ClickHouse: it checked snapshot, insert,
update, delete, an injected warehouse-write failure without an offset commit,
and replay. A disposable Celery worker also ran a test-only task calling the
real consumer against those events and confirmed that the normal MongoDB CDC
ingestion task remains blocked. The test-only worker was also terminated after a
warehouse write but before the Kafka commit; after restart, a manually
resubmitted task replayed and committed the event. Kafka group reassignment
temporarily failed during recovery. This does not exercise production ingestion
or prove automatic task recovery.

The expanded probe also validated rebuild-stage mapping, multi-partition topic
rejection, unknown-field rejection followed by metadata review and replay,
and an update during connector pause captured after resume. Pause/resume is not
a source outage or an oplog-retention recovery test. It now verifies that
selecting a new warehouse field is blocked pending a coordinated rebuild and
that previously selected fields continue updating while deselected. The
consumer also rejects missing or mismatched warehouse types rather than silently
adding columns without historical data.

Next step: validate production worker failure/restart and snapshot completion,
then implement and test the coordinated rebuild, historical backfill and oplog
recovery. Preserve the MongoDB worker gate in normal deployments.

November 27 checkpoint: the latest grouped run passed 63 focused tests and
the disposable MongoDB, Debezium, Kafka, Redis and
ClickHouse probe; an earlier grouped run passed 68 focused tests. The probe's
Celery task is test-only and does not prove production ingestion or automatic
retry. Existing services were left running and workers were not restarted for
these incomplete changes. MongoDB CDC and CDC-to-batch switching remain gated.

November 27 update: existing ReportHub, ScanHub and test containers were
started without resetting volumes. A grouped run passed 64 focused tests and
the expanded disposable probe, including the test-only Celery consumer task
and the real ingestion task's MongoDB rejection. Full worker ingestion remains
gated and untested; no production gate or mode-change behavior was changed.
The in-progress rebuild consumer now uses MongoDB's typed mapper, and both
consumers fail closed if topic partition metadata is unavailable or the topic
does not have exactly one partition. New top-level document fields absent from
discovered metadata fail closed; fields not previously selected remain excluded.
These safeguards do not solve historical backfill when selected fields change,
or prove safe MongoDB rebuild cutover.

Open decisions: whether the customer string should also drive batch ingestion
and whether `mongodb+srv` needs live validation.

Shared-task status: no database type has reached the shared readiness and
switching stage. PostgreSQL, MySQL, SQL Server and Oracle have basic CDC and
single-table rebuilds tested locally, but readiness checks and safe
source-wide switching are incomplete for all four.

CDC-to-batch switching and cleanup recovery remain parked, not completed.
Outstanding delayed requests still need service-specific termination proof or
an explicit audited risk override; a fresh inventory is not proof. Do not
inventory or delete shared resources.

## November 17 operations checkpoint

The Operations logging work improves investigation of CDC failures; it does not
advance the CDC ingestion, readiness, fencing or switching checklists:

- Structured Python and Java stdout records, source attribution and normal-size
  exception/redaction probes are implemented. The separate Operations Plane
  has Live logs, explicit paginated log search, saved history and saved-event AI
  analysis. Logs are diagnostic evidence, not authoritative transition state.
- Search results are saved page snapshots in PostgreSQL, normally up to 25
  events per queried page; older snapshots can contain 100. Historical-entry
  text/level/source filtering and pagination use only that saved snapshot.
  New AI runs also use only its matching saved events, not the entire original
  ClickHouse range. Each run retains its own filters and can be reopened.
- Recorded Debezium messages include MongoDB change-stream/standalone failures.
  Treat these as evidence of the recorded operation, not a current readiness
  assessment or permission to reconfigure a shared MongoDB instance.
  A RUNNING connector/task or successful log query does not prove CDC parity.
- Docker transport-fragment reassembly, broad source coverage and operational
  failure recovery remain open logging work. An AI summary cannot establish
  that a complete exception, source event stream or cutover was captured.

November 22 update: the running Collector's supervisor, source policy and
mounted configuration match the current source files. Live delivery and
attribution remain unverified; follow the current
[logging handoff](./OPERATIONS_LOGGING_HANDOFF.md). The November 18 stopping
checkpoint remains there as historical context.
The user requested host shutdown after saving documentation. Keep OTel;
fragment reassembly and replacement-collector work are parked. None of this
changes the MongoDB parity priority or any CDC switching/cleanup gate.

**Resume CDC work here:** extend validation of the in-progress MongoDB ingestion
mapping beyond the passing disposable consumer/ClickHouse probe, preserving
typed BSON identity, offset/write ordering and existing batch behavior. Exercise
actual workers and recovery scenarios. Extend the Control Plane topology check
to full readiness without silently rewriting URIs or enabling CDC. Resolve the open MongoDB
decisions above before expanding supported behavior. Keep the experimental
CDC-to-batch executor and MongoDB CDC gates closed until their separate safety
and end-to-end requirements are met.

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
  CDC. The consumer has experimental MongoDB event mapping, but MongoDB rebuilds
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
  URI comes from the customer connection string, and the read-only `setName`
  check is implemented and tested. The disposable probe verified Debezium using
  the URI from a stored encrypted profile. Still to validate: broader collection
  selection, snapshot completion, `mongodb+srv`, and supported deployment types.
- [ ] Decode MongoDB's observed Debezium format: snapshot/insert/update `after`
  values and keys are Extended JSON strings, while deletes have no before/after
  image and must use the key. Preserve BSON identity across types; an ObjectId
  and a string can contain the same text but identify different documents. The
  user chose type-aware IDs in reports. A decoder now produces canonical Extended
  JSON IDs and checks the key against the document. Unit tests cover malformed
  and mismatched keys; the isolated Debezium probe exercises the decoder on real
  snapshot, insert, update and delete events with distinct ObjectId and string
  IDs sharing the same text. The decoder is wired into the ingestion and
  rebuild consumers; the full MongoDB rebuild workflow and CDC remain disabled.
- [ ] Implement and test snapshot and streaming ingestion for MongoDB
  collections. Define stable `_id` identity and BSON/nested-value mapping,
  including non-ObjectId IDs, schema changes, missing/null fields, updates and
  deletes. Preserve ordering, replay/idempotence and commit offsets only after
  successful warehouse writes. Fail visibly when a value or event cannot be
  represented safely. The in-progress consumer uses canonical Extended JSON for
  `_id` and nested values, handles selected scalar types, and rejects type
  mismatches, non-finite floats and Decimal128 instead of losing precision.
  MongoDB field types are sampled, so mixed types and schema drift still need
  deliberate handling. Unknown top-level fields stop processing rather than
  silently losing schema changes. An existing warehouse keeps consuming
  previously selected fields even if deselected, and adding a field or changing
  its warehouse type fails closed pending a coordinated rebuild and historical
  backfill. Offset-derived versions require exactly one
  Kafka partition per MongoDB collection topic. The disposable probe exercises actual ClickHouse
  ingestion and replay, including a test-only Celery task; it does not exercise
  the gated production ingestion path or all failure scenarios.
- [ ] Add read-only MongoDB CDC readiness checks to the control plane for
  supported topology/version, change-stream access, permissions and
  connector-network reachability where verifiable. Show failed, unknown and
  stale results and actionable customer guidance; never silently change the
  database or enable CDC. Test standalone, healthy, insufficient-permission and
  unreachable cases. The source edit page offers administrator-only topology,
  version, deployment-wide change-stream and bounded collection access checks
  against saved configuration, with ephemeral results and explicit unverified
  prerequisites; it is not a full readiness verdict.
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

## Completed work

Completed staging, cleanup, migration and probe work is summarised in
[ROADMAP.md](./ROADMAP.md) and recorded in git history. Only open items are
tracked here.

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
- [ ] Check the cleanup source and attempt pages in a browser; they have only
  been covered by unit tests.
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

- [Operations logging continuation and validation](./OPERATIONS_LOGGING_HANDOFF.md)
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
