# Operations Logging Handoff

Last updated: 2026-11-22

## Current checkpoint and next steps

This section describes the current behavior. Later sections retain the rollout
history, including older full-range AI behavior that new saved-event runs
supersede.

**November 22 file check:** the running Collector's baked-in `supervise.py` and
`source_policy.py`, and its mounted `collector.yaml`, match the current source
files by SHA-256. This supersedes the November 18 warning that the image had not
been rebuilt, but does not verify the active source revision, delivery,
attribution or recovery. Those checks remain open. The earlier stopping
checkpoint below is retained as historical context.

- Live logs and explicit Log search are separate. Search pages use 25 events;
  saved history entries retain a bounded PostgreSQL snapshot and original
  range/filter metadata. Opening, filtering and paging within an entry do not
  query ClickHouse or create another search. Event-details expansion still
  fetches the original full event from ClickHouse on demand.
- History has owner-only search/status filters, explicit side-by-side View
  results/Delete actions, and confirmed deletion of directly linked AI runs.
  Its AI column shows status counts rather than individual run links; run
  selection remains inside View results.
  Active work blocks deletion. ClickHouse logs and standard audit history are
  not deleted by that action.
- New AI runs use only saved events matching the applied text/level/source
  filters across all saved pages. Each run persists its filters and is
  selectable independently; selecting one restores its inputs and shows one
  result, not a stack of summaries. Legacy full-range results are labelled.
  Queued/Processing/results and the run list update inline. Show/hide preserves
  the collapsed state while the worker runs. Clear filters appears after a
  saved-result search and sits beside Source on desktop.
- The AI panel keeps its scope/limits notice visible when results are collapsed:
  saved snapshot only, up to 300 severity-prioritized events and shortened
  messages/stack traces even at full event-count coverage. Next-run inputs,
  selected results and saved-run entries explicitly distinguish Filtered saved
  events from Unfiltered saved events; unfiltered still retains original search
  constraints. Legacy full-range results keep their separate scope label.
- Operations migrations through `0010_saved_result_analysis_filters` are
  applied. At the earlier UI milestone, the Operations suite passed **52 tests**; migration consistency,
  JavaScript syntax and diff whitespace checks passed. This is not a new
  full-repository test run, nor a repeat of all infrastructure probes below.
- Browser verification covered filtered rows, one-result-at-a-time run
  switching with restored filters, preserved collapse during processing,
  conditional inline Clear filters, side-by-side history actions and deletion
  of a disposable verification search. Real saved-event runs completed on
  100/100 unfiltered rows and 4/4 ERROR rows. The rollout-affected Analysis #8
  was rerun and verified at 4/4 with an ERROR-only breakdown; its old incorrect
  8,213/300 output is not the current result.

Next work, not implemented or validated by the UI milestone:

1. **Parked by user decision (November 18):** retain the current OTel Collector
   and defer Docker fragment reassembly, alternative collectors and transport
   redesign until explicitly resumed. The oversized-message completeness and
   redaction limitations remain unresolved; this is not acceptance of those
   limitations as fixed. Future work needs bounded reassembly before JSON
   parsing/redaction, with gap/timeout/size-limit reporting.
   The November 18 isolated OTel experimental and Fluent Bit partial-message
   trials both failed acceptance; neither is approved for the running
   pipeline. Preserve the evaluations below for future continuation, not as
   instructions to start another trial now.
2. Verify the running logical-source consolidation below, then verify
   representative delivery and attribution for every intended active source.
   Preserve current source choices; configuration
   confirmation and an available store are not proof of actual ingestion.
   The November 18 read-only checkpoint below found arrivals for 10 of 14
   categories; MinIO/ScanHub delivery and active loss/recovery probes remain
   unverified.
3. Exercise Collector outages, invalid-config rollback, persistent queue
   recovery, saturation/full disk and explicit loss reporting using disposable
   infrastructure. Benchmark storage/query impact and varied AI samples;
   bounded prompts do not guarantee a fixed total analysis duration.
4. Agree retention/quotas for raw ClickHouse logs, PostgreSQL result snapshots,
   AI runs and their audit copies. No automatic raw-log TTL is configured;
   per-entry Delete is not an audit-retention policy.
5. Add an automated deployment/readiness guard for AI input-scope changes.
   Current rollout protection is procedural: pause submissions, migrate,
   reload/verify the scope-aware worker, then accept requests. Complete
   redaction/privacy, least-privilege and transport/storage hardening before
   production use.
6. Continue MongoDB CDC parity separately using
   [CDC_TO_BATCH_HANDOFF.md](./CDC_TO_BATCH_HANDOFF.md). Better logs and AI
   diagnostics do not remove its ingestion or cutover safety gates.
7. Evaluate full-original-search CSV export with optional background AI
   analysis, as outlined below. This is future work, not the current inline
   saved-event analysis.

## Logical-source consolidation stopping checkpoint (November 18)

The user approved one generic control per logical source and one canonical
ingestion route, and confirmed that existing test logs need not be retained.
No historical-data migration, backfill, deletion or ClickHouse truncation was
needed or performed. Existing source-selection keys remain unchanged.

Implemented in the worktree:

| Logical control | Canonical input | Explicitly excluded overlap |
| --- | --- | --- |
| ETL | Structured `reporthub_etl` events from container output through Fluent Forward | Local `etl_execution.log` |
| Celery | Worker/Beat container output other than `reporthub_etl` events | ETL events belong to ETL, independently of Celery |
| Django | Web container output other than `reporthub_etl` events | ETL events belong to ETL, independently of Django |
| ClamAV | Host journal output with command `clamd` or `freshclam` | Local ClamAV/freshclam files |
| Host/system | Other host journal events | ClamAV daemon/updater output belongs to ClamAV |

- Removed the Collector's file receiver and its dedicated ETL-file Compose
  mount. Local application/daemon logging is unchanged. Known ETL/ClamAV file
  copies submitted through another receiver are also rejected, even when their
  logical source is enabled.
- Moved source filtering after structured decoding/source normalization so an
  ETL event cannot be accepted or dropped by the Celery/Django checkbox instead.
  ETL requires the Fluent Forward master switch. ClamAV is independent of the
  Host/system checkbox, although both use the same journal receiver.
- Search classification and effective-enabled status follow the same logical
  split. ClamAV service attribution uses `clamav`, not the redacted journal unit.
  Journal command identity is preserved across resource grouping.
- The route decision was based on current configuration and bounded read-only
  evidence, not equal row counts alone. ETL had 40 file and 40 forwarded events
  in the inspected window. ClamAV journal-only messages included an updater
  reload warning and database-test diagnostics, so dropping the journal in
  favour of files would have discarded useful output. Both clamd instances and
  freshclam run in the foreground with stdout/stderr directed to the journal.
- This prevents collection through the configured duplicate file inputs; it is
  **not** content-based deduplication or an exactly-once delivery guarantee.
  Oversized Docker fragments still cannot be reliably decoded/classified as
  ETL until the separately parked reassembly problem is solved.

Validation at this checkpoint:

- **35 tests passed** across `operations_plane.test_sources` and
  `operations_plane.test_log_screens`.
- **Two isolated real-Collector regression tests passed**, including nine source
  selection combinations, independent ETL/Celery/Django and ClamAV/host switches,
  Fluent Forward dependency, both ClamAV instances, always-rejected file mirrors,
  source attribution, severity and redaction. Probe rendering now mounts the
  current supervisor/policy, rather than silently testing the baked-in old code.
- The complete rendered production configuration passed Collector validation
  without connecting to shared services.
- Eight synthetic source classifications passed in the actual ClickHouse
  engine without reading or writing stored rows. Disposable probe containers
  were removed. This is not a new full-project test run or live rollout proof.

**November 18 resume instructions:** the YAML is bind-mounted, but
policy/supervisor code is baked into the image; an old image can mix source
semantics. The running image files matched the worktree on November 22 as noted
above. Rebuild the image when those baked-in files change.

```bash
cd ReportHub
docker compose -f docker-compose.yml -f docker-compose.cdc.yml build otel_collector
./reporthub-stack.sh start
```

After startup, verify health and a fresh matching source revision without
changing the saved choices. Verify ordinary new ETL, ClamAV daemon/updater and
host events in the logical categories, with no new file-input copies. Do not
replay old logs or recreate workers merely to generate verification events.
MinIO/ScanHub representative-delivery verification remains open.
Keep OTel and leave reassembly/alternative-collector work parked.

Shutdown: the user explicitly selected **the entire host**. Documentation and
source changes are saved locally and remain uncommitted. The ReportHub stack
stopped successfully through `reporthub-stack.sh stop`, and the ScanHub API
container was then stopped and verified absent from running containers. Test
database/Mailpit containers were left for normal host shutdown. No containers,
volumes or saved logs were deleted. Host power-off is the final action after
saving this checkpoint; consolidation was not deployed before shutdown.

## Read-only source coverage checkpoint (November 18, 17:28 UTC)

No source selections, logging drivers or running services were changed.
ClickHouse queries returned aggregates only, with ten-second/two-thread and
row/byte limits; no individual log messages were exported.

- All 14 source categories plus the Fluent Forward master switch were enabled.
  The saved revision was confirmed applied, with a fresh supervisor heartbeat
  and no reported configuration error. The Collector health endpoint returned
  `Server available`.
- All 19 intended service containers use the expected Fluentd driver/tag
  configuration. Test databases, Mailpit and SFTP use `json-file` and are not
  onboarded as forwarded Other containers. The Collector and ClickHouse also
  use `json-file`, consistent with avoiding self-ingestion.
- Queried arrivals in the preceding 24 hours, ending
  `2026-11-18 17:28:32.848 UTC`, grouped by category/service/input identity.
  All 68 groups fit the query's 100-group limit. Ten categories had arrivals;
  the counts below show the last 30 minutes of that same window, including
  startup output, not proof of continuous activity.

| Category | Arrivals in last 30 minutes |
| --- | ---: |
| Django | 70 |
| Celery | 588 |
| Redis | 20 |
| PostgreSQL | 4 |
| Ollama | 338 |
| Kafka | 4,263 |
| Debezium | 17,630 |
| Host/system journal | 5,464 |
| ETL file | 13 |
| ClamAV files | 93 |
| MinIO | 0 |
| ScanHub API | 0 |
| Other forwarded containers | 0 |
| Direct OTLP | 0 |

Every one of the ten workers plus Beat had container arrivals. Some streams
were quiet after startup; absence of later activity is not a failure finding.

**MinIO/ScanHub require follow-up, not a declaration of failure.** No rows for
either category appeared in the 24-hour query. Their current-container Docker
caches contained nine and two timestamped startup lines respectively, all at
16:59:15 UTC, and no later output through the audit cutoff. The Collector
container started around 16:59:13, but its process logs first reported ready at
17:00:13, followed by a configuration reload and readiness at 17:00:16.
This identifies a possible startup-delivery window; it does not prove which
records were dropped or why. A fresh ordinary source event or approved
controlled probe is needed to verify each path. None was generated here.
Other/OTLP remain enabled but unobserved, with no identified active producer;
an enabled switch alone does not establish an ingestion fault.

**Overlap candidates:** compared identical message hashes (bodies at least 40
characters) within five-second event-time buckets across distinct input
transports during the preceding hour:

- ETL file versus periodic/batch/CDC worker stdout: 11 matching body/time
  groups containing 26 stored rows in total.
- ClamAV scan/scan2 files versus host journal: two matching groups containing
  eight stored rows in total.

These are candidates for double collection, not a count of proven duplicate
events or rows safe to delete. Repeated messages from independent processes
can match. Journal identity was sometimes redacted as `[EMAIL]`, limiting
source attribution. Input identity was derived from tag/file/journal markers
rather than the optional `source` label, to avoid falsely treating unlabelled
journal rows as another transport.

Next small follow-ups: investigate boot-time Collector readiness versus
producer startup and async-driver buffering; verify MinIO/ScanHub with approved
activity; then review desired ETL/ClamAV overlap and journal-unit redaction.
Do not silently disable sources, replay cached output, delete matching rows or
resume the parked fragment-reassembly work.

## Isolated experimental reassembly evaluation (November 18)

The agreed first step was to keep the current Collector and evaluate the
upstream experimental `logstransform` processor before considering another
service, replacement collector or custom reassembly code.

**Outcome: not safe to deploy.** Built a separate, minimal Collector image
using upstream OCB/component version `0.165.0`; no upstream code was patched
and no custom reassembly processor was written. Production Compose,
Collector configuration, source selections, shared containers and ClickHouse
data were unchanged.

The opt-in probe uses random loopback ports, disposable resource-limited
containers and temporary local file output, never the shared database. It
places upstream stanza `recombine` ahead of the existing parsing/redaction
transforms, with composite tag/container/stdout-or-stderr/partial-ID identity,
a one-second timeout, 256 KiB flush threshold, 32-fragment threshold and eight
concurrent sources. These are evaluation settings, not production defaults.

Findings:

- Normal structured events, ordered 128 KiB synthetic fragments, separate HTTP
  batches, interleaved source identities and a secret split across fragments
  reconstructed with the expected severity, logger, service and redaction.
- Real Docker Fluent Forward reconstructed a 128 KiB message in the initial
  trial and three follow-up trials, but the final full gate also reproduced
  one two-record, nonmatching result. It is not reliably accepted; the exact
  cause of that intermittent failure has not been established.
- Missing-middle and duplicate-middle fragments produced valid-looking but
  altered message bodies without an error log. Out-of-order input also failed
  exact reconstruction. `recombine` concatenates arrival order; it does not
  validate `partial_ordinal` continuity or deduplicate fragments.
- Missing-final timeout and graceful shutdown exported incomplete bodies.
  Generic parsing errors do not establish fragment loss or mark exported
  records as incomplete. The retained partial metadata is not a completeness
  signal: healthy reconstructions also retain the oldest fragment's ordinal
  and `partial_last=false`.
- The 256 KiB threshold flushed an oversized unfinished message as two records;
  it did not identify them as incomplete. Exceeding eight sources logged an
  error and flushed unfinished records. Flush thresholds are not strict memory
  caps; aggregate memory/backpressure and abrupt-restart recovery remain
  unproven.

The final 13-case deployment gate exited **1**, reporting five accepted and
eight rejected cases, including the intermittent real-Docker failure. This is
an intentional rejection of the candidate, not a successful rollout. The
existing isolated mixed-source Collector regression still passed (one test).
No full application suite was rerun for this isolated infrastructure trial.

Reproduce from the ReportHub directory (Docker and Python/PyYAML required):

```bash
docker build -t reporthub-otel-reassembly-probe:0.165.0 \
  -f otel/experimental/Dockerfile otel/experimental
python3 tests/integration/collector_reassembly_probe.py
```

The probe prints individual acceptance results and exits nonzero if any case
is unsafe. Its explicit-incomplete output contract uses
`reporthub.reassembly.error`; the candidate does not currently supply it.
Correct full reconstruction is accepted instead when possible. This image
omits production exporters/receivers and must not replace the live image.
All disposable probe containers and temporary output files are removed.

Fluent Bit was evaluated next, as recorded below. Do not silently relax the
acceptance criteria, add a relay to the shared pipeline, replace the Collector
or start custom processor work.

References: [upstream experimental processor](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.165.0/processor/logstransformprocessor),
[recombine implementation](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.165.0/pkg/stanza/operator/transformer/recombine/transformer.go),
[isolated build manifest](./otel/experimental/builder.yaml) and
[acceptance probe](./tests/integration/collector_reassembly_probe.py).

## Fluent Bit raw reassembly trial and migration impact (November 18)

The user approved trying Fluent Bit and asked about the size and feature
impact of replacing OTel. Trialled upstream **Fluent Bit 5.2.0**, image digest
`sha256:e2718c47aaab7d99587fdffe78818549bdd09dee5e30a9199d2ff3da6e2c7628`,
without modifying shared containers, source choices or production configuration.

The same 13 input cases were reused through synthetic HTTP input plus one
actual Docker Fluent Forward case. This trial checks **raw concatenation
only**, with stdout capture in disposable containers, random loopback ports,
two CPUs and a 256 MiB process limit. It does not claim that Fluent Bit
reproduces OTel parsing, redaction, source controls or ClickHouse delivery.

**Outcome: raw gate rejected (exit 1), five accepted and eight rejected.**

- Ordered 128 KiB fragments, the real Docker case, separate HTTP batches,
  ordinary records and a split secret reconstructed byte-for-byte before
  parsing. This does not demonstrate end-to-end redaction.
- Missing-middle and duplicate-middle inputs produced changed bodies without
  error logs. Out-of-order input was not repaired or identified as incomplete.
- The isolation case interleaves containers/services/stdout/stderr using the
  same partial ID. Service tags distinguish some streams, but two streams
  sharing a tag and partial ID mixed; their different container/stream fields
  did not provide isolation. This is a deliberate collision test, not a claim
  that ordinary Docker-generated partial IDs routinely collide.
- Missing-final timeout and graceful shutdown emitted incomplete content with
  no explicit incomplete marker or error log.
- Input/emitter limits were 8 MiB each, but these are not a per-message
  reassembly cap. A 288 KiB unfinished message was emitted intact after timeout,
  without a truncation marker. Nine unfinished partial IDs also timed out
  unmarked. There is no equivalent configured eight-source/per-message limit
  in this candidate; the process memory cap is not graceful overflow handling.
- The upstream partial-message implementation groups by input, tag and partial
  ID and appends received content, rather than validating ordinals. Simply
  tuning flush timing does not address missing/duplicate fragments.

Reproduce from the ReportHub directory (Docker and Python/PyYAML required):

```bash
docker pull cr.fluentbit.io/fluent/fluent-bit:5.2.0
python3 tests/integration/fluent_bit_reassembly_probe.py
```

The raw gate accepts exact recovery or an explicit incomplete/truncated output
marker. Exit 1 means do not advance to end-to-end migration tests. Probe
containers are removed afterward. No generated output contains real logs;
all input is synthetic. No production service was restarted.

### Replacement impact, if reconsidered later

Replacing the Collector is a **medium-sized infrastructure migration**, not a
container-image swap. No core Operations UI capability inherently requires OTel,
but existing behavior must be deliberately preserved:

| Area | Work or potential loss |
| --- | --- |
| Live/Search/history/AI screens | Should remain unchanged only if `operations.otel_logs` columns, timestamps, IDs, service/severity and attribute semantics stay compatible. Existing snapshots/audits remain in PostgreSQL. |
| Admin source controls | Port desired/applied revisions, per-source filters, confirmation/heartbeat, validation and rollback; the current supervisor renders OTel configuration. |
| Parsing and redaction | Port OTTL JSON decoding, severity normalization, metadata attribution and redaction, including credentials split across fragments. None was implemented by this raw trial. |
| ClickHouse delivery | Replace OTel's native exporter with a verified Fluent Bit output path, such as HTTP insertion, mapping records to the existing schema. Do not assume raw JSON output matches it. |
| File/journal sources | Recreate Tail/Systemd inputs, checkpoints and start-at-end behavior without replay, unintended backfill or new duplicate streams. |
| Durability | Revalidate persistent buffering, retry/backpressure, restart recovery, disk exhaustion and explicit loss reporting; queue implementations are not interchangeable. |
| OpenTelemetry capabilities | Current Fluent Bit documents OTLP HTTP **and gRPC**, plus logs/metrics/traces. It is not limited to HTTP or logs. OTel's broader processor/connector ecosystem and existing processor configurations are not drop-in compatible. Only a logs pipeline is currently configured here. |

**Decision after the trial:** retain the current Collector. Neither candidate
meets the agreed completeness requirements unchanged; do not migrate merely
because the ordered happy path works. Review ingestion approaches that avoid
Docker transport fragmentation (for example complete structured source-file
records with the existing filelog receiver), or agree a separately scoped
completeness-validation design before more implementation. Such a review must
cover file rotation, permissions, source coverage, duplicate streams and
explicit handling of irrecoverable input; it is not an implemented solution.

References: [Fluent Bit multiline filter](https://docs.fluentbit.io/manual/data-pipeline/filters/multiline-stacktrace),
[v5.2.0 partial-message implementation](https://github.com/fluent/fluent-bit/blob/v5.2.0/plugins/filter_multiline/ml_concat.c),
[OTLP input support](https://docs.fluentbit.io/manual/data-pipeline/inputs/opentelemetry),
[trial configuration](./otel/experimental/fluent-bit.conf) and
[raw acceptance probe](./tests/integration/fluent_bit_reassembly_probe.py).

## Planned task: full-search export with optional slow AI analysis

**Requested direction; feasibility/design and implementation remain open.**
Provide a download of the whole original log search, optionally including AI
analysis in the downloadable file. This can take substantially longer than an
interactive request. The AI output belongs in the download, not an on-screen
summary; the UI should expose job progress/status and the eventual download.
Keep the existing fast, inline saved-snapshot analysis separate.

- [ ] Define export scope using the original search's fixed time range and
  original filters, querying all matching ClickHouse events rather than the
  saved page snapshot or its additional local filters. Capture the requested
  inputs and define consistency for late-arriving or expired logs so export
  and AI processing cover the same event set.
- [ ] Offer CSV export with an explicit **Include AI analysis** option.
  Evaluate a dedicated low-priority, low-concurrency "slow AI" Celery queue so
  long exports/analyses do not block interactive search, existing AI jobs or
  report workloads. Queue naming, scheduling and resource limits are design
  decisions, not existing configuration.
- [ ] Stream results in bounded, deterministic chunks; analyze every exported
  event rather than taking the current 300-event sample. Evaluate splitting
  oversized messages/stack traces into tracked segments instead of silently
  truncating them. Summarize chunks and combine their findings into an overall
  summary while recording coverage, chunk failures and any unavoidable limits.
  Full event coverage must not be presented as a verified diagnosis or a
  guarantee that summarization preserves every detail.
- [ ] Design the downloadable format so CSV data remains usable and AI output
  is clearly separated. Evaluate a single download bundle containing
  `logs.csv` plus an AI summary/coverage file, or another explicitly defined
  CSV-compatible layout. Settle this before implementation; do not append
  arbitrary prose to an otherwise rectangular CSV.
- [ ] Store completed export files/bundles in private MinIO object storage,
  retaining job status, ownership, original search inputs, AI inclusion and
  object metadata in PostgreSQL. Do not mark the export ready until its
  upload succeeds; define cleanup for failed uploads and expired objects.
- [ ] Represent each file-producing search as a distinct **File export** entry
  in **Search history**, with queued/processing/completed/failed status and
  whether AI was requested. When ready, show **Download** instead of **View
  results**; regular saved-snapshot entries retain View results. Pending,
  failed, expired or missing files must show their actual state rather than
  an unusable download link. Keep full-search AI output in the file, not an
  inline result page.
- [ ] Persist export/job state and generated files with owner-only downloads,
  redaction, CSV formula-injection protection, audit records, storage quotas
  and explicit expiry/cleanup. Define retries, cancellation and restart
  recovery without duplicate chunks; surface incomplete exports or AI failure
  explicitly rather than offering a misleading completed artifact. Authorize
  each download before issuing a short-lived MinIO link or streaming the
  file. Define how history deletion removes its export objects while retaining
  required audit records.
- [ ] Validate an export larger than both one search page and 300 events,
  including oversized messages, chunk boundaries, late/expired logs, worker
  interruption and unauthorized downloads. Verify exported event IDs/counts
  against AI chunk coverage and ensure optional AI output is in the download,
  not rendered as an inline full-search result. Verify MinIO upload failures,
  expiry/deletion and File export history entries with Download rather than
  View results.

## Goal and decisions

Continue the ReportHub internal logging work with an OpenTelemetry Collector
Contrib container and ClickHouse as the initial log store. The Operations
Plane search UI already exists and uses ClickHouse; it is not a replacement
for PostgreSQL-backed audit, task, DLQ or delivery records.

The user has chosen:

- The new source-choice page belongs under a new **Administration** dropdown.
- Source configuration should be Admin-only. The dropdown may later include
  Operations user management; do not add a dead Users link yet.
- Collector-side source changes should apply immediately by reloading or
  restarting only the Collector, not application workers.
- If an underlying Docker container still uses `json-file`, enabling its
  Fluent Forward output requires a separately scheduled container recreation.
- Start file/journald collection at the current end; do not backfill retained
  history without asking.
- Do not ingest ClickHouse server logs into the same ClickHouse exporter.

## Current implementation

- `otel/Dockerfile` builds Collector Contrib `0.165.0` and includes `journalctl`
  for journald input.
- `otel/collector.yaml` accepts Fluent Forward, OTLP, host journald, the ETL
  execution file and ClamAV files. It redacts selected secrets and emails,
  normalizes service/severity/task metadata, and uses a persistent exporter
  queue.
- ClickHouse table `operations.otel_logs` is provisioned idempotently by
  `python manage.py provision_operations_logs`. It matches the Collector
  exporter schema and adds a generated UUID `event_id` and `ingested_at`.
  Retention is unbounded; there is no TTL.
- `operations_plane.log_store` now searches the OpenTelemetry table while
  preserving the UI's recent time range, filters and UUID keyset pagination.
- Compose files configure Fluent Forward for ReportHub, CDC and ScanHub
  containers. Changed containers were recreated on the November 17 startup.
  An actual Docker Fluent Forward event was verified in ClickHouse.
- Admin-only **Administration > Log sources** is implemented, with audited
  choices and a Collector-local supervisor. Shared JSON control files carry
  only known source identifiers and a revision, not executable configuration.
  Generated filters are validated before restarting the Collector child
  process; health checks and a heartbeat report application status.
- Django, Celery and ETL-file loggers now use `config.logging.JsonLogFormatter`.
  One physical JSON line carries the full message and exception chain. The
  Collector decodes it before redaction and preserves the explicit severity,
  even when the traceback contains words such as WARNING or DEBUG.
- Debezium uses `otel/java/debezium-log4j2.xml` for native JSON stdout output.
  `KAFKA_OPTS` selects it even though the image entrypoint also sets the legacy
  `log4j.configuration` property. Existing local file logging is retained.
- Kafka builds from `apache/kafka:3.8.0` with `otel/java/Dockerfile.kafka`.
  A small reload4j layout uses the image's existing Jackson jars; the JDK is
  build-only. Only the stdout layout changes; local file appenders and levels
  are preserved.
- `groupbyattrs/source` separates Fluent tags, file paths and journal units
  before service-name resource mutations. The transform restores these keys to
  log attributes, preserving search metadata. Source filtering still runs first.

## Verification completed

- ReportHub, combined CDC, and ScanHub Compose configuration validation passed.
- Collector config validation passed.
- Collector image built and its health endpoint returned `Server available`.
- `operations_plane.tests.OperationsLogStoreTests` passed; the full
  `operations_plane` suite now passes 26 tests with `--keepdb`, including
  Admin/Support authorization, CSRF, audit attribution, input validation,
  publication failures and stale confirmation.
- A synthetic OTLP event was redacted (`password=[REDACTED]`), inserted into
  ClickHouse and returned by `search_logs`.
- A first live run exposed a feedback loop when ClickHouse server log files
  were read back into the same ClickHouse exporter. The Collector was stopped,
  ClickHouse-file input was removed, and the user approved clearing only the
  resulting `ServiceName='clickhouse' AND source='file'` rows. The cleanup
  completed; the remaining count for that predicate was zero.
- The Collector was restarted without the ClickHouse file input and remained
  healthy; the feedback-row count stayed zero.
- Initial retained log volume was about 3.9 GiB of journals and approximately
  4.8 GiB across `/var/log` and ClickHouse log files, so the chosen `start_at:
  end` behavior avoids a large first-run replay.

On November 17, after the structured Python exception round-trip passed,
collection was stopped and both `operations.otel_logs` and legacy
`operations.system_logs` were truncated with user approval. Each table was
verified at zero rows before collection restarted. No warehouse tables or
PostgreSQL audit/history records were cleared. New enabled-source events can
appear afterward; this was not a permanent ingestion pause.

The user temporarily disabled most source ingestion to limit VM disk growth.
After the Python rollout, the restored choices were `['redis']` with Fluent
Forward off. During the Java work the user changed the selection: the latest
verified choices include Fluent and every category except PostgreSQL. The Java
probes used that existing selection without changing it. The user has authorized
temporarily enabling whatever is needed for verification, but always read and
preserve the current choices rather than restoring an outdated snapshot.

## Vector retirement

The Vector service, config file, Compose volume declaration, stopped
`reporthub-vector` container and unused `reporthub_vector_data` Docker volume
have been removed. The current OTel host port is Fluent Forward `24224`.
`reporthub-stack.sh start` now starts the Collector first, waits for its
health endpoint, then starts ScanHub and the rest of the Compose stack.
On the next startup, the schema bootstrap needed the same application-source
mount and explicit PostgreSQL host/port as the app containers; these are now
configured. Bootstrap completed successfully, the Operations login returned
HTTP 200, and the Operations workers became ready. Starting the stack applied
the configured logging drivers by recreating changed containers; verify
per-source ingestion next rather than assuming coverage from container status.

## Source switches verified; remaining work

Migration `0005` is applied. Live probes through the Admin form and Collector
verified all 14 source categories enabled, all disabled, Redis alone disabled,
and the Fluent Forward master switch disabled. Synthetic self-log events were
excluded. Original source selections were restored after testing.

Collector confirmation now uses the shared Django success messages, shown once
per confirmed revision in each browser session. Pending/error status remains
on the page, including automatic refresh while application is pending.

The switch filters incoming events before export, not the underlying service
or already queued events. At this earlier checkpoint, journal/file streams could
overlap container output; the later logical-source consolidation above removes
the configured duplicate file inputs once deployed. There is no automatic
content-based deduplication. Choices cover the current Collector,
including remote senders using the same tags. Remote-host-specific choices
will need explicit host identity and scoping.

## Java rollout and oversized records verified

On November 17, isolated Kafka/reload4j and Debezium/Log4j2 probes verified
single-line JSON for chained and suppressed exceptions, quoted/control/Unicode
text and 128 KiB payloads. Normal-size live Docker Fluent Forward probes then
verified exactly one ERROR row per exception, a separate WARNING record,
correct service/logger metadata and redaction after JSON decoding.

With user approval, Connect was stopped gracefully, Kafka recreated with the
derived image, and Connect recreated with its native JSON configuration. The
Kafka data volume was retained. All nine original connectors and their tasks
returned to RUNNING. Databases and Celery workers were not restarted. Both
services' local file logs remain populated.

A strict check of the new container IDs reproduced service attribution leaking
between Kafka and Connect records. Source grouping now fixes this before the
transform runs. An isolated Collector regression and a live single-resource
OTLP batch both verified distinct container/file/journal/untagged service names,
severity, redaction and retained source keys. Stored pre-fix rows are unchanged.

Oversized live probes confirmed a separate transport limit: each 128 KiB Java
exception arrived as nine Docker fragments, plus its separate warning record.
All payload bytes were retained in ordinal order with one partial ID and a
final marker, but the Collector does not reassemble them. Incomplete JSON cannot
reliably retain decoded severity or complete-message redaction. Do not claim
this case is fixed by the Java layouts or by a UI expand button.

The three Java/Collector infrastructure tests and all 26 Operations tests pass.

Next:

1. Design bounded Docker fragment reassembly before parsing/redaction, keyed
   by container, stream and partial ID; validate ordinals and final markers.
   Surface gaps, timeouts and size limits explicitly. The Fluent Forward
   receiver has no stanza recombine operators; filelog multiline settings do
   not cover forwarded events. Avoid introducing a custom protocol relay as
   a shortcut. Arbitrary non-JSON service output may need separate handling.
2. Test invalid-config rollback, Collector outages, persistence/restarts, queue
   saturation, full disk and delivery-loss reporting more extensively.
3. Verify real source coverage and overlapping streams without recreating
   active workers unexpectedly; no source switch changes Docker daemon config.

## Split log screens implemented

The agreed redesign is implemented. Migrations `0006_split_log_screens` and
`0007_remove_recent_logs_widget` are applied. PostgreSQL stores widget
preferences and source filters for saved background jobs, not log entries.

- Dashboard: the Store connectivity card shows connection status and the latest
  received-event time, with Live logs and Log search links. The duplicate Recent
  Logs widget, snapshot timestamp, five-entry query and visibility preference
  have been removed. The card checks status on page load, without implying
  active ingestion. Ingestion settings stay on the log pages.
- Live logs: defaults to 25 entries, with 50/100 options; polls roughly every
  five seconds using ingestion time, a two-second overlap and ID deduplication.
  It retains only the selected window; busy intervals are not gap-free.
- Search: remains at `/logs/`, does not auto-refresh or preload entries.
  Successful recent searches are saved alongside background searches.
  Source/service/severity/text filters are primary;
  job-specific filters are optional. Disabled sources remain searchable.
  Explicit AI analysis has its own card, separate from the filters. The
  redundant Tools dropdown is removed; recent jobs remain on Dashboard.
- Full messages load only on demand in a plain-text, scrollable dialog.
  Escape/Close restore focus; clipboard unavailability is explicit. Docker
  fragments are marked as fragments, not complete reconstructed events.
- Pause freezes this browser's rows and scrolling only. It displays the
  shared dismissible confirmation plus persistent Paused/Resume controls.
  Opening an event or scrolling away from the bottom also pauses; explicit
  Resume follows the latest window. Hidden tabs suspend polling. Failures
  preserve rows and back off up to 60 seconds.
- Live and Search share `logs/components/log_store_status.html`, without
  navigation inside the card. Status reflects query access, not proof of
  ingestion. Source selection/Collector confirmation remain separate, and
  paused/hidden views identify that their connection status is not refreshing.
  Server and browser notifications also share one message template.

Log pages are grouped under `templates/operations_plane/logs/`, reusable
fragments under `logs/components/`, and dashboard cards under
`dashboard/components/`. Development log-screen scripts use fresh asset URLs
on reload so cached JavaScript does not mask changes.

With approval, only the Operations log-search and AI workers were restarted;
both responded to targeted Celery pings. One search submitted to the old worker
after migration failed on the new history column and remained marked Running.
After verifying that the worker had no active task, the affected search was
requeued with an audit reason through the existing queue helper. It completed
with 100 results using the updated source-aware query and history model.
No unfinished searches remained at verification. Other workers and ingestion
services were not restarted.

The redesign's grouped Operations suite passed 35 tests; migration checks and JavaScript
syntax checks pass. Browser checks verified 25/50/100 limits, source filtering,
unchanged paused rows, notification dismissal without resuming, matching
pause/resume notification geometry, Escape/focus handling, and an untruncated
6,084-character message in a scrollable dialog. A browser-only simulated 503
verified unavailable status, preserved rows and retry backoff without stopping
ClickHouse or changing ingestion settings.

The subsequent dashboard simplification passed two focused regression tests
covering connected/empty/unavailable store states, both links, absence of log
entry queries, rejection of the removed widget preference, and preservation of
other widget preferences. Migration checks remain clean.

## Saved searches, pagination and inline AI

- Search result pages are now capped at 25 rows, with reusable Previous/Next
  navigation. Both recent and background queries use strict time/UUID cursors;
  newer queries reverse the nearest bounded ascending page before display.
  Existing 100-row snapshots remain intact, but only 25 display at a time.
- Search history has its own owner-only page, with the shared Control Plane
  text/status search bar and 20 saved searches per page. Each queried result
  page is a separate saved record, including successful recent queries.
- AI POSTs accept a completed owned Search ID and derive filters server-side.
  Analyses have a nullable Search FK; old unlinked records are not guessed.
  The search row is locked to avoid duplicate active submissions, and tasks
  are queued after commit.
- The AI card submits asynchronously, displays Queued/Processing/results in
  place, polls an owner-only no-cache endpoint, and reports polling failures
  with retry/backoff. Prior runs are recoverable from Search history and the
  saved result page. Existing linked analysis URLs redirect there; older
  unlinked analyses keep their detail view with asynchronous status updates.
- Fixed the real Ollama timeout caused by a 14,516-token prompt being truncated
  before a 120-second failure. Group exact repeated sampled excerpts, bound
  fields and JSON-encoded UTF-8 prompts to 3,500 bytes, set `num_ctx=8192` /
  `num_predict=256` per analysis request, and hierarchically reduce batches.
  Existing report-generation callers retain their default Ollama options.
  Unique, varied samples can still require multiple requests; there is no
  universal completion-time guarantee.
- Applied migrations `0008_search_pagination_cursors` and
  `0009_link_analysis_to_search`. With approval, reloaded only the Operations
  workers and verified targeted pings. The search worker was reloaded again
  after the requested 25-row cap; ingestion settings were untouched.
- The grouped Operations and related report-AI tests passed 51 tests. Browser
  checks verified explicit-search gating, 25-row recent and saved pages,
  disjoint Next results and exact Previous restoration, inline submission
  without URL changes, completion and recovery via Search history.
- Real Analysis #3 on Search #7 completed in 41.7 seconds: 300 sampled events
  from 35,540 matches, reduced to seven distinct excerpts. Status, coverage,
  summary and breakdown persisted and were recovered from Search history.
  This is sampled evidence, not an exhaustive diagnosis.

### Saved-entry controls follow-up

- Replaced the redundant result-count panel with text, level and source
  filters over the complete saved JSON snapshot, before 25-row pagination.
  Search includes full saved messages, not just visible previews. Saved-entry
  pagination now stays within that snapshot; it never queries ClickHouse or
  creates another history entry. The original live/search query pagination is
  unchanged. Filtering does not alter linked AI inputs or outputs.
- Search history now provides explicit View results and two-step Delete
  controls per entry. Owner-only, CSRF-protected POST deletion locks the
  search, blocks queued/running searches or linked analyses, and removes
  linked AI records with the search. Original ClickHouse logs and standard
  Django audit history are retained.
- Saved AI-run status labels now follow the selected run's asynchronous
  status updates instead of remaining stuck at their page-load value.
- Initially, opening results automatically showed the operator's latest completed AI run
  for exactly the same original range and all filters, including equivalent
  paginated search entries. Newer failed runs do not hide completed summaries;
  explicit prior-run links still work. Re-running is labelled Run a new
  analysis. Records are not re-parented; deleting an entry still removes only
  its directly linked AI records.
- No migration or worker restart is needed for these view/template changes.

### Filtered saved-event analysis (supersedes full-range actions above)

- New AI runs analyze only saved snapshot rows matching the submitted, validated
  text/level/source filters. The shared filter helper is used by result views and
  the AI worker; filtering precedes sampling and 25-row display pagination.
  No ClickHouse query runs for new analyses. Original range/filter metadata is
  retained for context, but no longer determines new AI input rows.
- Nullable `saved_result_filters` on Analysis and its historical model records
  each new run's inputs. Null marks legacy full-range runs. Applied migration
  `0010_saved_result_analysis_filters` and, with approval, reloaded only the AI
  worker; its targeted Celery ping responded.
- The run list is scoped to the history entry, shows each run's filters and
  status, and updates asynchronously on submission/completion. Selecting a run
  replaces the selected result and restores its filters. A completed run for
  matching filters is preferred automatically. Different snapshot entries are
  no longer treated as interchangeable merely because their ranges match.
- Native show/hide controls collapse the large selected result without stopping
  the task; polling replaces only the inner content and preserves collapse.
  History View results/Delete actions are side by side. Saved-result filter
  actions sit beside Source on desktop; Clear filters appears after filtering
  or submitting a saved-result search and resets the selection.
- During deployment, Analysis #8 was submitted before the old AI worker was
  reloaded. It incorrectly reported 8,213 full-range matches for four saved
  ERROR rows. The discrepancy was verified against its exact snapshot/filter
  inputs, then only that run was cleared and requeued with an audit reason.
  Its corrected result completed with four matching/four sampled ERROR events.
  Future deployments must pause new AI submissions until the scope-aware
  worker is loaded, not just apply the nullable migration.

## Useful validation commands

Run from `ReportHub/`:

```bash
docker compose -f docker-compose.yml config -q
docker compose -f docker-compose.yml -f docker-compose.cdc.yml config -q
docker compose run --rm --no-deps --no-TTY otel_collector validate --config=/etc/otelcol-contrib/config.yaml
docker compose run --rm --no-deps --no-TTY app python manage.py test operations_plane --keepdb
docker build --target probe -f otel/java/Dockerfile.kafka -t reporthub-java-logging-probe otel/java
python3 -m unittest tests.infrastructure.test_java_logging tests.infrastructure.test_collector_logging -v
```

The infrastructure tests need the built `reporthub-otel_collector` and
`reporthub-java-logging-probe` images and the pinned Debezium image. They create
disposable containers, use an isolated Collector/file exporter, and do not
change production source settings or write to ClickHouse.

The ScanHub Compose check is run from `ScanHub-API/`:

```bash
docker compose -f docker-compose.yml config -q
```

The `--keepdb` option is important in this environment because the test
database already exists. Do not use a destructive test-database cleanup
without explicit approval.
