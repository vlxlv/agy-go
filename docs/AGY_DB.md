# agy-db — V1 complete; V2 archive/verification CP2; deletion CP3 BLOCKED

**agy-db never modifies AGY conversation database contents.**

**agy-db v1 never deletes AGY conversation databases. CP3 is BLOCKED.**

## Purpose

Independent binary within agy-go for AGY conversation inventory, structural
inspection, lifecycle evidence reporting, retention analysis, storage auditing
and external archival. Source handling remains read-only.
CP1 provides read-only inventory; CP2 adds a pure retention decision engine and
dry-run reporting. V2-CP1 adds creation of a detached, verified archive;
V2-CP2 adds offline verification and an agy-db-owned registry. There is no
restore, deletion, ledger write, conversation editing, database repair or
background process.
The current target is Linux, including Debian WSL; run it inside WSL.

## Scope

```sh
go build ./cmd/agy-db
./agy-db scan --source-dir /explicit/development/fixtures
./agy-db status --source-dir /explicit/development/fixtures --json
./agy-db inspect --source-dir /explicit/development/fixtures example.db
./agy-db plan --source-dir /explicit/development/fixtures --retention 720h
./agy-db plan --source-dir /explicit/development/fixtures --ledger /explicit/development/usage.db --retention 720h --json
./agy-db archive --source-dir /explicit/conversations --brain-dir /explicit/brain --summaries-db /explicit/conversation_summaries.db --id conversation-id --output /explicit/archives/conversation-id
./agy-db verify-archive --registry /explicit/agy-db/registry.db /explicit/archives/conversation-id
./agy-db archives --registry /explicit/agy-db/registry.db
```

Flags precede the inspect filename. `status` is a fresh scan, not cached state.
No arguments display help. No default source directory is resolved from HOME.
Only immediate `.db` entries are considered; no recursive discovery. `inspect`
accepts one basename under the supplied directory, not an arbitrary path.
Output is a text table (digest prefixes for readability) or versioned JSON
(full digests for comparison; never use display prefixes as identity proof).
Exit codes: 0 successful inventory
(including unknown lifecycle evidence), 1 inspection/I/O failure or unsupported
schema, 2 invalid arguments. One failed source does not hide other scan results.

## CP1 status

Complete: `scan`, `status`, and `inspect`; full SHA-256 file identity, guarded
read-only source access, private-copy SQLite validation, text and JSON output.

## CP2 status

Complete: typed activity/ingest assessments, pure retention decisions, explicit
blockers, fresh source verification, and dry-run `plan`. Positive eligibility
exists only in synthetic decision-engine cases; the current adapter cannot
prove complete ingest or authoritative activity.

## Known limitations

Read-only development references:

- `/home/ezhang/agy-pool-go`: Go module, SQLite/error conventions, test guards.
- `/home/ezhang/agy-go/internal/tokei/reader.go` and `reader_test.go`: source
  tables and synthetic fixtures.
- `internal/tokei/service.go`: documented discovery location
  `~/.gemini/antigravity-cli/conversations/*.db` (not accessed).
- `internal/tokei/ledger.go`: separate usage ledger and ingest manifests.

The recognized shape is exactly the three fixture tables `steps`,
`gen_metadata`, `trajectory_metadata_blob`, with the observed ordered columns,
types and primary keys, user_version 0, and no additional application tables,
views or triggers. This is labelled `tokei_fixture_v1`, not certified production
schema support. Indexes may be present. Unknown shapes remain blocked.

No real user DBs were supplied or needed. Development tests create all sources
inside `t.TempDir()`. In a Go test process, the opener rejects every directory
except roots explicitly registered by the same-package test helper; environment
variables cannot disable this guard. Known production-like paths are tested as
rejections before filesystem access. Normal CLI use requires an explicit source
directory; this is not an OS sandbox against a user deliberately specifying
their own live directory.

## Safety model

The source is opened O_RDONLY, O_NOFOLLOW and O_NONBLOCK inside an `os.Root`.
Symlink roots, nonregular files, and observed `-wal`, `-shm` or `-journal`
sidecars are rejected. Even empty sidecars block inspection. Source files are
never passed to SQLite. Instead, bytes are copied into a private 0700 temporary
directory / 0600 file. The source is hashed again and device/inode identity,
size, mtime and mode are checked before accepting the snapshot.

SQLite opens only that closed, private snapshot with `mode=ro`, `immutable=1`,
`query_only=ON`, and `trusted_schema=OFF`. It runs integrity_check, schema
inspection and counts; it never reads conversation payloads into output.
Immutability is appropriate for the detached copy, not for a live source.
Handles and temporary files are cleaned up on normal success/error/cancellation;
cleanup failures return an error. A process crash may leave a private
`agy-db-snapshot-*` temporary directory containing a source copy. Remove only
such confirmed leftover directories after the process has stopped. Secure
erasure is not promised. Inspection may update filesystem access time; source
bytes, mtime, permissions and SQLite sidecars are never intentionally changed.

This is observation, not an AGY writer lock: double hashing and stat checks
detect ordinary changes but do not prove inactivity or eliminate adversarial
change-and-restore races. No CP1 observation is deletion authorization.

## Source identity

- `source_id = sha256:<full main-file digest>`: identical copies/moves retain
  their file-content identity; changed bytes yield another file-content identity.
  This is NOT a logical SQLite generation proof: WAL can contain committed
  contents absent from the main file. CP1 rejects observed sidecars, but does
  not establish writer quiescence or an atomic snapshot boundary.
- `path_ref = path-sha256:<absolute lexical path digest>`: correlates an entry
  without printing private names. It is pseudonymous, not an anonymization
  guarantee against guessing known paths.
- Filesystem identity is compared during each read. A byte-identical replacement
  between scans cannot be distinguished from the same content generation;
  CP1 stores no persistent inode history. A future deletion contract needs both
  content generation and fresh filesystem identity after a real writer claim.
- Schema/user versions and size/mtime are metadata, never identity proof alone.
- Step and generation row counts are reported. They are not message counts or
  complete-ingest coverage. No conversation count is inferred from filenames.

Inventory commands retain the CP1 fields and remain ineligible: `activity_unverified`, `ingest_unverified`,
`retention_not_evaluated`. Last activity is null; mtime is never substituted.
Inspection failures add a fixed, non-sensitive blocker. No SQL error text,
absolute source path, filename, prompt, response or credentials are printed.
No persistent audit database is needed at this checkpoint; JSON format starts
at version 1. agy-pool is untouched; agy-tokei remains the usage authority.

CP2 `plan` uses the typed decisions below. Inventory compatibility fields are
not used as positive policy evidence.

## Validation

```sh
gofmt -w .
go vet ./...
go test ./... -count=1
go test -race ./... -count=1
git diff --check
```

Tests cover fixture inventory, corrupt/empty/missing/unknown sources, sidecars,
symlinks, escaped paths, content identity, privacy, source byte/mtime invariants,
test-root rejection, cancellation, concurrent scans and temporary-file cleanup.
CP3 deletion remains evidence-blocked; persistent audit behavior is outside v1.

## Ingest evidence model

**Current agy-tokei data CANNOT prove complete ingest of an exact source
generation. CP3 is BLOCKED. No deletion or prune command exists.**

| Evidence in local source | Actual meaning | Why it cannot authorize retention |
| --- | --- | --- |
| `steps.metadata`, protobuf fields 1 / 8 (`proto.go: ParseStepMetadata`) | Field 1 is created_at; field 8 is completed_at; parser prefers 8 then 1 | No contract proves every kind of conversation activity has a timestamped step, or how `status` denotes a final inactive state |
| `gen_metadata.data`, protobuf path 1 → 9 → 4 (`ParseGenMetadata`) | Generation event timestamp | Generations do not cover all edits, messages, tool events or other source mutations |
| `trajectory_metadata_blob.data`, field 2 (`ParseTrajectoryTimestamp`) | Fallback base timestamp | Not established as last activity; reader further falls back to filesystem mtime |
| Catalog `conversation_summaries.last_modified_time`; ledger `conversation_metadata.last_modified_time` | Separately synchronized catalog metadata | No atomic binding to the exact conversation file generation; not used as source authority |
| `ingest_manifests.source_path`, `source_size`, `source_mtime_ns`, `source_fingerprint` | Location and cheap change indicators | Filename/path is not identity; fingerprint is size/mtime/highest indices, not a digest |
| `ingest_manifests.source_sha256` | Optional digest column | Normal `Service.Ingest` does not populate it; `CommitIngest` retains an old digest with COALESCE when a new one is absent |
| `is_complete`, `status`, `last_scan_status` | Reader saw unchanged size/mtime and no nonempty WAL; last scan result | Not proof of inactivity or full source coverage |
| `generation_count` and `usage_records` count | Imported token-bearing deduplicated records | Zero-usage events are omitted; matching counts prove internal consistency only, not expected source coverage |
| `highest_step_index`, `highest_gen_index` | Observed high-water indices | Not counts or coverage; UPSERT retains historical maxima |
| `first_usage_timestamp`, `last_usage_timestamp`, `usage_records.timestamp` | Usage-only time range, with fallback/earliest-alias behavior | Cannot prove latest non-usage activity was represented |
| `ingested_at`, `parser_version` and transactional `CommitIngest` | Import time, parser provenance, atomic ledger replacement | Useful positive facts but not a generation-bound source completeness certificate |

The evidence map is based on local `internal/tokei/{proto,reader,service,ledger,types}.go`
and synthetic schema fixtures. No real AGY DB or external catalog was opened.

## Activity model

`ActivityKnown`, `ActivityUnknown`, `ActivityAmbiguous` are typed states.
`ResolveActivity` accepts authoritative claims about the **same last-activity
fact**, validates absolute times and normalizes UTC. Missing, zero, out-of-range
or non-authoritative claims are unknown. Unequal claims are ambiguous; no
arbitrary clock-skew tolerance is invented. Different step starts/ends are
different events and must not be fed to this reconciler as conflicting claims.

The AGY activity adapter is deliberately stopped at the evidence gap above:
actual plans return unknown/null last activity and deadline. A maximum event
timestamp, catalog timestamp or mtime is not presented as authoritative last
activity. Positive activity/eligibility tests are pure synthetic contract tests,
not a claim that current AGY files supply those proofs.

### Ingest verification and source rechecks

`--ledger` is optional and explicit; omitted means missing evidence. The current
schema-v2 ledger is copied and read using the same CP1 source guards. WAL/SHM/
journal presence blocks ledger inspection too; agy-db never checkpoints a live
ledger. Its path is never passed to SQLite. Unsupported schemas, malformed
evidence, duplicate source paths or relative manifest paths fail closed.

Exact absolute source paths locate candidate manifests only. A valid full digest
is compared with the freshly computed source SHA-256; size and mtime differences
also block. Matching paths or size/mtime never establish identity. Copied/moved
sources without an exact manifest path are conservatively missing evidence.
Partial/failed scans and imported-count mismatches are partial ingest. Missing,
future or pre-source-mtime checkpoints are stale (mtime is only a negative
staleness check, never the activity signal). Unknown parsers are unsupported.
Even **all matching current fields** result in `incomplete_ingest_evidence`.

Plan discovers sources, reads ledger evidence, then takes a fresh source
snapshot for each entry. A generation change since discovery blocks it.
`verified_snapshot` means CP1 integrity/schema/identity checks passed for that
observation; it is not a writer lock or a deletion authorization. It does not
prevent changes after planning. No production adapter can emit `IngestVerified`.

Additional evidence needed from tokei/AGY, reported only (not implemented):

1. A documented authoritative last-activity/inactive-state contract covering all
   source activity, with strict invalid/missing timestamp handling.
2. A fresh digest of the exact coherent source artifact set for every import,
   atomically bound to the ledger checkpoint; never carry forward a stale digest.
3. Versioned coverage proof: expected source step/generation/event sets or
   digests, parsed/imported/excluded counts and identities, explicit exclusion
   rules and zero parse/coverage failures. Usage-only row counts are insufficient.
4. A successful run/checkpoint identity binding that source digest, parser and
   coverage contract to the committed imported rows and authoritative source
   activity watermark. Failed/partial runs must invalidate that certification.
5. For eventual CP3, an independently proven writer coordination/claim contract
   and the documented ledger backup prerequisite; CP2 establishes neither.

## Retention decision model

There is no production default. `--retention` accepts Go durations consisting of
positive whole seconds (e.g. `720h`); days such as `30d` are not Go duration
syntax. Omitted retention is `retention_unset`. The pure engine takes an explicit
evaluation time, duration, source identity/status, activity and ingest assessment.
The CLI takes one UTC evaluation time per plan. No wall clock is read in the
engine. Future activity is blocked. Deadline = last activity + retention;
before it is blocked, exactly at it and after it satisfy the time gate.

Eligible requires all gates positive: valid source SHA-256 and verified snapshot,
known valid activity, matching verified ingest generation, a checkpoint between
last activity and evaluation time, exactly covered activity, and reached positive
retention duration. Typed blockers cover unknown/ambiguous/future activity,
missing/partial/incomplete/unsupported/stale ingest, changed/failed/unsupported
source, unset/invalid/unreached retention and invalid evaluation time.

## Plan semantics

Plan JSON retains `format_version: 1` and adds `dry_run`, `evaluated_at`,
`retention_seconds`, `sources` (typed decisions), `total_reclaimable_bytes`,
`evidence_gaps` and sanitized `errors`. Text prints the same decision facts with
digest prefixes. Only positively eligible decisions contribute bytes; totals
are checked for overflow. Current actual plans always total **0 bytes**.

Example decision for a readable source and a matching current manifest:

```text
ACTIVITY  DEADLINE  INGEST               SOURCE             ELIGIBLE  BYTES  BLOCKERS
unknown   unknown   incomplete_evidence  verified_snapshot  false     0      [activity_unknown incomplete_ingest_evidence]
Total reclaimable bytes: 0
```

Normal policy/evidence blockers return exit 0 with a successfully generated
report. Source/ledger inspection errors return 1; argument errors return 2.
Raw paths, filenames, SQL error text, prompts, responses and credentials are
never included. Missing ledger evidence does not trigger a default HOME lookup.
No input flag/file can provide a forged positive assessment to the engine.

## CP3 blocker

**EVIDENCE STILL INSUFFICIENT — CP3 REMAINS BLOCKED.** This is a semantic and
provenance blocker, not a shortage of tests. Current evidence cannot prove:

1. Complete enumeration of all logical conversation activity.
2. Complete discovery of all independent sessions and sub-trajectories.
3. An externally verifiable barrier establishing that all asynchronous updates
   have been committed.
4. A persistent authoritative activity watermark that cannot be lost or move
   backward through truncation or replacement.
5. Exact complete-ingest proof for one coherent source generation, binding
   coverage, activity and checkpoint to committed usage data.
6. Permanent finalization/non-resumability: a conversation cannot receive future
   writes or be reopened. **IDLE != FINALIZED.**

Prior read-only static analysis of the installed AGY binary established the
following persistence behavior. Evidence is version-specific, not a supported
upstream persistence contract. Binary SHA-256:
`c4c8a6722f9b570e370941b0953ba29051336307d7999ec842bdf7500b0ca7c8`.
Analyzed binary: `/home/ezhang/.local/bin/agy`. Compiled source paths include
`third_party/jetski/cortex/trajectory/dbtrajectory/sqlite_store.go`,
`third_party/jetski/cortex/trajectory/dbtrajectory/dbtrajectory.go`, and
`third_party/jetski/cortex/trajectory_store/sqlite_store.go`.

| Established evidence | Consequence |
| --- | --- |
| `saveStepToDB` (ELF VA `0x7eb1a40`) and `SaveStepBatch` (`0x7eb2040`) use conflict updates; `steps.idx` is the primary key | Existing steps can be overwritten; this is not an append-only activity log |
| `TruncateSteps` (`0x7eb26a0`) deletes `idx >= boundary` | Existing activity-bearing records and embedded descendants can disappear |
| Metadata collection setters delete and rebuild collections in separate transactions; single-record setters also exist | Both snapshot replacement and incremental updates occur; no whole-conversation commit barrier follows |
| `decomposeStep` (`0x7eb4200`) serializes the complete Step, including its nested subtrajectory, into `step_payload` | Reading only `steps.metadata` cannot enumerate all embedded descendants |
| Independent child/session discovery is not proven complete | A root traversal is not a completeness certificate |
| `openOrCreateSQLiteStore` (`0x7eafae0`) configures WAL/NORMAL; checkpointing is separate | Main DB SHA-256 alone cannot identify the complete committed logical state |
| Higher-level asynchronous commit work exists | Unchanged file bytes do not prove all activity has reached disk |
| Timestamp assignments cover particular paths, with additional status-transition timestamps | Neither one timestamp nor the current candidate-field maximum is proven to cover all activity |
| Current tokei imports token-bearing, deduplicated usage records | Usage rows and matching row counts are not a complete source representation |

Additional confirmed static write-side findings:

- initSchema uses GORM AutoMigrate for all seven model families below.
- Step saving does not filter by token usage; nil metadata does not prevent
  persistence. has_subtrajectory derives from Step.subtrajectory != nil.
- SaveStepBatch writes transactionally. SaveTrajectoryMetadata replaces
  trajectory metadata state; SetGenMetadatas, SetExecMetadatas and SetParentRefs
  each replace their whole collection in a transaction.
- The observed save path serializes nested subtrajectory in the full Step
  protobuf payload; it does not recursively flatten descendants into SQL rows.
- TruncateSteps does not itself maintain an independent activity watermark.
- WAL checkpointing exists but is not proven to follow every commit.

The installed writer has seven model families: trajectory metadata, steps,
generation metadata, executor metadata, parent references, battle-mode records,
and trajectory metadata blobs. The current three-table fixture recognizer is
intentionally not promoted to support for this production schema. No new schema
adapter, migration or provenance table was added from this research.

## CP3 Resume Conditions

CP3 may only be reconsidered when authoritative AGY writer source or an
equivalently authoritative versioned persistence/schema contract establishes
**all** of the following:

A. **Complete logical source boundary:** SQLite DB, WAL state, external
   brain/session artifacts and applicable cross-session relationships.
B. **Consistent committed snapshot boundary:** all durable writes for generation
   X must be represented, including WAL and an externally verifiable barrier
   for pending asynchronous/in-memory updates.
C. **Complete descendant/session enumeration:** no untracked children or related
   independent stores; defined identities, retries, aliases and exclusion rules.
D. **Authoritative activity watermark:** complete over relevant activity,
   persistent, resistant to truncation/replacement, non-regressing or otherwise
   formally interpretable, with missing/invalid timestamp semantics.
E. **Permanent finalization/non-resumability:** prove the conversation cannot
   later receive writes. Idle status, empty WAL, absent open handles and old
   mtime do not establish this.
F. **Transactionally bound ingest provenance:** the same successfully committed
   ingest run must bind exact generation X, full enumeration and ingestion,
   authoritative activity watermark T, and checkpoint to committed usage data.

Any missing contract keeps CP3 blocked. A future destructive design also needs
separately reviewed ownership/revalidation and backup prerequisites. More tests,
matching ledger fields or matching hashes at two sampling times do not meet
these conditions or prove absence of intervening mutation.

## created_at / started_at finding (documentation only)

Confirmed CortexStepMetadata mapping:

| Field | Meaning |
| --- | --- |
| 1 | created_at |
| 6 | viewable_at |
| 7 | finished_generating_at |
| 8 | completed_at |
| 22 | last_completed_chunk_at |
| 32 | started_at |

Treating field 1 as startTime is semantically incorrect naming. Current tokei `ParseStepMetadata`
names field 1 `startTime`, prefers `completed_at` (field 8), then falls back to
field 1; its test builder uses the same naming. Downstream usage records and
first/last usage timestamps inherit that selection, including earliest-alias
merging. Step enumeration itself is ordered by `idx`.

Correcting names alone is behavior-neutral. If usage timestamps are intended to
mean execution start, changing the selected field can be correctness-impacting.
Product semantics must first define created, started, completed or explicit
fallback ordering. Switching to field 32 is not automatically justified; a
separate contract decision is required before changing tokei behavior. This finding does not establish authoritative conversation activity
or unlock retention. No tokei code is changed by agy-db CP1/CP2.

## Production Evidence Snapshot

**OBSERVED CURRENT PRODUCTION BEHAVIOR — not a permanent API contract.**

Source: the user-supplied “Production Data-Structure Audit for agy-db”, reported
on 2026-09-22 in the agy-pool-go discussion. The DDL below is copied from that
audit, not inferred from ORM metadata. This local finalization did not connect
to the VPS or open production databases. Only schema and safe aggregates are
reproduced; audit interpretations are not promoted to authoritative contracts.

### Storage and source scope

- `/home/codex/.gemini/antigravity-cli/conversations/`: 109 conversation DBs,
  109 matching -wal files and 109 matching -shm files.
- `/home/codex/.gemini/antigravity-cli/brain/`: 109 matching conversation
  directories, one per observed conversation.
- `/home/codex/.gemini/antigravity-cli/conversation_summaries.db`: 109 rows.
- `/home/codex/.local/share/agy-tokei/usage.db`: separate usage ledger.
- `/home/codex/.local/share/agy-pool/state.db`: separate proxy routing/quota
  state, not conversation storage and not managed by agy-db.

Session artifacts may exist in brain/<conversation-id>/... and central metadata
in conversation_summaries.db. A single conversation DB is not proven to be the
entire semantic lifecycle object.

### Exact production SQLite schema

All 109 DBs reportedly use PRAGMA user_version=1 and one uniform schema variant:
seven tables, exactly two secondary indexes (idx_steps_status on steps(status),
idx_steps_step_type on steps(step_type)), zero declared foreign keys and zero
triggers. The following CREATE TABLE definitions are copied from audit evidence:

```sql
CREATE TABLE `trajectory_meta` (
  `trajectory_id` text,
  `cascade_id` text,
  `trajectory_type` integer,
  `source` integer,
  PRIMARY KEY (`trajectory_id`)
);

CREATE TABLE `trajectory_metadata_blob` (
  `id` text DEFAULT "main",
  `data` blob,
  PRIMARY KEY (`id`)
);

CREATE TABLE `steps` (
  `idx` integer,
  `step_type` integer NOT NULL DEFAULT 0,
  `status` integer NOT NULL DEFAULT 0,
  `has_subtrajectory` numeric NOT NULL DEFAULT false,
  `metadata` blob,
  `error_details` blob,
  `permissions` blob,
  `task_details` blob,
  `render_info` blob,
  `step_payload` blob,
  `step_format` integer NOT NULL DEFAULT 0,
  PRIMARY KEY (`idx`)
);

CREATE TABLE `gen_metadata` (
  `idx` integer,
  `data` blob,
  `size` integer NOT NULL DEFAULT 0,
  PRIMARY KEY (`idx`)
);

CREATE TABLE `executor_metadata` (
  `idx` integer,
  `data` blob,
  PRIMARY KEY (`idx`)
);

CREATE TABLE `parent_references` (
  `idx` integer,
  `data` blob,
  PRIMARY KEY (`idx`)
);

CREATE TABLE `battle_mode_infos` (
  `idx` integer,
  `data` blob,
  PRIMARY KEY (`idx`)
);
```

This is documentation, not a migration or production adapter. The fixture-only
recognizer and sidecar blocking remain unchanged.

### Aggregate rows and steps

| Table | Total rows | Observed relationship |
| --- | ---: | --- |
| trajectory_meta | 109 | Exactly one row per DB |
| trajectory_metadata_blob | 109 | Exactly one id="main" row per DB |
| steps | 44,644 | idx contiguous from 0 through N-1 in 109/109 DBs |
| gen_metadata | 21,953 | Every observed idx also exists in steps |
| executor_metadata | 867 | Every observed idx also exists in steps |
| parent_references | 0 | Unused in the observed production fleet |
| battle_mode_infos | 0 | Unused in the observed production fleet |

The idx correspondences are observed relationships, not declared foreign-key
constraints or permanent identity contracts. Currently zero does not mean
never used.

steps.idx is INTEGER PRIMARY KEY. Steps have no native SQL timestamp columns;
timestamps live in protobuf metadata. All 44,644 observed rows had non-NULL,
non-empty metadata and step_payload; all had has_subtrajectory=false. Static
writer evidence still permits nil metadata and nested trajectories. Contiguity
does not make idx a generation identifier: rows are UPSERTable, overwriteable
and truncatable. step_count/max(idx) are useful change indicators only, not
complete-generation proofs; same-count row changes can escape them.

### WAL and source-generation evidence

108 idle DBs had zero-byte WAL files. One active DB had a 4,494,952-byte WAL
(about 4.5 MB). Active committed state may therefore reside outside the main
DB. WAL size alone does not identify which frames are committed.

SHA256(main.db) is insufficient for an active WAL-mode logical generation.
Hashing main DB and WAL independently does not establish an atomic snapshot;
SHM is not a logical generation ID. Ordinary file copying is not proven to
produce a consistent SQLite snapshot. No application-level monotonic generation
counter was found; user_version is schema version, not content generation.

A proven consistent SQLite snapshot mechanism is the plausible future direction,
but alone does not solve external child/session scope, pending in-memory updates
or permanent finalization. Zero-byte WAL does not prove future immutability.
CP1 rejects even empty WAL/SHM files, so the observed fleet layout remains
blocked by the current adapter.

### Production timestamp evidence

| Field | Present | Missing |
| --- | ---: | ---: |
| created_at (1) | 44,644 | 0 |
| viewable_at (6) | 42,974 | 1,670 |
| finished_generating_at (7) | 42,983 | 1,661 |
| completed_at (8) | 44,157 | 487 |
| last_completed_chunk_at (22) | 0 | 44,644 |
| started_at (32) | 43,986 | 658 |

The audit reported two backward step-to-step timestamp transitions. Its
aggregate description does not establish universal per-field semantics or a
cause for reversals. Timestamps are approximately monotonic in that observation,
not strictly monotonic. No protobuf timestamp can be promoted to a generation
or activity sequence counter. No last_activity rule follows from these counts.

### Child and cross-session observations

All observed steps had has_subtrajectory=false; all parent_references tables
were empty. Catalog parent_conversation_id and group_id were empty and
nesting_depth=0. There was exactly one cascade_id per DB and one brain directory
per conversation. These are OBSERVED CURRENT PRODUCTION BEHAVIOR only. Static
binary evidence proves the feature exists; no guarantee about future sessions
or complete discovery of independent child stores follows from this sample.

### usage.db limitations

The audit reported 109 ingest manifests: 108 marked complete and one
active/incomplete. source_fingerprint uses size/mtime
(size=<size>:mtime=<mtime_ns>). source_sha256 exists but was NULL for all 109
observed manifests.

is_complete=1 is not agy-db CompleteIngestVerified. Current tokei cannot prove
exact cryptographic logical generation, permanent source immutability,
authoritative last activity, session finalization or complete semantic closure.
Usage rows are not a complete source representation; matching current fields
must continue to yield incomplete evidence.

### Activity versus permanent finalization

Observed idle sessions commonly had conversation_summaries.status equal to
CASCADE_RUN_STATUS_IDLE, WAL size zero, no active open handles and stale mtime.
None establishes permanent finalization. Whether a historical idle conversation
can be reopened/resumed and receive new steps remains unresolved by an
authoritative contract. **IDLE != FINALIZED.** Inactivity cannot be converted
into automatic deletion eligibility.

## V2 archive model (CP1)

AGY owns resume, rename, delete and import semantics. agy-db owns external
archive, verification and storage-audit semantics. V2-CP1 adds only:

```text
agy-db archive --source-dir DIR --id ID --output DIR \
  [--brain-dir DIR] [--summaries-db FILE] [--json]
```

The source bundle is bounded by explicitly configured roots and may contain
`conversations/<id>.db`, its WAL/SHM coordination files, `brain/<id>/`, and one
matching row in `conversation_summaries.db`. Missing optional brain or summary
artifacts are recorded as absent. Arbitrary source trees, symlinks, path
escapes, multiply linked regular files, special files and an existing output
path are rejected.

### Consistent detached snapshot

agy-db opens the live main DB and optional WAL with `O_NOFOLLOW` for byte reads;
it never gives the live path or file descriptor to SQLite. It hashes and stats
main DB, WAL and SHM before capture, copies main DB plus any WAL into a private
0700 staging directory, then repeats every observation. Any appearance,
disappearance, replacement, byte change, size change, mode change or mtime
change fails closed as `blocked_source_changed`.

Only after those observations match does the existing modernc SQLite Online
Backup API open the private captured DB/WAL pair and normalize committed WAL
state into `conversation/main.db`. The detached result must pass
`integrity_check`, exact recognized schema checks and sidecar-absence checks.
The live source is never checkpointed, SQLite-opened, written or locked by
agy-db. A continuously changing source is blocked rather than retried forever.
Raw `cp main.db` is not the snapshot mechanism.

SHM is transient SQLite coordination state: it is observed for mutation but is
not copied into the archive. WAL is captured only in private staging and is not
published. The archive contains one closed, self-contained SQLite snapshot.

### Archive format and manifest

Format version 1 is a directory:

```text
archive/
  manifest.json
  conversation/main.db
  brain/...                 # optional
  summary/metadata.json     # optional catalog-row payload
```

`manifest.json` contains `format_version`, a random `archive_id`, UTC
`created_at`, `conversation_id`, recognized source schema/version, observed WAL
state, source-main and detached-snapshot SHA-256 values, source size, optional
artifact presence, tool version, and a sorted `archive_files` array. Every file
entry has a local relative path, byte size, SHA-256 and role. The manifest never
contains prompts, responses, credentials, tokens, raw protobufs or host source
paths. Conversation content remains inside the archived payload files as
expected. Normal text and JSON command output contains IDs, state and hashes
only.

The summary database receives the same private main/WAL capture and mutation
checks. agy-db exports at most the exact matching catalog row as archive payload;
it does not update or forge AGY catalog state. A later restore checkpoint must
either use an authoritative AGY registration/import contract or leave this row
as handoff metadata.

### Atomic publication and crash safety

agy-db builds under a private sibling directory with 0700 directory and 0600
file permissions. It hashes every payload, parses the manifest, rejects
unlisted/symlink/special files, reopens the detached DB read-only, and verifies
all declared sizes and hashes. Linux `renameat2(RENAME_NOREPLACE)` publishes the
verified directory atomically and refuses overwrite. Ordinary failures remove
staging best-effort. A process or host crash may leave a hidden staging
directory, but never a partial archive at the requested final path.

## V2 offline verification (CP2)

```text
agy-db verify-archive [--registry FILE] [--json] ARCHIVE
```

The public command uses the same `verifyArchiveDirectory` implementation used
before atomic archive publication. It pins the archive root by directory file
descriptor and performs no archive writes. It validates the manifest version,
archive/conversation IDs, canonical local paths, duplicate paths, sizes,
SHA-256 values, declared roles, required detached DB, optional brain and summary
payloads, absence of DB sidecars, exact recognized SQLite schema and SQLite
integrity. Symlinks, multiply linked files, special files and normalization or
path-escape attempts fail closed. The DB is opened through a pinned descriptor
with `mode=ro`, `immutable=1`, `query_only=ON` and `trusted_schema=OFF`.

Verification returns exactly one typed state:

- `valid`: every gate passed; exit 0.
- `corrupt`: malformed metadata, hash/size mismatch, invalid SQLite, or invalid
  payload; exit 1.
- `incomplete`: a required artifact is absent or the closed DB depends on a
  WAL/SHM/journal sidecar; exit 1.
- `unsupported`: archive format or SQLite schema is unknown; exit 1.
- `unsafe`: archive root, path or file safety cannot be established; exit 1.

Invalid CLI arguments use exit 2. Text and JSON output contain typed state,
IDs, counts, hashes and a fixed reason code; they do not contain archive paths
or conversation payloads.

## V2 archive registry (CP2)

The registry is a dedicated agy-db SQLite database. `--registry FILE` selects
it. Without that flag, the default is
`$XDG_STATE_HOME/agy-db/registry.db`, or
`~/.local/state/agy-db/registry.db` when XDG_STATE_HOME is unset. This database
is agy-db state and is never stored in an AGY conversation source or archive.

Schema version 1 uses `PRAGMA user_version=1`, an agy-db application ID and two
tables: `archives` and `archive_files`. It records archive/conversation IDs,
the internal archive location, creation and verification times, manifest and
source snapshot identities, typed verification status, and per-file path,
role, size and hash. It stores no prompt, response, credential, token or raw
protobuf content. Registry files use 0600 permissions; their parent directory
uses 0700 where created. Unknown, malformed or differently versioned schemas
fail closed rather than being implicitly migrated.

A successful `archive` is immediately verified and registered. Public
`verify-archive` updates the matching status and can register a valid archive
that was created elsewhere. An archive-ID collision is accepted only when the
immutable manifest/source identity agrees. Registry updates are transactional;
concurrent verification is supported by SQLite WAL and a bounded busy timeout.

```text
agy-db archives [--registry FILE] [--id ARCHIVE_ID] [--json]
```

The command lists registry status, verification time, file/byte totals and a
hashed path reference. `--id` selects one exact archive. It does not scan,
modify or delete archives. The registry may have its own SQLite WAL/SHM files;
those belong to agy-db and are unrelated to AGY source WAL state.

V2-CP2 does not implement restore; that remains a later reviewed checkpoint.
Archive existence or registry status does not authorize source deletion, and
the CP3 deletion blocker remains unchanged.

## Explicit non-goals

No prune/delete/unlink of source databases; conversation edits or repair;
source/ledger migrations or writes; AGY execution; production/VPS operations;
background garbage collection; usage analytics replacement for agy-tokei;
agy-pool state management; or changes to agy-pool/agy-tokei behavior.
Production removal calls are confined to freshly created private snapshot and
archive-staging paths; none receives a source root, source DB, WAL, SHM, brain
artifact or summary database path. Tests may remove synthetic sources and
sidecars inside registered temporary roots.
