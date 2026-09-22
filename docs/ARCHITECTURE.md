# Architecture

`agy-go` is a suite designed to provide three independent binaries:

### Components

1. **`agy-pool`**
   - Stable multi-account proxy and runtime component.
   - Current implementation originates from the verified `agy-pool-go` baseline.

2. **`agy-tokei`**
   - AGY usage ledger and analytics, with text tables and JSON output.

3. **`agy-db`**
   - Independent binary for read-only inventory and retention dry-run planning (CP2).
   - Complete-ingest evidence is insufficient; deletion is blocked and not implemented. See [safety contract](AGY_DB.md).

---

### Invariants

1. **Critical Path:** `agy-pool` is the critical runtime path.
2. **Path Isolation:** `agy-tokei` and `agy-db` must remain strictly outside the `agy-pool` proxy/SSE request path.
3. **Failure Containment:** Failure of `agy-tokei` or `agy-db` must never affect:
   - Proxy availability
   - Authentication
   - Account scheduling
   - Quota refresh
   - SSE streaming
   - Failover
   - No-replay guarantees
4. **Accounting Truth:** Future AGY token accounting uses AGY's own conversation databases as the usage source of truth, not proxy/SSE interception.
5. **Infrastructure Independence:** The currently installed `agy-pool` used to develop this project is external development infrastructure and is not to be replaced during development.

---

### Ledger Durability & Retention Design

`agy-tokei` maintains an authoritative, deduplicated token ledger in SQLite (`usage.db`).

#### 1. Long-Term Survival After Source Deletion
- In future milestones, `agy-db` will provide retention management to purge or compress older AGY conversation SQLite databases (`~/.gemini/antigravity-cli/conversations/*.db`) to reclaim VPS disk space.
- Once `agy-db` deletes an original conversation database, `usage.db` becomes the sole historical source of truth for token usage, model attribution, and project analytics.
- The ledger must therefore be completely self-contained: it never relies on re-reading deleted source files for past statistics, and its schema guarantees generation deduplication and metadata persistence across time.

#### 2. Durability vs. Ingestion Performance Trade-Off
`usage.db` is configured with targeted SQLite pragmas to balance high-speed incremental ingestion with robust crash resilience:
- **`PRAGMA journal_mode = WAL`:** Write-Ahead Logging allows concurrent readers without blocking writes, provides atomic transaction commits, and avoids in-place DB file rewriting.
- **`PRAGMA synchronous = NORMAL`:** In WAL mode, `NORMAL` syncs the WAL file during checkpointing rather than at every single transaction commit. This yields an order-of-magnitude faster ingestion speed for incremental scans while ensuring the database structure cannot be corrupted by application crashes (only an OS crash/power failure during uncheckpointed commits could lose the most recent uncheckpointed transaction, with zero corruption of previously checkpointed data).
- **`PRAGMA busy_timeout = 5000`:** Enforces a 5-second wait on SQLite locks to handle transient concurrency between CLI commands or future dashboard readers gracefully.
- **Filesystem Permissions:** The ledger directory is initialized with `0700` and database files with `0600` permissions to protect confidential generation metadata.

#### 3. SHA-256 and Backup Prerequisites for Source Deletion
Before `agy-db` may safely delete or archive any original AGY conversation database, the following prerequisites must be met:
- **Authoritative SHA-256 Hash (`source_sha256`):** Computed and recorded in `ingest_manifests` before deletion.
- **Distinction of Hash Contracts:**
  - `source_fingerprint` (`size:mtime:steps:gen`): Fast change detector (<1ms scan) used during regular CLI and daemon ingestion loops.
  - `source_sha256`: Cryptographically authoritative full content digest computed only during explicit archiving/retention contracts before permanent source deletion.
- **Verification Invariants:** Ingestion must have `is_complete = 1`, `last_scan_status = 'completed'`, and invariant verification (`agy-tokei verify`) must report `PASS`.
- **Backup Prerequisite:** `usage.db` itself must have an active backup or replica prior to executing destructive source deletions.
