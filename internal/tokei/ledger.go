package tokei

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	// CurrentLedgerSchemaVersion represents the active schema version for usage.db.
	CurrentLedgerSchemaVersion = 2
)

// Ledger represents an authoritative SQLite storage handle for agy-tokei usage data.
type Ledger struct {
	db   *sql.DB
	path string
	mu   sync.RWMutex
}

// OpenLedger opens or initializes the agy-tokei SQLite usage ledger.
func OpenLedger(dbPath string) (*Ledger, error) {
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create ledger directory %s: %w", dir, err)
	}
	_ = os.Chmod(dir, 0o700)

	dsn := fmt.Sprintf("file:%s?_txlock=immediate&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open ledger %s: %w", dbPath, err)
	}

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(time.Hour)

	if _, err := db.Exec("PRAGMA journal_mode = WAL;"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set WAL mode on ledger %s: %w", dbPath, err)
	}
	if _, err := db.Exec("PRAGMA synchronous = NORMAL;"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set synchronous mode on ledger %s: %w", dbPath, err)
	}

	l := &Ledger{
		db:   db,
		path: dbPath,
	}

	if err := l.migrate(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate ledger %s: %w", dbPath, err)
	}

	// Enforce 0600 on db file and wal
	if fi, err := os.Stat(dbPath); err == nil && fi.Mode().Perm() != 0o600 {
		_ = os.Chmod(dbPath, 0o600)
	}

	return l, nil
}

// Close closes the database handle.
func (l *Ledger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.db == nil {
		return nil
	}
	err := l.db.Close()
	l.db = nil
	return err
}

// Path returns the filesystem path to usage.db.
func (l *Ledger) Path() string {
	return l.path
}

// Size returns the file size of usage.db.
func (l *Ledger) Size() int64 {
	fi, err := os.Stat(l.path)
	if err != nil {
		return 0
	}
	size := fi.Size()
	if fiWal, err := os.Stat(l.path + "-wal"); err == nil {
		size += fiWal.Size()
	}
	return size
}

// migrate initializes schema tables idempotently across version upgrades.
func (l *Ledger) migrate() error {
	if _, err := l.db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			applied_at DATETIME NOT NULL
		);
	`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	var maxVersion int
	_ = l.db.QueryRow("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&maxVersion)

	if maxVersion < 1 {
		v1Schema := `
		CREATE TABLE IF NOT EXISTS usage_records (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			conversation_id TEXT NOT NULL,
			generation_id TEXT NOT NULL UNIQUE,
			step_index INTEGER NOT NULL DEFAULT 0,
			timestamp DATETIME NOT NULL,
			workspace_uri TEXT NOT NULL DEFAULT '',
			project_id TEXT NOT NULL DEFAULT '',
			model TEXT NOT NULL,
			provider_id INTEGER NOT NULL DEFAULT 0,
			response_id TEXT NOT NULL DEFAULT '',
			provider_assigned_message_id TEXT NOT NULL DEFAULT '',
			message_id TEXT NOT NULL DEFAULT '',
			input_tokens INTEGER NOT NULL DEFAULT 0,
			cache_read_tokens INTEGER NOT NULL DEFAULT 0,
			cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
			visible_output_tokens INTEGER NOT NULL DEFAULT 0,
			reasoning_tokens INTEGER NOT NULL DEFAULT 0,
			total_output_tokens INTEGER NOT NULL DEFAULT 0,
			total_tokens INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME NOT NULL
		);

		CREATE INDEX IF NOT EXISTS idx_usage_conv ON usage_records(conversation_id);
		CREATE INDEX IF NOT EXISTS idx_usage_ts ON usage_records(timestamp);
		CREATE INDEX IF NOT EXISTS idx_usage_project ON usage_records(project_id);
		CREATE INDEX IF NOT EXISTS idx_usage_model ON usage_records(model);

		CREATE TABLE IF NOT EXISTS ingest_manifests (
			conversation_id TEXT PRIMARY KEY,
			source_path TEXT NOT NULL,
			source_size INTEGER NOT NULL,
			source_mtime_ns INTEGER NOT NULL,
			source_fingerprint TEXT NOT NULL,
			highest_step_index INTEGER NOT NULL DEFAULT 0,
			highest_gen_index INTEGER NOT NULL DEFAULT 0,
			generation_count INTEGER NOT NULL DEFAULT 0,
			first_usage_timestamp DATETIME,
			last_usage_timestamp DATETIME,
			parser_version TEXT NOT NULL,
			ingested_at DATETIME NOT NULL,
			is_complete INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL DEFAULT 'completed',
			error_message TEXT NOT NULL DEFAULT ''
		);
		`
		if _, err := l.db.Exec(v1Schema); err != nil {
			return fmt.Errorf("apply v1 schema: %w", err)
		}
		if _, err := l.db.Exec("INSERT OR IGNORE INTO schema_migrations (version, applied_at) VALUES (1, ?)", time.Now().UTC()); err != nil {
			return fmt.Errorf("record v1 migration: %w", err)
		}
	}

	if maxVersion < 2 {
		cols := make(map[string]bool)
		rows, err := l.db.Query("PRAGMA table_info(ingest_manifests)")
		if err != nil {
			return fmt.Errorf("inspect ingest_manifests schema: %w", err)
		}
		for rows.Next() {
			var cid int
			var name, ctype string
			var notnull, pk int
			var dfltValue sql.NullString
			if err := rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk); err == nil {
				cols[name] = true
			}
		}
		rows.Close()

		if !cols["last_scan_at"] {
			if _, err := l.db.Exec("ALTER TABLE ingest_manifests ADD COLUMN last_scan_at DATETIME"); err != nil {
				return fmt.Errorf("add last_scan_at: %w", err)
			}
		}
		if !cols["last_scan_status"] {
			if _, err := l.db.Exec("ALTER TABLE ingest_manifests ADD COLUMN last_scan_status TEXT NOT NULL DEFAULT 'completed'"); err != nil {
				return fmt.Errorf("add last_scan_status: %w", err)
			}
		}
		if !cols["last_scan_error"] {
			if _, err := l.db.Exec("ALTER TABLE ingest_manifests ADD COLUMN last_scan_error TEXT NOT NULL DEFAULT ''"); err != nil {
				return fmt.Errorf("add last_scan_error: %w", err)
			}
		}
		if !cols["source_sha256"] {
			if _, err := l.db.Exec("ALTER TABLE ingest_manifests ADD COLUMN source_sha256 TEXT"); err != nil {
				return fmt.Errorf("add source_sha256: %w", err)
			}
		}

		// Backfill last_scan fields for existing manifests where last_scan_at is NULL
		if _, err := l.db.Exec(`
			UPDATE ingest_manifests
			SET last_scan_at = ingested_at,
			    last_scan_status = status,
			    last_scan_error = error_message
			WHERE last_scan_at IS NULL
		`); err != nil {
			return fmt.Errorf("backfill last_scan fields: %w", err)
		}

		v2Tables := `
		CREATE TABLE IF NOT EXISTS conversation_metadata (
			conversation_id TEXT PRIMARY KEY,
			title TEXT NOT NULL DEFAULT '',
			workspace_uri TEXT NOT NULL DEFAULT '',
			workspace_uris TEXT NOT NULL DEFAULT '',
			project_id TEXT NOT NULL DEFAULT '',
			agent_name TEXT NOT NULL DEFAULT '',
			last_modified_time DATETIME,
			step_count INTEGER NOT NULL DEFAULT 0,
			updated_at DATETIME NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_meta_project ON conversation_metadata(project_id);

		CREATE TABLE IF NOT EXISTS catalog_sync_state (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			catalog_path TEXT NOT NULL,
			catalog_size INTEGER NOT NULL,
			catalog_mtime_ns INTEGER NOT NULL,
			catalog_sha256 TEXT NOT NULL DEFAULT '',
			conversation_count INTEGER NOT NULL DEFAULT 0,
			synced_at DATETIME NOT NULL
		);
		`
		if _, err := l.db.Exec(v2Tables); err != nil {
			return fmt.Errorf("apply v2 tables: %w", err)
		}

		if _, err := l.db.Exec("INSERT OR IGNORE INTO schema_migrations (version, applied_at) VALUES (2, ?)", time.Now().UTC()); err != nil {
			return fmt.Errorf("record v2 migration: %w", err)
		}
	}

	return nil
}

// GetManifests returns all stored ingest manifests keyed by conversation_id.
func (l *Ledger) GetManifests() (map[string]*SourceManifest, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	rows, err := l.db.Query(`
		SELECT conversation_id, source_path, source_size, source_mtime_ns, source_fingerprint,
		       highest_step_index, highest_gen_index, generation_count,
		       first_usage_timestamp, last_usage_timestamp, parser_version,
		       ingested_at, is_complete, status, error_message,
		       last_scan_at, last_scan_status, last_scan_error, source_sha256
		FROM ingest_manifests
	`)
	if err != nil {
		return nil, fmt.Errorf("query manifests: %w", err)
	}
	defer rows.Close()

	result := make(map[string]*SourceManifest)
	for rows.Next() {
		var m SourceManifest
		var isCompInt int
		var firstTS, lastTS, lastScanTS sql.NullTime
		var lastScanStatus, lastScanError, sha256 sql.NullString
		if err := rows.Scan(
			&m.ConversationID, &m.SourcePath, &m.SourceSize, &m.SourceMtimeNs, &m.SourceFingerprint,
			&m.HighestStepIndex, &m.HighestGenIndex, &m.GenerationCount,
			&firstTS, &lastTS, &m.ParserVersion,
			&m.IngestedAt, &isCompInt, &m.Status, &m.ErrorMessage,
			&lastScanTS, &lastScanStatus, &lastScanError, &sha256,
		); err != nil {
			return nil, fmt.Errorf("scan manifest: %w", err)
		}
		m.IsComplete = (isCompInt == 1)
		if firstTS.Valid {
			t := firstTS.Time.UTC()
			m.FirstUsageTimestamp = &t
		}
		if lastTS.Valid {
			t := lastTS.Time.UTC()
			m.LastUsageTimestamp = &t
		}
		if lastScanTS.Valid {
			t := lastScanTS.Time.UTC()
			m.LastScanAt = &t
		}
		if lastScanStatus.Valid {
			m.LastScanStatus = lastScanStatus.String
		}
		if lastScanError.Valid {
			m.LastScanError = lastScanError.String
		}
		if sha256.Valid && sha256.String != "" {
			s := sha256.String
			m.SourceSHA256 = &s
		}
		result[m.ConversationID] = &m
	}

	return result, nil
}

// CommitIngest saves all generation records and updates the conversation manifest atomically.
func (l *Ledger) CommitIngest(manifest *SourceManifest, records []*UsageRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	tx, err := l.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	now := time.Now().UTC()

	// Reader supplies a complete, validated SQLite snapshot, including active sources.
	// Replace this conversation atomically so corrections and identity upgrades remove old rows.
	if _, err := tx.Exec("DELETE FROM usage_records WHERE conversation_id = ?", manifest.ConversationID); err != nil {
		return fmt.Errorf("replace conversation snapshot: %w", err)
	}

	// Insert or replace generation records
	recordStmt, err := tx.Prepare(`
		INSERT INTO usage_records (
			conversation_id, generation_id, step_index, timestamp,
			workspace_uri, project_id, model, provider_id,
			response_id, provider_assigned_message_id, message_id,
			input_tokens, cache_read_tokens, cache_creation_tokens,
			visible_output_tokens, reasoning_tokens, total_output_tokens, total_tokens,
			created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare record stmt: %w", err)
	}
	defer recordStmt.Close()

	var firstTS, lastTS *time.Time
	for _, r := range records {
		if r == nil || r.ConversationID != manifest.ConversationID {
			return fmt.Errorf("record does not belong to conversation %s", manifest.ConversationID)
		}
		normalized := *r
		normalized.Normalize()
		r = &normalized
		if !r.IsTokenBearing() {
			continue
		}
		if firstTS == nil || r.Timestamp.Before(*firstTS) {
			t := r.Timestamp
			firstTS = &t
		}
		if lastTS == nil || r.Timestamp.After(*lastTS) {
			t := r.Timestamp
			lastTS = &t
		}

		_, err := recordStmt.Exec(
			r.ConversationID, r.GenerationID, r.StepIndex, r.Timestamp.UTC(),
			r.WorkspaceURI, r.ProjectID, r.Model, r.ProviderID,
			r.ResponseID, r.ProviderAssignedMessageID, r.MessageID,
			r.InputTokens, r.CacheReadTokens, r.CacheCreationTokens,
			r.VisibleOutputTokens, r.ReasoningTokens, r.TotalOutputTokens, r.TotalTokens,
			now,
		)
		if err != nil {
			return fmt.Errorf("upsert usage record %s: %w", r.GenerationID, err)
		}
	}

	manifest.FirstUsageTimestamp = firstTS
	manifest.LastUsageTimestamp = lastTS
	manifest.GenerationCount = int64(len(records))
	manifest.IngestedAt = now
	manifest.ParserVersion = ParserVersion

	isCompInt := 0
	if manifest.IsComplete {
		isCompInt = 1
	}

	if manifest.LastScanStatus == "" {
		manifest.LastScanStatus = manifest.Status
	}
	if manifest.LastScanAt == nil || manifest.LastScanAt.IsZero() {
		manifest.LastScanAt = &now
	}
	manifest.LastScanError = manifest.ErrorMessage

	var sha256Val sql.NullString
	if manifest.SourceSHA256 != nil && *manifest.SourceSHA256 != "" {
		sha256Val = sql.NullString{String: *manifest.SourceSHA256, Valid: true}
	}

	manifestStmt, err := tx.Prepare(`
		INSERT INTO ingest_manifests (
			conversation_id, source_path, source_size, source_mtime_ns, source_fingerprint,
			highest_step_index, highest_gen_index, generation_count,
			first_usage_timestamp, last_usage_timestamp, parser_version,
			ingested_at, is_complete, status, error_message,
			last_scan_at, last_scan_status, last_scan_error, source_sha256
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(conversation_id) DO UPDATE SET
			source_path = excluded.source_path,
			source_size = excluded.source_size,
			source_mtime_ns = excluded.source_mtime_ns,
			source_fingerprint = excluded.source_fingerprint,
			highest_step_index = MAX(ingest_manifests.highest_step_index, excluded.highest_step_index),
			highest_gen_index = MAX(ingest_manifests.highest_gen_index, excluded.highest_gen_index),
			generation_count = excluded.generation_count,
			first_usage_timestamp = excluded.first_usage_timestamp,
			last_usage_timestamp = excluded.last_usage_timestamp,
			parser_version = excluded.parser_version,
			ingested_at = excluded.ingested_at,
			is_complete = excluded.is_complete,
			status = excluded.status,
			error_message = excluded.error_message,
			last_scan_at = excluded.last_scan_at,
			last_scan_status = excluded.last_scan_status,
			last_scan_error = excluded.last_scan_error,
			source_sha256 = COALESCE(excluded.source_sha256, ingest_manifests.source_sha256)
	`)
	if err != nil {
		return fmt.Errorf("prepare manifest stmt: %w", err)
	}
	defer manifestStmt.Close()

	_, err = manifestStmt.Exec(
		manifest.ConversationID, manifest.SourcePath, manifest.SourceSize, manifest.SourceMtimeNs, manifest.SourceFingerprint,
		manifest.HighestStepIndex, manifest.HighestGenIndex, manifest.GenerationCount,
		manifest.FirstUsageTimestamp, manifest.LastUsageTimestamp, manifest.ParserVersion,
		manifest.IngestedAt, isCompInt, manifest.Status, manifest.ErrorMessage,
		manifest.LastScanAt, manifest.LastScanStatus, manifest.LastScanError, sha256Val,
	)
	if err != nil {
		return fmt.Errorf("upsert manifest %s: %w", manifest.ConversationID, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit ingest: %w", err)
	}
	tx = nil

	return nil
}

// RecordScanFailure records a failed read/decode attempt without destroying the last known-good ingest state.
func (l *Ledger) RecordScanFailure(cid string, sourcePath string, scanErr error) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now().UTC()
	errMsg := ""
	if scanErr != nil {
		errMsg = scanErr.Error()
	}

	var exists bool
	err := l.db.QueryRow("SELECT 1 FROM ingest_manifests WHERE conversation_id = ?", cid).Scan(&exists)
	if err == sql.ErrNoRows {
		_, insertErr := l.db.Exec(`
			INSERT INTO ingest_manifests (
				conversation_id, source_path, source_size, source_mtime_ns, source_fingerprint,
				highest_step_index, highest_gen_index, generation_count,
				parser_version, ingested_at, is_complete, status, error_message,
				last_scan_at, last_scan_status, last_scan_error
			) VALUES (?, ?, 0, 0, '', 0, 0, 0, ?, ?, 0, 'failed', ?, ?, 'failed', ?)
		`, cid, sourcePath, ParserVersion, now, errMsg, now, errMsg)
		return insertErr
	} else if err != nil {
		return err
	}

	_, updateErr := l.db.Exec(`
		UPDATE ingest_manifests
		SET last_scan_at = ?,
		    last_scan_status = 'failed',
		    last_scan_error = ?
		WHERE conversation_id = ?
	`, now, errMsg, cid)
	return updateErr
}

// GetCatalogSyncState returns the cached sync state for conversation_summaries.db.
func (l *Ledger) GetCatalogSyncState() (*CatalogSyncState, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var st CatalogSyncState
	err := l.db.QueryRow(`
		SELECT catalog_path, catalog_size, catalog_mtime_ns, conversation_count, synced_at
		FROM catalog_sync_state
		WHERE id = 1
	`).Scan(&st.CatalogPath, &st.CatalogSize, &st.CatalogMtimeNs, &st.ConversationCount, &st.SyncedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("query catalog_sync_state: %w", err)
	}
	return &st, nil
}

// SyncCatalog records conversation metadata and propagates project/workspace updates to usage_records.
func (l *Ledger) SyncCatalog(path string, size int64, mtimeNs int64, catalog map[string]*ConversationMeta) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	tx, err := l.db.Begin()
	if err != nil {
		return fmt.Errorf("begin sync catalog tx: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	now := time.Now().UTC()

	metaStmt, err := tx.Prepare(`
		INSERT INTO conversation_metadata (
			conversation_id, title, workspace_uri, workspace_uris,
			project_id, agent_name, last_modified_time, step_count, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(conversation_id) DO UPDATE SET
			title = excluded.title,
			workspace_uri = excluded.workspace_uri,
			workspace_uris = excluded.workspace_uris,
			project_id = excluded.project_id,
			agent_name = excluded.agent_name,
			last_modified_time = excluded.last_modified_time,
			step_count = excluded.step_count,
			updated_at = excluded.updated_at
	`)
	if err != nil {
		return fmt.Errorf("prepare meta stmt: %w", err)
	}
	defer metaStmt.Close()

	updateRecordStmt, err := tx.Prepare(`
		UPDATE usage_records
		SET project_id = ?,
		    workspace_uri = ?
		WHERE conversation_id = ?
		  AND (project_id != ? OR workspace_uri != ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare update usage records stmt: %w", err)
	}
	defer updateRecordStmt.Close()

	for cid, meta := range catalog {
		var lastMod *time.Time
		if !meta.LastModified.IsZero() {
			t := meta.LastModified.UTC()
			lastMod = &t
		}

		_, err := metaStmt.Exec(
			cid, meta.Title, meta.WorkspaceURI, meta.WorkspaceURIs,
			meta.ProjectID, meta.AgentName, lastMod, meta.StepCount, now,
		)
		if err != nil {
			return fmt.Errorf("upsert meta for %s: %w", cid, err)
		}

		projID := meta.ProjectID
		if projID == "" {
			projID = "unknown"
		}
		_, err = updateRecordStmt.Exec(projID, meta.WorkspaceURI, cid, projID, meta.WorkspaceURI)
		if err != nil {
			return fmt.Errorf("update usage records for %s: %w", cid, err)
		}
	}

	_, err = tx.Exec(`
		INSERT INTO catalog_sync_state (id, catalog_path, catalog_size, catalog_mtime_ns, conversation_count, synced_at)
		VALUES (1, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			catalog_path = excluded.catalog_path,
			catalog_size = excluded.catalog_size,
			catalog_mtime_ns = excluded.catalog_mtime_ns,
			conversation_count = excluded.conversation_count,
			synced_at = excluded.synced_at
	`, path, size, mtimeNs, len(catalog), now)
	if err != nil {
		return fmt.Errorf("upsert catalog sync state: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit catalog sync: %w", err)
	}
	tx = nil
	return nil
}

// GetAllConversationMetadata retrieves all persisted conversation metadata from usage.db.
func (l *Ledger) GetAllConversationMetadata() (map[string]*ConversationMeta, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	rows, err := l.db.Query(`
		SELECT conversation_id, title, workspace_uri, workspace_uris, project_id, agent_name, last_modified_time, step_count, updated_at
		FROM conversation_metadata
	`)
	if err != nil {
		return nil, fmt.Errorf("query conversation_metadata: %w", err)
	}
	defer rows.Close()

	result := make(map[string]*ConversationMeta)
	for rows.Next() {
		var m ConversationMeta
		var lastMod sql.NullTime
		if err := rows.Scan(
			&m.ConversationID, &m.Title, &m.WorkspaceURI, &m.WorkspaceURIs,
			&m.ProjectID, &m.AgentName, &lastMod, &m.StepCount, &m.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan conversation_metadata: %w", err)
		}
		if lastMod.Valid {
			m.LastModified = lastMod.Time.UTC()
		}
		result[m.ConversationID] = &m
	}
	return result, nil
}

// StatusReport provides a health and progress overview of the usage ledger.
type StatusReport struct {
	LedgerPath              string `json:"ledger_path"`
	LedgerSize              int64  `json:"ledger_size"`
	DiscoveredConversations int    `json:"discovered_conversations"`
	IndexedConversations    int    `json:"indexed_conversations"`
	CompletedConversations  int    `json:"completed_conversations"`
	ActiveConversations     int    `json:"active_conversations"`
	FailedConversations     int    `json:"failed_conversations"`
	FailedScans             int    `json:"failed_scans"`
	MetadataCount           int    `json:"metadata_count"`
	TotalGenerations        int64  `json:"total_generations"`
	TotalTokens             uint64 `json:"total_tokens"`
}

// GetStatus calculates the current ledger state.
func (l *Ledger) GetStatus(discoveredCount int) (*StatusReport, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	rep := &StatusReport{
		LedgerPath:              l.path,
		LedgerSize:              l.Size(),
		DiscoveredConversations: discoveredCount,
	}

	row := l.db.QueryRow(`
		SELECT COUNT(*),
		       COALESCE(SUM(CASE WHEN is_complete = 1 THEN 1 ELSE 0 END), 0),
		       COALESCE(SUM(CASE WHEN is_complete = 0 AND status = 'active' THEN 1 ELSE 0 END), 0),
		       COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0),
		       COALESCE(SUM(CASE WHEN last_scan_status = 'failed' THEN 1 ELSE 0 END), 0)
		FROM ingest_manifests
	`)
	if err := row.Scan(&rep.IndexedConversations, &rep.CompletedConversations, &rep.ActiveConversations, &rep.FailedConversations, &rep.FailedScans); err != nil {
		return nil, fmt.Errorf("scan status counts: %w", err)
	}

	_ = l.db.QueryRow(`SELECT COUNT(*) FROM conversation_metadata`).Scan(&rep.MetadataCount)

	rowGen := l.db.QueryRow(`SELECT COUNT(*), COALESCE(SUM(total_tokens), 0) FROM usage_records`)
	if err := rowGen.Scan(&rep.TotalGenerations, &rep.TotalTokens); err != nil {
		return nil, fmt.Errorf("scan total generations: %w", err)
	}

	return rep, nil
}

// VerifyReport holds consistency and invariant verification results.
type VerifyReport struct {
	Valid                   bool     `json:"valid"`
	SchemaVersion           int      `json:"schema_version"`
	TotalRecords            int64    `json:"total_records"`
	TotalManifests          int64    `json:"total_manifests"`
	TotalMetadata           int64    `json:"total_metadata"`
	DiscrepantOutputTokens  int64    `json:"discrepant_output_tokens"`
	DiscrepantTotalTokens   int64    `json:"discrepant_total_tokens"`
	DuplicateGenerations    int64    `json:"duplicate_generations"`
	MismatchedManifestCount int64    `json:"mismatched_manifest_count"`
	Errors                  []string `json:"errors"`
}

// Verify executes schema integrity and invariant validation across all stored data.
func (l *Ledger) Verify() (*VerifyReport, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	rep := &VerifyReport{Valid: true, SchemaVersion: CurrentLedgerSchemaVersion}

	// 1. Quick integrity check
	var integrity string
	if err := l.db.QueryRow("PRAGMA quick_check;").Scan(&integrity); err != nil || integrity != "ok" {
		rep.Valid = false
		rep.Errors = append(rep.Errors, fmt.Sprintf("SQLite quick_check failed: %s (err: %v)", integrity, err))
	}

	// 2. Count records, manifests, metadata
	_ = l.db.QueryRow("SELECT COUNT(*) FROM usage_records").Scan(&rep.TotalRecords)
	_ = l.db.QueryRow("SELECT COUNT(*) FROM ingest_manifests").Scan(&rep.TotalManifests)
	_ = l.db.QueryRow("SELECT COUNT(*) FROM conversation_metadata").Scan(&rep.TotalMetadata)

	// 3. Invariant check: TotalOutputTokens >= known VisibleOutputTokens + ReasoningTokens
	rowOut := l.db.QueryRow(`
		SELECT COUNT(*) FROM usage_records
		WHERE total_output_tokens < (visible_output_tokens + reasoning_tokens)
	`)
	_ = rowOut.Scan(&rep.DiscrepantOutputTokens)
	if rep.DiscrepantOutputTokens > 0 {
		rep.Valid = false
		rep.Errors = append(rep.Errors, fmt.Sprintf("%d records have total_output_tokens below visible + reasoning", rep.DiscrepantOutputTokens))
	}

	// 4. Invariant check: TotalTokens == InputTokens + CacheReadTokens + TotalOutputTokens
	rowTot := l.db.QueryRow(`
		SELECT COUNT(*) FROM usage_records
		WHERE total_tokens != (input_tokens + cache_read_tokens + total_output_tokens)
	`)
	_ = rowTot.Scan(&rep.DiscrepantTotalTokens)
	if rep.DiscrepantTotalTokens > 0 {
		rep.Valid = false
		rep.Errors = append(rep.Errors, fmt.Sprintf("%d records have inconsistent total_tokens != input + cache_read + total_output", rep.DiscrepantTotalTokens))
	}

	// 5. Uniqueness of generation_id
	rowDup := l.db.QueryRow(`
		SELECT COUNT(*) FROM (
			SELECT generation_id FROM usage_records GROUP BY generation_id HAVING COUNT(*) > 1
		)
	`)
	_ = rowDup.Scan(&rep.DuplicateGenerations)
	if rep.DuplicateGenerations > 0 {
		rep.Valid = false
		rep.Errors = append(rep.Errors, fmt.Sprintf("%d duplicate generation_id records detected", rep.DuplicateGenerations))
	}

	// 6. Invariant check: manifest generation_count matches usage_records count for completed manifests
	rowMismatch := l.db.QueryRow(`
		SELECT COUNT(*) FROM ingest_manifests m
		WHERE m.status = 'completed' AND m.generation_count != (
			SELECT COUNT(*) FROM usage_records u WHERE u.conversation_id = m.conversation_id
		)
	`)
	_ = rowMismatch.Scan(&rep.MismatchedManifestCount)
	if rep.MismatchedManifestCount > 0 {
		rep.Valid = false
		rep.Errors = append(rep.Errors, fmt.Sprintf("%d completed manifests have mismatched generation counts against usage records", rep.MismatchedManifestCount))
	}

	return rep, nil
}

// TokenTotals aggregates input, cache, and output tokens.
type TokenTotals struct {
	InputTokens         uint64 `json:"input_tokens"`
	CacheReadTokens     uint64 `json:"cache_read_tokens"`
	CacheCreationTokens uint64 `json:"cache_creation_tokens"`
	VisibleOutputTokens uint64 `json:"visible_output_tokens"`
	ReasoningTokens     uint64 `json:"reasoning_tokens"`
	TotalOutputTokens   uint64 `json:"total_output_tokens"`
	TotalTokens         uint64 `json:"total_tokens"`
	GenerationCount     int64  `json:"generation_count"`
}

// SummaryReport provides aggregate totals by overall dataset, model, and project.
type SummaryReport struct {
	Overall       TokenTotals            `json:"overall"`
	ByModel       map[string]TokenTotals `json:"by_model"`
	ByProject     map[string]TokenTotals `json:"by_project"`
	Conversations int                    `json:"conversations"`
}

// GetSummary returns token totals aggregated across all usage records.
func (l *Ledger) GetSummary() (*SummaryReport, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	rep := &SummaryReport{
		ByModel:   make(map[string]TokenTotals),
		ByProject: make(map[string]TokenTotals),
	}

	// Distinct conversations
	_ = l.db.QueryRow("SELECT COUNT(DISTINCT conversation_id) FROM usage_records").Scan(&rep.Conversations)

	// Overall totals
	row := l.db.QueryRow(`
		SELECT
			COUNT(*),
			COALESCE(SUM(input_tokens), 0),
			COALESCE(SUM(cache_read_tokens), 0),
			COALESCE(SUM(cache_creation_tokens), 0),
			COALESCE(SUM(visible_output_tokens), 0),
			COALESCE(SUM(reasoning_tokens), 0),
			COALESCE(SUM(total_output_tokens), 0),
			COALESCE(SUM(total_tokens), 0)
		FROM usage_records
	`)
	if err := row.Scan(
		&rep.Overall.GenerationCount,
		&rep.Overall.InputTokens,
		&rep.Overall.CacheReadTokens,
		&rep.Overall.CacheCreationTokens,
		&rep.Overall.VisibleOutputTokens,
		&rep.Overall.ReasoningTokens,
		&rep.Overall.TotalOutputTokens,
		&rep.Overall.TotalTokens,
	); err != nil {
		return nil, fmt.Errorf("query overall summary: %w", err)
	}

	// Group by model
	mRows, err := l.db.Query(`
		SELECT
			model,
			COUNT(*),
			COALESCE(SUM(input_tokens), 0),
			COALESCE(SUM(cache_read_tokens), 0),
			COALESCE(SUM(cache_creation_tokens), 0),
			COALESCE(SUM(visible_output_tokens), 0),
			COALESCE(SUM(reasoning_tokens), 0),
			COALESCE(SUM(total_output_tokens), 0),
			COALESCE(SUM(total_tokens), 0)
		FROM usage_records
		GROUP BY model
	`)
	if err == nil {
		defer mRows.Close()
		for mRows.Next() {
			var m string
			var t TokenTotals
			if err := mRows.Scan(
				&m, &t.GenerationCount, &t.InputTokens, &t.CacheReadTokens, &t.CacheCreationTokens,
				&t.VisibleOutputTokens, &t.ReasoningTokens, &t.TotalOutputTokens, &t.TotalTokens,
			); err == nil {
				rep.ByModel[m] = t
			}
		}
	}

	// Group by project
	pRows, err := l.db.Query(`
		SELECT
			project_id,
			COUNT(*),
			COALESCE(SUM(input_tokens), 0),
			COALESCE(SUM(cache_read_tokens), 0),
			COALESCE(SUM(cache_creation_tokens), 0),
			COALESCE(SUM(visible_output_tokens), 0),
			COALESCE(SUM(reasoning_tokens), 0),
			COALESCE(SUM(total_output_tokens), 0),
			COALESCE(SUM(total_tokens), 0)
		FROM usage_records
		GROUP BY project_id
	`)
	if err == nil {
		defer pRows.Close()
		for pRows.Next() {
			var p string
			var t TokenTotals
			if err := pRows.Scan(
				&p, &t.GenerationCount, &t.InputTokens, &t.CacheReadTokens, &t.CacheCreationTokens,
				&t.VisibleOutputTokens, &t.ReasoningTokens, &t.TotalOutputTokens, &t.TotalTokens,
			); err == nil {
				rep.ByProject[p] = t
			}
		}
	}

	return rep, nil
}
