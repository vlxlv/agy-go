package tokei

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestLedger_IdempotentCommitAndManifest(t *testing.T) {
	tempDir := t.TempDir()
	ledgerPath := filepath.Join(tempDir, "test_usage.db")

	ledger, err := OpenLedger(ledgerPath)
	if err != nil {
		t.Fatalf("OpenLedger failed: %v", err)
	}
	defer ledger.Close()

	convID := "conv-test-idempotent"
	now := time.Now().UTC().Truncate(time.Millisecond)

	rec1 := &UsageRecord{
		ConversationID:      convID,
		GenerationID:        "response:resp-100",
		StepIndex:           1,
		Timestamp:           now,
		WorkspaceURI:        "file:///work/test",
		ProjectID:           "test-proj",
		Model:               "gemini-3.8-flash",
		InputTokens:         1000,
		CacheReadTokens:     2000,
		VisibleOutputTokens: 100,
		ReasoningTokens:     200,
		TotalOutputTokens:   300,
		TotalTokens:         3300,
	}

	manifest := &SourceManifest{
		ConversationID:    convID,
		SourcePath:        "/dummy/path.db",
		SourceSize:        10240,
		SourceMtimeNs:     now.UnixNano(),
		SourceFingerprint: "size=10240",
		HighestStepIndex:  1,
		IsComplete:        true,
		Status:            "completed",
	}

	// First commit
	if err := ledger.CommitIngest(manifest, []*UsageRecord{rec1}); err != nil {
		t.Fatalf("first CommitIngest failed: %v", err)
	}

	// Status check
	st, err := ledger.GetStatus(1)
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if st.TotalGenerations != 1 {
		t.Errorf("expected 1 generation, got %d", st.TotalGenerations)
	}
	if st.TotalTokens != 3300 {
		t.Errorf("expected 3300 tokens, got %d", st.TotalTokens)
	}

	// Second commit with updated token counts on same generation ID (idempotent upsert)
	rec1Updated := &UsageRecord{
		ConversationID:      convID,
		GenerationID:        "response:resp-100",
		StepIndex:           1,
		Timestamp:           now,
		WorkspaceURI:        "file:///work/test",
		ProjectID:           "test-proj",
		Model:               "gemini-3.8-flash",
		InputTokens:         1200, // increased
		CacheReadTokens:     2000,
		VisibleOutputTokens: 150,
		ReasoningTokens:     200,
		TotalOutputTokens:   350,
		TotalTokens:         3550,
	}

	if err := ledger.CommitIngest(manifest, []*UsageRecord{rec1Updated}); err != nil {
		t.Fatalf("second CommitIngest failed: %v", err)
	}

	st2, err := ledger.GetStatus(1)
	if err != nil {
		t.Fatalf("second GetStatus failed: %v", err)
	}
	// Total generations count MUST still be 1 (deduplicated)
	if st2.TotalGenerations != 1 {
		t.Errorf("expected still 1 generation after re-ingest, got %d", st2.TotalGenerations)
	}
	if st2.TotalTokens != 3550 {
		t.Errorf("expected updated 3550 total tokens, got %d", st2.TotalTokens)
	}

	// Verify check
	vReport, err := ledger.Verify()
	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}
	if !vReport.Valid {
		t.Errorf("expected ledger to be valid, got errors: %v", vReport.Errors)
	}
	if vReport.DuplicateGenerations > 0 {
		t.Errorf("expected 0 duplicate generations, got %d", vReport.DuplicateGenerations)
	}

	// Summary check
	sum, err := ledger.GetSummary()
	if err != nil {
		t.Fatalf("GetSummary failed: %v", err)
	}
	if sum.Overall.GenerationCount != 1 {
		t.Errorf("expected overall gen count 1, got %d", sum.Overall.GenerationCount)
	}
	if sum.Overall.InputTokens != 1200 {
		t.Errorf("expected input tokens 1200, got %d", sum.Overall.InputTokens)
	}
	if sum.Overall.TotalTokens != 3550 {
		t.Errorf("expected total tokens 3550, got %d", sum.Overall.TotalTokens)
	}
	if sum.ByModel["gemini-3.8-flash"].TotalTokens != 3550 {
		t.Errorf("expected model breakdown tokens 3550, got %d", sum.ByModel["gemini-3.8-flash"].TotalTokens)
	}
	if sum.ByProject["test-proj"].TotalTokens != 3550 {
		t.Errorf("expected project breakdown tokens 3550, got %d", sum.ByProject["test-proj"].TotalTokens)
	}
}

func TestLedger_TransientFailurePreservesManifest(t *testing.T) {
	tempDir := t.TempDir()
	ledgerPath := filepath.Join(tempDir, "test_transient.db")

	ledger, err := OpenLedger(ledgerPath)
	if err != nil {
		t.Fatalf("OpenLedger failed: %v", err)
	}
	defer ledger.Close()

	convID := "conv-test-transient"
	now := time.Now().UTC().Truncate(time.Millisecond)

	rec := &UsageRecord{
		ConversationID:      convID,
		GenerationID:        "response:resp-ok",
		StepIndex:           1,
		Timestamp:           now,
		WorkspaceURI:        "file:///work/proj",
		ProjectID:           "proj-alpha",
		Model:               "gemini-3.8-flash",
		InputTokens:         1000,
		CacheReadTokens:     500,
		VisibleOutputTokens: 200,
		ReasoningTokens:     300,
		TotalOutputTokens:   500,
		TotalTokens:         2000,
	}

	manifest := &SourceManifest{
		ConversationID:    convID,
		SourcePath:        "/path/to/conv.db",
		SourceSize:        50000,
		SourceMtimeNs:     now.UnixNano(),
		SourceFingerprint: "size=50000:mtime=1",
		HighestStepIndex:  1,
		HighestGenIndex:   1,
		IsComplete:        true,
		Status:            "completed",
	}

	// 1. Successful initial ingest
	if err := ledger.CommitIngest(manifest, []*UsageRecord{rec}); err != nil {
		t.Fatalf("initial CommitIngest failed: %v", err)
	}

	// Verify last known-good manifest
	manifests, err := ledger.GetManifests()
	if err != nil {
		t.Fatalf("GetManifests failed: %v", err)
	}
	m1 := manifests[convID]
	if m1 == nil || !m1.IsComplete || m1.Status != "completed" || m1.GenerationCount != 1 {
		t.Fatalf("expected completed manifest with 1 generation, got %+v", m1)
	}

	// 2. Transient read failure occurs on next scan
	scanErr := fmt.Errorf("transient i/o error: disk busy")
	if err := ledger.RecordScanFailure(convID, "/path/to/conv.db", scanErr); err != nil {
		t.Fatalf("RecordScanFailure failed: %v", err)
	}

	// Verify that last known-good manifest is preserved!
	manifestsAfterFail, err := ledger.GetManifests()
	if err != nil {
		t.Fatalf("GetManifests after fail: %v", err)
	}
	m2 := manifestsAfterFail[convID]
	if m2 == nil {
		t.Fatalf("manifest missing after scan failure")
	}
	if !m2.IsComplete {
		t.Errorf("expected IsComplete to remain true, got false")
	}
	if m2.Status != "completed" {
		t.Errorf("expected Status to remain 'completed', got %s", m2.Status)
	}
	if m2.GenerationCount != 1 {
		t.Errorf("expected GenerationCount to remain 1, got %d", m2.GenerationCount)
	}
	if m2.LastScanStatus != "failed" {
		t.Errorf("expected LastScanStatus to be 'failed', got %s", m2.LastScanStatus)
	}
	if m2.LastScanError != "transient i/o error: disk busy" {
		t.Errorf("expected LastScanError to match, got %s", m2.LastScanError)
	}

	// Verify status reports
	st, err := ledger.GetStatus(1)
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if st.CompletedConversations != 1 {
		t.Errorf("expected 1 completed conversation, got %d", st.CompletedConversations)
	}
	if st.FailedScans != 1 {
		t.Errorf("expected 1 failed scan, got %d", st.FailedScans)
	}
	if st.TotalGenerations != 1 {
		t.Errorf("expected 1 total generation preserved, got %d", st.TotalGenerations)
	}

	// Verify invariant check still passes
	vReport, err := ledger.Verify()
	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}
	if !vReport.Valid {
		t.Errorf("expected ledger to remain valid, got errors: %v", vReport.Errors)
	}

	// 3. Recovery on a subsequent successful scan
	recRecovered := &UsageRecord{
		ConversationID:      convID,
		GenerationID:        "response:resp-ok",
		StepIndex:           1,
		Timestamp:           now,
		WorkspaceURI:        "file:///work/proj",
		ProjectID:           "proj-alpha",
		Model:               "gemini-3.8-flash",
		InputTokens:         1000,
		CacheReadTokens:     500,
		VisibleOutputTokens: 200,
		ReasoningTokens:     300,
		TotalOutputTokens:   500,
		TotalTokens:         2000,
	}
	if err := ledger.CommitIngest(manifest, []*UsageRecord{recRecovered}); err != nil {
		t.Fatalf("subsequent CommitIngest failed: %v", err)
	}

	manifestsRecovered, err := ledger.GetManifests()
	if err != nil {
		t.Fatalf("GetManifests after recovery: %v", err)
	}
	m3 := manifestsRecovered[convID]
	if m3.LastScanStatus != "completed" {
		t.Errorf("expected LastScanStatus to recover to 'completed', got %s", m3.LastScanStatus)
	}
	if m3.LastScanError != "" {
		t.Errorf("expected LastScanError to be cleared, got %s", m3.LastScanError)
	}

	stRecovered, err := ledger.GetStatus(1)
	if err != nil {
		t.Fatalf("GetStatus after recovery: %v", err)
	}
	if stRecovered.FailedScans != 0 {
		t.Errorf("expected 0 failed scans after recovery, got %d", stRecovered.FailedScans)
	}
}

func TestLedger_SchemaMigrationV1ToV2(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "v1_legacy.db")

	// 1. Manually create V1 schema
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	v1Schema := `
	CREATE TABLE schema_migrations (
		version INTEGER PRIMARY KEY,
		applied_at DATETIME NOT NULL
	);

	CREATE TABLE usage_records (
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

	CREATE TABLE ingest_manifests (
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

	INSERT INTO schema_migrations (version, applied_at) VALUES (1, '2026-09-20T10:00:00Z');

	INSERT INTO usage_records (
		conversation_id, generation_id, step_index, timestamp,
		workspace_uri, project_id, model, input_tokens, visible_output_tokens, total_output_tokens, total_tokens, created_at
	) VALUES ('v1-conv', 'gen-100', 1, '2026-09-20T10:00:00Z', '/work', 'proj-1', 'gemini-3.8-flash', 100, 50, 50, 150, '2026-09-20T10:00:00Z');

	INSERT INTO ingest_manifests (
		conversation_id, source_path, source_size, source_mtime_ns, source_fingerprint,
		generation_count, parser_version, ingested_at, is_complete, status
	) VALUES ('v1-conv', '/path/v1.db', 1000, 1000, 'fp1', 1, '1.0.0', '2026-09-20T10:00:00Z', 1, 'completed');
	`
	if _, err := db.Exec(v1Schema); err != nil {
		t.Fatalf("init v1 db failed: %v", err)
	}
	db.Close()

	// 2. Open with OpenLedger which triggers migrate() to V2
	ledger, err := OpenLedger(dbPath)
	if err != nil {
		t.Fatalf("OpenLedger on v1 db failed: %v", err)
	}
	defer ledger.Close()

	// 3. Verify schema version is now 2
	var v2Version int
	if err := ledger.db.QueryRow("SELECT MAX(version) FROM schema_migrations").Scan(&v2Version); err != nil {
		t.Fatalf("query version failed: %v", err)
	}
	if v2Version != 2 {
		t.Errorf("expected schema version 2, got %d", v2Version)
	}

	// 4. Verify that existing manifest has backfilled fields
	manifests, err := ledger.GetManifests()
	if err != nil {
		t.Fatalf("GetManifests failed: %v", err)
	}
	m := manifests["v1-conv"]
	if m == nil {
		t.Fatalf("manifest v1-conv missing after migration")
	}
	if m.LastScanStatus != "completed" {
		t.Errorf("expected backfilled LastScanStatus 'completed', got %s", m.LastScanStatus)
	}
	if m.LastScanAt == nil {
		t.Errorf("expected backfilled LastScanAt to be non-nil")
	}

	// 5. Verify records and verification pass
	vReport, err := ledger.Verify()
	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}
	if !vReport.Valid {
		t.Errorf("expected valid ledger after migration, got: %v", vReport.Errors)
	}
	if vReport.TotalRecords != 1 {
		t.Errorf("expected 1 record preserved, got %d", vReport.TotalRecords)
	}
}

func TestLedgerSnapshotCorrectionAndRollback(t *testing.T) {
	l, err := OpenLedger(filepath.Join(t.TempDir(), "usage.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	m := &SourceManifest{ConversationID: "c", IsComplete: true, Status: "completed"}
	a := &UsageRecord{ConversationID: "c", GenerationID: "message:m", InputTokens: 100, VisibleOutputTokens: 10}
	if err := l.CommitIngest(m, []*UsageRecord{a}); err != nil {
		t.Fatal(err)
	}
	// Identity upgrade and downward correction must replace the old observation.
	b := &UsageRecord{ConversationID: "c", GenerationID: "response:r", InputTokens: 10, VisibleOutputTokens: 100}
	if err := l.CommitIngest(m, []*UsageRecord{b}); err != nil {
		t.Fatal(err)
	}
	sum, err := l.GetSummary()
	if err != nil {
		t.Fatal(err)
	}
	if sum.Overall.GenerationCount != 1 || sum.Overall.InputTokens != 10 || sum.Overall.TotalTokens != 110 {
		t.Fatalf("incorrect corrected snapshot: %+v", sum.Overall)
	}
	// A failed replacement must retain the last successful snapshot.
	if err := l.CommitIngest(m, []*UsageRecord{{ConversationID: "other", GenerationID: "bad"}}); err == nil {
		t.Fatal("accepted foreign record")
	}
	sum, err = l.GetSummary()
	if err != nil || sum.Overall.TotalTokens != 110 {
		t.Fatalf("rollback failed: %v %+v", err, sum)
	}
	if err := l.CommitIngest(m, nil); err != nil {
		t.Fatal(err)
	}
	sum, err = l.GetSummary()
	if err != nil || sum.Overall.GenerationCount != 0 {
		t.Fatalf("deleted generation retained: %v %+v", err, sum)
	}
}

func TestLedgerUnknownOutputBreakdown(t *testing.T) {
	l, err := OpenLedger(filepath.Join(t.TempDir(), "usage.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	r := &UsageRecord{ConversationID: "c", GenerationID: "r", TotalOutputTokens: 20}
	if err := l.CommitIngest(&SourceManifest{ConversationID: "c", IsComplete: true, Status: "completed"}, []*UsageRecord{r}); err != nil {
		t.Fatal(err)
	}
	report, err := l.Verify()
	if err != nil || !report.Valid {
		t.Fatalf("unknown breakdown rejected: %v %+v", err, report)
	}
	sum, err := l.GetSummary()
	if err != nil {
		t.Fatal(err)
	}
	if sum.Overall.VisibleOutputTokens != 0 || sum.Overall.ReasoningTokens != 0 || sum.Overall.TotalTokens != 20 {
		t.Fatal("invented output breakdown")
	}
}
