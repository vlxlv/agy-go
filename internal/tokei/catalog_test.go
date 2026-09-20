package tokei

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// Helper to create a test conversation_summaries.db
func createTestSummariesDB(t *testing.T, dir string, entries []*ConversationMeta) string {
	sumPath := filepath.Join(dir, "conversation_summaries.db")
	db, err := sql.Open("sqlite", sumPath)
	if err != nil {
		t.Fatalf("open test summaries db: %v", err)
	}
	defer db.Close()

	schema := `
	CREATE TABLE conversation_summaries (
		conversation_id TEXT PRIMARY KEY,
		title TEXT,
		workspace_uris TEXT,
		project_id TEXT,
		agent_name TEXT,
		last_modified_time TEXT,
		step_count INTEGER
	);
	`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create summaries schema: %v", err)
	}

	for _, e := range entries {
		wsJSON := `["` + e.WorkspaceURI + `"]`
		if e.WorkspaceURIs != "" {
			wsJSON = e.WorkspaceURIs
		}
		lastMod := e.LastModified.Format(time.RFC3339)
		if _, err := db.Exec(`
			INSERT INTO conversation_summaries (
				conversation_id, title, workspace_uris, project_id, agent_name, last_modified_time, step_count
			) VALUES (?, ?, ?, ?, ?, ?, ?)
		`, e.ConversationID, e.Title, wsJSON, e.ProjectID, e.AgentName, lastMod, e.StepCount); err != nil {
			t.Fatalf("insert summary entry: %v", err)
		}
	}

	return sumPath
}

func updateSummaryEntry(t *testing.T, sumPath string, cid, title, wsURI, projID, agent string, stepCount int64) {
	db, err := sql.Open("sqlite", sumPath)
	if err != nil {
		t.Fatalf("open summaries db for update: %v", err)
	}
	defer db.Close()

	wsJSON := `["` + wsURI + `"]`
	lastMod := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(`
		UPDATE conversation_summaries
		SET title = ?, workspace_uris = ?, project_id = ?, agent_name = ?, last_modified_time = ?, step_count = ?
		WHERE conversation_id = ?
	`, title, wsJSON, projID, agent, lastMod, stepCount, cid); err != nil {
		t.Fatalf("update summary entry: %v", err)
	}

	// Update mtime on the file so stat detects the change
	newTime := time.Now().Add(time.Second)
	_ = os.Chtimes(sumPath, newTime, newTime)
}

func TestCatalogChangeDetection(t *testing.T) {
	tempDir := t.TempDir()
	convDir := filepath.Join(tempDir, "conversations")
	if err := os.MkdirAll(convDir, 0700); err != nil {
		t.Fatalf("mkdir conversations: %v", err)
	}

	cid := "test-conv-catalog-1"
	u := &rawUsage{
		inputTokens:         1000,
		visibleOutputTokens: 200,
		reasoningTokens:     300,
		totalOutputTokens:   500,
		responseID:          "resp-1",
	}
	stepBlob := buildStepMetadata(time.Now(), time.Now(), "gemini-3.8-flash", 1318, u, nil)
	createTestConversationDB(t, convDir, cid, [][]byte{stepBlob}, nil)

	sumPath := createTestSummariesDB(t, tempDir, []*ConversationMeta{
		{
			ConversationID: cid,
			Title:          "Initial Title",
			WorkspaceURI:   "file:///work/init",
			ProjectID:      "proj-alpha",
			AgentName:      "antigravity",
			LastModified:   time.Now().UTC(),
			StepCount:      1,
		},
	})

	ledgerPath := filepath.Join(tempDir, "usage.db")
	svc := NewService(ServiceOptions{
		LedgerPath:       ledgerPath,
		ConversationsDir: convDir,
		SummariesPath:    sumPath,
	})

	// 1. Initial Ingest
	stats1, err := svc.Ingest()
	if err != nil {
		t.Fatalf("initial ingest failed: %v", err)
	}
	if stats1.IngestedCount != 1 {
		t.Errorf("expected 1 ingested, got %d", stats1.IngestedCount)
	}

	ledger, err := OpenLedger(ledgerPath)
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	defer ledger.Close()

	metaMap, err := ledger.GetAllConversationMetadata()
	if err != nil {
		t.Fatalf("get all meta: %v", err)
	}
	if metaMap[cid].Title != "Initial Title" || metaMap[cid].ProjectID != "proj-alpha" || metaMap[cid].WorkspaceURI != "file:///work/init" {
		t.Fatalf("unexpected initial metadata: %+v", metaMap[cid])
	}

	// 2. Unchanged catalog fast-path
	statsFast, err := svc.Ingest()
	if err != nil {
		t.Fatalf("fast ingest failed: %v", err)
	}
	if statsFast.AlreadyUpToDate != 1 {
		t.Errorf("expected 1 already up to date, got %d", statsFast.AlreadyUpToDate)
	}
	if statsFast.IngestedCount != 0 {
		t.Errorf("expected 0 ingested on fast path, got %d", statsFast.IngestedCount)
	}

	// 3. Title-only update in catalog
	updateSummaryEntry(t, sumPath, cid, "Updated Title Only", "file:///work/init", "proj-alpha", "antigravity", 1)
	statsTitle, err := svc.Ingest()
	if err != nil {
		t.Fatalf("title update ingest failed: %v", err)
	}
	// The conversation DB did not change, so it must NOT be re-decoded: AlreadyUpToDate must be 1!
	if statsTitle.AlreadyUpToDate != 1 {
		t.Errorf("expected 1 already up to date on title update, got %d", statsTitle.AlreadyUpToDate)
	}
	metaMap2, _ := ledger.GetAllConversationMetadata()
	if metaMap2[cid].Title != "Updated Title Only" {
		t.Errorf("expected title 'Updated Title Only', got %s", metaMap2[cid].Title)
	}
	// Project and workspace records in usage_records must remain intact
	var recProj string
	_ = ledger.db.QueryRow("SELECT project_id FROM usage_records WHERE conversation_id = ?", cid).Scan(&recProj)
	if recProj != "proj-alpha" {
		t.Errorf("expected project_id proj-alpha in usage_records, got %s", recProj)
	}

	// 4. Project_id-only update in catalog
	updateSummaryEntry(t, sumPath, cid, "Updated Title Only", "file:///work/init", "proj-beta", "antigravity", 1)
	statsProj, err := svc.Ingest()
	if err != nil {
		t.Fatalf("project update ingest failed: %v", err)
	}
	// The conversation DB did not change: AlreadyUpToDate must be 1
	if statsProj.AlreadyUpToDate != 1 {
		t.Errorf("expected 1 already up to date on project update, got %d", statsProj.AlreadyUpToDate)
	}
	metaMap3, _ := ledger.GetAllConversationMetadata()
	if metaMap3[cid].ProjectID != "proj-beta" {
		t.Errorf("expected project_id 'proj-beta', got %s", metaMap3[cid].ProjectID)
	}
	// usage_records must be updated to proj-beta directly!
	_ = ledger.db.QueryRow("SELECT project_id FROM usage_records WHERE conversation_id = ?", cid).Scan(&recProj)
	if recProj != "proj-beta" {
		t.Errorf("expected updated project_id proj-beta in usage_records, got %s", recProj)
	}

	// 5. Workspace-only update in catalog
	updateSummaryEntry(t, sumPath, cid, "Updated Title Only", "file:///work/new-workspace", "proj-beta", "antigravity", 1)
	statsWS, err := svc.Ingest()
	if err != nil {
		t.Fatalf("workspace update ingest failed: %v", err)
	}
	if statsWS.AlreadyUpToDate != 1 {
		t.Errorf("expected 1 already up to date on workspace update, got %d", statsWS.AlreadyUpToDate)
	}
	metaMap4, _ := ledger.GetAllConversationMetadata()
	if metaMap4[cid].WorkspaceURI != "file:///work/new-workspace" {
		t.Errorf("expected workspace_uri 'file:///work/new-workspace', got %s", metaMap4[cid].WorkspaceURI)
	}
	var recWS string
	_ = ledger.db.QueryRow("SELECT workspace_uri FROM usage_records WHERE conversation_id = ?", cid).Scan(&recWS)
	if recWS != "file:///work/new-workspace" {
		t.Errorf("expected updated workspace_uri in usage_records, got %s", recWS)
	}
}

func TestService_MalformedSourceAttempt(t *testing.T) {
	tempDir := t.TempDir()
	convDir := filepath.Join(tempDir, "conversations")
	if err := os.MkdirAll(convDir, 0700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	cid := "test-conv-malformed"
	u := &rawUsage{inputTokens: 500, visibleOutputTokens: 100, totalOutputTokens: 100, responseID: "resp-good"}
	stepBlob := buildStepMetadata(time.Now(), time.Now(), "gemini-3.8-flash", 1318, u, nil)
	dbPath := createTestConversationDB(t, convDir, cid, [][]byte{stepBlob}, nil)

	ledgerPath := filepath.Join(tempDir, "usage.db")
	svc := NewService(ServiceOptions{
		LedgerPath:       ledgerPath,
		ConversationsDir: convDir,
	})

	// 1. Ingest valid database
	stats1, err := svc.Ingest()
	if err != nil {
		t.Fatalf("initial ingest failed: %v", err)
	}
	if stats1.IngestedCount != 1 {
		t.Fatalf("expected 1 ingested, got %d", stats1.IngestedCount)
	}

	// 2. Corrupt source file (simulate malformed source attempt)
	if err := os.WriteFile(dbPath, []byte("NOT A SQLITE FILE! CORRUPT!"), 0600); err != nil {
		t.Fatalf("corrupt db file: %v", err)
	}
	newTime := time.Now().Add(time.Second)
	_ = os.Chtimes(dbPath, newTime, newTime)

	// Ingest attempt on corrupted source
	stats2, err := svc.Ingest()
	if err != nil {
		t.Fatalf("ingest on malformed db failed with error: %v", err)
	}
	if stats2.FailedCount != 1 {
		t.Errorf("expected 1 failed db count, got %d", stats2.FailedCount)
	}

	// 3. Inspect ledger: original usage records and completed manifest MUST be preserved!
	ledger, err := OpenLedger(ledgerPath)
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	defer ledger.Close()

	manifests, err := ledger.GetManifests()
	if err != nil {
		t.Fatalf("get manifests: %v", err)
	}
	m := manifests[cid]
	if m == nil {
		t.Fatalf("manifest unexpectedly missing")
	}
	if !m.IsComplete {
		t.Errorf("expected IsComplete to remain true despite malformed source attempt")
	}
	if m.Status != "completed" {
		t.Errorf("expected Status to remain 'completed', got %s", m.Status)
	}
	if m.GenerationCount != 1 {
		t.Errorf("expected GenerationCount to remain 1, got %d", m.GenerationCount)
	}
	if m.LastScanStatus != "failed" {
		t.Errorf("expected LastScanStatus to be 'failed', got %s", m.LastScanStatus)
	}

	// Records must still exist
	var recCount int
	_ = ledger.db.QueryRow("SELECT COUNT(*) FROM usage_records WHERE conversation_id = ?", cid).Scan(&recCount)
	if recCount != 1 {
		t.Errorf("expected 1 usage record preserved, got %d", recCount)
	}

	// Invariants must hold
	vReport, err := ledger.Verify()
	if err != nil {
		t.Fatalf("verify failed: %v", err)
	}
	if !vReport.Valid {
		t.Errorf("expected verify to pass, got errors: %v", vReport.Errors)
	}
}
