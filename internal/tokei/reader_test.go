package tokei

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func createTestConversationDB(t *testing.T, dir, convID string, stepBlobs [][]byte, genBlobs [][]byte) string {
	dbPath := filepath.Join(dir, convID+".db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to create test db %s: %v", dbPath, err)
	}
	defer db.Close()

	schema := `
	CREATE TABLE steps (idx integer, step_type integer, status integer, has_subtrajectory numeric, metadata blob, PRIMARY KEY (idx));
	CREATE TABLE gen_metadata (idx integer, data blob, size integer, PRIMARY KEY (idx));
	CREATE TABLE trajectory_metadata_blob (id text PRIMARY KEY, data blob);
	`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("failed to init schema: %v", err)
	}

	for i, b := range stepBlobs {
		if _, err := db.Exec("INSERT INTO steps (idx, metadata) VALUES (?, ?)", i, b); err != nil {
			t.Fatalf("failed to insert step: %v", err)
		}
	}
	for i, b := range genBlobs {
		if _, err := db.Exec("INSERT INTO gen_metadata (idx, data, size) VALUES (?, ?, ?)", i, b, len(b)); err != nil {
			t.Fatalf("failed to insert gen: %v", err)
		}
	}
	return dbPath
}

func TestReader_DeduplicationAndMerging(t *testing.T) {
	tempDir := t.TempDir()
	convID := "test-conv-dedup"

	ts1 := time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
	ts2 := time.Date(2026, 9, 20, 10, 0, 2, 0, time.UTC)

	// Step generation: response-abc
	uStep := &rawUsage{
		inputTokens:         1000,
		cacheReadTokens:     5000,
		visibleOutputTokens: 200,
		reasoningTokens:     300,
		totalOutputTokens:   500,
		responseID:          "response-abc",
		messageID:           "msg-1",
	}
	stepBlob := buildStepMetadata(ts1, ts2, "gemini-3.8-flash", 1318, uStep, nil)

	// Gen generation: same response-abc, but with retry record
	uGen := &rawUsage{
		inputTokens:         1000,
		cacheReadTokens:     5000,
		visibleOutputTokens: 200,
		reasoningTokens:     300,
		totalOutputTokens:   500,
		responseID:          "response-abc",
	}
	uRetry := &rawUsage{
		inputTokens:         1200, // higher input observed during retry
		cacheReadTokens:     5000,
		visibleOutputTokens: 250,
		reasoningTokens:     350,
		totalOutputTokens:   600,
		responseID:          "response-abc", // same ID
	}
	genBlob := buildGenMetadata(ts2, "gemini-3.8-flash", 1318, uGen, []*rawUsage{uRetry})

	// Zero-usage generation: should be dropped
	uZero := &rawUsage{modelID: 1318}
	zeroGenBlob := buildGenMetadata(ts2, "", 1318, uZero, nil)

	// Generation without response_id: should fall back to message_id
	uNoResp := &rawUsage{
		inputTokens:         500,
		totalOutputTokens:   100,
		visibleOutputTokens: 100,
		messageID:           "msg-fallback-99",
	}
	noRespStepBlob := buildStepMetadata(ts1, ts1, "gemini-2.5-pro", 246, uNoResp, nil)

	dbPath := createTestConversationDB(t, tempDir, convID,
		[][]byte{stepBlob, noRespStepBlob},
		[][]byte{genBlob, zeroGenBlob},
	)

	reader := NewReader("")
	meta := &ConversationMeta{
		ConversationID: convID,
		WorkspaceURI:   "file:///test/project",
		ProjectID:      "test-proj",
	}

	res, err := reader.ReadConversation(dbPath, meta)
	if err != nil {
		t.Fatalf("ReadConversation failed: %v", err)
	}

	// We expect exactly 2 deduplicated records: "response:response-abc" and "message:msg-fallback-99"
	// The zero-usage record must be dropped.
	if len(res.Records) != 2 {
		t.Fatalf("expected exactly 2 deduplicated records, got %d", len(res.Records))
	}

	var recABC *UsageRecord
	var recFallback *UsageRecord
	for _, r := range res.Records {
		if r.GenerationID == "response:response-abc" {
			recABC = r
		} else if r.GenerationID == "message:msg-fallback-99" {
			recFallback = r
		}
	}

	if recABC == nil {
		t.Fatalf("record response-abc not found")
	}
	if recFallback == nil {
		t.Fatalf("record msg-fallback-99 not found")
	}

	// Verify conservative max merging on recABC
	if recABC.InputTokens != 1200 {
		t.Errorf("expected max input tokens 1200, got %d", recABC.InputTokens)
	}
	if recABC.TotalOutputTokens != 600 {
		t.Errorf("expected max total output tokens 600, got %d", recABC.TotalOutputTokens)
	}
	if recABC.WorkspaceURI != "file:///test/project" {
		t.Errorf("expected workspace URI file:///test/project, got %s", recABC.WorkspaceURI)
	}
	if recABC.ProjectID != "test-proj" {
		t.Errorf("expected project ID test-proj, got %s", recABC.ProjectID)
	}

	if recFallback.Model != "gemini-2.5-pro" {
		t.Errorf("expected model gemini-2.5-pro, got %s", recFallback.Model)
	}
}

func TestReader_ActiveWALDetection(t *testing.T) {
	tempDir := t.TempDir()
	convID := "test-conv-wal"

	u := &rawUsage{inputTokens: 100, totalOutputTokens: 50, responseID: "resp-wal"}
	stepBlob := buildStepMetadata(time.Now(), time.Now(), "gemini-3.8-flash", 1318, u, nil)

	dbPath := createTestConversationDB(t, tempDir, convID, [][]byte{stepBlob}, nil)

	// Create an active WAL file
	walPath := dbPath + "-wal"
	if err := os.WriteFile(walPath, []byte("simulated active wal content"), 0600); err != nil {
		t.Fatalf("failed to write wal: %v", err)
	}

	reader := NewReader("")
	res, err := reader.ReadConversation(dbPath, nil)
	if err != nil {
		t.Fatalf("ReadConversation failed: %v", err)
	}

	if res.IsComplete {
		t.Errorf("expected IsComplete == false when active WAL is present")
	}
	if res.Status != "active" {
		t.Errorf("expected status 'active', got %s", res.Status)
	}
}
