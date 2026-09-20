package tokei

import (
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
