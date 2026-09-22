package inventory

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vlxlv/agy-go/internal/tokei"
)

func TestCurrentManifestCannotProveCompleteness(t *testing.T) {
	in := positiveInput()
	mtime := instant().Add(-72 * time.Hour)
	e := Entry{SourceID: in.SourceID, Size: 8192, Mtime: mtime}
	full := manifest{digest: strings.TrimPrefix(in.SourceID, "sha256:"), size: e.Size, mtime: mtime.UnixNano(), complete: true, status: "completed", scanStatus: "completed", parser: "1.1.0", checkpoint: instant().Add(-time.Hour), count: 2, imported: 2}
	for _, tc := range []struct {
		name   string
		change func(*manifest)
		want   IngestState
	}{
		{"all existing fields match", func(*manifest) {}, IngestEvidenceGap},
		{"missing digest", func(m *manifest) { m.digest = "" }, IngestEvidenceGap},
		{"partial", func(m *manifest) { m.complete = false }, IngestPartial},
		{"coverage mismatch", func(m *manifest) { m.imported = 1 }, IngestPartial},
		{"last scan failed", func(m *manifest) { m.scanStatus = "failed" }, IngestPartial},
		{"identity mismatch", func(m *manifest) { m.digest = strings.Repeat("b", 64) }, IngestMismatch},
		{"source changed", func(m *manifest) { m.mtime++ }, IngestMismatch},
		{"stale checkpoint", func(m *manifest) { m.checkpoint = mtime.Add(-time.Second) }, IngestStale},
		{"future checkpoint", func(m *manifest) { m.checkpoint = instant().Add(time.Second) }, IngestStale},
		{"unsupported parser", func(m *manifest) { m.parser = "999" }, IngestUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := full
			tc.change(&m)
			got := assessManifest(&m, e, instant())
			if got.State != tc.want {
				t.Fatalf("%+v", got)
			}
		})
	}
	if assessManifest(nil, e, instant()).State != IngestMissing {
		t.Fatal("missing evidence accepted")
	}
}

func TestPlanActualLedgerReadOnlyAndMutation(t *testing.T) {
	inv, root, scratch := setup(t)
	source := fixture(t, root, "private-source.db", fixtureSchema)
	e := inv.Inspect(context.Background(), "private-source.db")
	// Actual current tokei schema and writer; all paths are synthetic temp roots.
	_, ledgerRoot, _ := setup(t)
	ledgerPath := filepath.Join(ledgerRoot, "usage.db")
	ledger, err := tokei.OpenLedger(ledgerPath)
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.TrimPrefix(e.SourceID, "sha256:")
	err = ledger.CommitIngest(&tokei.SourceManifest{ConversationID: "private-conversation", SourcePath: source, SourceSize: e.Size, SourceMtimeNs: e.Mtime.UnixNano(), SourceSHA256: &digest, IsComplete: true, Status: "completed"}, nil)
	closeErr := ledger.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("ledger fixture: %v %v", err, closeErr)
	}
	sourceHash := sha256.Sum256(contents(t, source))
	ledgerHash := sha256.Sum256(contents(t, ledgerPath))
	info, _ := os.Stat(source)
	ledgerInfo, _ := os.Stat(ledgerPath)
	retention := 24 * time.Hour
	now := time.Now().UTC().Add(time.Hour)
	report, err := inv.Plan(context.Background(), ledgerPath, &retention, now)
	if err != nil || len(report.Errors) != 0 || len(report.Sources) != 1 {
		t.Fatalf("%+v %v", report, err)
	}
	d := report.Sources[0]
	if !d.SourceVerified || d.Eligible || d.IngestVerified || !hasBlock(d, BlockedIncompleteIngestEvidence) || !hasBlock(d, BlockedActivityUnknown) || report.TotalReclaimableBytes != 0 {
		t.Fatalf("%+v", report)
	}
	encoded, _ := json.Marshal(report)
	for _, secret := range []string{root, ledgerRoot, "private-source", "private-conversation"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatal("privacy leak")
		}
	}
	after, _ := os.Stat(source)
	ledgerAfter, _ := os.Stat(ledgerPath)
	if sha256.Sum256(contents(t, source)) != sourceHash || sha256.Sum256(contents(t, ledgerPath)) != ledgerHash || !unchanged(info, after) || !unchanged(ledgerInfo, ledgerAfter) {
		t.Fatal("source or ledger changed")
	}
	files, _ := os.ReadDir(root)
	if len(files) != 1 {
		t.Fatal("source sidecars created")
	}
	files, _ = os.ReadDir(ledgerRoot)
	if len(files) != 1 {
		t.Fatal("ledger sidecars created")
	}
	files, _ = os.ReadDir(scratch)
	if len(files) != 0 {
		t.Fatal("snapshot leaked")
	}
	baseline, err := inv.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO steps(idx) VALUES(1)")
	closeErr = db.Close()
	if err != nil || closeErr != nil {
		t.Fatal("fixture mutation failed")
	}
	report, err = inv.plan(context.Background(), baseline, ledgerPath, &retention, now)
	if err != nil || !hasBlock(report.Sources[0], BlockedSourceChanged) || report.Sources[0].Eligible {
		t.Fatalf("mutation not blocked: %+v %v", report, err)
	}
}

func TestPlanMissingAndUnsupportedLedger(t *testing.T) {
	inv, root, _ := setup(t)
	fixture(t, root, "a.db", fixtureSchema)
	report, err := inv.Plan(context.Background(), "", nil, instant())
	if err != nil || !hasBlock(report.Sources[0], BlockedMissingIngestEvidence) || !hasBlock(report.Sources[0], BlockedRetentionUnset) {
		t.Fatalf("%+v %v", report, err)
	}
	_, ledgerRoot, _ := setup(t)
	bad := fixture(t, ledgerRoot, "usage.db", "CREATE TABLE schema_migrations(version INTEGER); INSERT INTO schema_migrations VALUES(999);")
	report, err = inv.Plan(context.Background(), bad, nil, instant())
	if err != nil || len(report.Errors) == 0 || !hasBlock(report.Sources[0], BlockedUnsupportedEvidence) {
		t.Fatalf("%+v %v", report, err)
	}
	if _, err := readManifests(context.Background(), "/home/ezhang/.local/share/agy-tokei/usage.db"); err == nil {
		t.Fatal("test guard bypassed")
	}
}
