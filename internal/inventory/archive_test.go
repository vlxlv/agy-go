package inventory

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type fileState struct {
	mode   os.FileMode
	size   int64
	mtime  time.Time
	digest string
}

func registerTestRoot(t *testing.T, root string) {
	t.Helper()
	testRoots.Store(root, root)
	t.Cleanup(func() { testRoots.Delete(root) })
}

func treeState(t *testing.T, root string) map[string]fileState {
	t.Helper()
	state := map[string]fileState{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil || rel == "." {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		item := fileState{mode: info.Mode(), size: info.Size(), mtime: info.ModTime()}
		if info.Mode().IsRegular() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			sum := sha256.Sum256(data)
			item.digest = hex.EncodeToString(sum[:])
		}
		state[filepath.ToSlash(rel)] = item
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func archiveFixture(t *testing.T, includeBrain, includeSummary bool) (*Inventory, string, ArchiveOptions) {
	t.Helper()
	inv, sourceRoot, _ := setup(t)
	fixture(t, sourceRoot, "conversation-1.db", fixtureSchema+`INSERT INTO steps(idx,metadata) VALUES(0,'private prompt'); INSERT INTO gen_metadata VALUES(0,'private response',16);`)
	base := t.TempDir()
	registerTestRoot(t, base)
	opt := ArchiveOptions{ConversationID: "conversation-1", Output: filepath.Join(base, "archive"), now: func() time.Time { return time.Unix(123, 456).UTC() }}
	if includeBrain {
		brain := filepath.Join(base, "brain-root")
		if err := os.MkdirAll(filepath.Join(brain, "conversation-1", "nested"), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(brain, "conversation-1", "nested", "artifact.bin"), []byte("brain payload"), 0600); err != nil {
			t.Fatal(err)
		}
		opt.BrainDir = brain
	}
	if includeSummary {
		summary := filepath.Join(base, "summaries.db")
		db, err := sql.Open("sqlite", summary)
		if err != nil {
			t.Fatal(err)
		}
		_, execErr := db.Exec(`CREATE TABLE conversation_summaries(conversation_id TEXT PRIMARY KEY,title TEXT,last_modified_time INTEGER); INSERT INTO conversation_summaries VALUES('conversation-1','private title',123);`)
		closeErr := db.Close()
		if execErr != nil || closeErr != nil {
			t.Fatalf("summary fixture: %v %v", execErr, closeErr)
		}
		opt.SummariesDB = summary
	}
	return inv, sourceRoot, opt
}

func TestArchiveClosedBundleAndManifest(t *testing.T) {
	inv, sourceRoot, opt := archiveFixture(t, true, true)
	before := treeState(t, sourceRoot)
	brainBefore := treeState(t, opt.BrainDir)
	summaryBefore := singleFileState(t, opt.SummariesDB)
	result, err := inv.Archive(context.Background(), opt)
	if err != nil || result.State != ArchiveConsistentSnapshot || result.ArchiveID == "" || len(result.ManifestSHA256) != 64 {
		t.Fatalf("archive: %+v %v", result, err)
	}
	if got := treeState(t, sourceRoot); !statesEqual(before, got) {
		t.Fatalf("source changed: %#v %#v", before, got)
	}
	if got := treeState(t, opt.BrainDir); !statesEqual(brainBefore, got) {
		t.Fatal("brain source changed")
	}
	if got := singleFileState(t, opt.SummariesDB); got != summaryBefore {
		t.Fatal("summary source changed")
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Stat(opt.SummariesDB + suffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("summary sidecar created: %s", suffix)
		}
	}
	manifest, err := verifyArchiveDirectory(context.Background(), opt.Output)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.FormatVersion != 1 || manifest.ConversationID != "conversation-1" || manifest.SourceSchema != "tokei_fixture_v1" || !manifest.BrainPresent || !manifest.SummaryMetadataPresent || len(manifest.ArchiveFiles) != 3 {
		t.Fatalf("manifest: %+v", manifest)
	}
	data, err := os.ReadFile(filepath.Join(opt.Output, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	manifestSum := sha256.Sum256(data)
	if result.ManifestSHA256 != hex.EncodeToString(manifestSum[:]) {
		t.Fatal("manifest result hash mismatch")
	}
	for _, secret := range []string{"private prompt", "private response", "private title", sourceRoot, opt.BrainDir, opt.SummariesDB} {
		if strings.Contains(string(data), secret) {
			t.Fatalf("manifest leaked %q", secret)
		}
	}
	if info, err := os.Stat(opt.Output); err != nil || info.Mode().Perm() != 0700 {
		t.Fatalf("archive permissions: %v %v", info, err)
	}
	for _, file := range manifest.ArchiveFiles {
		info, err := os.Stat(filepath.Join(opt.Output, filepath.FromSlash(file.Path)))
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("payload permissions: %s %v %v", file.Path, info, err)
		}
	}
}

func TestArchiveIncludesCommittedWALStateWithoutChangingSource(t *testing.T) {
	inv, sourceRoot, _ := setup(t)
	path := filepath.Join(sourceRoot, "wal.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; ` + fixtureSchema + `INSERT INTO steps(idx,metadata) VALUES(0,'only in wal');`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	walInfo, err := os.Stat(path + "-wal")
	if err != nil || walInfo.Size() == 0 {
		t.Fatalf("fixture has no WAL: %v %v", walInfo, err)
	}
	base := t.TempDir()
	registerTestRoot(t, base)
	before := treeState(t, sourceRoot)
	result, err := inv.Archive(context.Background(), ArchiveOptions{ConversationID: "wal", Output: filepath.Join(base, "archive")})
	if err != nil || result.State != ArchiveConsistentSnapshot {
		t.Fatalf("archive WAL: %+v %v\nbefore=%#v\nafter=%#v", result, err, before, treeState(t, sourceRoot))
	}
	if got := treeState(t, sourceRoot); !statesEqual(before, got) {
		t.Fatalf("WAL source changed: %#v %#v", before, got)
	}
	snapshot, err := sql.Open("sqlite", "file:"+filepath.Join(base, "archive", "conversation", "main.db")+"?mode=ro&immutable=1")
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	var count int
	if err := snapshot.QueryRow("SELECT count(*) FROM steps").Scan(&count); err != nil || count != 1 {
		t.Fatalf("WAL content absent: count=%d err=%v", count, err)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Stat(filepath.Join(base, "archive", "conversation", "main.db") + suffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("snapshot sidecar exists: %s", suffix)
		}
	}
}

func TestArchiveFailsClosedAndCleansStage(t *testing.T) {
	for _, tc := range []struct {
		name string
		hook func(string) func() error
	}{
		{"changed", func(path string) func() error {
			return func() error { return os.WriteFile(path, []byte("changed"), 0600) }
		}},
		{"disappeared", func(path string) func() error { return func() error { return os.Remove(path) } }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inv, sourceRoot, opt := archiveFixture(t, false, false)
			opt.afterSnapshot = tc.hook(filepath.Join(sourceRoot, "conversation-1.db"))
			result, err := inv.Archive(context.Background(), opt)
			if err == nil || result.State != ArchiveBlockedChanged {
				t.Fatalf("fail closed: %+v %v", result, err)
			}
			if _, err := os.Stat(opt.Output); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("partial archive published")
			}
			entries, _ := os.ReadDir(filepath.Dir(opt.Output))
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), ".agy-db-archive-staging-") {
					t.Fatal("staging leaked")
				}
			}
		})
	}
}

func TestArchiveRejectsUnsafeAndExistingDestinations(t *testing.T) {
	inv, sourceRoot, opt := archiveFixture(t, false, false)
	valid := filepath.Join(sourceRoot, "conversation-1.db")
	if err := os.Symlink(valid, filepath.Join(sourceRoot, "link.db")); err != nil {
		t.Fatal(err)
	}
	bad := opt
	bad.ConversationID = "link"
	bad.Output += "-link"
	if _, err := inv.Archive(context.Background(), bad); err == nil {
		t.Fatal("symlink source accepted")
	}
	if err := os.Link(valid, filepath.Join(sourceRoot, "hard.db")); err != nil {
		t.Fatal(err)
	}
	hard := opt
	hard.ConversationID = "hard"
	hard.Output += "-hard"
	if _, err := inv.Archive(context.Background(), hard); err == nil {
		t.Fatal("hardlinked source accepted")
	}
	if err := os.Remove(filepath.Join(sourceRoot, "hard.db")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(opt.Output, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := inv.Archive(context.Background(), opt); err == nil || err.Error() != "archive_destination_exists" {
		t.Fatalf("existing destination accepted: %v", err)
	}
	if _, err := inv.Archive(context.Background(), ArchiveOptions{ConversationID: "../escape", Output: opt.Output + "-escape"}); err == nil {
		t.Fatal("invalid ID accepted")
	}
	realParent := t.TempDir()
	linkedParent := filepath.Join(filepath.Dir(opt.Output), "linked-parent")
	if err := os.Symlink(realParent, linkedParent); err != nil {
		t.Fatal(err)
	}
	if _, err := inv.Archive(context.Background(), ArchiveOptions{ConversationID: "conversation-1", Output: filepath.Join(linkedParent, "archive")}); err == nil {
		t.Fatal("symlink output parent accepted")
	}
}

func TestArchiveOptionalArtifactsAndVerifier(t *testing.T) {
	inv, _, opt := archiveFixture(t, false, false)
	manifestResult, err := inv.Archive(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	var manifest ArchiveManifest
	data, err := os.ReadFile(filepath.Join(opt.Output, "manifest.json"))
	if err != nil || json.Unmarshal(data, &manifest) != nil || manifest.BrainPresent || manifest.SummaryMetadataPresent {
		t.Fatalf("optional artifacts: %+v %v", manifest, err)
	}
	if manifestResult.ManifestSHA256 == "" {
		t.Fatal("missing manifest hash")
	}
	if err := os.WriteFile(filepath.Join(opt.Output, "unexpected"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyArchiveDirectory(context.Background(), opt.Output); err == nil || err.Error() != "archive_unexpected_file" {
		t.Fatalf("unexpected file accepted: %v", err)
	}
}

func TestArchiveOmitsUndeclaredEmptyBrainDirectories(t *testing.T) {
	inv, sourceRoot, _ := setup(t)
	fixture(t, sourceRoot, "empty-brain.db", fixtureSchema)
	base := t.TempDir()
	registerTestRoot(t, base)
	brainRoot := filepath.Join(base, "brain-root")
	if err := os.MkdirAll(filepath.Join(brainRoot, "empty-brain", "empty", "nested"), 0700); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(base, "archive")
	if _, err := inv.Archive(context.Background(), ArchiveOptions{ConversationID: "empty-brain", BrainDir: brainRoot, Output: output}); err != nil {
		t.Fatal(err)
	}
	manifest, err := verifyArchiveDirectory(context.Background(), output)
	if err != nil || !manifest.BrainPresent || len(manifest.ArchiveFiles) != 1 {
		t.Fatalf("manifest: %+v %v", manifest, err)
	}
	if _, err := os.Stat(filepath.Join(output, "brain", "empty")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("undeclared empty directory archived")
	}
}

func TestSimultaneousArchivePublishesOnce(t *testing.T) {
	inv, _, opt := archiveFixture(t, false, false)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Go(func() {
			_, err := inv.Archive(context.Background(), opt)
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	var succeeded, existed int
	for err := range errs {
		if err == nil {
			succeeded++
		} else if err.Error() == "archive_destination_exists" {
			existed++
		} else {
			t.Fatalf("unexpected result: %v", err)
		}
	}
	if succeeded != 1 || existed != 1 {
		t.Fatalf("success=%d exists=%d", succeeded, existed)
	}
	if _, err := verifyArchiveDirectory(context.Background(), opt.Output); err != nil {
		t.Fatal(err)
	}
}

func statesEqual(a, b map[string]fileState) bool {
	if len(a) != len(b) {
		return false
	}
	for name, left := range a {
		right, ok := b[name]
		if !ok || left != right {
			return false
		}
	}
	return true
}

func singleFileState(t *testing.T, path string) fileState {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return fileState{mode: info.Mode(), size: info.Size(), mtime: info.ModTime(), digest: hex.EncodeToString(sum[:])}
}
