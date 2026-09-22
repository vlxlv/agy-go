package inventory

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func setupRestore(t *testing.T, brain, summary bool) (ArchiveOptions, *ArchiveRegistry, RestoreOptions) {
	t.Helper()
	inv, _, archiveOpt := archiveFixture(t, brain, summary)
	if _, err := inv.Archive(context.Background(), archiveOpt); err != nil {
		t.Fatal(err)
	}
	destination := t.TempDir()
	registerTestRoot(t, destination)
	conversationDir := filepath.Join(destination, "conversations")
	brainDir := filepath.Join(destination, "brain")
	if err := os.Mkdir(conversationDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(brainDir, 0700); err != nil {
		t.Fatal(err)
	}
	registry, err := OpenArchiveRegistry(filepath.Join(destination, "registry", "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	return archiveOpt, registry, RestoreOptions{ArchivePath: archiveOpt.Output, ConversationDir: conversationDir, BrainDir: brainDir, Registry: registry, now: func() time.Time { return time.Unix(1700, 0).UTC() }}
}

func TestRestoreDBOnlyAndConflict(t *testing.T) {
	archiveOpt, registry, opt := setupRestore(t, false, false)
	archiveBefore := treeState(t, archiveOpt.Output)
	result, err := Restore(context.Background(), opt)
	if err != nil || result.Status != RestoreRestored || !slicesEqual(result.Published, []string{"conversation_db"}) || !result.CatalogRegistrationRequired || result.SummaryMetadataPresent {
		t.Fatalf("restore: %+v %v", result, err)
	}
	dbPath := filepath.Join(opt.ConversationDir, "conversation-1.db")
	if info, err := os.Stat(dbPath); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("restored DB: %v %v", info, err)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(dbPath + suffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("sidecar created: %s", suffix)
		}
	}
	if got := treeState(t, archiveOpt.Output); !statesEqual(archiveBefore, got) {
		t.Fatal("archive modified")
	}
	var events int
	if registry.db.QueryRow("SELECT count(*) FROM restore_events WHERE result='restored' AND db_published=1 AND brain_published=0 AND catalog_registered=0").Scan(&events) != nil || events != 1 {
		t.Fatal("restore event missing")
	}
	before := singleFileState(t, dbPath)
	repeated, err := Restore(context.Background(), opt)
	if err == nil || repeated.Status != RestoreConflict || singleFileState(t, dbPath) != before {
		t.Fatalf("repeated restore: %+v %v", repeated, err)
	}
	assertNoRestoreStages(t, opt.ConversationDir, opt.BrainDir)
}

func TestRestoreBrainAndSummaryHandoff(t *testing.T) {
	_, _, opt := setupRestore(t, true, true)
	result, err := Restore(context.Background(), opt)
	if err != nil || result.Status != RestoreRestored || !slicesEqual(result.Published, []string{"conversation_db", "brain"}) || !result.SummaryMetadataPresent || !result.CatalogRegistrationRequired {
		t.Fatalf("restore: %+v %v", result, err)
	}
	brainFile := filepath.Join(opt.BrainDir, "conversation-1", "nested", "artifact.bin")
	if data, err := os.ReadFile(brainFile); err != nil || string(data) != "brain payload" {
		t.Fatalf("brain: %q %v", data, err)
	}
	if info, err := os.Stat(brainFile); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("brain mode: %v %v", info, err)
	}
	if _, err := os.Stat(filepath.Join(opt.ConversationDir, "conversation_summaries.db")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("catalog database created")
	}
}

func TestRestoreFailsBeforePublication(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status RestoreStatus
		setup  func(*testing.T, *RestoreOptions)
	}{
		{"invalid_archive", RestoreInvalidArchive, func(t *testing.T, opt *RestoreOptions) {
			mustWrite(t, filepath.Join(opt.ArchivePath, "conversation", "main.db"), []byte("corrupt"))
		}},
		{"db_conflict", RestoreConflict, func(t *testing.T, opt *RestoreOptions) {
			mustWrite(t, filepath.Join(opt.ConversationDir, "conversation-1.db"), []byte("existing"))
		}},
		{"brain_conflict", RestoreConflict, func(t *testing.T, opt *RestoreOptions) {
			if err := os.Mkdir(filepath.Join(opt.BrainDir, "conversation-1"), 0700); err != nil {
				t.Fatal(err)
			}
		}},
		{"staging_failure", RestoreStagingFailed, func(t *testing.T, opt *RestoreOptions) {
			opt.afterDBStage = func(string) error { return errors.New("injected") }
		}},
		{"staged_hash", RestoreVerificationFailed, func(t *testing.T, opt *RestoreOptions) {
			opt.afterDBStage = func(path string) error { return os.WriteFile(path, []byte("changed"), 0600) }
		}},
		{"staged_schema", RestoreVerificationFailed, func(t *testing.T, opt *RestoreOptions) {
			opt.afterDBStage = func(path string) error {
				db, err := sql.Open("sqlite", path)
				if err != nil {
					return err
				}
				_, execErr := db.Exec("DROP TABLE gen_metadata")
				closeErr := db.Close()
				if execErr != nil {
					return execErr
				}
				return closeErr
			}
		}},
		{"db_publish_failure", RestoreStagingFailed, func(t *testing.T, opt *RestoreOptions) {
			opt.beforeDBPublish = func() error { return errors.New("injected") }
		}},
		{"root_replaced", RestoreUnsafeDestination, func(t *testing.T, opt *RestoreOptions) {
			opt.beforeDBPublish = func() error {
				moved := opt.ConversationDir + "-moved"
				if err := os.Rename(opt.ConversationDir, moved); err != nil {
					return err
				}
				return os.Mkdir(opt.ConversationDir, 0700)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, opt := setupRestore(t, true, false)
			tc.setup(t, &opt)
			result, err := Restore(context.Background(), opt)
			if err == nil || result.Status != tc.status || len(result.Published) != 0 {
				t.Fatalf("result: %+v %v", result, err)
			}
			if _, err := os.Stat(filepath.Join(opt.ConversationDir, "conversation-1.db")); tc.name != "db_conflict" && !errors.Is(err, os.ErrNotExist) {
				t.Fatal("conversation DB published")
			}
			assertNoRestoreStages(t, opt.ConversationDir, opt.BrainDir)
		})
	}
}

func TestRestoreDestinationSafety(t *testing.T) {
	_, _, opt := setupRestore(t, false, false)
	real := t.TempDir()
	link := filepath.Join(filepath.Dir(opt.ConversationDir), "linked-conversations")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	opt.ConversationDir = link
	result, err := Restore(context.Background(), opt)
	if err == nil || result.Status != RestoreUnsafeDestination {
		t.Fatalf("symlink root accepted: %+v %v", result, err)
	}

	_, _, opt = setupRestore(t, false, false)
	opt.ConversationDir = opt.ArchivePath
	result, err = Restore(context.Background(), opt)
	if err == nil || result.Status != RestoreUnsafeDestination {
		t.Fatalf("archive overlap accepted: %+v %v", result, err)
	}

	_, _, opt = setupRestore(t, false, false)
	mutateManifest(t, opt.ArchivePath, func(m *ArchiveManifest) {
		m.ArchiveFiles = append(m.ArchiveFiles, ArchiveFile{Path: "../escape", Size: 0, SHA256: strings.Repeat("0", 64), Role: "brain"})
	})
	result, err = Restore(context.Background(), opt)
	if err == nil || result.Status != RestoreInvalidArchive {
		t.Fatalf("unsafe archive accepted: %+v %v", result, err)
	}

	for _, tc := range []struct {
		name string
		add  func(*testing.T, string)
	}{
		{"hardlink", func(t *testing.T, root string) {
			if err := os.Link(filepath.Join(root, "conversation", "main.db"), filepath.Join(root, "linked.db")); err != nil {
				t.Fatal(err)
			}
		}},
		{"special", func(t *testing.T, root string) {
			if err := unix.Mkfifo(filepath.Join(root, "special"), 0600); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, opt := setupRestore(t, false, false)
			tc.add(t, opt.ArchivePath)
			result, err := Restore(context.Background(), opt)
			if err == nil || result.Status != RestoreInvalidArchive {
				t.Fatalf("unsafe archive accepted: %+v %v", result, err)
			}
		})
	}
}

func TestStagedDBSchemaMismatch(t *testing.T) {
	_, _, opt := setupRestore(t, false, false)
	verification := VerifyArchive(context.Background(), opt.ArchivePath, time.Now())
	manifest := *verification.manifest
	mainFile, _ := manifestFile(manifest, "conversation/main.db", "sqlite_snapshot")
	path := filepath.Join(t.TempDir(), "changed.db")
	data, err := os.ReadFile(filepath.Join(opt.ArchivePath, "conversation", "main.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, execErr := db.Exec("DROP TABLE gen_metadata")
	closeErr := db.Close()
	if execErr != nil || closeErr != nil {
		t.Fatalf("schema edit: %v %v", execErr, closeErr)
	}
	changed, err := archiveFileRecord(path, mainFile.Path, mainFile.Role)
	if err != nil {
		t.Fatal(err)
	}
	manifest.SourceSnapshotSHA256 = changed.SHA256
	if err := verifyStagedDB(context.Background(), path, manifest, changed); err == nil || err.Error() != "restored_db_schema_mismatch" {
		t.Fatalf("schema mismatch accepted: %v", err)
	}
}

func TestRestorePartialPublishAndRegistryFailure(t *testing.T) {
	t.Run("brain_publish", func(t *testing.T) {
		_, registry, opt := setupRestore(t, true, false)
		opt.beforeBrainPublish = func() error {
			return os.Mkdir(filepath.Join(opt.BrainDir, "conversation-1"), 0700)
		}
		result, err := Restore(context.Background(), opt)
		if err == nil || result.Status != RestorePartialPublish || !slicesEqual(result.Published, []string{"conversation_db"}) || result.RecoveryGuidance == "" {
			t.Fatalf("partial: %+v %v", result, err)
		}
		if _, err := os.Stat(filepath.Join(opt.ConversationDir, "conversation-1.db")); err != nil {
			t.Fatal("published DB missing")
		}
		var status string
		if registry.db.QueryRow("SELECT result FROM restore_events ORDER BY restored_at DESC LIMIT 1").Scan(&status) != nil || status != "partial_publish" {
			t.Fatal("partial event missing")
		}
	})
	t.Run("registry", func(t *testing.T) {
		_, _, opt := setupRestore(t, false, false)
		opt.beforeRegistry = func() error { return errors.New("injected") }
		result, err := Restore(context.Background(), opt)
		if err == nil || result.Status != RestoreRegistryFailed || result.FileStatus != RestoreRestored || !slicesEqual(result.Published, []string{"conversation_db"}) {
			t.Fatalf("registry failure: %+v %v", result, err)
		}
		if _, err := os.Stat(filepath.Join(opt.ConversationDir, "conversation-1.db")); err != nil {
			t.Fatal("restored DB rolled back")
		}
	})
}

func TestConcurrentRestores(t *testing.T) {
	t.Run("same", func(t *testing.T) {
		_, _, opt := setupRestore(t, true, false)
		results := make(chan RestoreResult, 2)
		var wg sync.WaitGroup
		for range 2 {
			wg.Go(func() {
				result, _ := Restore(context.Background(), opt)
				results <- result
			})
		}
		wg.Wait()
		close(results)
		var restored, conflicts int
		for result := range results {
			if result.Status == RestoreRestored {
				restored++
			} else if result.Status == RestoreConflict {
				conflicts++
			}
		}
		if restored != 1 || conflicts != 1 {
			t.Fatalf("restored=%d conflicts=%d", restored, conflicts)
		}
	})
	t.Run("different", func(t *testing.T) {
		_, registry, first := setupRestore(t, false, false)
		inv, sourceRoot, _ := setup(t)
		fixture(t, sourceRoot, "conversation-2.db", fixtureSchema+`INSERT INTO steps(idx) VALUES(0);`)
		archiveBase := t.TempDir()
		registerTestRoot(t, archiveBase)
		secondArchive := filepath.Join(archiveBase, "archive")
		if _, err := inv.Archive(context.Background(), ArchiveOptions{ConversationID: "conversation-2", Output: secondArchive}); err != nil {
			t.Fatal(err)
		}
		second := first
		second.ArchivePath = secondArchive
		second.Registry = registry
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		wg.Go(func() { _, err := Restore(context.Background(), first); errs <- err })
		wg.Go(func() { _, err := Restore(context.Background(), second); errs <- err })
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, id := range []string{"conversation-1", "conversation-2"} {
			if _, err := os.Stat(filepath.Join(first.ConversationDir, id+".db")); err != nil {
				t.Fatalf("%s missing: %v", id, err)
			}
		}
	})
}

func TestRegistryV1MigratesRestoreEvents(t *testing.T) {
	root := t.TempDir()
	registerTestRoot(t, root)
	path := filepath.Join(root, "registry.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, execErr := db.Exec(`CREATE TABLE archives (archive_id TEXT PRIMARY KEY,conversation_id TEXT NOT NULL,archive_path TEXT NOT NULL UNIQUE,format_version INTEGER NOT NULL,created_at TEXT NOT NULL,source_main_sha256 TEXT NOT NULL,source_snapshot_sha256 TEXT NOT NULL,manifest_sha256 TEXT NOT NULL,verification_state TEXT NOT NULL,verified_at TEXT,registered_at TEXT NOT NULL);
CREATE TABLE archive_files (archive_id TEXT NOT NULL REFERENCES archives(archive_id) ON DELETE CASCADE,path TEXT NOT NULL,size INTEGER NOT NULL,sha256 TEXT NOT NULL,role TEXT NOT NULL,PRIMARY KEY(archive_id,path));
PRAGMA user_version=1; PRAGMA application_id=1095190852;`)
	closeErr := db.Close()
	if execErr != nil || closeErr != nil {
		t.Fatalf("v1 fixture: %v %v", execErr, closeErr)
	}
	registry, err := OpenArchiveRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	var version int
	var tables int
	if registry.db.QueryRow("PRAGMA user_version").Scan(&version) != nil || registry.db.QueryRow("SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='restore_events'").Scan(&tables) != nil || version != 2 || tables != 1 {
		t.Fatalf("migration: version=%d tables=%d", version, tables)
	}
}

func assertNoRestoreStages(t *testing.T, roots ...string) {
	t.Helper()
	for _, root := range roots {
		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), ".agy-db-restore-") {
				t.Fatalf("staging leaked: %s", entry.Name())
			}
		}
	}
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for n := range a {
		if a[n] != b[n] {
			return false
		}
	}
	return true
}
