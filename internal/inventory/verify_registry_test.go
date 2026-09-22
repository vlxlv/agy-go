package inventory

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestVerifyArchiveStatesAndReadOnly(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		inv, _, opt := archiveFixture(t, true, true)
		if _, err := inv.Archive(context.Background(), opt); err != nil {
			t.Fatal(err)
		}
		before := treeState(t, opt.Output)
		result := VerifyArchive(context.Background(), opt.Output, time.Unix(500, 0))
		if result.State != ArchiveValid || result.ArchiveID == "" || result.ConversationID != "conversation-1" || result.Files != 3 || result.Bytes <= 0 || result.Reason != "" {
			t.Fatalf("result: %+v", result)
		}
		if got := treeState(t, opt.Output); !statesEqual(before, got) {
			t.Fatal("verification modified archive")
		}
	})

	for _, tc := range []struct {
		name  string
		state ArchiveVerificationState
		edit  func(*testing.T, string)
	}{
		{"missing", ArchiveIncomplete, func(t *testing.T, root string) { mustRemove(t, filepath.Join(root, "conversation", "main.db")) }},
		{"modified", ArchiveCorrupt, func(t *testing.T, root string) {
			mustWrite(t, filepath.Join(root, "conversation", "main.db"), []byte("corrupt"))
		}},
		{"modified_brain", ArchiveCorrupt, func(t *testing.T, root string) {
			mustWrite(t, filepath.Join(root, "brain", "nested", "artifact.bin"), []byte("corrupt"))
		}},
		{"modified_summary", ArchiveCorrupt, func(t *testing.T, root string) {
			mustWrite(t, filepath.Join(root, "summary", "metadata.json"), []byte("{}"))
		}},
		{"malformed_manifest", ArchiveCorrupt, func(t *testing.T, root string) { mustWrite(t, filepath.Join(root, "manifest.json"), []byte("{")) }},
		{"unsupported_format", ArchiveUnsupported, func(t *testing.T, root string) {
			mutateManifest(t, root, func(m *ArchiveManifest) { m.FormatVersion++ })
		}},
		{"traversal", ArchiveUnsafe, func(t *testing.T, root string) {
			mutateManifest(t, root, func(m *ArchiveManifest) {
				m.ArchiveFiles = append(m.ArchiveFiles, ArchiveFile{Path: "../escape", Size: 0, SHA256: zeroSHA(), Role: "brain"})
			})
		}},
		{"normalization", ArchiveUnsafe, func(t *testing.T, root string) {
			mutateManifest(t, root, func(m *ArchiveManifest) {
				m.ArchiveFiles = append(m.ArchiveFiles, ArchiveFile{Path: "brain/../conversation/main.db", Size: 0, SHA256: zeroSHA(), Role: "brain"})
			})
		}},
		{"duplicate", ArchiveUnsafe, func(t *testing.T, root string) {
			mutateManifest(t, root, func(m *ArchiveManifest) { m.ArchiveFiles = append(m.ArchiveFiles, m.ArchiveFiles[0]) })
		}},
		{"hardlink", ArchiveUnsafe, func(t *testing.T, root string) {
			if err := os.Link(filepath.Join(root, "conversation", "main.db"), filepath.Join(root, "linked.db")); err != nil {
				t.Fatal(err)
			}
		}},
		{"symlink", ArchiveUnsafe, func(t *testing.T, root string) {
			if err := os.Symlink("manifest.json", filepath.Join(root, "linked")); err != nil {
				t.Fatal(err)
			}
		}},
		{"unexpected", ArchiveCorrupt, func(t *testing.T, root string) {
			mustWrite(t, filepath.Join(root, "unexpected"), []byte("x"))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inv, _, opt := archiveFixture(t, true, true)
			if _, err := inv.Archive(context.Background(), opt); err != nil {
				t.Fatal(err)
			}
			tc.edit(t, opt.Output)
			if got := VerifyArchive(context.Background(), opt.Output, time.Unix(500, 0)); got.State != tc.state {
				t.Fatalf("state=%s want=%s reason=%s", got.State, tc.state, got.Reason)
			}
		})
	}
	t.Run("root_symlink", func(t *testing.T) {
		inv, _, opt := archiveFixture(t, false, false)
		if _, err := inv.Archive(context.Background(), opt); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(filepath.Dir(opt.Output), "archive-link")
		if err := os.Symlink(opt.Output, link); err != nil {
			t.Fatal(err)
		}
		if got := VerifyArchive(context.Background(), link, time.Now()); got.State != ArchiveUnsafe {
			t.Fatalf("result: %+v", got)
		}
	})
}

func TestVerifyArchiveUnsupportedSQLiteSchema(t *testing.T) {
	inv, _, opt := archiveFixture(t, false, false)
	if _, err := inv.Archive(context.Background(), opt); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(opt.Output, "conversation", "main.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	_, execErr := db.Exec("DROP TABLE gen_metadata")
	closeErr := db.Close()
	if execErr != nil || closeErr != nil {
		t.Fatalf("edit snapshot: %v %v", execErr, closeErr)
	}
	data, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	mutateManifest(t, opt.Output, func(m *ArchiveManifest) {
		m.SourceSnapshotSHA256 = digest
		for n := range m.ArchiveFiles {
			if m.ArchiveFiles[n].Path == "conversation/main.db" {
				m.ArchiveFiles[n].SHA256 = digest
				m.ArchiveFiles[n].Size = int64(len(data))
			}
		}
	})
	if got := VerifyArchive(context.Background(), opt.Output, time.Now()); got.State != ArchiveUnsupported {
		t.Fatalf("result: %+v", got)
	}
}

func TestArchiveRegistryLifecycleAndConcurrency(t *testing.T) {
	inv, _, opt := archiveFixture(t, true, false)
	if _, err := inv.Archive(context.Background(), opt); err != nil {
		t.Fatal(err)
	}
	verifiedAt := time.Unix(900, 0).UTC()
	result := VerifyArchive(context.Background(), opt.Output, verifiedAt)
	registryPath := filepath.Join(filepath.Dir(opt.Output), "registry", "registry.db")
	registry, err := OpenArchiveRegistry(registryPath)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Go(func() { errs <- registry.RecordVerification(context.Background(), opt.Output, result) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	report, err := registry.List(context.Background(), "")
	if err != nil || len(report.Archives) != 1 {
		t.Fatalf("list: %+v %v", report, err)
	}
	entry := report.Archives[0]
	if entry.ArchiveID != result.ArchiveID || entry.VerificationState != ArchiveValid || entry.VerifiedAt == nil || !entry.VerifiedAt.Equal(verifiedAt) || entry.Files != result.Files || entry.Bytes != result.Bytes || !validSHA256(entry.SourceSnapshotSHA256) || entry.PathRef == "" {
		t.Fatalf("entry: %+v", entry)
	}
	if filtered, err := registry.List(context.Background(), result.ArchiveID); err != nil || len(filtered.Archives) != 1 {
		t.Fatalf("filtered: %+v %v", filtered, err)
	}
	if _, err := registry.List(context.Background(), "invalid"); err == nil {
		t.Fatal("invalid ID accepted")
	}
	mustWrite(t, filepath.Join(opt.Output, "brain", "nested", "artifact.bin"), []byte("changed"))
	corrupt := VerifyArchive(context.Background(), opt.Output, time.Unix(901, 0))
	if corrupt.State != ArchiveCorrupt || registry.RecordVerification(context.Background(), opt.Output, corrupt) != nil {
		t.Fatalf("corrupt update: %+v", corrupt)
	}
	report, err = registry.List(context.Background(), result.ArchiveID)
	if err != nil || report.Archives[0].VerificationState != ArchiveCorrupt {
		t.Fatalf("updated entry: %+v %v", report, err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	registry, err = OpenArchiveRegistry(registryPath)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	if report, err := registry.List(context.Background(), ""); err != nil || len(report.Archives) != 1 {
		t.Fatalf("reopen: %+v %v", report, err)
	}
}

func TestArchiveRegistryFailsClosed(t *testing.T) {
	root := t.TempDir()
	registerTestRoot(t, root)
	bad := filepath.Join(root, "bad.db")
	mustWrite(t, bad, []byte("not sqlite"))
	if registry, err := OpenArchiveRegistry(bad); err == nil {
		registry.Close()
		t.Fatal("corrupt registry accepted")
	}
	unsupported := filepath.Join(root, "unsupported.db")
	db, err := sql.Open("sqlite", unsupported)
	if err != nil {
		t.Fatal(err)
	}
	_, execErr := db.Exec("CREATE TABLE other(x); PRAGMA user_version=99")
	closeErr := db.Close()
	if execErr != nil || closeErr != nil {
		t.Fatalf("fixture: %v %v", execErr, closeErr)
	}
	if registry, err := OpenArchiveRegistry(unsupported); err == nil {
		registry.Close()
		t.Fatal("unsupported registry accepted")
	}
	linked := filepath.Join(root, "linked.db")
	if err := os.Link(unsupported, linked); err != nil {
		t.Fatal(err)
	}
	if registry, err := OpenArchiveRegistry(linked); err == nil {
		registry.Close()
		t.Fatal("hardlinked registry accepted")
	}
	t.Setenv("XDG_STATE_HOME", root)
	if got, err := DefaultArchiveRegistryPath(); err != nil || got != filepath.Join(root, "agy-db", "registry.db") {
		t.Fatalf("default path: %q %v", got, err)
	}
}

func mutateManifest(t *testing.T, root string, mutate func(*ArchiveManifest)) {
	t.Helper()
	path := filepath.Join(root, "manifest.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var manifest ArchiveManifest
	if json.Unmarshal(data, &manifest) != nil {
		t.Fatal("fixture manifest invalid")
	}
	mutate(&manifest)
	data, err = json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, path, append(data, '\n'))
}

func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func mustRemove(t *testing.T, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
}

func zeroSHA() string { return string(make([]byte, 64)) }
