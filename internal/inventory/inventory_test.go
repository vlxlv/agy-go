package inventory

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

const fixtureSchema = `CREATE TABLE steps (idx integer, step_type integer, status integer, has_subtrajectory numeric, metadata blob, PRIMARY KEY(idx));
CREATE TABLE gen_metadata (idx integer, data blob, size integer, PRIMARY KEY(idx));
CREATE TABLE trajectory_metadata_blob (id text PRIMARY KEY, data blob);`

func setup(t *testing.T) (*Inventory, string, string) {
	t.Helper()
	root := t.TempDir()
	scratch := t.TempDir()
	t.Cleanup(func() {
		files, err := os.ReadDir(scratch)
		if err != nil || len(files) != 0 {
			t.Errorf("snapshot cleanup: %v, entries=%d", err, len(files))
		}
	})
	testRoots.Store(root, scratch)
	t.Cleanup(func() { testRoots.Delete(root) })
	inv, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := inv.Close(); err != nil {
			t.Error(err)
		}
	})
	return inv, root, scratch
}

func fixture(t *testing.T, root, name, schema string) string {
	t.Helper()
	path := filepath.Join(root, name)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(schema)
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("fixture: %v %v", err, closeErr)
	}
	return path
}
func contents(t *testing.T, path string) []byte {
	t.Helper()
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func blocker(e Entry, b string) bool {
	for _, v := range e.Blockers {
		if v == b {
			return true
		}
	}
	return false
}

func TestInventoryReadOnlyAndPrivacy(t *testing.T) {
	inv, root, scratch := setup(t)
	path := fixture(t, root, "private-credential.db", fixtureSchema+`INSERT INTO steps(idx,metadata) VALUES(1,'secret prompt credential'); INSERT INTO gen_metadata VALUES(1,'private response',16);`)
	before := sha256.Sum256(contents(t, path))
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := inv.Scan(context.Background())
	if err != nil || len(entries) != 1 {
		t.Fatalf("scan: %v %v", entries, err)
	}
	e := entries[0]
	if e.Schema != "tokei_fixture_v1" || e.Steps == nil || *e.Steps != 1 || *e.Generations != 1 || e.Eligible || len(e.Blockers) != 3 {
		t.Fatalf("entry: %+v", e)
	}
	b, _ := json.Marshal(e)
	for _, private := range []string{root, "private-credential", "secret prompt", "private response"} {
		if strings.Contains(string(b), private) {
			t.Fatal("private content leaked")
		}
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if before != sha256.Sum256(contents(t, path)) || !unchanged(info, after) {
		t.Fatal("source changed")
	}
	files, _ := os.ReadDir(root)
	if len(files) != 1 {
		t.Fatal("source side effects")
	}
	files, _ = os.ReadDir(scratch)
	if len(files) != 0 {
		t.Fatal("snapshot leaked")
	}
}

func TestRejectedSources(t *testing.T) {
	inv, root, _ := setup(t)
	for _, tc := range []struct{ name, data, reason string }{{"empty.db", "", "empty_source"}, {"corrupt.db", "not sqlite", "sqlite_integrity_failed"}} {
		if err := os.WriteFile(filepath.Join(root, tc.name), []byte(tc.data), 0600); err != nil {
			t.Fatal(err)
		}
		if e := inv.Inspect(context.Background(), tc.name); !blocker(e, tc.reason) {
			t.Fatalf("%s: %+v", tc.name, e)
		}
	}
	fixture(t, root, "unsupported.db", "CREATE TABLE unrelated(x)")
	fixture(t, root, "valid.db", fixtureSchema)
	if err := os.Symlink(filepath.Join(root, "valid.db"), filepath.Join(root, "link.db")); err != nil {
		t.Fatal(err)
	}
	for name, reason := range map[string]string{"unsupported.db": "unsupported_schema", "missing.db": "source_missing_or_unreadable", "link.db": "source_not_regular", "../outside.db": "invalid_source_name", "/home/ezhang/.gemini/state.db": "invalid_source_name"} {
		if e := inv.Inspect(context.Background(), name); !blocker(e, reason) {
			t.Fatalf("%s: %+v", name, e)
		}
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		side := filepath.Join(root, "valid.db"+suffix)
		if err := os.WriteFile(side, []byte("sidecar"), 0600); err != nil {
			t.Fatal(err)
		}
		if e := inv.Inspect(context.Background(), "valid.db"); !blocker(e, "sidecar_present_or_unreadable") {
			t.Fatalf("sidecar: %+v", e)
		}
		if string(contents(t, side)) != "sidecar" {
			t.Fatal("sidecar changed")
		}
		if err := os.Remove(side); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "missing.db")); !os.IsNotExist(err) {
		t.Fatal("missing source created")
	}
}

func TestIdentityAndConcurrentScans(t *testing.T) {
	inv, root, _ := setup(t)
	path := fixture(t, root, "a.db", fixtureSchema)
	first := inv.Inspect(context.Background(), "a.db")
	if err := os.WriteFile(filepath.Join(root, "b.db"), contents(t, path), 0600); err != nil {
		t.Fatal(err)
	}
	copied := inv.Inspect(context.Background(), "b.db")
	if first.SourceID == "" || first.SourceID != copied.SourceID || first.PathRef == copied.PathRef {
		t.Fatal("identity contract")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO steps(idx) VALUES(1)")
	db.Close()
	if err != nil {
		t.Fatal(err)
	}
	changed := inv.Inspect(context.Background(), "a.db")
	if first.SourceID == changed.SourceID {
		t.Fatal("mutation not detected")
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			entries, err := inv.Scan(context.Background())
			if err != nil || len(entries) != 2 {
				t.Errorf("parallel scan: %v", err)
			} else if !reflect.DeepEqual(entries[0], changed) {
				t.Error("unstable inventory")
			}
		})
	}
	wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !blocker(inv.Inspect(ctx, "a.db"), "cancelled") {
		t.Fatal("cancellation ignored")
	}
}

func TestProductionPathGuard(t *testing.T) {
	for _, path := range []string{"/home/ezhang/.gemini/antigravity-cli/conversations", "/root/.gemini", "/home/ezhang/.local/share/agy-tokei", "/home/ezhang/.config/agy-pool", t.TempDir()} {
		if _, err := New(path); err == nil || !strings.Contains(err.Error(), "test guard") {
			t.Fatal("unregistered path accepted")
		}
	}
}
