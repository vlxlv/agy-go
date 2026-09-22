// Package inventory inspects explicitly supplied, quiescent conversation files.
// It never passes a source path to SQLite and has no deletion operation.
package inventory

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// Test roots are registered only by same-package tests, never by environment.
var testRoots sync.Map

type Inventory struct {
	root    *os.Root
	path    string
	scratch string
}

// Entry contains no source names, SQL errors, blobs, prompts or credentials.
// SourceID identifies file bytes, not a conversation or an ingest checkpoint.
type Entry struct {
	name          string
	SourceID      string     `json:"source_id,omitempty"`
	PathRef       string     `json:"path_ref"`
	Size          int64      `json:"size_bytes"`
	Mtime         time.Time  `json:"mtime"`
	Schema        string     `json:"schema"`
	UserVersion   int        `json:"user_version"`
	SchemaVersion int        `json:"schema_version"`
	Steps         *int64     `json:"steps,omitempty"`
	Generations   *int64     `json:"generations,omitempty"`
	LastActivity  *time.Time `json:"last_activity_at"`
	Ingest        string     `json:"ingest"`
	Eligible      bool       `json:"eligible"`
	Blockers      []string   `json:"blockers"`
}

func New(path string) (*Inventory, error) {
	if path == "" {
		return nil, errors.New("source directory is required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, errors.New("invalid source directory")
	}
	scratch := ""
	if testing.Testing() {
		value, ok := testRoots.Load(abs)
		if !ok {
			return nil, errors.New("test guard: source outside registered temporary root")
		}
		scratch = value.(string)
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil || resolved != abs {
		return nil, errors.New("source directory missing or contains symlinks")
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		return nil, errors.New("cannot open source directory")
	}
	return &Inventory{root: root, path: abs, scratch: scratch}, nil
}

func (i *Inventory) Close() error { return i.root.Close() }

func (i *Inventory) Scan(ctx context.Context) ([]Entry, error) {
	dir, err := i.root.Open(".")
	if err != nil {
		return nil, errors.New("cannot list source directory")
	}
	names, err := dir.Readdirnames(-1)
	closeErr := dir.Close()
	if err != nil || closeErr != nil {
		return nil, errors.New("cannot list source directory")
	}
	// os.ReadDir sorts names; Readdirnames does not.
	slices.Sort(names)
	entries := []Entry{}
	for _, name := range names {
		if filepath.Ext(name) != ".db" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return entries, err
		}
		entries = append(entries, i.Inspect(ctx, name))
	}
	return entries, nil
}

func (i *Inventory) Inspect(ctx context.Context, name string) Entry {
	ref := sha256.Sum256([]byte(filepath.Join(i.path, name)))
	e := Entry{name: name, PathRef: "path-sha256:" + hex.EncodeToString(ref[:]), Schema: "unverified", Ingest: "unverified", Blockers: []string{"activity_unverified", "ingest_unverified", "retention_not_evaluated"}}
	if !filepath.IsLocal(name) || filepath.Base(name) != name || filepath.Ext(name) != ".db" {
		e.Blockers = append(e.Blockers, "invalid_source_name")
		return e
	}
	if err := i.inspect(ctx, name, &e); err != nil {
		e.Blockers = append(e.Blockers, err.Error())
	}
	return e
}

func (i *Inventory) sidecars(name string) error {
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		_, err := i.root.Lstat(name + suffix)
		if !errors.Is(err, os.ErrNotExist) {
			return errors.New("sidecar_present_or_unreadable")
		}
	}
	return nil
}

func unchanged(a, b os.FileInfo) bool {
	return os.SameFile(a, b) && a.Size() == b.Size() && a.ModTime() == b.ModTime() && a.Mode() == b.Mode()
}

// The detached copy is the only file opened by SQLite. Immutable is valid for
// that private copy; using it on a live AGY source would hide WAL changes.
func (i *Inventory) inspect(ctx context.Context, name string, e *Entry) (retErr error) {
	return i.snapshot(ctx, name, e, inspectSnapshot)
}

// snapshot is shared by source and ledger inspection. The callback only sees
// the private copy; all source guards and rechecks remain in this function.
func (i *Inventory) snapshot(ctx context.Context, name string, e *Entry, inspect func(context.Context, string, *Entry) error) (retErr error) {
	if ctx.Err() != nil {
		return errors.New("cancelled")
	}
	before, err := i.root.Lstat(name)
	if err != nil {
		return errors.New("source_missing_or_unreadable")
	}
	if !before.Mode().IsRegular() {
		return errors.New("source_not_regular")
	}
	e.Size, e.Mtime = before.Size(), before.ModTime().UTC()
	if e.Size == 0 {
		return errors.New("empty_source")
	}
	if err := i.sidecars(name); err != nil {
		return err
	}
	f, err := i.root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return errors.New("source_open_failed")
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !unchanged(before, opened) {
		return errors.New("source_changed")
	}
	temp, err := os.MkdirTemp(i.scratch, "agy-db-snapshot-")
	if err != nil {
		return errors.New("snapshot_create_failed")
	}
	defer func() {
		if os.RemoveAll(temp) != nil {
			retErr = errors.New("snapshot_cleanup_failed")
		}
	}()
	snapshot := filepath.Join(temp, "snapshot.db")
	out, err := os.OpenFile(snapshot, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return errors.New("snapshot_create_failed")
	}
	hash := sha256.New()
	n, copyErr := copyContext(ctx, io.MultiWriter(out, hash), io.LimitReader(f, before.Size()+1))
	closeErr := out.Close()
	if copyErr != nil || closeErr != nil || n != before.Size() {
		return errors.New("snapshot_copy_failed")
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return errors.New("source_read_failed")
	}
	hash.Reset()
	n, err = copyContext(ctx, hash, io.LimitReader(f, before.Size()+1))
	after, statErr := f.Stat()
	atPath, pathErr := i.root.Lstat(name)
	if err != nil || statErr != nil || pathErr != nil || n != before.Size() || hex.EncodeToString(hash.Sum(nil)) != digest || !unchanged(before, after) || !unchanged(before, atPath) {
		return errors.New("source_changed")
	}
	if err := i.sidecars(name); err != nil {
		return err
	}
	e.SourceID = "sha256:" + digest
	if err := inspect(ctx, snapshot, e); err != nil {
		return err
	}
	// Recheck after SQL inspection too; this is observation, not a writer lock.
	after, err = i.root.Lstat(name)
	if err != nil || !unchanged(before, after) {
		return errors.New("source_changed")
	}
	return i.sidecars(name)
}

func copyContext(ctx context.Context, w io.Writer, r io.Reader) (int64, error) {
	buf := make([]byte, 128*1024)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, err := r.Read(buf)
		if n > 0 {
			written, werr := w.Write(buf[:n])
			total += int64(written)
			if werr != nil {
				return total, werr
			}
			if written != n {
				return total, io.ErrShortWrite
			}
		}
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
	}
}

func inspectSnapshot(ctx context.Context, path string, e *Entry) error {
	dsn := (&url.URL{Scheme: "file", Path: path}).String() + "?mode=ro&immutable=1&_pragma=query_only(ON)&_pragma=trusted_schema(OFF)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return errors.New("sqlite_open_failed")
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	var check string
	if err = db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&check); err != nil || check != "ok" {
		return errors.New("sqlite_integrity_failed")
	}
	if err = db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&e.UserVersion); err != nil {
		return errors.New("sqlite_metadata_failed")
	}
	if err = db.QueryRowContext(ctx, "PRAGMA schema_version").Scan(&e.SchemaVersion); err != nil {
		return errors.New("sqlite_metadata_failed")
	}
	// Recognize only the synthetic schema evidenced in agy-tokei reader tests.
	expected := map[string][]string{
		"steps":                    {"idx:INTEGER:1", "step_type:INTEGER:0", "status:INTEGER:0", "has_subtrajectory:numeric:0", "metadata:BLOB:0"},
		"gen_metadata":             {"idx:INTEGER:1", "data:BLOB:0", "size:INTEGER:0"},
		"trajectory_metadata_blob": {"id:TEXT:1", "data:BLOB:0"},
	}
	var tables int
	if err = db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' AND type IN ('table','view','trigger')").Scan(&tables); err != nil {
		return errors.New("sqlite_schema_failed")
	}
	if tables != 3 || e.UserVersion != 0 {
		e.Schema = "unsupported"
		return errors.New("unsupported_schema")
	}
	if err = db.QueryRowContext(ctx, "SELECT count(*) FROM pragma_table_list WHERE schema='main' AND name NOT LIKE 'sqlite_%' AND type='table'").Scan(&tables); err != nil || tables != 3 {
		e.Schema = "unsupported"
		return errors.New("unsupported_schema")
	}
	for table, want := range expected {
		rows, err := db.QueryContext(ctx, "SELECT name,type,pk FROM pragma_table_xinfo(?) ORDER BY cid", table)
		if err != nil {
			return errors.New("sqlite_schema_failed")
		}
		got := []string{}
		for rows.Next() {
			var name, typ string
			var pk int
			if err = rows.Scan(&name, &typ, &pk); err != nil {
				break
			}
			got = append(got, fmt.Sprintf("%s:%s:%d", name, typ, pk))
		}
		rowErr := rows.Err()
		rows.Close()
		if err != nil || rowErr != nil {
			return errors.New("sqlite_schema_failed")
		}
		if !strings.EqualFold(strings.Join(got, "|"), strings.Join(want, "|")) {
			e.Schema = "unsupported"
			return errors.New("unsupported_schema")
		}
	}
	var steps, gens int64
	if db.QueryRowContext(ctx, "SELECT count(*) FROM steps").Scan(&steps) != nil || db.QueryRowContext(ctx, "SELECT count(*) FROM gen_metadata").Scan(&gens) != nil {
		return errors.New("sqlite_count_failed")
	}
	e.Schema = "tokei_fixture_v1"
	e.Steps = &steps
	e.Generations = &gens
	return nil
}
