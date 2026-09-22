package inventory

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"modernc.org/sqlite"
)

const ArchiveFormatVersion = 1

type ArchiveState string

const (
	ArchiveConsistentSnapshot ArchiveState = "archived_consistent_snapshot"
	ArchiveBlockedUnstable    ArchiveState = "blocked_active_unstable"
	ArchiveBlockedChanged     ArchiveState = "blocked_source_changed"
	ArchiveBlockedSnapshot    ArchiveState = "blocked_snapshot_failed"
)

type ArchiveFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
	Role   string `json:"role"`
}

type ArchiveManifest struct {
	FormatVersion          int           `json:"format_version"`
	ArchiveID              string        `json:"archive_id"`
	CreatedAt              time.Time     `json:"created_at"`
	ConversationID         string        `json:"conversation_id"`
	SourceSchema           string        `json:"source_schema"`
	SourceSchemaVersion    int           `json:"source_schema_version"`
	SourceObservedState    string        `json:"source_observed_state"`
	SourceMainSHA256       string        `json:"source_main_sha256"`
	SourceSnapshotSHA256   string        `json:"source_snapshot_sha256"`
	SourceSize             int64         `json:"source_size"`
	ArchiveFiles           []ArchiveFile `json:"archive_files"`
	BrainPresent           bool          `json:"brain_present"`
	SummaryMetadataPresent bool          `json:"summary_metadata_present"`
	ToolVersion            string        `json:"tool_version"`
}

type ArchiveOptions struct {
	ConversationID string
	BrainDir       string
	SummariesDB    string
	Output         string
	now            func() time.Time
	afterSnapshot  func() error
}

// ConversationBundleSource is the bounded live source resolved by Archive.
// SHM is coordination state and is observed for change detection, never archived.
type ConversationBundleSource struct {
	ConversationID string
	DBPath         string
	WALPath        string
	SHMPath        string
	BrainPath      string
	SummariesDB    string
}

type ArchiveResult struct {
	FormatVersion  int          `json:"format_version"`
	ArchiveID      string       `json:"archive_id,omitempty"`
	ConversationID string       `json:"conversation_id"`
	State          ArchiveState `json:"state"`
	ManifestSHA256 string       `json:"manifest_sha256,omitempty"`
}

type observedFile struct {
	exists bool
	info   os.FileInfo
	digest string
}

type sqliteBackuper interface {
	NewBackup(string) (*sqlite.Backup, error)
}

func validConversationID(id string) bool {
	if id == "" || len(id) > 128 || id == "." || id == ".." {
		return false
	}
	for _, r := range id {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

// Archive creates a self-contained point-in-time snapshot. It captures an
// unchanged main/WAL pair, then lets SQLite normalize that private capture.
// The live database is never opened by SQLite or checkpointed.
func (i *Inventory) Archive(ctx context.Context, opt ArchiveOptions) (result ArchiveResult, retErr error) {
	result = ArchiveResult{FormatVersion: ArchiveFormatVersion, ConversationID: opt.ConversationID, State: ArchiveBlockedSnapshot}
	if !validConversationID(opt.ConversationID) || opt.Output == "" {
		return result, errors.New("invalid_archive_arguments")
	}
	output, err := filepath.Abs(opt.Output)
	if err != nil || filepath.Base(output) == "." || filepath.Base(output) == string(filepath.Separator) {
		return result, errors.New("invalid_archive_output")
	}
	parent := filepath.Dir(output)
	resolved, err := filepath.EvalSymlinks(parent)
	if err != nil || resolved != parent {
		return result, errors.New("unsafe_archive_output")
	}
	if testingRootRequired(parent) {
		return result, errors.New("test guard: output outside registered temporary root")
	}
	parentHandle, err := os.OpenFile(parent, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_DIRECTORY, 0)
	if err != nil {
		return result, errors.New("unsafe_archive_output")
	}
	defer parentHandle.Close()
	if _, err := os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
		return result, errors.New("archive_destination_exists")
	}

	name := opt.ConversationID + ".db"
	main, err := i.root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return result, errors.New("source_open_failed")
	}
	defer main.Close()
	mainBefore, err := observeOpenFile(main)
	if err != nil || !mainBefore.info.Mode().IsRegular() || mainBefore.info.Size() == 0 || hasMultipleLinks(mainBefore.info) {
		return result, errors.New("source_invalid")
	}
	pathBefore, err := i.root.Lstat(name)
	if err != nil || !unchanged(mainBefore.info, pathBefore) {
		result.State = ArchiveBlockedChanged
		return result, errors.New("source_changed")
	}
	walBefore, err := observeOptional(i.root, name+"-wal")
	if err != nil {
		return result, err
	}
	shmBefore, err := observeOptional(i.root, name+"-shm")
	if err != nil {
		return result, err
	}
	journalBefore, err := observeOptional(i.root, name+"-journal")
	if err != nil || journalBefore.exists {
		result.State = ArchiveBlockedUnstable
		return result, errors.New("rollback_journal_present")
	}

	staging, err := os.MkdirTemp(fmt.Sprintf("/proc/self/fd/%d", parentHandle.Fd()), ".agy-db-archive-staging-")
	if err != nil {
		return result, errors.New("archive_stage_failed")
	}
	if err := os.Chmod(staging, 0700); err != nil {
		_ = os.RemoveAll(staging)
		return result, errors.New("archive_stage_failed")
	}
	defer func() {
		if staging != "" && os.RemoveAll(staging) != nil && retErr == nil {
			retErr = errors.New("archive_stage_cleanup_failed")
		}
	}()
	conversationDir := filepath.Join(staging, "conversation")
	if err := os.Mkdir(conversationDir, 0700); err != nil {
		return result, errors.New("archive_stage_failed")
	}
	capture := filepath.Join(staging, ".source-capture")
	if err := os.Mkdir(capture, 0700); err != nil || copyOpenFile(ctx, main, filepath.Join(capture, "main.db")) != nil {
		return result, errors.New("archive_stage_failed")
	}
	if walBefore.exists {
		if err := copyRootFile(ctx, i.root, name+"-wal", filepath.Join(capture, "main.db-wal")); err != nil {
			result.State = ArchiveBlockedChanged
			return result, errors.New("source_changed")
		}
	}
	if opt.afterSnapshot != nil {
		if err := opt.afterSnapshot(); err != nil {
			return result, err
		}
	}

	mainAfter, err := observeOpenFile(main)
	pathAfter, pathErr := i.root.Lstat(name)
	walAfter, walErr := observeOptional(i.root, name+"-wal")
	shmAfter, shmErr := observeOptional(i.root, name+"-shm")
	journalAfter, journalErr := observeOptional(i.root, name+"-journal")
	if err != nil || pathErr != nil || walErr != nil || shmErr != nil || journalErr != nil || journalAfter.exists || !sameObserved(mainBefore, mainAfter) || !unchanged(mainBefore.info, pathAfter) || !sameObserved(walBefore, walAfter) || !sameObserved(shmBefore, shmAfter) {
		result.State = ArchiveBlockedChanged
		return result, errors.New("source_changed")
	}
	snapshot := filepath.Join(conversationDir, "main.db")
	if err := backupPrivateSQLite(ctx, filepath.Join(capture, "main.db"), snapshot); err != nil {
		result.State = ArchiveBlockedUnstable
		return result, errors.New("consistent_snapshot_unavailable")
	}
	if err := os.RemoveAll(capture); err != nil {
		return result, errors.New("archive_stage_cleanup_failed")
	}

	schema, schemaVersion, err := inspectArchiveSnapshot(ctx, snapshot)
	if err != nil {
		return result, err
	}
	files := []ArchiveFile{}
	mainArchive, err := archiveFileRecord(snapshot, "conversation/main.db", "sqlite_snapshot")
	if err != nil {
		return result, err
	}
	files = append(files, mainArchive)

	brainPresent, brainFiles, err := archiveBrain(ctx, opt.BrainDir, opt.ConversationID, staging)
	if err != nil {
		if errors.Is(err, errSourceChanged) {
			result.State = ArchiveBlockedChanged
		}
		return result, err
	}
	files = append(files, brainFiles...)
	summaryPresent, summaryFile, err := archiveSummary(ctx, opt.SummariesDB, opt.ConversationID, staging)
	if err != nil {
		return result, err
	}
	if summaryPresent {
		files = append(files, summaryFile)
	}
	slices.SortFunc(files, func(a, b ArchiveFile) int { return strings.Compare(a.Path, b.Path) })

	archiveID, err := randomID()
	if err != nil {
		return result, errors.New("archive_id_failed")
	}
	created := time.Now().UTC()
	if opt.now != nil {
		created = opt.now().UTC()
	}
	observedState := "no_wal"
	if walBefore.exists && walBefore.info.Size() == 0 {
		observedState = "wal_empty"
	} else if walBefore.exists {
		observedState = "wal_nonempty"
	}
	manifest := ArchiveManifest{
		FormatVersion: ArchiveFormatVersion, ArchiveID: archiveID, CreatedAt: created,
		ConversationID: opt.ConversationID, SourceSchema: schema, SourceSchemaVersion: schemaVersion,
		SourceObservedState: observedState, SourceMainSHA256: mainBefore.digest,
		SourceSnapshotSHA256: mainArchive.SHA256, SourceSize: mainBefore.info.Size(),
		ArchiveFiles: files, BrainPresent: brainPresent, SummaryMetadataPresent: summaryPresent,
		ToolVersion: "agy-db-v2-cp1",
	}
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return result, errors.New("manifest_encode_failed")
	}
	manifestBytes = append(manifestBytes, '\n')
	manifestPath := filepath.Join(staging, "manifest.json")
	if err := os.WriteFile(manifestPath, manifestBytes, 0600); err != nil {
		return result, errors.New("manifest_write_failed")
	}
	manifestHash := sha256.Sum256(manifestBytes)
	if _, err := verifyArchiveDirectory(ctx, staging); err != nil {
		return result, err
	}
	if err := unix.Renameat2(int(parentHandle.Fd()), filepath.Base(staging), int(parentHandle.Fd()), filepath.Base(output), unix.RENAME_NOREPLACE); err != nil {
		if errors.Is(err, unix.EEXIST) {
			return result, errors.New("archive_destination_exists")
		}
		return result, errors.New("archive_publish_failed")
	}
	staging = ""
	result.State = ArchiveConsistentSnapshot
	result.ArchiveID = archiveID
	result.ManifestSHA256 = hex.EncodeToString(manifestHash[:])
	return result, nil
}

func testingRootRequired(path string) bool {
	if !testing.Testing() {
		return false
	}
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		if _, ok := testRoots.Load(current); ok {
			return false
		}
		if filepath.Dir(current) == current {
			return true
		}
	}
}

func observeOpenFile(f *os.File) (observedFile, error) {
	info, err := f.Stat()
	if err != nil {
		return observedFile{}, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return observedFile{}, err
	}
	h := sha256.New()
	if _, err := copyContext(context.Background(), h, f); err != nil {
		return observedFile{}, err
	}
	return observedFile{exists: true, info: info, digest: hex.EncodeToString(h.Sum(nil))}, nil
}

func observeOptional(root *os.Root, name string) (observedFile, error) {
	info, err := root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return observedFile{}, nil
	}
	if err != nil || !info.Mode().IsRegular() || hasMultipleLinks(info) {
		return observedFile{}, errors.New("source_artifact_unsafe")
	}
	f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return observedFile{}, errors.New("source_artifact_unreadable")
	}
	defer f.Close()
	got, err := observeOpenFile(f)
	after, statErr := root.Lstat(name)
	if err != nil || statErr != nil || !unchanged(info, got.info) || !unchanged(info, after) {
		return observedFile{}, errors.New("source_changed")
	}
	return got, nil
}

func sameObserved(a, b observedFile) bool {
	if a.exists != b.exists {
		return false
	}
	return !a.exists || (a.digest == b.digest && unchanged(a.info, b.info))
}

func hasMultipleLinks(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Nlink > 1
}

func copyOpenFile(ctx context.Context, source *os.File, destination string) error {
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return err
	}
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, copyErr := copyContext(ctx, out, source)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func copyRootFile(ctx context.Context, root *os.Root, name, destination string) error {
	source, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer source.Close()
	return copyOpenFile(ctx, source, destination)
}

func backupPrivateSQLite(ctx context.Context, source, destination string) (retErr error) {
	if err := os.Remove(destination); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	sourceURI := (&url.URL{Scheme: "file", Path: source}).String() + "?mode=rw&_busy_timeout=1000&_pragma=trusted_schema(OFF)"
	db, err := sql.Open("sqlite", sourceURI)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return err
	}
	defer conn.ExecContext(context.Background(), "ROLLBACK")
	var schemaVersion int
	if err := conn.QueryRowContext(ctx, "PRAGMA schema_version").Scan(&schemaVersion); err != nil {
		return err
	}
	destinationURI := (&url.URL{Scheme: "file", Path: destination}).String() + "?mode=rwc&_journal_mode=DELETE&_synchronous=FULL"
	if err := conn.Raw(func(driverConn any) error {
		backuper, ok := driverConn.(sqliteBackuper)
		if !ok {
			return errors.New("sqlite_backup_unsupported")
		}
		backup, err := backuper.NewBackup(destinationURI)
		if err != nil {
			return err
		}
		finished := false
		defer func() {
			if !finished {
				_ = backup.Finish()
			}
		}()
		more, err := backup.Step(-1)
		if err != nil {
			return err
		}
		if more {
			return errors.New("sqlite_backup_incomplete")
		}
		finished = true
		return backup.Finish()
	}); err != nil {
		_ = os.Remove(destination)
		return err
	}
	if err := os.Chmod(destination, 0600); err != nil {
		return err
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(destination + suffix); !errors.Is(err, os.ErrNotExist) {
			return errors.New("snapshot_sidecar_present")
		}
	}
	return nil
}

func inspectArchiveSnapshot(ctx context.Context, path string) (string, int, error) {
	source, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return "", 0, errors.New("snapshot_open_failed")
	}
	defer source.Close()
	info, err := source.Stat()
	atPath, pathErr := os.Lstat(path)
	if err != nil || pathErr != nil || !info.Mode().IsRegular() || hasMultipleLinks(info) || !unchanged(info, atPath) {
		return "", 0, errors.New("snapshot_open_failed")
	}
	dsn := (&url.URL{Scheme: "file", Path: fmt.Sprintf("/proc/self/fd/%d", source.Fd())}).String() + "?mode=ro&immutable=1&_pragma=query_only(ON)&_pragma=trusted_schema(OFF)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return "", 0, errors.New("snapshot_open_failed")
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	var check string
	var version int
	if db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&check) != nil || check != "ok" || db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version) != nil {
		return "", 0, errors.New("snapshot_integrity_failed")
	}
	rows, err := db.QueryContext(ctx, "SELECT name,type FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' AND type IN ('table','index','view','trigger') ORDER BY type,name")
	if err != nil {
		return "", 0, errors.New("snapshot_schema_failed")
	}
	defer rows.Close()
	items := []string{}
	for rows.Next() {
		var name, typ string
		if rows.Scan(&name, &typ) != nil {
			return "", 0, errors.New("snapshot_schema_failed")
		}
		items = append(items, typ+":"+name)
	}
	if rows.Err() != nil {
		return "", 0, errors.New("snapshot_schema_failed")
	}
	fixture := []string{"table:gen_metadata", "table:steps", "table:trajectory_metadata_blob"}
	production := []string{"index:idx_steps_status", "index:idx_steps_step_type", "table:battle_mode_infos", "table:executor_metadata", "table:gen_metadata", "table:parent_references", "table:steps", "table:trajectory_meta", "table:trajectory_metadata_blob"}
	fixtureColumns := map[string][]string{
		"steps":                    {"idx:INTEGER:1", "step_type:INTEGER:0", "status:INTEGER:0", "has_subtrajectory:numeric:0", "metadata:BLOB:0"},
		"gen_metadata":             {"idx:INTEGER:1", "data:BLOB:0", "size:INTEGER:0"},
		"trajectory_metadata_blob": {"id:TEXT:1", "data:BLOB:0"},
	}
	productionColumns := map[string][]string{
		"trajectory_meta":          {"trajectory_id:TEXT:1", "cascade_id:TEXT:0", "trajectory_type:INTEGER:0", "source:INTEGER:0"},
		"trajectory_metadata_blob": {"id:TEXT:1", "data:BLOB:0"},
		"steps":                    {"idx:INTEGER:1", "step_type:INTEGER:0", "status:INTEGER:0", "has_subtrajectory:numeric:0", "metadata:BLOB:0", "error_details:BLOB:0", "permissions:BLOB:0", "task_details:BLOB:0", "render_info:BLOB:0", "step_payload:BLOB:0", "step_format:INTEGER:0"},
		"gen_metadata":             {"idx:INTEGER:1", "data:BLOB:0", "size:INTEGER:0"},
		"executor_metadata":        {"idx:INTEGER:1", "data:BLOB:0"},
		"parent_references":        {"idx:INTEGER:1", "data:BLOB:0"},
		"battle_mode_infos":        {"idx:INTEGER:1", "data:BLOB:0"},
	}
	if version == 0 && slices.Equal(items, fixture) && archiveSchemaMatches(ctx, db, fixtureColumns) {
		return "tokei_fixture_v1", version, nil
	}
	if version == 1 && slices.Equal(items, production) && archiveSchemaMatches(ctx, db, productionColumns) && archiveIndexMatches(ctx, db, "idx_steps_status", "status") && archiveIndexMatches(ctx, db, "idx_steps_step_type", "step_type") {
		return "agy_production_v1", version, nil
	}
	return "", version, errors.New("snapshot_schema_unsupported")
}

func archiveIndexMatches(ctx context.Context, db *sql.DB, index, column string) bool {
	var got string
	return db.QueryRowContext(ctx, "SELECT name FROM pragma_index_info(?) WHERE seqno=0 AND cid>=0", index).Scan(&got) == nil && got == column
}

func archiveSchemaMatches(ctx context.Context, db *sql.DB, expected map[string][]string) bool {
	for table, want := range expected {
		rows, err := db.QueryContext(ctx, "SELECT name,type,pk FROM pragma_table_xinfo(?) ORDER BY cid", table)
		if err != nil {
			return false
		}
		got := []string{}
		for rows.Next() {
			var name, typ string
			var pk int
			if rows.Scan(&name, &typ, &pk) != nil {
				rows.Close()
				return false
			}
			got = append(got, fmt.Sprintf("%s:%s:%d", name, typ, pk))
		}
		rowErr := rows.Err()
		rows.Close()
		if rowErr != nil || !strings.EqualFold(strings.Join(got, "|"), strings.Join(want, "|")) {
			return false
		}
	}
	return true
}

var errSourceChanged = errors.New("source_changed")

type brainObservation struct {
	rel    string
	info   os.FileInfo
	digest string
}

func archiveBrain(ctx context.Context, rootPath, id, staging string) (bool, []ArchiveFile, error) {
	if rootPath == "" {
		return false, nil, nil
	}
	abs, err := filepath.Abs(rootPath)
	if err != nil || testingRootRequired(abs) {
		return false, nil, errors.New("brain_root_unsafe")
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil || resolved != abs {
		return false, nil, errors.New("brain_root_unsafe")
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		return false, nil, errors.New("brain_root_unavailable")
	}
	defer root.Close()
	info, err := root.Lstat(id)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil, nil
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return false, nil, errors.New("brain_source_unsafe")
	}
	observed, dirs, err := observeBrain(root, id)
	if err != nil {
		return false, nil, err
	}
	destination := filepath.Join(staging, "brain")
	if err := os.Mkdir(destination, 0700); err != nil {
		return false, nil, errors.New("archive_stage_failed")
	}
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		if err := os.MkdirAll(filepath.Join(destination, filepath.FromSlash(dir)), 0700); err != nil {
			return false, nil, errors.New("archive_stage_failed")
		}
	}
	files := make([]ArchiveFile, 0, len(observed))
	for _, source := range observed {
		if err := ctx.Err(); err != nil {
			return false, nil, err
		}
		in, err := root.OpenFile(filepath.Join(id, filepath.FromSlash(source.rel)), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return false, nil, errSourceChanged
		}
		dst := filepath.Join(destination, filepath.FromSlash(source.rel))
		out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			in.Close()
			return false, nil, errors.New("archive_stage_failed")
		}
		h := sha256.New()
		n, copyErr := copyContext(ctx, io.MultiWriter(out, h), in)
		closeOut := out.Close()
		closeIn := in.Close()
		if copyErr != nil || closeOut != nil || closeIn != nil || n != source.info.Size() || hex.EncodeToString(h.Sum(nil)) != source.digest {
			return false, nil, errSourceChanged
		}
		files = append(files, ArchiveFile{Path: filepath.ToSlash(filepath.Join("brain", source.rel)), Size: n, SHA256: source.digest, Role: "brain"})
	}
	after, _, err := observeBrain(root, id)
	if err != nil || !slices.EqualFunc(observed, after, func(a, b brainObservation) bool {
		return a.rel == b.rel && a.digest == b.digest && unchanged(a.info, b.info)
	}) {
		return false, nil, errSourceChanged
	}
	return true, files, nil
}

func observeBrain(root *os.Root, id string) ([]brainObservation, []string, error) {
	dir, err := root.Open(id)
	if err != nil {
		return nil, nil, err
	}
	defer dir.Close()
	rootFS := os.DirFS(filepath.Join("/proc/self/fd", fmt.Sprint(dir.Fd())))
	files := []brainObservation{}
	dirs := []string{""}
	err = fs.WalkDir(rootFS, ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == "." {
			return nil
		}
		rel := filepath.ToSlash(strings.TrimPrefix(path, "./"))
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || (!info.Mode().IsRegular() && !info.IsDir()) || (info.Mode().IsRegular() && hasMultipleLinks(info)) {
			return errors.New("brain_source_unsafe")
		}
		if info.IsDir() {
			dirs = append(dirs, rel)
			return nil
		}
		f, err := root.OpenFile(filepath.Join(id, filepath.FromSlash(rel)), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return err
		}
		h := sha256.New()
		_, copyErr := io.Copy(h, f)
		opened, statErr := f.Stat()
		closeErr := f.Close()
		if copyErr != nil || statErr != nil || closeErr != nil || !unchanged(info, opened) {
			return errSourceChanged
		}
		files = append(files, brainObservation{rel: rel, info: info, digest: hex.EncodeToString(h.Sum(nil))})
		return nil
	})
	slices.SortFunc(files, func(a, b brainObservation) int { return strings.Compare(a.rel, b.rel) })
	slices.Sort(dirs)
	return files, dirs, err
}

func archiveSummary(ctx context.Context, sourcePath, id, staging string) (bool, ArchiveFile, error) {
	if sourcePath == "" {
		return false, ArchiveFile{}, nil
	}
	abs, err := filepath.Abs(sourcePath)
	if err != nil || testingRootRequired(filepath.Dir(abs)) {
		return false, ArchiveFile{}, errors.New("summary_source_unsafe")
	}
	resolvedParent, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err != nil || resolvedParent != filepath.Dir(abs) {
		return false, ArchiveFile{}, errors.New("summary_source_unsafe")
	}
	root, err := os.OpenRoot(filepath.Dir(abs))
	if err != nil {
		return false, ArchiveFile{}, errors.New("summary_source_unavailable")
	}
	defer root.Close()
	name := filepath.Base(abs)
	source, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return false, ArchiveFile{}, errors.New("summary_source_unavailable")
	}
	defer source.Close()
	before, err := observeOpenFile(source)
	if err != nil || !before.info.Mode().IsRegular() || hasMultipleLinks(before.info) {
		return false, ArchiveFile{}, errors.New("summary_source_unavailable")
	}
	walBefore, err := observeOptional(root, name+"-wal")
	if err != nil {
		return false, ArchiveFile{}, errors.New("summary_source_unavailable")
	}
	shmBefore, err := observeOptional(root, name+"-shm")
	if err != nil {
		return false, ArchiveFile{}, errors.New("summary_source_unavailable")
	}
	journalBefore, err := observeOptional(root, name+"-journal")
	if err != nil || journalBefore.exists {
		return false, ArchiveFile{}, errors.New("summary_source_unavailable")
	}
	capture := filepath.Join(staging, ".summary-capture")
	if err := os.Mkdir(capture, 0700); err != nil || copyOpenFile(ctx, source, filepath.Join(capture, "main.db")) != nil {
		return false, ArchiveFile{}, errors.New("summary_snapshot_failed")
	}
	if walBefore.exists {
		if err := copyRootFile(ctx, root, name+"-wal", filepath.Join(capture, "main.db-wal")); err != nil {
			return false, ArchiveFile{}, errSourceChanged
		}
	}
	after, err := observeOpenFile(source)
	pathAfter, pathErr := root.Lstat(name)
	walAfter, walErr := observeOptional(root, name+"-wal")
	shmAfter, shmErr := observeOptional(root, name+"-shm")
	journalAfter, journalErr := observeOptional(root, name+"-journal")
	if err != nil || pathErr != nil || walErr != nil || shmErr != nil || journalErr != nil || journalAfter.exists || !sameObserved(before, after) || !unchanged(before.info, pathAfter) || !sameObserved(walBefore, walAfter) || !sameObserved(shmBefore, shmAfter) {
		return false, ArchiveFile{}, errSourceChanged
	}
	temp := filepath.Join(staging, ".summary-snapshot.db")
	if err := backupPrivateSQLite(ctx, filepath.Join(capture, "main.db"), temp); err != nil {
		return false, ArchiveFile{}, errors.New("summary_snapshot_failed")
	}
	if err := os.RemoveAll(capture); err != nil {
		return false, ArchiveFile{}, errors.New("archive_stage_cleanup_failed")
	}
	dsn := (&url.URL{Scheme: "file", Path: temp}).String() + "?mode=ro&immutable=1&_pragma=query_only(ON)&_pragma=trusted_schema(OFF)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return false, ArchiveFile{}, errors.New("summary_read_failed")
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, "SELECT * FROM conversation_summaries WHERE conversation_id = ?", id)
	if err != nil {
		return false, ArchiveFile{}, errors.New("summary_schema_unsupported")
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return false, ArchiveFile{}, errors.New("summary_read_failed")
	}
	var record map[string]any
	if rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for n := range values {
			pointers[n] = &values[n]
		}
		if rows.Scan(pointers...) != nil {
			return false, ArchiveFile{}, errors.New("summary_read_failed")
		}
		record = map[string]any{}
		for n, column := range columns {
			record[column] = values[n]
		}
		if rows.Next() {
			return false, ArchiveFile{}, errors.New("summary_identity_ambiguous")
		}
	}
	if rows.Err() != nil {
		return false, ArchiveFile{}, errors.New("summary_read_failed")
	}
	if err := os.Remove(temp); err != nil {
		return false, ArchiveFile{}, errors.New("archive_stage_cleanup_failed")
	}
	if record == nil {
		return false, ArchiveFile{}, nil
	}
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return false, ArchiveFile{}, errors.New("summary_encode_failed")
	}
	data = append(data, '\n')
	dir := filepath.Join(staging, "summary")
	if os.Mkdir(dir, 0700) != nil || os.WriteFile(filepath.Join(dir, "metadata.json"), data, 0600) != nil {
		return false, ArchiveFile{}, errors.New("archive_stage_failed")
	}
	file, err := archiveFileRecord(filepath.Join(dir, "metadata.json"), "summary/metadata.json", "summary_metadata")
	return true, file, err
}

func archiveFileRecord(path, archivePath, role string) (ArchiveFile, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return ArchiveFile{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	atPath, pathErr := os.Lstat(path)
	if err != nil || pathErr != nil || !info.Mode().IsRegular() || hasMultipleLinks(info) || !unchanged(info, atPath) {
		return ArchiveFile{}, errors.New("archive_file_invalid")
	}
	h := sha256.New()
	n, err := io.Copy(h, f)
	after, statErr := f.Stat()
	pathAfter, pathAfterErr := os.Lstat(path)
	if err != nil || statErr != nil || pathAfterErr != nil || n != info.Size() || !unchanged(info, after) || !unchanged(info, pathAfter) {
		return ArchiveFile{}, errors.New("archive_hash_failed")
	}
	return ArchiveFile{Path: filepath.ToSlash(archivePath), Size: n, SHA256: hex.EncodeToString(h.Sum(nil)), Role: role}, nil
}

func randomID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func validArchiveID(id string) bool {
	if len(id) != 32 || strings.ToLower(id) != id {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

func validSHA256(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func readArchiveFile(path string, limit int64) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	atPath, pathErr := os.Lstat(path)
	if err != nil || pathErr != nil || !info.Mode().IsRegular() || hasMultipleLinks(info) || !unchanged(info, atPath) {
		return nil, errors.New("archive_manifest_unsafe")
	}
	if info.Size() < 0 || info.Size() > limit {
		return nil, errors.New("archive_manifest_invalid")
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	after, statErr := f.Stat()
	pathAfter, pathAfterErr := os.Lstat(path)
	if err != nil || statErr != nil || pathAfterErr != nil || !unchanged(info, after) || !unchanged(info, pathAfter) {
		return nil, errors.New("archive_manifest_unsafe")
	}
	return data, nil
}

func verifyArchiveDirectory(ctx context.Context, root string) (ArchiveManifest, error) {
	data, err := readArchiveFile(filepath.Join(root, "manifest.json"), 1<<20)
	if errors.Is(err, os.ErrNotExist) {
		return ArchiveManifest{}, errors.New("archive_incomplete")
	}
	if err != nil {
		return ArchiveManifest{}, err
	}
	var manifest ArchiveManifest
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&manifest) != nil {
		return ArchiveManifest{}, errors.New("archive_manifest_invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ArchiveManifest{}, errors.New("archive_manifest_invalid")
	}
	if manifest.FormatVersion != ArchiveFormatVersion {
		return manifest, errors.New("archive_format_unsupported")
	}
	if !validArchiveID(manifest.ArchiveID) || !validConversationID(manifest.ConversationID) || manifest.CreatedAt.IsZero() || !validSHA256(manifest.SourceMainSHA256) || !validSHA256(manifest.SourceSnapshotSHA256) {
		return manifest, errors.New("archive_manifest_invalid")
	}
	seen := map[string]bool{}
	allowedDirs := map[string]bool{".": true}
	snapshotHash := ""
	summarySeen := false
	var summaryExpected ArchiveFile
	brainSeen := false
	for _, expected := range manifest.ArchiveFiles {
		clean := filepath.Clean(filepath.FromSlash(expected.Path))
		if !filepath.IsLocal(clean) || filepath.ToSlash(clean) != expected.Path || seen[expected.Path] || expected.Path == "manifest.json" || expected.Size < 0 || !validSHA256(expected.SHA256) {
			return ArchiveManifest{}, errors.New("archive_manifest_unsafe")
		}
		seen[expected.Path] = true
		for dir := filepath.ToSlash(filepath.Dir(clean)); dir != "."; dir = filepath.ToSlash(filepath.Dir(filepath.FromSlash(dir))) {
			allowedDirs[dir] = true
		}
		actual, err := archiveFileRecord(filepath.Join(root, clean), expected.Path, expected.Role)
		if errors.Is(err, os.ErrNotExist) {
			return ArchiveManifest{}, errors.New("archive_incomplete")
		}
		if err != nil && err.Error() == "archive_file_invalid" {
			return ArchiveManifest{}, errors.New("archive_manifest_unsafe")
		}
		if err != nil || actual.Size != expected.Size || actual.SHA256 != expected.SHA256 {
			return ArchiveManifest{}, errors.New("archive_verification_failed")
		}
		switch {
		case expected.Path == "conversation/main.db" && expected.Role == "sqlite_snapshot":
			snapshotHash = expected.SHA256
		case expected.Path == "summary/metadata.json" && expected.Role == "summary_metadata":
			summarySeen = true
			summaryExpected = expected
		case strings.HasPrefix(expected.Path, "brain/") && expected.Role == "brain":
			brainSeen = true
		default:
			return ArchiveManifest{}, errors.New("archive_manifest_invalid")
		}
	}
	if manifest.BrainPresent {
		allowedDirs["brain"] = true
	}
	if !seen["conversation/main.db"] || snapshotHash != manifest.SourceSnapshotSHA256 || summarySeen != manifest.SummaryMetadataPresent || (brainSeen && !manifest.BrainPresent) {
		return ArchiveManifest{}, errors.New("archive_incomplete")
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(filepath.Join(root, "conversation", "main.db") + suffix); err == nil {
			return ArchiveManifest{}, errors.New("archive_incomplete")
		} else if !errors.Is(err, os.ErrNotExist) {
			return ArchiveManifest{}, errors.New("archive_manifest_unsafe")
		}
	}
	if err := fs.WalkDir(os.DirFS(root), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == "." {
			return nil
		}
		if entry.IsDir() {
			if !allowedDirs[filepath.ToSlash(path)] {
				return errors.New("archive_unexpected_file")
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return errors.New("archive_manifest_unsafe")
		}
		rel := filepath.ToSlash(path)
		if rel != "manifest.json" && !seen[rel] {
			return errors.New("archive_unexpected_file")
		}
		return nil
	}); err != nil {
		return ArchiveManifest{}, err
	}
	snapshotPath := filepath.Join(root, "conversation", "main.db")
	if _, _, err := inspectArchiveSnapshot(ctx, snapshotPath); err != nil {
		return ArchiveManifest{}, err
	}
	snapshotAfter, err := archiveFileRecord(snapshotPath, "conversation/main.db", "sqlite_snapshot")
	if err != nil || snapshotAfter.SHA256 != snapshotHash {
		return ArchiveManifest{}, errors.New("archive_verification_failed")
	}
	if manifest.SummaryMetadataPresent {
		var summary map[string]any
		summaryPath := filepath.Join(root, "summary", "metadata.json")
		data, err := readArchiveFile(summaryPath, 16<<20)
		sum := sha256.Sum256(data)
		if err != nil || int64(len(data)) != summaryExpected.Size || hex.EncodeToString(sum[:]) != summaryExpected.SHA256 || json.Unmarshal(data, &summary) != nil || len(summary) == 0 {
			return ArchiveManifest{}, errors.New("archive_summary_invalid")
		}
	}
	return manifest, nil
}
