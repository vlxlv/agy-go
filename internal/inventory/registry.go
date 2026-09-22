package inventory

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
)

const ArchiveRegistryVersion = 2

type ArchiveRegistry struct {
	db      *sql.DB
	dir     *os.File
	pathRef string
}

type ArchiveRegistryEntry struct {
	ArchiveID            string                   `json:"archive_id"`
	ConversationID       string                   `json:"conversation_id"`
	PathRef              string                   `json:"path_ref"`
	CreatedAt            time.Time                `json:"created_at"`
	SourceSnapshotSHA256 string                   `json:"source_snapshot_sha256"`
	VerificationState    ArchiveVerificationState `json:"verification_state"`
	VerifiedAt           *time.Time               `json:"verified_at,omitempty"`
	Files                int                      `json:"files"`
	Bytes                int64                    `json:"bytes"`
}

type ArchiveRegistryReport struct {
	FormatVersion int                    `json:"format_version"`
	RegistryRef   string                 `json:"registry_ref"`
	Archives      []ArchiveRegistryEntry `json:"archives"`
}

func DefaultArchiveRegistryPath() (string, error) {
	if state := os.Getenv("XDG_STATE_HOME"); state != "" {
		if !filepath.IsAbs(state) {
			return "", errors.New("XDG_STATE_HOME must be absolute")
		}
		return filepath.Join(state, "agy-db", "registry.db"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", errors.New("cannot resolve registry path")
	}
	return filepath.Join(home, ".local", "state", "agy-db", "registry.db"), nil
}

func OpenArchiveRegistry(path string) (*ArchiveRegistry, error) {
	abs, err := filepath.Abs(path)
	if err != nil || path == "" || filepath.Base(abs) == "." || filepath.Base(abs) == string(filepath.Separator) {
		return nil, errors.New("invalid registry path")
	}
	parent := filepath.Dir(abs)
	if testingRootRequired(parent) {
		return nil, errors.New("test guard: registry outside registered temporary root")
	}
	if err := os.MkdirAll(parent, 0700); err != nil {
		return nil, errors.New("cannot create registry directory")
	}
	resolved, err := filepath.EvalSymlinks(parent)
	if err != nil || resolved != parent {
		return nil, errors.New("unsafe registry path")
	}
	dir, err := os.OpenFile(parent, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_DIRECTORY, 0)
	if err != nil {
		return nil, errors.New("unsafe registry path")
	}
	name := filepath.Base(abs)
	if info, err := os.Lstat(abs); err == nil {
		if !info.Mode().IsRegular() || hasMultipleLinks(info) {
			dir.Close()
			return nil, errors.New("unsafe registry file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		dir.Close()
		return nil, errors.New("cannot inspect registry")
	}
	pinned := fmt.Sprintf("/proc/self/fd/%d/%s", dir.Fd(), name)
	dsn := (&url.URL{Scheme: "file", Path: pinned}).String() + "?mode=rwc&_busy_timeout=5000&_pragma=foreign_keys(ON)&_pragma=trusted_schema(OFF)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		dir.Close()
		return nil, errors.New("cannot open registry")
	}
	db.SetMaxOpenConns(4)
	if err := initializeArchiveRegistry(db); err != nil {
		db.Close()
		dir.Close()
		return nil, err
	}
	if err := os.Chmod(pinned, 0600); err != nil {
		db.Close()
		dir.Close()
		return nil, errors.New("cannot secure registry")
	}
	ref := sha256.Sum256([]byte(abs))
	return &ArchiveRegistry{db: db, dir: dir, pathRef: "path-sha256:" + hex.EncodeToString(ref[:])}, nil
}

func initializeArchiveRegistry(db *sql.DB) error {
	if _, err := db.Exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON"); err != nil {
		return errors.New("registry initialization failed")
	}
	var version, tables int
	if db.QueryRow("PRAGMA user_version").Scan(&version) != nil || db.QueryRow("SELECT count(*) FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'").Scan(&tables) != nil {
		return errors.New("registry unreadable")
	}
	if version == 0 && tables == 0 {
		tx, err := db.Begin()
		if err != nil {
			return errors.New("registry initialization failed")
		}
		defer tx.Rollback()
		if _, err := tx.Exec(`
CREATE TABLE archives (
  archive_id TEXT PRIMARY KEY,
  conversation_id TEXT NOT NULL,
  archive_path TEXT NOT NULL UNIQUE,
  format_version INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  source_main_sha256 TEXT NOT NULL,
  source_snapshot_sha256 TEXT NOT NULL,
  manifest_sha256 TEXT NOT NULL,
  verification_state TEXT NOT NULL,
  verified_at TEXT,
  registered_at TEXT NOT NULL
);
CREATE TABLE archive_files (
  archive_id TEXT NOT NULL REFERENCES archives(archive_id) ON DELETE CASCADE,
  path TEXT NOT NULL,
  size INTEGER NOT NULL,
  sha256 TEXT NOT NULL,
  role TEXT NOT NULL,
  PRIMARY KEY (archive_id,path)
);
CREATE TABLE restore_events (
  event_id TEXT PRIMARY KEY,
  archive_id TEXT,
  conversation_id TEXT,
  restored_at TEXT NOT NULL,
  result TEXT NOT NULL,
  destination_ref TEXT NOT NULL,
  db_published INTEGER NOT NULL,
  brain_published INTEGER NOT NULL,
  catalog_registered INTEGER NOT NULL,
  tool_version TEXT NOT NULL
);
PRAGMA user_version=2;
PRAGMA application_id=1095190852;`); err != nil || tx.Commit() != nil {
			return errors.New("registry initialization failed")
		}
		version = ArchiveRegistryVersion
	}
	if version == 1 && archiveRegistryV1SchemaMatches(db) {
		tx, err := db.Begin()
		if err != nil {
			return errors.New("registry migration failed")
		}
		defer tx.Rollback()
		if _, err := tx.Exec(`CREATE TABLE restore_events (
  event_id TEXT PRIMARY KEY,
  archive_id TEXT,
  conversation_id TEXT,
  restored_at TEXT NOT NULL,
  result TEXT NOT NULL,
  destination_ref TEXT NOT NULL,
  db_published INTEGER NOT NULL,
  brain_published INTEGER NOT NULL,
  catalog_registered INTEGER NOT NULL,
  tool_version TEXT NOT NULL
); PRAGMA user_version=2;`); err != nil || tx.Commit() != nil {
			return errors.New("registry migration failed")
		}
		version = ArchiveRegistryVersion
	}
	if version != ArchiveRegistryVersion || !archiveRegistrySchemaMatches(db) {
		return errors.New("registry schema unsupported")
	}
	var integrity string
	if db.QueryRow("PRAGMA integrity_check").Scan(&integrity) != nil || integrity != "ok" {
		return errors.New("registry corrupt")
	}
	return nil
}

func archiveRegistrySchemaMatches(db *sql.DB) bool {
	var applicationID int
	if db.QueryRow("PRAGMA application_id").Scan(&applicationID) != nil || applicationID != 1095190852 {
		return false
	}
	rows, err := db.Query("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		return false
	}
	defer rows.Close()
	got := []string{}
	for rows.Next() {
		var name string
		if rows.Scan(&name) != nil {
			return false
		}
		got = append(got, name)
	}
	if rows.Err() != nil || !slices.Equal(got, []string{"archive_files", "archives", "restore_events"}) {
		return false
	}
	expected := map[string][]string{
		"archives":       {"archive_id:TEXT:1", "conversation_id:TEXT:0", "archive_path:TEXT:0", "format_version:INTEGER:0", "created_at:TEXT:0", "source_main_sha256:TEXT:0", "source_snapshot_sha256:TEXT:0", "manifest_sha256:TEXT:0", "verification_state:TEXT:0", "verified_at:TEXT:0", "registered_at:TEXT:0"},
		"archive_files":  {"archive_id:TEXT:1", "path:TEXT:2", "size:INTEGER:0", "sha256:TEXT:0", "role:TEXT:0"},
		"restore_events": {"event_id:TEXT:1", "archive_id:TEXT:0", "conversation_id:TEXT:0", "restored_at:TEXT:0", "result:TEXT:0", "destination_ref:TEXT:0", "db_published:INTEGER:0", "brain_published:INTEGER:0", "catalog_registered:INTEGER:0", "tool_version:TEXT:0"},
	}
	return archiveSchemaMatches(context.Background(), db, expected)
}

func archiveRegistryV1SchemaMatches(db *sql.DB) bool {
	var applicationID int
	if db.QueryRow("PRAGMA application_id").Scan(&applicationID) != nil || applicationID != 1095190852 {
		return false
	}
	rows, err := db.Query("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		return false
	}
	defer rows.Close()
	got := []string{}
	for rows.Next() {
		var name string
		if rows.Scan(&name) != nil {
			return false
		}
		got = append(got, name)
	}
	if rows.Err() != nil || !slices.Equal(got, []string{"archive_files", "archives"}) {
		return false
	}
	return archiveSchemaMatches(context.Background(), db, map[string][]string{
		"archives":      {"archive_id:TEXT:1", "conversation_id:TEXT:0", "archive_path:TEXT:0", "format_version:INTEGER:0", "created_at:TEXT:0", "source_main_sha256:TEXT:0", "source_snapshot_sha256:TEXT:0", "manifest_sha256:TEXT:0", "verification_state:TEXT:0", "verified_at:TEXT:0", "registered_at:TEXT:0"},
		"archive_files": {"archive_id:TEXT:1", "path:TEXT:2", "size:INTEGER:0", "sha256:TEXT:0", "role:TEXT:0"},
	})
}

func (r *ArchiveRegistry) Close() error {
	dbErr := r.db.Close()
	dirErr := r.dir.Close()
	if dbErr != nil {
		return dbErr
	}
	return dirErr
}

func (r *ArchiveRegistry) RecordVerification(ctx context.Context, archivePath string, result ArchiveVerificationResult) error {
	abs, err := filepath.Abs(archivePath)
	if err != nil {
		return errors.New("invalid archive path")
	}
	if result.State == ArchiveValid && result.manifest != nil {
		if err := r.registerValid(ctx, abs, result); err != nil {
			return err
		}
	}
	_, err = r.db.ExecContext(ctx, "UPDATE archives SET verification_state=?,verified_at=? WHERE archive_path=?", result.State, result.VerifiedAt.Format(time.RFC3339Nano), abs)
	if err != nil {
		return errors.New("registry update failed")
	}
	return nil
}

func (r *ArchiveRegistry) RecordRestoreEvent(ctx context.Context, result RestoreResult) error {
	if !validRestoreStatus(result.Status) || result.RestoredAt.IsZero() {
		return errors.New("invalid restore event")
	}
	eventID, err := randomID()
	if err != nil {
		return errors.New("registry restore event failed")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("registry restore event failed")
	}
	defer tx.Rollback()
	destinationRef := result.DBDestinationRef + "|" + result.BrainDestinationRef
	_, err = tx.ExecContext(ctx, `INSERT INTO restore_events
(event_id,archive_id,conversation_id,restored_at,result,destination_ref,db_published,brain_published,catalog_registered,tool_version)
VALUES(?,?,?,?,?,?,?,?,?,?)`, eventID, result.ArchiveID, result.ConversationID, result.RestoredAt.Format(time.RFC3339Nano), result.Status, destinationRef, slices.Contains(result.Published, "conversation_db"), slices.Contains(result.Published, "brain"), false, "agy-db-v2-cp3")
	if err != nil || tx.Commit() != nil {
		return errors.New("registry restore event failed")
	}
	return nil
}

func validRestoreStatus(status RestoreStatus) bool {
	return status == RestoreRestored || status == RestoreConflict || status == RestoreInvalidArchive || status == RestoreUnsafeDestination || status == RestoreStagingFailed || status == RestoreVerificationFailed || status == RestorePartialPublish || status == RestoreRegistryFailed
}

func (r *ArchiveRegistry) registerValid(ctx context.Context, archivePath string, result ArchiveVerificationResult) error {
	m := result.manifest
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("registry update failed")
	}
	defer tx.Rollback()
	registered := result.VerifiedAt.Format(time.RFC3339Nano)
	resultSQL, err := tx.ExecContext(ctx, `INSERT INTO archives
(archive_id,conversation_id,archive_path,format_version,created_at,source_main_sha256,source_snapshot_sha256,manifest_sha256,verification_state,verified_at,registered_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(archive_id) DO UPDATE SET archive_path=excluded.archive_path,verification_state=excluded.verification_state,verified_at=excluded.verified_at
WHERE archives.conversation_id=excluded.conversation_id AND archives.format_version=excluded.format_version AND archives.source_main_sha256=excluded.source_main_sha256 AND archives.source_snapshot_sha256=excluded.source_snapshot_sha256 AND archives.manifest_sha256=excluded.manifest_sha256`,
		m.ArchiveID, m.ConversationID, archivePath, m.FormatVersion, m.CreatedAt.Format(time.RFC3339Nano), m.SourceMainSHA256, m.SourceSnapshotSHA256, result.ManifestSHA256, result.State, registered, registered)
	if err != nil {
		return errors.New("registry update failed")
	}
	if affected, err := resultSQL.RowsAffected(); err != nil || affected != 1 {
		return errors.New("archive id collision")
	}
	for _, file := range m.ArchiveFiles {
		if _, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO archive_files(archive_id,path,size,sha256,role) VALUES(?,?,?,?,?)", m.ArchiveID, file.Path, file.Size, file.SHA256, file.Role); err != nil {
			return errors.New("registry update failed")
		}
	}
	if err := tx.Commit(); err != nil {
		return errors.New("registry update failed")
	}
	return nil
}

func (r *ArchiveRegistry) List(ctx context.Context, archiveID string) (ArchiveRegistryReport, error) {
	query := `SELECT a.archive_id,a.conversation_id,a.archive_path,a.created_at,a.source_snapshot_sha256,a.verification_state,a.verified_at,count(f.path),coalesce(sum(f.size),0)
FROM archives a LEFT JOIN archive_files f ON f.archive_id=a.archive_id`
	args := []any{}
	if archiveID != "" {
		if !validArchiveID(archiveID) {
			return ArchiveRegistryReport{}, errors.New("invalid archive id")
		}
		query += " WHERE a.archive_id=?"
		args = append(args, archiveID)
	}
	query += " GROUP BY a.archive_id ORDER BY a.created_at DESC,a.archive_id"
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return ArchiveRegistryReport{}, errors.New("registry query failed")
	}
	defer rows.Close()
	report := ArchiveRegistryReport{FormatVersion: ArchiveRegistryVersion, RegistryRef: r.pathRef, Archives: []ArchiveRegistryEntry{}}
	for rows.Next() {
		var entry ArchiveRegistryEntry
		var path, created, verified sql.NullString
		if rows.Scan(&entry.ArchiveID, &entry.ConversationID, &path, &created, &entry.SourceSnapshotSHA256, &entry.VerificationState, &verified, &entry.Files, &entry.Bytes) != nil {
			return ArchiveRegistryReport{}, errors.New("registry query failed")
		}
		entry.PathRef = registryPathRef(path.String)
		if !validArchiveID(entry.ArchiveID) || !validConversationID(entry.ConversationID) || !filepath.IsAbs(path.String) || !validSHA256(entry.SourceSnapshotSHA256) || !validArchiveVerificationState(entry.VerificationState) || entry.Files < 0 || entry.Bytes < 0 {
			return ArchiveRegistryReport{}, errors.New("registry data invalid")
		}
		parsed, err := time.Parse(time.RFC3339Nano, created.String)
		if err != nil {
			return ArchiveRegistryReport{}, errors.New("registry data invalid")
		}
		entry.CreatedAt = parsed
		if verified.Valid && verified.String != "" {
			parsed, err := time.Parse(time.RFC3339Nano, verified.String)
			if err != nil {
				return ArchiveRegistryReport{}, errors.New("registry data invalid")
			}
			entry.VerifiedAt = &parsed
		}
		report.Archives = append(report.Archives, entry)
	}
	if rows.Err() != nil {
		return ArchiveRegistryReport{}, errors.New("registry query failed")
	}
	return report, nil
}

func validArchiveVerificationState(state ArchiveVerificationState) bool {
	return state == ArchiveValid || state == ArchiveCorrupt || state == ArchiveIncomplete || state == ArchiveUnsupported || state == ArchiveUnsafe
}

func registryPathRef(path string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(path)))
	return "path-sha256:" + hex.EncodeToString(sum[:])
}
