package inventory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type RestoreStatus string

const (
	RestoreRestored           RestoreStatus = "restored"
	RestoreConflict           RestoreStatus = "conflict"
	RestoreInvalidArchive     RestoreStatus = "invalid_archive"
	RestoreUnsafeDestination  RestoreStatus = "unsafe_destination"
	RestoreStagingFailed      RestoreStatus = "staging_failed"
	RestoreVerificationFailed RestoreStatus = "verification_failed"
	RestorePartialPublish     RestoreStatus = "partial_publish"
	RestoreRegistryFailed     RestoreStatus = "registry_failed"
)

type RestoreResult struct {
	FormatVersion               int           `json:"format_version"`
	ArchiveID                   string        `json:"archive_id,omitempty"`
	ConversationID              string        `json:"conversation_id,omitempty"`
	Status                      RestoreStatus `json:"status"`
	FileStatus                  RestoreStatus `json:"file_status,omitempty"`
	Reason                      string        `json:"reason,omitempty"`
	Published                   []string      `json:"published"`
	DBDestinationRef            string        `json:"db_destination_ref,omitempty"`
	BrainDestinationRef         string        `json:"brain_destination_ref,omitempty"`
	SummaryMetadataPresent      bool          `json:"summary_metadata_present"`
	CatalogRegistrationRequired bool          `json:"catalog_registration_required"`
	RecoveryGuidance            string        `json:"recovery_guidance,omitempty"`
	RestoredAt                  time.Time     `json:"restored_at"`
}

type RestoreOptions struct {
	ArchivePath        string
	ConversationDir    string
	BrainDir           string
	Registry           *ArchiveRegistry
	now                func() time.Time
	afterDBStage       func(string) error
	beforeDBPublish    func() error
	beforeBrainPublish func() error
	beforeRegistry     func() error
}

func Restore(ctx context.Context, opt RestoreOptions) (RestoreResult, error) {
	now := time.Now().UTC()
	if opt.now != nil {
		now = opt.now().UTC()
	}
	result := RestoreResult{FormatVersion: 1, Status: RestoreInvalidArchive, Published: []string{}, CatalogRegistrationRequired: true, RestoredAt: now}
	verification := VerifyArchive(ctx, opt.ArchivePath, now)
	if verification.State != ArchiveValid || verification.manifest == nil {
		result.Reason = "archive_verification_" + string(verification.State)
		return result, errors.New(result.Reason)
	}
	manifest := *verification.manifest
	result.ArchiveID = manifest.ArchiveID
	result.ConversationID = manifest.ConversationID
	result.SummaryMetadataPresent = manifest.SummaryMetadataPresent
	if opt.Registry == nil {
		result.Status = RestoreRegistryFailed
		result.Reason = "restore_registry_unavailable"
		return result, errors.New(result.Reason)
	}
	if err := opt.Registry.RecordVerification(ctx, opt.ArchivePath, verification); err != nil {
		result.Status = RestoreRegistryFailed
		result.Reason = "restore_registry_update_failed"
		return result, errors.New(result.Reason)
	}

	finish := func(status RestoreStatus, reason string) (RestoreResult, error) {
		result.Status, result.Reason = status, reason
		registryErr := error(nil)
		if opt.beforeRegistry != nil {
			registryErr = opt.beforeRegistry()
		}
		if registryErr == nil {
			registryErr = opt.Registry.RecordRestoreEvent(ctx, result)
		}
		if registryErr != nil && (len(result.Published) > 0 || status == RestoreRestored) {
			result.FileStatus = status
			result.Status = RestoreRegistryFailed
			result.Reason = "restore_registry_update_failed"
			result.RecoveryGuidance = "restored files remain published; reconcile the agy-db registry"
			return result, errors.New(result.Reason)
		}
		if status != RestoreRestored {
			return result, errors.New(reason)
		}
		return result, nil
	}

	conversationRoot, conversationAbs, err := openRestoreRoot(opt.ConversationDir)
	if err != nil {
		return finish(RestoreUnsafeDestination, "conversation_destination_unsafe")
	}
	defer conversationRoot.Close()
	brainRoot, brainAbs, err := openRestoreRoot(opt.BrainDir)
	if err != nil {
		return finish(RestoreUnsafeDestination, "brain_destination_unsafe")
	}
	defer brainRoot.Close()
	archiveAbs, err := filepath.Abs(opt.ArchivePath)
	if err != nil || pathWithin(conversationAbs, archiveAbs) || pathWithin(brainAbs, archiveAbs) {
		return finish(RestoreUnsafeDestination, "destination_overlaps_archive")
	}
	dbName := manifest.ConversationID + ".db"
	result.DBDestinationRef = registryPathRef(filepath.Join(conversationAbs, dbName))
	result.BrainDestinationRef = registryPathRef(filepath.Join(brainAbs, manifest.ConversationID))
	for _, name := range []string{dbName, dbName + "-wal", dbName + "-shm", dbName + "-journal"} {
		if absent, err := restoreTargetAbsent(conversationRoot, name); err != nil || !absent {
			return finish(RestoreConflict, "conversation_destination_exists")
		}
	}
	if absent, err := restoreTargetAbsent(brainRoot, manifest.ConversationID); err != nil || !absent {
		return finish(RestoreConflict, "brain_destination_exists")
	}

	archiveRoot, _, err := openRestoreRoot(opt.ArchivePath)
	if err != nil {
		return finish(RestoreInvalidArchive, "archive_changed_after_verification")
	}
	defer archiveRoot.Close()
	archivePinned := fmt.Sprintf("/proc/self/fd/%d", archiveRoot.Fd())
	pinnedManifest, err := verifyArchiveDirectory(ctx, archivePinned)
	if err != nil || pinnedManifest.ArchiveID != manifest.ArchiveID || pinnedManifest.SourceSnapshotSHA256 != manifest.SourceSnapshotSHA256 {
		return finish(RestoreInvalidArchive, "archive_changed_after_verification")
	}
	manifest = pinnedManifest

	conversationStage, err := os.MkdirTemp(fmt.Sprintf("/proc/self/fd/%d", conversationRoot.Fd()), ".agy-db-restore-")
	if err != nil || os.Chmod(conversationStage, 0700) != nil {
		return finish(RestoreStagingFailed, "conversation_staging_failed")
	}
	defer os.RemoveAll(conversationStage)
	stagedDB := filepath.Join(conversationStage, "main.db")
	mainFile, ok := manifestFile(manifest, "conversation/main.db", "sqlite_snapshot")
	if !ok || copyRestoreFile(ctx, filepath.Join(archivePinned, "conversation", "main.db"), stagedDB, mainFile) != nil {
		return finish(RestoreStagingFailed, "conversation_staging_failed")
	}
	if opt.afterDBStage != nil {
		if err := opt.afterDBStage(stagedDB); err != nil {
			return finish(RestoreStagingFailed, "conversation_staging_failed")
		}
	}
	if err := verifyStagedDB(ctx, stagedDB, manifest, mainFile); err != nil {
		return finish(RestoreVerificationFailed, "staged_conversation_verification_failed")
	}

	brainStage := ""
	brainPayload := ""
	brainFiles := manifestRoleFiles(manifest, "brain")
	if manifest.BrainPresent {
		brainStage, err = os.MkdirTemp(fmt.Sprintf("/proc/self/fd/%d", brainRoot.Fd()), ".agy-db-restore-")
		if err != nil || os.Chmod(brainStage, 0700) != nil {
			return finish(RestoreStagingFailed, "brain_staging_failed")
		}
		defer os.RemoveAll(brainStage)
		brainPayload = filepath.Join(brainStage, "payload")
		if os.Mkdir(brainPayload, 0700) != nil {
			return finish(RestoreStagingFailed, "brain_staging_failed")
		}
		for _, file := range brainFiles {
			rel := strings.TrimPrefix(file.Path, "brain/")
			destination := filepath.Join(brainPayload, filepath.FromSlash(rel))
			if os.MkdirAll(filepath.Dir(destination), 0700) != nil || copyRestoreFile(ctx, filepath.Join(archivePinned, filepath.FromSlash(file.Path)), destination, file) != nil {
				return finish(RestoreStagingFailed, "brain_staging_failed")
			}
		}
		if verifyRestoredBrain(brainPayload, brainFiles) != nil {
			return finish(RestoreVerificationFailed, "staged_brain_verification_failed")
		}
	}

	if opt.beforeDBPublish != nil {
		if err := opt.beforeDBPublish(); err != nil {
			return finish(RestoreStagingFailed, "conversation_publish_failed")
		}
	}
	if !restoreRootUnchanged(conversationRoot, conversationAbs) || !restoreRootUnchanged(brainRoot, brainAbs) {
		return finish(RestoreUnsafeDestination, "destination_root_changed")
	}
	conversationStageHandle, err := os.OpenFile(conversationStage, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_DIRECTORY, 0)
	if err != nil {
		return finish(RestoreStagingFailed, "conversation_publish_failed")
	}
	renameErr := unix.Renameat2(int(conversationStageHandle.Fd()), "main.db", int(conversationRoot.Fd()), dbName, unix.RENAME_NOREPLACE)
	conversationStageHandle.Close()
	if renameErr != nil {
		if errors.Is(renameErr, unix.EEXIST) {
			return finish(RestoreConflict, "conversation_destination_exists")
		}
		return finish(RestoreStagingFailed, "conversation_publish_failed")
	}
	result.Published = append(result.Published, "conversation_db")
	publishedDB := filepath.Join(fmt.Sprintf("/proc/self/fd/%d", conversationRoot.Fd()), dbName)
	if err := verifyStagedDB(ctx, publishedDB, manifest, mainFile); err != nil {
		result.RecoveryGuidance = "conversation DB was published but failed post-publication verification"
		return finish(RestorePartialPublish, "published_conversation_verification_failed")
	}

	if manifest.BrainPresent {
		if opt.beforeBrainPublish != nil {
			if err := opt.beforeBrainPublish(); err != nil {
				result.RecoveryGuidance = "conversation DB is published; resolve the brain destination before recovery"
				return finish(RestorePartialPublish, "brain_publish_failed")
			}
		}
		if !restoreRootUnchanged(conversationRoot, conversationAbs) || !restoreRootUnchanged(brainRoot, brainAbs) {
			result.RecoveryGuidance = "conversation DB is published; a destination root changed before brain publication"
			return finish(RestorePartialPublish, "destination_root_changed")
		}
		brainStageHandle, err := os.OpenFile(brainStage, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_DIRECTORY, 0)
		if err != nil {
			result.RecoveryGuidance = "conversation DB is published; brain staging could not be reopened"
			return finish(RestorePartialPublish, "brain_publish_failed")
		}
		renameErr := unix.Renameat2(int(brainStageHandle.Fd()), "payload", int(brainRoot.Fd()), manifest.ConversationID, unix.RENAME_NOREPLACE)
		brainStageHandle.Close()
		if renameErr != nil {
			result.RecoveryGuidance = "conversation DB is published; resolve the brain destination before recovery"
			return finish(RestorePartialPublish, "brain_publish_failed")
		}
		result.Published = append(result.Published, "brain")
		publishedBrain := filepath.Join(fmt.Sprintf("/proc/self/fd/%d", brainRoot.Fd()), manifest.ConversationID)
		if verifyRestoredBrain(publishedBrain, brainFiles) != nil {
			result.RecoveryGuidance = "DB and brain were published; brain failed post-publication verification"
			return finish(RestorePartialPublish, "published_brain_verification_failed")
		}
	}
	if !restoreRootUnchanged(conversationRoot, conversationAbs) || !restoreRootUnchanged(brainRoot, brainAbs) {
		result.RecoveryGuidance = "restored files were published through pinned roots, but a configured root path changed"
		return finish(RestorePartialPublish, "destination_root_changed")
	}
	return finish(RestoreRestored, "")
}

func pathWithin(path, parent string) bool {
	rel, err := filepath.Rel(parent, path)
	return err == nil && (rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))))
}

func restoreRootUnchanged(root *os.File, path string) bool {
	opened, err := root.Stat()
	atPath, pathErr := os.Lstat(path)
	return err == nil && pathErr == nil && unchanged(opened, atPath)
}

func openRestoreRoot(path string) (*os.File, string, error) {
	abs, err := filepath.Abs(path)
	if err != nil || path == "" || testingRootRequired(abs) {
		return nil, "", errors.New("unsafe_root")
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil || resolved != abs {
		return nil, "", errors.New("unsafe_root")
	}
	root, err := os.OpenFile(abs, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_DIRECTORY, 0)
	if err != nil {
		return nil, "", errors.New("unsafe_root")
	}
	info, err := root.Stat()
	atPath, pathErr := os.Lstat(abs)
	if err != nil || pathErr != nil || !info.IsDir() || !unchanged(info, atPath) {
		root.Close()
		return nil, "", errors.New("unsafe_root")
	}
	return root, abs, nil
}

func restoreTargetAbsent(root *os.File, name string) (bool, error) {
	var stat unix.Stat_t
	err := unix.Fstatat(int(root.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) {
		return true, nil
	}
	return false, err
}

func manifestFile(manifest ArchiveManifest, path, role string) (ArchiveFile, bool) {
	for _, file := range manifest.ArchiveFiles {
		if file.Path == path && file.Role == role {
			return file, true
		}
	}
	return ArchiveFile{}, false
}

func manifestRoleFiles(manifest ArchiveManifest, role string) []ArchiveFile {
	files := []ArchiveFile{}
	for _, file := range manifest.ArchiveFiles {
		if file.Role == role {
			files = append(files, file)
		}
	}
	slices.SortFunc(files, func(a, b ArchiveFile) int { return strings.Compare(a.Path, b.Path) })
	return files
}

func copyRestoreFile(ctx context.Context, sourcePath, destination string, expected ArchiveFile) error {
	source, err := os.OpenFile(sourcePath, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer source.Close()
	info, err := source.Stat()
	atPath, pathErr := os.Lstat(sourcePath)
	if err != nil || pathErr != nil || !info.Mode().IsRegular() || hasMultipleLinks(info) || !unchanged(info, atPath) || info.Size() != expected.Size {
		return errors.New("unsafe_archive_file")
	}
	destinationFile, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	hash := sha256.New()
	n, copyErr := copyContext(ctx, io.MultiWriter(destinationFile, hash), source)
	closeErr := destinationFile.Close()
	after, statErr := source.Stat()
	pathAfter, pathAfterErr := os.Lstat(sourcePath)
	if copyErr != nil || closeErr != nil || statErr != nil || pathAfterErr != nil || n != expected.Size || hex.EncodeToString(hash.Sum(nil)) != expected.SHA256 || !unchanged(info, after) || !unchanged(info, pathAfter) {
		return errors.New("archive_changed")
	}
	return nil
}

func verifyStagedDB(ctx context.Context, path string, manifest ArchiveManifest, expected ArchiveFile) error {
	info, err := os.Lstat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		return errors.New("restored_db_mode_invalid")
	}
	actual, err := archiveFileRecord(path, expected.Path, expected.Role)
	if err != nil || actual.Size != expected.Size || actual.SHA256 != expected.SHA256 || actual.SHA256 != manifest.SourceSnapshotSHA256 {
		return errors.New("restored_db_hash_mismatch")
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(path + suffix); !errors.Is(err, os.ErrNotExist) {
			return errors.New("restored_db_sidecar")
		}
	}
	schema, version, err := inspectArchiveSnapshot(ctx, path)
	if err != nil || schema != manifest.SourceSchema || version != manifest.SourceSchemaVersion {
		return errors.New("restored_db_schema_mismatch")
	}
	return nil
}

func verifyRestoredBrain(root string, expected []ArchiveFile) error {
	want := map[string]ArchiveFile{}
	allowedDirs := map[string]bool{".": true}
	for _, file := range expected {
		rel := strings.TrimPrefix(file.Path, "brain/")
		if rel == file.Path || !filepath.IsLocal(filepath.FromSlash(rel)) {
			return errors.New("restored_brain_manifest_invalid")
		}
		want[rel] = file
		for dir := filepath.ToSlash(filepath.Dir(filepath.FromSlash(rel))); dir != "."; dir = filepath.ToSlash(filepath.Dir(filepath.FromSlash(dir))) {
			allowedDirs[dir] = true
		}
	}
	rootInfo, err := os.Lstat(root)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode().Perm() != 0700 {
		return errors.New("restored_brain_mode_invalid")
	}
	seen := map[string]bool{}
	err = fs.WalkDir(os.DirFS(root), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == "." {
			return nil
		}
		if entry.IsDir() {
			info, err := entry.Info()
			if err != nil || !allowedDirs[filepath.ToSlash(path)] || info.Mode().Perm() != 0700 {
				return errors.New("restored_brain_unsafe")
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return errors.New("restored_brain_unsafe")
		}
		rel := filepath.ToSlash(path)
		expectedFile, ok := want[rel]
		if !ok || seen[rel] {
			return errors.New("restored_brain_unexpected")
		}
		seen[rel] = true
		actual, err := archiveFileRecord(filepath.Join(root, filepath.FromSlash(rel)), expectedFile.Path, expectedFile.Role)
		info, infoErr := entry.Info()
		if err != nil || infoErr != nil || info.Mode().Perm() != 0600 || actual.Size != expectedFile.Size || actual.SHA256 != expectedFile.SHA256 {
			return errors.New("restored_brain_mismatch")
		}
		return nil
	})
	if err != nil || len(seen) != len(want) {
		return errors.New("restored_brain_verification_failed")
	}
	return nil
}
