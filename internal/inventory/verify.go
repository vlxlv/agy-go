package inventory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

type ArchiveVerificationState string

const (
	ArchiveValid       ArchiveVerificationState = "valid"
	ArchiveCorrupt     ArchiveVerificationState = "corrupt"
	ArchiveIncomplete  ArchiveVerificationState = "incomplete"
	ArchiveUnsupported ArchiveVerificationState = "unsupported"
	ArchiveUnsafe      ArchiveVerificationState = "unsafe"
)

type ArchiveVerificationResult struct {
	FormatVersion  int                      `json:"format_version"`
	State          ArchiveVerificationState `json:"state"`
	ArchiveID      string                   `json:"archive_id,omitempty"`
	ConversationID string                   `json:"conversation_id,omitempty"`
	PathRef        string                   `json:"path_ref"`
	ManifestSHA256 string                   `json:"manifest_sha256,omitempty"`
	Files          int                      `json:"files,omitempty"`
	Bytes          int64                    `json:"bytes,omitempty"`
	Reason         string                   `json:"reason,omitempty"`
	VerifiedAt     time.Time                `json:"verified_at"`
	manifest       *ArchiveManifest
}

// VerifyArchive validates an archive without writing to it. The opened archive
// directory is pinned by file descriptor before the shared staged verifier runs.
func VerifyArchive(ctx context.Context, path string, now time.Time) ArchiveVerificationResult {
	result := ArchiveVerificationResult{FormatVersion: 1, State: ArchiveUnsafe, Reason: "archive_root_unsafe", VerifiedAt: now.UTC()}
	abs, err := filepath.Abs(path)
	if err != nil || testingRootRequired(abs) {
		return result
	}
	ref := sha256.Sum256([]byte(abs))
	result.PathRef = "path-sha256:" + hex.EncodeToString(ref[:])
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil || resolved != abs {
		return result
	}
	root, err := os.OpenFile(abs, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_DIRECTORY, 0)
	if err != nil {
		return result
	}
	defer root.Close()
	info, err := root.Stat()
	atPath, pathErr := os.Lstat(abs)
	if err != nil || pathErr != nil || !info.IsDir() || !unchanged(info, atPath) {
		return result
	}
	pinned := fmt.Sprintf("/proc/self/fd/%d", root.Fd())
	manifest, verifyErr := verifyArchiveDirectory(ctx, pinned)
	if verifyErr != nil {
		result.State, result.Reason = classifyArchiveVerificationError(verifyErr)
		return result
	}
	manifestBytes, err := readArchiveFile(filepath.Join(pinned, "manifest.json"), 1<<20)
	if err != nil {
		result.State, result.Reason = classifyArchiveVerificationError(err)
		return result
	}
	sum := sha256.Sum256(manifestBytes)
	result.State = ArchiveValid
	result.Reason = ""
	result.ArchiveID = manifest.ArchiveID
	result.ConversationID = manifest.ConversationID
	result.ManifestSHA256 = hex.EncodeToString(sum[:])
	result.Files = len(manifest.ArchiveFiles)
	for _, file := range manifest.ArchiveFiles {
		result.Bytes += file.Size
	}
	result.manifest = &manifest
	return result
}

func classifyArchiveVerificationError(err error) (ArchiveVerificationState, string) {
	switch err.Error() {
	case "archive_incomplete":
		return ArchiveIncomplete, "required_archive_artifact_missing"
	case "archive_format_unsupported", "snapshot_schema_unsupported":
		return ArchiveUnsupported, "archive_format_or_schema_unsupported"
	case "archive_manifest_unsafe", "archive_file_invalid":
		return ArchiveUnsafe, "archive_path_or_file_unsafe"
	case "archive_manifest_invalid":
		return ArchiveCorrupt, "manifest_invalid"
	case "archive_verification_failed":
		return ArchiveCorrupt, "archive_hash_or_size_mismatch"
	case "snapshot_integrity_failed", "snapshot_open_failed", "snapshot_schema_failed":
		return ArchiveCorrupt, "sqlite_snapshot_invalid"
	case "archive_summary_invalid", "archive_unexpected_file":
		return ArchiveCorrupt, "archive_payload_invalid"
	default:
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return ArchiveIncomplete, "verification_interrupted"
		}
		return ArchiveUnsafe, "archive_unreadable"
	}
}
