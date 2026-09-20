package cli

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestCriticalEntrypointsTracked verifies that critical cmd/* entry points
// exist in the worktree, are not ignored by .gitignore, and are tracked in git.
func TestCriticalEntrypointsTracked(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	repoRoot := ""
	for d := dir; d != "/" && d != "."; d = filepath.Dir(d) {
		if _, err := os.Stat(filepath.Join(d, "go.mod")); err == nil {
			repoRoot = d
			break
		}
	}
	if repoRoot == "" {
		t.Skip("could not determine repository root; skipping tracked entrypoints test")
	}

	criticalPaths := []string{
		"cmd/agy-pool/main.go",
		"cmd/agy-tokei/main.go",
	}

	for _, p := range criticalPaths {
		fullPath := filepath.Join(repoRoot, p)
		if _, err := os.Stat(fullPath); os.IsNotExist(err) {
			t.Errorf("critical entrypoint file missing in worktree: %s", p)
			continue
		}

		// Verify git check-ignore does not ignore the file (exit code 1 means NOT ignored)
		cmdIgnore := exec.Command("git", "check-ignore", "-q", p)
		cmdIgnore.Dir = repoRoot
		if err := cmdIgnore.Run(); err == nil {
			t.Errorf("critical entrypoint %s is unexpectedly ignored by .gitignore", p)
		}

		// Verify file is tracked in git index
		cmdTracked := exec.Command("git", "ls-files", "--error-unmatch", p)
		cmdTracked.Dir = repoRoot
		out, err := cmdTracked.CombinedOutput()
		if err != nil {
			t.Errorf("critical entrypoint %s is not tracked in git: %s (%v)", p, strings.TrimSpace(string(out)), err)
		}
	}
}
