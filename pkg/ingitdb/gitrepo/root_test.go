package gitrepo

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// realAbsFn is a shorthand for the real filepath.Abs so table-driven tests can
// reference it concisely.
var realAbsFn = filepath.Abs

// realStatFn is a shorthand for the real os.Stat so table-driven tests can
// reference it concisely.
var realStatFn = os.Stat

// errFakeAbs is a sentinel used by the injected absFn stub.
var errFakeAbs = errors.New("fake abs error")

// errFakeStat is a sentinel used by the injected statFn stub.
var errFakeStat = errors.New("fake stat error")

// failingAbsFn always returns errFakeAbs.
func failingAbsFn(_ string) (string, error) {
	return "", errFakeAbs
}

// TestFindRepoRoot_PublicAPI verifies that the public FindRepoRoot function
// delegates to the real OS functions and behaves correctly for the happy path
// and the not-found path.
func TestFindRepoRoot_PublicAPI(t *testing.T) {
	t.Parallel()

	t.Run("finds .git directory", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		gitDir := filepath.Join(tmpDir, ".git")
		if err := os.Mkdir(gitDir, 0o755); err != nil {
			t.Fatalf("setup: create .git dir: %v", err)
		}

		got, err := FindRepoRoot(tmpDir)
		if err != nil {
			t.Fatalf("FindRepoRoot() unexpected error: %v", err)
		}
		if got != tmpDir {
			t.Errorf("FindRepoRoot() = %q, want %q", got, tmpDir)
		}
	})

	t.Run("returns error when no .git found", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		// No .git created — must reach the filesystem root and error.
		_, err := FindRepoRoot(tmpDir)
		if err == nil {
			t.Fatal("FindRepoRoot() expected error, got nil")
		}
		if !strings.Contains(err.Error(), "no .git directory found") {
			t.Errorf("FindRepoRoot() error = %q, want message containing \"no .git directory found\"", err.Error())
		}
	})
}

// TestFinder_Find exercises every branch of finder.find via injected stubs.
func TestFinder_Find(t *testing.T) {
	t.Parallel()

	t.Run("finds .git directory in start path", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		gitDir := filepath.Join(tmpDir, ".git")
		if err := os.Mkdir(gitDir, 0o755); err != nil {
			t.Fatalf("setup: create .git dir: %v", err)
		}

		f := finder{absFn: realAbsFn, statFn: realStatFn}
		got, err := f.find(tmpDir)
		if err != nil {
			t.Fatalf("find() unexpected error: %v", err)
		}
		if got != tmpDir {
			t.Errorf("find() = %q, want %q", got, tmpDir)
		}
	})

	t.Run("finds .git directory in parent path", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		gitDir := filepath.Join(tmpDir, ".git")
		if err := os.Mkdir(gitDir, 0o755); err != nil {
			t.Fatalf("setup: create .git dir: %v", err)
		}
		subDir := filepath.Join(tmpDir, "sub")
		if err := os.Mkdir(subDir, 0o755); err != nil {
			t.Fatalf("setup: create subdir: %v", err)
		}

		f := finder{absFn: realAbsFn, statFn: realStatFn}
		got, err := f.find(subDir)
		if err != nil {
			t.Fatalf("find() unexpected error: %v", err)
		}
		if got != tmpDir {
			t.Errorf("find() = %q, want %q", got, tmpDir)
		}
	})

	t.Run("finds .git file (worktree pointer) in parent path", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		gitFile := filepath.Join(tmpDir, ".git")
		if err := os.WriteFile(gitFile, []byte("gitdir: .git"), 0o644); err != nil {
			t.Fatalf("setup: write .git file: %v", err)
		}
		subDir := filepath.Join(tmpDir, "sub", "dir")
		if err := os.MkdirAll(subDir, 0o755); err != nil {
			t.Fatalf("setup: create nested subdir: %v", err)
		}

		f := finder{absFn: realAbsFn, statFn: realStatFn}
		got, err := f.find(subDir)
		if err != nil {
			t.Fatalf("find() unexpected error: %v", err)
		}
		if got != tmpDir {
			t.Errorf("find() = %q, want %q", got, tmpDir)
		}
	})

	t.Run("returns error when no .git found anywhere", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()

		f := finder{absFn: realAbsFn, statFn: realStatFn}
		_, err := f.find(tmpDir)
		if err == nil {
			t.Fatal("find() expected error, got nil")
		}
		if !strings.Contains(err.Error(), "no .git directory found") {
			t.Errorf("find() error = %q, want message containing \"no .git directory found\"", err.Error())
		}
	})

	t.Run("absFn error is propagated", func(t *testing.T) {
		t.Parallel()
		f := finder{absFn: failingAbsFn, statFn: realStatFn}
		_, err := f.find("any-path")
		if err == nil {
			t.Fatal("find() expected error from absFn, got nil")
		}
		if !errors.Is(err, errFakeAbs) {
			t.Errorf("find() error = %v, want wrapping errFakeAbs", err)
		}
		if !strings.Contains(err.Error(), "failed to get absolute path") {
			t.Errorf("find() error = %q, want message containing \"failed to get absolute path\"", err.Error())
		}
	})

	t.Run("statFn non-NotExist error is propagated", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()

		// statFn always returns a non-IsNotExist error so the permission-denied
		// branch (line "if !os.IsNotExist(err)") is exercised.
		stubStatFn := func(_ string) (os.FileInfo, error) {
			return nil, errFakeStat
		}

		f := finder{absFn: realAbsFn, statFn: stubStatFn}
		_, err := f.find(tmpDir)
		if err == nil {
			t.Fatal("find() expected error from statFn, got nil")
		}
		if !errors.Is(err, errFakeStat) {
			t.Errorf("find() error = %v, want wrapping errFakeStat", err)
		}
		if !strings.Contains(err.Error(), "failed to check for .git") {
			t.Errorf("find() error = %q, want message containing \"failed to check for .git\"", err.Error())
		}
	})
}
