package gitrepo

import (
	"fmt"
	"os"
	"path/filepath"
)

// finder holds injectable OS-level functions so that error paths can be tested
// without modifying the filesystem.
type finder struct {
	absFn  func(string) (string, error)
	statFn func(string) (os.FileInfo, error)
}

// defaultFinder uses the real OS functions.
var defaultFinder = finder{
	absFn:  filepath.Abs,
	statFn: os.Stat,
}

// FindRepoRoot walks up the directory tree from startPath looking for a .git entry
// (either a file or a directory) and returns the absolute path of the first directory
// that contains .git. It returns an error if the filesystem root is reached without
// finding .git.
func FindRepoRoot(startPath string) (string, error) {
	return defaultFinder.find(startPath)
}

// find is the testable implementation of FindRepoRoot.
func (f finder) find(startPath string) (string, error) {
	absPath, err := f.absFn(startPath)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute path for %q: %w", startPath, err)
	}

	// Start from the given path and walk up the directory tree
	current := absPath

	for {
		gitPath := filepath.Join(current, ".git")
		_, err := f.statFn(gitPath)
		if err == nil {
			// .git exists, return this directory
			return current, nil
		}
		if !os.IsNotExist(err) {
			// Some other error occurred (permission denied, etc.)
			return "", fmt.Errorf("failed to check for .git in %q: %w", current, err)
		}

		// Move to parent directory
		parent := filepath.Dir(current)
		if parent == current {
			// We've reached the filesystem root
			return "", fmt.Errorf("no .git directory found in %q or any parent directory", absPath)
		}
		current = parent
	}
}
