package repodojo

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestRun(t *testing.T) {
	tempDir := t.TempDir()

	runOpts := RunOpts{
		Out: io.Discard,
		Err: io.Discard,
	}

	opts := RepoOpts{
		Hash:   SHA1Hash,
		IsTest: true,
	}

	// init repo
	err := Run(opts, []string{"init", "repo"}, tempDir, runOpts)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}

	workPath := filepath.Join(tempDir, "repo")

	// verify .git dir exists
	gitDir := filepath.Join(workPath, ".git")
	info, err := os.Stat(gitDir)
	if err != nil {
		t.Fatalf(".git dir not found: %v", err)
	}
	if !info.IsDir() {
		t.Fatal(".git is not a directory")
	}

	// verify subdirectories
	for _, sub := range []string{"objects", "objects/pack", "refs", "refs/heads"} {
		p := filepath.Join(gitDir, sub)
		info, err := os.Stat(p)
		if err != nil {
			t.Fatalf("%s not found: %v", sub, err)
		}
		if !info.IsDir() {
			t.Fatalf("%s is not a directory", sub)
		}
	}

	// verify HEAD exists and points to master
	headPath := filepath.Join(gitDir, "HEAD")
	headContent, err := os.ReadFile(headPath)
	if err != nil {
		t.Fatalf("HEAD not found: %v", err)
	}
	expected := "ref: refs/heads/master\n"
	if string(headContent) != expected {
		t.Fatalf("HEAD content = %q, want %q", string(headContent), expected)
	}

	// verify we can open the repo
	repo, err := OpenRepo(workPath, opts)
	if err != nil {
		t.Fatalf("open repo failed: %v", err)
	}
	defer repo.Close()
}
