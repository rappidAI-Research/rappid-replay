package ignore_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/internal/ignore"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestPolicyGlobSemanticsAndReservedGit(t *testing.T) {
	policy, err := ignore.New([]string{
		"node_modules/**",
		"**/*.log",
		"/root-only/**",
		"cache/",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	tests := []struct {
		path  string
		dir   bool
		want  bool
		label string
	}{
		{path: ".git", dir: true, want: true, label: "reserved root git directory"},
		{path: "nested/.git/config", want: true, label: "reserved nested git metadata"},
		{path: "node_modules", dir: true, want: true, label: "globstar matches zero descendants"},
		{path: "packages/web/node_modules/react/index.js", want: true, label: "unanchored pattern matches at depth"},
		{path: "logs/run.log", want: true, label: "globstar log file"},
		{path: "run.log", want: true, label: "globstar may match zero directories"},
		{path: "root-only", dir: true, want: true, label: "anchored root directory"},
		{path: "root-only/a.txt", want: true, label: "anchored root descendants"},
		{path: "nested/root-only/a.txt", want: false, label: "anchored pattern does not float"},
		{path: "cache", dir: true, want: true, label: "directory-only pattern"},
		{path: "cache", dir: false, want: false, label: "directory-only pattern does not match file"},
		{path: "src/main.go", want: false, label: "ordinary source remains included"},
	}

	for _, test := range tests {
		t.Run(test.label, func(t *testing.T) {
			if got := policy.Match(test.path, test.dir); got != test.want {
				t.Fatalf("Match(%q, dir=%v) = %v, want %v", test.path, test.dir, got, test.want)
			}
		})
	}
}

func TestPolicyRejectsAmbiguousPatterns(t *testing.T) {
	for _, pattern := range []string{"", " ../secret", "../secret", "foo\\bar", "foo/**bar", "!keep.txt", "foo//bar"} {
		t.Run(pattern, func(t *testing.T) {
			if _, err := ignore.New([]string{pattern}); err == nil {
				t.Fatalf("New(%q) succeeded, want error", pattern)
			}
		})
	}
}

func TestSnapshotterHonorsIgnorePolicy(t *testing.T) {
	workspace := t.TempDir()
	mustMkdirAll(t, filepath.Join(workspace, ".git"))
	mustMkdirAll(t, filepath.Join(workspace, "node_modules", "pkg"))
	mustMkdirAll(t, filepath.Join(workspace, "src"))
	mustWriteFile(t, filepath.Join(workspace, ".git", "config"), []byte("sensitive git metadata"))
	mustWriteFile(t, filepath.Join(workspace, "node_modules", "pkg", "index.js"), []byte("generated dependency"))
	mustWriteFile(t, filepath.Join(workspace, "src", "main.go"), []byte("package main\n"))

	policy, err := ignore.New([]string{"node_modules/**"})
	if err != nil {
		t.Fatal(err)
	}
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x31}, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cas.Close() })

	snapshot, err := (state.Snapshotter{CAS: cas, Exclude: policy.Exclude}).Capture(workspace)
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if snapshot.Files != 1 || snapshot.Directories != 1 {
		t.Fatalf("snapshot stats = %+v, want one source file in one included directory", snapshot)
	}

	rootObject, err := cas.GetObject(snapshot.RootTreeID)
	if err != nil {
		t.Fatal(err)
	}
	rootTree, err := state.ParseCanonicalTree(rootObject.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(rootTree.Entries) != 1 || string(rootTree.Entries[0].Name) != "src" {
		t.Fatalf("root entries = %+v, want only src", rootTree.Entries)
	}
}

func mustMkdirAll(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
}

func mustWriteFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
