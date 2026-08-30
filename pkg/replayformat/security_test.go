package replayformat

import (
	"archive/zip"
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func TestReadRejectsDuplicateEntries(t *testing.T) {
	var encoded bytes.Buffer
	zw := zip.NewWriter(&encoded)
	for i := 0; i < 2; i++ {
		entry, err := zw.Create(ManifestPath)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write([]byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	_, err := Read(bytes.NewReader(encoded.Bytes()), int64(encoded.Len()))
	if err == nil || !strings.Contains(err.Error(), "duplicate entry") {
		t.Fatalf("Read() error = %v, want duplicate-entry rejection", err)
	}
}

func TestReadRejectsSymlinkEntries(t *testing.T) {
	var encoded bytes.Buffer
	zw := zip.NewWriter(&encoded)
	header := &zip.FileHeader{Name: ManifestPath, Method: zip.Store}
	header.SetMode(os.ModeSymlink | 0o777)
	entry, err := zw.CreateHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.Write([]byte("target")); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = Read(bytes.NewReader(encoded.Bytes()), int64(encoded.Len()))
	if err == nil || !strings.Contains(err.Error(), "must be a regular file") {
		t.Fatalf("Read() error = %v, want symlink-entry rejection", err)
	}
}

func TestReadRejectsChecksumMismatch(t *testing.T) {
	bundle := validTestBundle(t)
	var original bytes.Buffer
	if err := Write(&original, bundle); err != nil {
		t.Fatal(err)
	}

	corrupted := rewriteArchiveEntry(t, original.Bytes(), ManifestPath, func(data []byte) []byte {
		return append(append([]byte(nil), data...), ' ')
	})
	_, err := Read(bytes.NewReader(corrupted), int64(len(corrupted)))
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("Read() error = %v, want checksum mismatch", err)
	}
}

func TestWriteRejectsUnknownRequiredFeature(t *testing.T) {
	bundle := validTestBundle(t)
	bundle.Manifest.RequiredFeatures = []string{"future.feature"}
	var encoded bytes.Buffer
	if err := Write(&encoded, bundle); err == nil || !strings.Contains(err.Error(), "unsupported required_features") {
		t.Fatalf("Write() error = %v, want unsupported required feature", err)
	}
}

func TestValidateArchivePathRejectsUnsafeForms(t *testing.T) {
	for _, name := range []string{
		"/absolute",
		"../parent",
		"a/../b",
		"a\\b",
		"a//b",
		"./a",
	} {
		if err := validateArchivePath(name); err == nil {
			t.Fatalf("validateArchivePath(%q) unexpectedly succeeded", name)
		}
	}
}

func rewriteArchiveEntry(t *testing.T, original []byte, target string, mutate func([]byte) []byte) []byte {
	t.Helper()
	zr, err := zip.NewReader(bytes.NewReader(original), int64(len(original)))
	if err != nil {
		t.Fatal(err)
	}
	var rewritten bytes.Buffer
	zw := zip.NewWriter(&rewritten)
	found := false
	for _, file := range zr.File {
		r, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(r)
		closeErr := r.Close()
		if err != nil {
			t.Fatal(err)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		if file.Name == target {
			data = mutate(data)
			found = true
		}
		header := file.FileHeader
		entry, err := zw.CreateHeader(&header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if !found {
		t.Fatalf("archive entry %q not found", target)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return rewritten.Bytes()
}
