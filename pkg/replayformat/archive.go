package replayformat

import (
	"archive/zip"
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
	"lukechampine.com/blake3"
)

const (
	maxArchiveEntries           = 250000
	maxArchiveUncompressedBytes = 64 << 30
	maxManifestBytes            = 4 << 20
	maxSessionMetadataBytes     = 4 << 20
	maxEnvironmentBytes         = 16 << 20
	maxCompressedListBytes      = 512 << 20
	maxNDJSONLineBytes          = 8 << 20
	maxObjectFrameBytes         = 64 << 20
	maxChecksumsBytes           = 64 << 20
)

var archiveTimestamp = time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)

// Write serializes a validated logical bundle as a deterministic ZIP64-capable
// .rplay archive. Large event/state/artifact lists are zstd-compressed NDJSON.
func Write(dst io.Writer, bundle Bundle) error {
	if dst == nil {
		return fmt.Errorf("archive destination is required")
	}
	if err := validateBundle(bundle); err != nil {
		return err
	}
	if err := ValidateObjectGraphs(bundle); err != nil {
		return err
	}

	zw := zip.NewWriter(dst)
	checksums := make(map[string]string)
	writeEntry := func(name string, write func(io.Writer) error) error {
		header := &zip.FileHeader{Name: name, Method: zip.Store}
		header.SetMode(0o600)
		header.SetModTime(archiveTimestamp)
		entry, err := zw.CreateHeader(header)
		if err != nil {
			return fmt.Errorf("create archive entry %q: %w", name, err)
		}
		hasher := blake3.New(32, nil)
		if err := write(io.MultiWriter(entry, hasher)); err != nil {
			return fmt.Errorf("write archive entry %q: %w", name, err)
		}
		checksums[name] = hex.EncodeToString(hasher.Sum(nil))
		return nil
	}

	manifestBytes, err := json.Marshal(bundle.Manifest)
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}
	if err := writeEntry(ManifestPath, func(w io.Writer) error {
		_, err := w.Write(manifestBytes)
		return err
	}); err != nil {
		return err
	}

	sessions := append([]SessionData(nil), bundle.Sessions...)
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].Metadata.ID < sessions[j].Metadata.ID })
	for _, session := range sessions {
		prefix := "sessions/" + session.Metadata.ID + "/"
		metadata, err := json.Marshal(session.Metadata)
		if err != nil {
			return fmt.Errorf("encode session %s metadata: %w", session.Metadata.ID, err)
		}
		if err := writeEntry(prefix+"session.json", func(w io.Writer) error {
			_, err := w.Write(metadata)
			return err
		}); err != nil {
			return err
		}
		if err := writeEntry(prefix+"events.ndjson.zst", func(w io.Writer) error {
			return writeRawNDJSONZstd(w, session.Events)
		}); err != nil {
			return err
		}

		stateLines := make([]json.RawMessage, 0, len(session.States))
		for _, item := range session.States {
			encoded, err := json.Marshal(item)
			if err != nil {
				return fmt.Errorf("encode state %s: %w", item.ID, err)
			}
			stateLines = append(stateLines, encoded)
		}
		if err := writeEntry(prefix+"states.ndjson.zst", func(w io.Writer) error {
			return writeRawNDJSONZstd(w, stateLines)
		}); err != nil {
			return err
		}

		if len(session.Environment) != 0 {
			if err := writeEntry(prefix+"environment.json", func(w io.Writer) error {
				_, err := w.Write(session.Environment)
				return err
			}); err != nil {
				return err
			}
		}

		artifactLines := make([]json.RawMessage, 0, len(session.Artifacts))
		for _, item := range session.Artifacts {
			encoded, err := json.Marshal(item)
			if err != nil {
				return fmt.Errorf("encode artifact %s: %w", item.ID, err)
			}
			artifactLines = append(artifactLines, encoded)
		}
		if err := writeEntry(prefix+"artifacts.ndjson.zst", func(w io.Writer) error {
			return writeRawNDJSONZstd(w, artifactLines)
		}); err != nil {
			return err
		}
	}

	objectIDs := make([]string, 0, len(bundle.Objects))
	for id := range bundle.Objects {
		objectIDs = append(objectIDs, id)
	}
	sort.Strings(objectIDs)
	for _, id := range objectIDs {
		framed := bundle.Objects[id]
		name := objectPath(id)
		if err := writeEntry(name, func(w io.Writer) error {
			_, err := w.Write(framed)
			return err
		}); err != nil {
			return err
		}
	}

	names := make([]string, 0, len(checksums))
	for name := range checksums {
		names = append(names, name)
	}
	sort.Strings(names)
	var checksumText strings.Builder
	for _, name := range names {
		fmt.Fprintf(&checksumText, "%s  %s\n", checksums[name], name)
	}
	header := &zip.FileHeader{Name: ChecksumsPath, Method: zip.Store}
	header.SetMode(0o600)
	header.SetModTime(archiveTimestamp)
	entry, err := zw.CreateHeader(header)
	if err != nil {
		return fmt.Errorf("create checksums entry: %w", err)
	}
	if _, err := io.WriteString(entry, checksumText.String()); err != nil {
		return fmt.Errorf("write checksums entry: %w", err)
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("close .rplay archive: %w", err)
	}
	return nil
}

// Read validates archive paths, BLAKE3 entry checksums, manifest compatibility,
// typed-object identities, complete state graphs, and structured session entries
// before returning data.
func Read(src io.ReaderAt, size int64) (Bundle, error) {
	if src == nil || size <= 0 {
		return Bundle{}, fmt.Errorf("archive source is required")
	}
	zr, err := zip.NewReader(src, size)
	if err != nil {
		return Bundle{}, fmt.Errorf("open .rplay archive: %w", err)
	}
	if len(zr.File) == 0 || len(zr.File) > maxArchiveEntries {
		return Bundle{}, fmt.Errorf("archive entry count %d is outside supported bounds", len(zr.File))
	}

	files := make(map[string]*zip.File, len(zr.File))
	var totalUncompressed uint64
	for _, file := range zr.File {
		if err := validateArchivePath(file.Name); err != nil {
			return Bundle{}, err
		}
		if file.FileInfo().IsDir() || file.Mode()&fs.ModeSymlink != 0 || !file.Mode().IsRegular() {
			return Bundle{}, fmt.Errorf("archive entry %q must be a regular file", file.Name)
		}
		if _, exists := files[file.Name]; exists {
			return Bundle{}, fmt.Errorf("archive contains duplicate entry %q", file.Name)
		}
		if file.UncompressedSize64 > uint64(maxArchiveUncompressedBytes)-totalUncompressed {
			return Bundle{}, fmt.Errorf("archive exceeds %d-byte uncompressed limit", maxArchiveUncompressedBytes)
		}
		totalUncompressed += file.UncompressedSize64
		files[file.Name] = file
	}

	checksumFile, ok := files[ChecksumsPath]
	if !ok {
		return Bundle{}, fmt.Errorf("archive is missing %s", ChecksumsPath)
	}
	checksumBytes, err := readZipFile(checksumFile, maxChecksumsBytes)
	if err != nil {
		return Bundle{}, err
	}
	checksums, err := parseChecksums(checksumBytes)
	if err != nil {
		return Bundle{}, err
	}
	if len(checksums) != len(files)-1 {
		return Bundle{}, fmt.Errorf("checksum coverage = %d entries, want %d", len(checksums), len(files)-1)
	}
	for name, file := range files {
		if name == ChecksumsPath {
			continue
		}
		expected, ok := checksums[name]
		if !ok {
			return Bundle{}, fmt.Errorf("archive entry %q is not covered by checksums", name)
		}
		if err := verifyZipChecksum(file, expected); err != nil {
			return Bundle{}, err
		}
	}
	for name := range checksums {
		if _, ok := files[name]; !ok {
			return Bundle{}, fmt.Errorf("checksums references missing entry %q", name)
		}
	}

	manifestFile, ok := files[ManifestPath]
	if !ok {
		return Bundle{}, fmt.Errorf("archive is missing %s", ManifestPath)
	}
	manifestBytes, err := readZipFile(manifestFile, maxManifestBytes)
	if err != nil {
		return Bundle{}, err
	}
	var manifest Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return Bundle{}, fmt.Errorf("decode manifest: %w", err)
	}
	if err := ValidateManifest(manifest); err != nil {
		return Bundle{}, err
	}
	if err := validateArchiveEntrySet(files, manifest); err != nil {
		return Bundle{}, err
	}

	bundle := Bundle{Manifest: manifest, Objects: make(map[string][]byte)}
	for _, descriptor := range manifest.Sessions {
		prefix := "sessions/" + descriptor.ID + "/"
		metadataBytes, err := readRequired(files, prefix+"session.json", maxSessionMetadataBytes)
		if err != nil {
			return Bundle{}, err
		}
		var metadata Session
		if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
			return Bundle{}, fmt.Errorf("decode session %s metadata: %w", descriptor.ID, err)
		}
		if metadata.ID != descriptor.ID || metadata.ParentSessionID != descriptor.ParentSessionID || metadata.ForkEventSeq != descriptor.ForkEventSeq {
			return Bundle{}, fmt.Errorf("session %s metadata does not match manifest lineage", descriptor.ID)
		}

		eventLines, err := readZstdNDJSONRequired(files, prefix+"events.ndjson.zst")
		if err != nil {
			return Bundle{}, err
		}
		stateLines, err := readZstdNDJSONRequired(files, prefix+"states.ndjson.zst")
		if err != nil {
			return Bundle{}, err
		}
		states := make([]State, 0, len(stateLines))
		for _, line := range stateLines {
			var item State
			if err := json.Unmarshal(line, &item); err != nil {
				return Bundle{}, fmt.Errorf("decode state for session %s: %w", descriptor.ID, err)
			}
			states = append(states, item)
		}

		artifactLines, err := readZstdNDJSONRequired(files, prefix+"artifacts.ndjson.zst")
		if err != nil {
			return Bundle{}, err
		}
		artifacts := make([]Artifact, 0, len(artifactLines))
		for _, line := range artifactLines {
			var item Artifact
			if err := json.Unmarshal(line, &item); err != nil {
				return Bundle{}, fmt.Errorf("decode artifact for session %s: %w", descriptor.ID, err)
			}
			artifacts = append(artifacts, item)
		}

		var environment json.RawMessage
		if file, ok := files[prefix+"environment.json"]; ok {
			value, err := readZipFile(file, maxEnvironmentBytes)
			if err != nil {
				return Bundle{}, err
			}
			if !json.Valid(value) {
				return Bundle{}, fmt.Errorf("session %s environment is invalid JSON", descriptor.ID)
			}
			environment = append(json.RawMessage(nil), value...)
		}
		bundle.Sessions = append(bundle.Sessions, SessionData{
			Metadata:    metadata,
			Events:      eventLines,
			States:      states,
			Environment: environment,
			Artifacts:   artifacts,
		})
	}

	for name, file := range files {
		if !strings.HasPrefix(name, "objects/") {
			continue
		}
		if !strings.HasSuffix(name, ".rpobj") {
			return Bundle{}, fmt.Errorf("unexpected object archive entry %q", name)
		}
		digest := strings.TrimSuffix(strings.TrimPrefix(name, "objects/"), ".rpobj")
		idText := "b3:" + digest
		id, err := store.ParseObjectID(idText)
		if err != nil {
			return Bundle{}, fmt.Errorf("invalid object archive path %q: %w", name, err)
		}
		framed, err := readZipFile(file, maxObjectFrameBytes)
		if err != nil {
			return Bundle{}, err
		}
		if store.SumObject(framed) != id {
			return Bundle{}, fmt.Errorf("object %s content hash does not match archive path", id)
		}
		if _, err := store.DecodeObject(framed); err != nil {
			return Bundle{}, fmt.Errorf("object %s frame is invalid: %w", id, err)
		}
		bundle.Objects[idText] = framed
	}
	if err := validateBundle(bundle); err != nil {
		return Bundle{}, err
	}
	if err := ValidateObjectGraphs(bundle); err != nil {
		return Bundle{}, err
	}
	return bundle, nil
}

func validateBundle(bundle Bundle) error {
	if err := ValidateManifest(bundle.Manifest); err != nil {
		return err
	}
	if len(bundle.Sessions) != len(bundle.Manifest.Sessions) {
		return fmt.Errorf("bundle session count = %d, manifest declares %d", len(bundle.Sessions), len(bundle.Manifest.Sessions))
	}

	descriptors := make(map[string]SessionDescriptor, len(bundle.Manifest.Sessions))
	for _, item := range bundle.Manifest.Sessions {
		descriptors[item.ID] = item
	}
	seenSessions := make(map[string]struct{}, len(bundle.Sessions))
	allStates := make(map[string]State)
	for _, session := range bundle.Sessions {
		descriptor, ok := descriptors[session.Metadata.ID]
		if !ok {
			return fmt.Errorf("bundle contains undeclared session %s", session.Metadata.ID)
		}
		if _, exists := seenSessions[session.Metadata.ID]; exists {
			return fmt.Errorf("bundle contains duplicate session %s", session.Metadata.ID)
		}
		seenSessions[session.Metadata.ID] = struct{}{}
		if session.Metadata.ParentSessionID != descriptor.ParentSessionID || session.Metadata.ForkEventSeq != descriptor.ForkEventSeq {
			return fmt.Errorf("session %s lineage differs from manifest", session.Metadata.ID)
		}
		if len(session.Metadata.Command) == 0 || session.Metadata.Command[0] == "" {
			return fmt.Errorf("session %s has empty command", session.Metadata.ID)
		}
		if session.Metadata.CWD == "" || session.Metadata.StartedAt.IsZero() {
			return fmt.Errorf("session %s has incomplete metadata", session.Metadata.ID)
		}
		if session.Metadata.Status == "recording" || session.Metadata.Status == "" {
			return fmt.Errorf("session %s is not sealed", session.Metadata.ID)
		}
		for i, raw := range session.Events {
			if len(raw) == 0 || !json.Valid(raw) {
				return fmt.Errorf("session %s event line %d is invalid JSON", session.Metadata.ID, i+1)
			}
		}
		for _, item := range session.States {
			if item.ID == "" || item.SessionID != session.Metadata.ID || item.EventSeq == 0 || item.RootTreeID == "" || item.CreatedAt.IsZero() {
				return fmt.Errorf("session %s contains invalid state metadata", session.Metadata.ID)
			}
			if _, exists := allStates[item.ID]; exists {
				return fmt.Errorf("duplicate state id %s", item.ID)
			}
			allStates[item.ID] = item
			if _, ok := bundle.Objects[item.RootTreeID]; !ok {
				return fmt.Errorf("state %s references missing root object %s", item.ID, item.RootTreeID)
			}
		}
		if len(session.Environment) != 0 && !json.Valid(session.Environment) {
			return fmt.Errorf("session %s environment is invalid JSON", session.Metadata.ID)
		}
	}

	for _, session := range bundle.Sessions {
		if session.Metadata.InitialStateID != "" {
			stateRecord, ok := allStates[session.Metadata.InitialStateID]
			if !ok || stateRecord.SessionID != session.Metadata.ID {
				return fmt.Errorf("session %s initial state %s is missing or foreign", session.Metadata.ID, session.Metadata.InitialStateID)
			}
		}
		if session.Metadata.FinalStateID != "" {
			stateRecord, ok := allStates[session.Metadata.FinalStateID]
			if !ok || stateRecord.SessionID != session.Metadata.ID {
				return fmt.Errorf("session %s final state %s is missing or foreign", session.Metadata.ID, session.Metadata.FinalStateID)
			}
		}
		for _, artifact := range session.Artifacts {
			if artifact.ID == "" || artifact.SessionID != session.Metadata.ID || artifact.EventSeq == 0 || artifact.StateID == "" || artifact.FromStateID == "" || artifact.ObjectID == "" {
				return fmt.Errorf("session %s contains invalid artifact metadata", session.Metadata.ID)
			}
			fromState, fromOK := allStates[artifact.FromStateID]
			toState, toOK := allStates[artifact.StateID]
			if !fromOK || !toOK || fromState.SessionID != session.Metadata.ID || toState.SessionID != session.Metadata.ID {
				return fmt.Errorf("artifact %s references missing or foreign state", artifact.ID)
			}
			if _, ok := bundle.Objects[artifact.ObjectID]; !ok {
				return fmt.Errorf("artifact %s references missing object %s", artifact.ID, artifact.ObjectID)
			}
			if artifact.PreviousObjectID != "" {
				if _, ok := bundle.Objects[artifact.PreviousObjectID]; !ok {
					return fmt.Errorf("artifact %s references missing previous object %s", artifact.ID, artifact.PreviousObjectID)
				}
			}
		}
	}

	for id, framed := range bundle.Objects {
		parsed, err := store.ParseObjectID(id)
		if err != nil {
			return fmt.Errorf("invalid bundle object id %q: %w", id, err)
		}
		if store.SumObject(framed) != parsed {
			return fmt.Errorf("bundle object %s has mismatched content hash", id)
		}
		if _, err := store.DecodeObject(framed); err != nil {
			return fmt.Errorf("bundle object %s is invalid: %w", id, err)
		}
	}
	return nil
}

func validateArchiveEntrySet(files map[string]*zip.File, manifest Manifest) error {
	allowed := map[string]struct{}{
		ManifestPath:  {},
		ChecksumsPath: {},
	}
	for _, descriptor := range manifest.Sessions {
		prefix := "sessions/" + descriptor.ID + "/"
		allowed[prefix+"session.json"] = struct{}{}
		allowed[prefix+"events.ndjson.zst"] = struct{}{}
		allowed[prefix+"states.ndjson.zst"] = struct{}{}
		allowed[prefix+"artifacts.ndjson.zst"] = struct{}{}
		allowed[prefix+"environment.json"] = struct{}{}
	}
	for name := range files {
		if strings.HasPrefix(name, "objects/") {
			continue
		}
		if _, ok := allowed[name]; !ok {
			return fmt.Errorf("archive contains unexpected entry %q", name)
		}
	}
	return nil
}

func writeRawNDJSONZstd(dst io.Writer, lines []json.RawMessage) error {
	encoder, err := zstd.NewWriter(dst, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return err
	}
	for _, line := range lines {
		if len(line) == 0 || !json.Valid(line) {
			_ = encoder.Close()
			return fmt.Errorf("NDJSON contains invalid JSON")
		}
		if _, err := encoder.Write(line); err != nil {
			_ = encoder.Close()
			return err
		}
		if _, err := encoder.Write([]byte{'\n'}); err != nil {
			_ = encoder.Close()
			return err
		}
	}
	return encoder.Close()
}

func readZstdNDJSONRequired(files map[string]*zip.File, name string) ([]json.RawMessage, error) {
	file, ok := files[name]
	if !ok {
		return nil, fmt.Errorf("archive is missing %s", name)
	}
	compressed, err := readZipFile(file, maxCompressedListBytes)
	if err != nil {
		return nil, err
	}
	decoder, err := zstd.NewReader(bytes.NewReader(compressed), zstd.WithDecoderMaxMemory(maxCompressedListBytes))
	if err != nil {
		return nil, fmt.Errorf("open zstd entry %q: %w", name, err)
	}
	defer decoder.Close()

	scanner := bufio.NewScanner(io.LimitReader(decoder, maxCompressedListBytes+1))
	scanner.Buffer(make([]byte, 64<<10), maxNDJSONLineBytes)
	lines := make([]json.RawMessage, 0)
	var total int64
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		total += int64(len(line)) + 1
		if total > maxCompressedListBytes {
			return nil, fmt.Errorf("decompressed entry %q exceeds supported limit", name)
		}
		if len(line) == 0 || !json.Valid(line) {
			return nil, fmt.Errorf("entry %q contains invalid NDJSON", name)
		}
		lines = append(lines, json.RawMessage(line))
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read entry %q: %w", name, err)
	}
	return lines, nil
}

func readRequired(files map[string]*zip.File, name string, limit int64) ([]byte, error) {
	file, ok := files[name]
	if !ok {
		return nil, fmt.Errorf("archive is missing %s", name)
	}
	return readZipFile(file, limit)
}

func readZipFile(file *zip.File, limit int64) ([]byte, error) {
	if file.UncompressedSize64 > uint64(limit) {
		return nil, fmt.Errorf("archive entry %q exceeds %d-byte limit", file.Name, limit)
	}
	r, err := file.Open()
	if err != nil {
		return nil, fmt.Errorf("open archive entry %q: %w", file.Name, err)
	}
	defer r.Close()
	data, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read archive entry %q: %w", file.Name, err)
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("archive entry %q exceeds %d-byte limit", file.Name, limit)
	}
	return data, nil
}

func validateArchivePath(name string) error {
	if name == "" || strings.ContainsRune(name, '\\') || strings.HasPrefix(name, "/") || path.IsAbs(name) || path.Clean(name) != name {
		return fmt.Errorf("unsafe archive path %q", name)
	}
	for _, part := range strings.Split(name, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("unsafe archive path %q", name)
		}
	}
	return nil
}

func parseChecksums(data []byte) (map[string]string, error) {
	result := make(map[string]string)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 64<<10), maxNDJSONLineBytes)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "  ", 2)
		if len(parts) != 2 || len(parts[0]) != 64 {
			return nil, fmt.Errorf("invalid checksum line %q", line)
		}
		if _, err := hex.DecodeString(parts[0]); err != nil || parts[0] != strings.ToLower(parts[0]) {
			return nil, fmt.Errorf("invalid BLAKE3 checksum %q", parts[0])
		}
		if err := validateArchivePath(parts[1]); err != nil {
			return nil, err
		}
		if parts[1] == ChecksumsPath {
			return nil, fmt.Errorf("checksums file cannot checksum itself")
		}
		if _, exists := result[parts[1]]; exists {
			return nil, fmt.Errorf("duplicate checksum for %q", parts[1])
		}
		result[parts[1]] = parts[0]
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read checksums: %w", err)
	}
	return result, nil
}

func verifyZipChecksum(file *zip.File, expected string) error {
	r, err := file.Open()
	if err != nil {
		return fmt.Errorf("open archive entry %q: %w", file.Name, err)
	}
	defer r.Close()
	hasher := blake3.New(32, nil)
	if _, err := io.Copy(hasher, r); err != nil {
		return fmt.Errorf("hash archive entry %q: %w", file.Name, err)
	}
	got := hex.EncodeToString(hasher.Sum(nil))
	if got != expected {
		return fmt.Errorf("archive checksum mismatch for %q", file.Name)
	}
	return nil
}

func objectPath(id string) string {
	return "objects/" + strings.TrimPrefix(id, "b3:") + ".rpobj"
}
