// Package replay implements deterministic read-only replay operations over
// published session evidence.
package replay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

// Dependencies are the durable local sources used by restore and verify.
type Dependencies struct {
	DB  *persistence.DB
	CAS *store.LocalStore
}

// VerifyResult describes one successfully authenticated published state.
type VerifyResult struct {
	State        persistence.StateRecord
	Verification state.Verification
}

// RestoreOptions controls materialization of one state.
type RestoreOptions struct {
	StateID     id.StateID
	Destination string
	Force       bool
}

// RestoreResult describes a completed restore.
type RestoreResult struct {
	VerifyResult
	Destination string
}

// DefaultRestoreDestination returns a deterministic new-directory name for a
// state when the caller does not provide --to.
func DefaultRestoreDestination(stateID id.StateID) string {
	return "rappid-restore-" + stateID.String()
}

// VerifyState authenticates the complete CAS graph referenced by a published
// state without executing code or writing to the restored workspace.
func VerifyState(ctx context.Context, deps Dependencies, stateID id.StateID) (VerifyResult, error) {
	if err := validateDependencies(deps); err != nil {
		return VerifyResult{}, err
	}
	record, err := deps.DB.GetState(ctx, stateID)
	if err != nil {
		return VerifyResult{}, err
	}
	verification, err := state.VerifySnapshot(deps.CAS, record.RootTreeID)
	if err != nil {
		return VerifyResult{}, markCorruptState(ctx, deps.DB, record, fmt.Errorf("verify state %s: %w", stateID, err))
	}
	return VerifyResult{State: record, Verification: verification}, nil
}

// RestoreState verifies a published state, validates path portability for the
// current OS, materializes it into a private sibling staging directory, and
// only then commits the completed tree to the requested destination.
func RestoreState(ctx context.Context, deps Dependencies, opts RestoreOptions) (RestoreResult, error) {
	if strings.TrimSpace(opts.Destination) == "" {
		return RestoreResult{}, fmt.Errorf("restore destination is required")
	}
	verified, err := VerifyState(ctx, deps, opts.StateID)
	if err != nil {
		return RestoreResult{}, err
	}
	if err := state.ValidateSnapshotForOS(deps.CAS, verified.State.RootTreeID, runtime.GOOS); err != nil {
		return RestoreResult{}, fmt.Errorf("state %s cannot be materialized on %s: %w", opts.StateID, runtime.GOOS, err)
	}

	destination, existed, err := validateDestination(opts.Destination, opts.Force)
	if err != nil {
		return RestoreResult{}, err
	}
	parent := filepath.Dir(destination)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return RestoreResult{}, fmt.Errorf("create restore parent %q: %w", parent, err)
	}
	staging, err := os.MkdirTemp(parent, "."+filepath.Base(destination)+".rappid-restore-")
	if err != nil {
		return RestoreResult{}, fmt.Errorf("create restore staging directory: %w", err)
	}
	stagingLive := true
	defer func() {
		if stagingLive {
			_ = os.RemoveAll(staging)
		}
	}()

	if err := materializeTree(ctx, deps.CAS, verified.State.RootTreeID, staging); err != nil {
		wrapped := fmt.Errorf("materialize state %s: %w", opts.StateID, err)
		return RestoreResult{}, markCorruptState(ctx, deps.DB, verified.State, wrapped)
	}
	if err := commitRestore(staging, destination, existed, opts.Force); err != nil {
		return RestoreResult{}, err
	}
	stagingLive = false
	return RestoreResult{VerifyResult: verified, Destination: destination}, nil
}

func validateDependencies(deps Dependencies) error {
	if deps.DB == nil {
		return fmt.Errorf("Replay database is required")
	}
	if deps.CAS == nil {
		return fmt.Errorf("Replay CAS is required")
	}
	return nil
}

func validateDestination(name string, force bool) (string, bool, error) {
	abs, err := filepath.Abs(name)
	if err != nil {
		return "", false, fmt.Errorf("resolve restore destination: %w", err)
	}
	abs = filepath.Clean(abs)
	if filepath.Dir(abs) == abs {
		return "", false, fmt.Errorf("refusing to restore over filesystem root %q", abs)
	}
	info, err := os.Lstat(abs)
	if errors.Is(err, os.ErrNotExist) {
		return abs, false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("inspect restore destination %q: %w", abs, err)
	}
	if !force {
		return "", true, fmt.Errorf("restore destination %q already exists; use --force to replace it", abs)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", true, fmt.Errorf("--force restore destination %q must be an existing directory, not %s", abs, info.Mode())
	}
	return abs, true, nil
}

func commitRestore(staging, destination string, existed, force bool) error {
	if !existed {
		if err := os.Rename(staging, destination); err != nil {
			return fmt.Errorf("commit restore to %q: %w", destination, err)
		}
		return nil
	}
	if !force {
		return fmt.Errorf("internal error: existing destination without --force")
	}

	backup := staging + ".previous"
	if err := os.Rename(destination, backup); err != nil {
		return fmt.Errorf("move existing destination aside: %w", err)
	}
	if err := os.Rename(staging, destination); err != nil {
		rollbackErr := os.Rename(backup, destination)
		if rollbackErr != nil {
			return fmt.Errorf("commit restore: %w; rollback also failed: %v", err, rollbackErr)
		}
		return fmt.Errorf("commit restore: %w", err)
	}
	if err := os.RemoveAll(backup); err != nil {
		return fmt.Errorf("restore committed to %q but previous destination cleanup failed: %w", destination, err)
	}
	return nil
}

func materializeTree(ctx context.Context, cas *store.LocalStore, treeID store.ObjectID, directory string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	obj, err := cas.GetObject(treeID)
	if err != nil {
		return fmt.Errorf("load tree %s: %w", treeID, err)
	}
	if obj.Kind != store.ObjectTree {
		return fmt.Errorf("object %s kind = %q, want %q", treeID, obj.Kind, store.ObjectTree)
	}
	tree, err := state.ParseCanonicalTree(obj.Payload)
	if err != nil {
		return fmt.Errorf("parse tree %s: %w", treeID, err)
	}

	for _, entry := range tree.Entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := string(entry.Name)
		path := filepath.Join(directory, name)
		switch entry.Kind {
		case state.EntryDir:
			if err := os.Mkdir(path, 0o700); err != nil {
				return fmt.Errorf("create directory %q: %w", path, err)
			}
			if err := materializeTree(ctx, cas, entry.ObjectID, path); err != nil {
				return err
			}
			if err := os.Chmod(path, os.FileMode(entry.Mode)&os.ModePerm); err != nil {
				return fmt.Errorf("set directory mode %q: %w", path, err)
			}
		case state.EntryFile:
			if err := materializeFile(ctx, cas, entry, path); err != nil {
				return err
			}
		case state.EntrySymlink:
			if err := materializeLink(cas, entry, path); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported tree entry kind %q", entry.Kind)
		}
	}
	return nil
}

func materializeFile(ctx context.Context, cas *store.LocalStore, entry state.Entry, path string) (retErr error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create file %q: %w", path, err)
	}
	defer func() {
		if closeErr := file.Close(); retErr == nil && closeErr != nil {
			retErr = fmt.Errorf("close file %q: %w", path, closeErr)
		}
	}()

	written, err := writeFileObject(ctx, cas, entry.ObjectID, file)
	if err != nil {
		return fmt.Errorf("restore file %q: %w", path, err)
	}
	if written != entry.Size {
		return fmt.Errorf("restore file %q wrote %d bytes, tree declares %d", path, written, entry.Size)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync file %q: %w", path, err)
	}
	if err := file.Chmod(os.FileMode(entry.Mode) & os.ModePerm); err != nil {
		return fmt.Errorf("set file mode %q: %w", path, err)
	}
	return nil
}

func writeFileObject(ctx context.Context, cas *store.LocalStore, objectID store.ObjectID, dst io.Writer) (int64, error) {
	obj, err := cas.GetObject(objectID)
	if err != nil {
		return 0, err
	}
	switch obj.Kind {
	case store.ObjectBlob:
		n, err := writeAll(dst, obj.Payload)
		return int64(n), err
	case store.ObjectChunkList:
		list, err := state.DecodeChunkList(obj.Payload)
		if err != nil {
			return 0, fmt.Errorf("decode chunk list %s: %w", objectID, err)
		}
		var written int64
		for index, ref := range list.Chunks {
			if err := ctx.Err(); err != nil {
				return written, err
			}
			chunk, err := cas.GetObject(ref.ObjectID)
			if err != nil {
				return written, fmt.Errorf("load chunk %d %s: %w", index, ref.ObjectID, err)
			}
			if chunk.Kind != store.ObjectBlob {
				return written, fmt.Errorf("chunk %d %s kind = %q, want %q", index, ref.ObjectID, chunk.Kind, store.ObjectBlob)
			}
			if len(chunk.Payload) != int(ref.Size) {
				return written, fmt.Errorf("chunk %d %s size = %d, list declares %d", index, ref.ObjectID, len(chunk.Payload), ref.Size)
			}
			n, err := writeAll(dst, chunk.Payload)
			written += int64(n)
			if err != nil {
				return written, err
			}
		}
		if written != list.Size {
			return written, fmt.Errorf("chunk list %s wrote %d bytes, declares %d", objectID, written, list.Size)
		}
		return written, nil
	default:
		return 0, fmt.Errorf("file object %s kind = %q", objectID, obj.Kind)
	}
}

func writeAll(dst io.Writer, data []byte) (int, error) {
	written := 0
	for written < len(data) {
		n, err := dst.Write(data[written:])
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func materializeLink(cas *store.LocalStore, entry state.Entry, path string) error {
	obj, err := cas.GetObject(entry.ObjectID)
	if err != nil {
		return fmt.Errorf("load symlink object %s: %w", entry.ObjectID, err)
	}
	if obj.Kind != store.ObjectLink {
		return fmt.Errorf("symlink object %s kind = %q, want %q", entry.ObjectID, obj.Kind, store.ObjectLink)
	}
	if int64(len(obj.Payload)) != entry.Size {
		return fmt.Errorf("symlink object %s size = %d, tree declares %d", entry.ObjectID, len(obj.Payload), entry.Size)
	}
	if err := os.Symlink(string(obj.Payload), path); err != nil {
		return fmt.Errorf("create symlink %q: %w", path, err)
	}
	return nil
}

func markCorruptState(ctx context.Context, db *persistence.DB, record persistence.StateRecord, err error) error {
	if !errors.Is(err, store.ErrCorruptObject) {
		return err
	}
	if degradeErr := db.MarkSessionIntegrityDegraded(ctx, record.SessionID, err.Error()); degradeErr != nil {
		return fmt.Errorf("%w; additionally failed to mark session %s degraded: %v", err, record.SessionID, degradeErr)
	}
	return err
}
