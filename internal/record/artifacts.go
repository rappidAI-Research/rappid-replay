package record

import (
	"bytes"
	"context"
	"fmt"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

type artifactCandidate struct {
	Path []byte
	ChangeKind persistence.ArtifactChangeKind
	ObjectID store.ObjectID
	PreviousObjectID store.ObjectID
	Mode uint32
	Size int64
}

func discoverArtifactDelta(cas *store.LocalStore, fromRoot, toRoot store.ObjectID) ([]artifactCandidate, error) {
	if cas == nil {
		return nil, fmt.Errorf("artifact discovery CAS is required")
	}
	if fromRoot == toRoot {
		return nil, nil
	}
	var artifacts []artifactCandidate
	if err := walkArtifactDelta(cas, fromRoot, toRoot, nil, &artifacts); err != nil {
		return nil, err
	}
	return artifacts, nil
}

func walkArtifactDelta(cas *store.LocalStore, fromRoot, toRoot store.ObjectID, prefix []byte, artifacts *[]artifactCandidate) error {
	toTree, err := loadArtifactTree(cas, toRoot)
	if err != nil {
		return fmt.Errorf("load destination tree %s: %w", toRoot, err)
	}
	fromEntries := make(map[string]state.Entry)
	if fromRoot != "" {
		fromTree, err := loadArtifactTree(cas, fromRoot)
		if err != nil {
			return fmt.Errorf("load source tree %s: %w", fromRoot, err)
		}
		for _, entry := range fromTree.Entries {
			fromEntries[string(entry.Name)] = entry
		}
	}
	for _, current := range toTree.Entries {
		previous, existed := fromEntries[string(current.Name)]
		path := joinArtifactPath(prefix, current.Name)
		switch current.Kind {
		case state.EntryFile:
			candidate := artifactCandidate{Path: path, ObjectID: current.ObjectID, Mode: current.Mode, Size: current.Size}
			switch {
			case !existed:
				candidate.ChangeKind = persistence.ArtifactCreated
			case previous.Kind != state.EntryFile:
				candidate.ChangeKind = persistence.ArtifactReplaced
				candidate.PreviousObjectID = previous.ObjectID
			case previous.ObjectID != current.ObjectID:
				candidate.ChangeKind = persistence.ArtifactModified
				candidate.PreviousObjectID = previous.ObjectID
			default:
				continue
			}
			*artifacts = append(*artifacts, candidate)
		case state.EntryDir:
			if existed && previous.Kind == state.EntryDir {
				if previous.ObjectID == current.ObjectID {
					continue
				}
				if err := walkArtifactDelta(cas, previous.ObjectID, current.ObjectID, path, artifacts); err != nil {
					return err
				}
			} else if err := walkArtifactDelta(cas, "", current.ObjectID, path, artifacts); err != nil {
				return err
			}
		case state.EntrySymlink:
			continue
		default:
			return fmt.Errorf("unknown entry kind %q", current.Kind)
		}
	}
	return nil
}

func loadArtifactTree(cas *store.LocalStore, root store.ObjectID) (state.Tree, error) {
	object, err := cas.GetObject(root)
	if err != nil {
		return state.Tree{}, err
	}
	if object.Kind != store.ObjectTree {
		return state.Tree{}, fmt.Errorf("object kind = %q, want %q", object.Kind, store.ObjectTree)
	}
	return state.ParseCanonicalTree(object.Payload)
}

func joinArtifactPath(prefix, name []byte) []byte {
	if len(prefix) == 0 {
		return bytes.Clone(name)
	}
	path := make([]byte, 0, len(prefix)+1+len(name))
	path = append(path, prefix...)
	path = append(path, '/')
	path = append(path, name...)
	return path
}

func persistArtifactDelta(ctx context.Context, deps Dependencies, sink *eventSink, sessionID id.SessionID, from, to checkpointPosition) error {
	artifacts, err := discoverArtifactDelta(deps.CAS, from.RootTreeID, to.RootTreeID)
	if err != nil {
		return fmt.Errorf("discover workspace artifacts: %w", err)
	}
	for _, artifact := range artifacts {
		if _, err := sink.publishArtifact(ctx, persistence.PublishArtifactRequest{
			SessionID: sessionID,
			FromStateID: from.StateID,
			StateID: to.StateID,
			Path: artifact.Path,
			ChangeKind: artifact.ChangeKind,
			ObjectID: artifact.ObjectID,
			PreviousObjectID: artifact.PreviousObjectID,
			Mode: artifact.Mode,
			Size: artifact.Size,
			Discovery: persistence.ArtifactDiscoveryWorkspaceDelta,
			Source: recorderSource,
		}); err != nil {
			return err
		}
	}
	return nil
}
