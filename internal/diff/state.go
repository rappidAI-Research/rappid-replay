package diff

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

type stateAccumulator struct {
	result StateDiff
	limit  int
}

func diffTrees(cas *store.LocalStore, leftRoot, rightRoot store.ObjectID, limit int) (StateDiff, error) {
	result := StateDiff{
		Comparable:      true,
		LeftRootTreeID:  leftRoot.String(),
		RightRootTreeID: rightRoot.String(),
		Equal:           leftRoot == rightRoot,
	}
	if result.Equal {
		return result, nil
	}
	acc := &stateAccumulator{result: result, limit: limit}
	if err := compareTree(cas, leftRoot, rightRoot, nil, acc); err != nil {
		return StateDiff{}, err
	}
	acc.result.TotalChanges = acc.result.Added + acc.result.Removed + acc.result.Modified + acc.result.TypeChanged
	acc.result.ChangesTruncated = acc.result.TotalChanges > len(acc.result.Changes)
	return acc.result, nil
}

func compareTree(cas *store.LocalStore, leftID, rightID store.ObjectID, prefix [][]byte, acc *stateAccumulator) error {
	if leftID == rightID {
		return nil
	}
	leftTree, err := loadTree(cas, leftID)
	if err != nil {
		return fmt.Errorf("load left tree %s: %w", leftID, err)
	}
	rightTree, err := loadTree(cas, rightID)
	if err != nil {
		return fmt.Errorf("load right tree %s: %w", rightID, err)
	}

	i, j := 0, 0
	for i < len(leftTree.Entries) || j < len(rightTree.Entries) {
		switch {
		case i >= len(leftTree.Entries):
			right := rightTree.Entries[j]
			acc.add(makeChange(prefix, right.Name, "added", subtreeReason(right.Kind), nil, treeNode(right)))
			j++
		case j >= len(rightTree.Entries):
			left := leftTree.Entries[i]
			acc.add(makeChange(prefix, left.Name, "removed", subtreeReason(left.Kind), treeNode(left), nil))
			i++
		default:
			left := leftTree.Entries[i]
			right := rightTree.Entries[j]
			cmp := bytes.Compare(left.Name, right.Name)
			if cmp < 0 {
				acc.add(makeChange(prefix, left.Name, "removed", subtreeReason(left.Kind), treeNode(left), nil))
				i++
				continue
			}
			if cmp > 0 {
				acc.add(makeChange(prefix, right.Name, "added", subtreeReason(right.Kind), nil, treeNode(right)))
				j++
				continue
			}

			path := appendPath(prefix, left.Name)
			if left.Kind != right.Kind {
				acc.add(changeForPath(path, "type_changed", "kind", treeNode(left), treeNode(right)))
				i++
				j++
				continue
			}

			switch left.Kind {
			case state.EntryDir:
				if left.Mode != right.Mode {
					acc.add(changeForPath(path, "modified", "mode", treeNode(left), treeNode(right)))
				}
				if left.ObjectID != right.ObjectID {
					if err := compareTree(cas, left.ObjectID, right.ObjectID, path, acc); err != nil {
						return err
					}
				}
			case state.EntryFile, state.EntrySymlink:
				contentChanged := left.ObjectID != right.ObjectID || left.Size != right.Size
				modeChanged := left.Mode != right.Mode
				if contentChanged || modeChanged {
					reason := "content"
					if !contentChanged && modeChanged {
						reason = "mode"
					} else if contentChanged && modeChanged {
						reason = "content_and_mode"
					}
					acc.add(changeForPath(path, "modified", reason, treeNode(left), treeNode(right)))
				}
			default:
				return fmt.Errorf("unsupported tree entry kind %q", left.Kind)
			}
			i++
			j++
		}
	}
	return nil
}

func loadTree(cas *store.LocalStore, id store.ObjectID) (state.Tree, error) {
	obj, err := cas.GetObject(id)
	if err != nil {
		return state.Tree{}, err
	}
	if obj.Kind != store.ObjectTree {
		return state.Tree{}, fmt.Errorf("object %s kind = %q, want %q", id, obj.Kind, store.ObjectTree)
	}
	tree, err := state.ParseCanonicalTree(obj.Payload)
	if err != nil {
		return state.Tree{}, fmt.Errorf("parse canonical tree: %w", err)
	}
	return tree, nil
}

func (a *stateAccumulator) add(change StateChange) {
	switch change.Change {
	case "added":
		a.result.Added++
	case "removed":
		a.result.Removed++
	case "modified":
		a.result.Modified++
	case "type_changed":
		a.result.TypeChanged++
	}
	if a.limit == 0 || len(a.result.Changes) < a.limit {
		a.result.Changes = append(a.result.Changes, change)
	}
}

func makeChange(prefix [][]byte, name []byte, change, reason string, left, right *TreeNode) StateChange {
	return changeForPath(appendPath(prefix, name), change, reason, left, right)
}

func changeForPath(path [][]byte, change, reason string, left, right *TreeNode) StateChange {
	encoded := make([]string, len(path))
	for i, component := range path {
		encoded[i] = base64.StdEncoding.EncodeToString(component)
	}
	return StateChange{
		PathComponentsB64: encoded,
		DisplayPath:       displayPath(path),
		Change:            change,
		Reason:            reason,
		Left:              left,
		Right:             right,
	}
}

func appendPath(prefix [][]byte, name []byte) [][]byte {
	path := make([][]byte, 0, len(prefix)+1)
	for _, component := range prefix {
		path = append(path, append([]byte(nil), component...))
	}
	path = append(path, append([]byte(nil), name...))
	return path
}

func treeNode(entry state.Entry) *TreeNode {
	return &TreeNode{
		Kind:     string(entry.Kind),
		Mode:     entry.Mode,
		Size:     entry.Size,
		ObjectID: entry.ObjectID.String(),
	}
}

func subtreeReason(kind state.EntryKind) string {
	if kind == state.EntryDir {
		return "subtree"
	}
	return ""
}

func displayPath(path [][]byte) string {
	parts := make([]string, len(path))
	for i, component := range path {
		if safeDisplayComponent(component) {
			parts[i] = string(component)
		} else {
			parts[i] = strconv.QuoteToASCII(string(component))
		}
	}
	return strings.Join(parts, "/")
}

func safeDisplayComponent(component []byte) bool {
	if !utf8.Valid(component) {
		return false
	}
	for _, r := range string(component) {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}
