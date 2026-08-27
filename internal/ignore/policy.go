// Package ignore implements Replay's deterministic workspace exclusion policy.
package ignore

import (
	"fmt"
	"io/fs"
	"path"
	"strings"
)

// Policy is an immutable set of compiled workspace-relative ignore patterns.
// Logical paths always use '/' separators, independent of the host OS.
type Policy struct {
	patterns []pattern
}

type pattern struct {
	raw      string
	anchored bool
	dirOnly  bool
	segments []string
}

// New compiles ignore patterns. Replay intentionally implements a small,
// deterministic glob language instead of inheriting host-shell semantics:
//
//   - *, ?, and [...] match within one path component.
//   - ** is valid only as a complete component and matches zero or more
//     components.
//   - a leading / anchors a pattern at the workspace root.
//   - an unanchored pattern may match at any path depth.
//   - a trailing / restricts a match to directories.
//
// Negation is deliberately not part of v1. The .git directory is a reserved
// exclusion and cannot be re-included by configuration.
func New(patterns []string) (Policy, error) {
	compiled := make([]pattern, 0, len(patterns))
	for _, raw := range patterns {
		item, err := compile(raw)
		if err != nil {
			return Policy{}, err
		}
		compiled = append(compiled, item)
	}
	return Policy{patterns: compiled}, nil
}

// Exclude adapts a Policy to state.Snapshotter's exclusion callback without
// creating a dependency from this package to the state package.
func (p Policy) Exclude(relPath string, entry fs.DirEntry) bool {
	return p.Match(relPath, entry.IsDir())
}

// Match reports whether a logical workspace-relative path is excluded.
func (p Policy) Match(relPath string, isDir bool) bool {
	segments, ok := logicalSegments(relPath)
	if !ok {
		return false
	}
	if containsReservedGit(segments) {
		return true
	}

	for _, item := range p.patterns {
		if item.dirOnly && !isDir {
			continue
		}
		if item.matches(segments) {
			return true
		}
	}
	return false
}

func compile(raw string) (pattern, error) {
	if raw == "" {
		return pattern{}, fmt.Errorf("ignore pattern must not be empty")
	}
	if strings.TrimSpace(raw) != raw {
		return pattern{}, fmt.Errorf("ignore pattern %q has leading or trailing whitespace", raw)
	}
	if strings.HasPrefix(raw, "!") {
		return pattern{}, fmt.Errorf("ignore pattern %q uses unsupported negation", raw)
	}
	if strings.Contains(raw, "\\") {
		return pattern{}, fmt.Errorf("ignore pattern %q must use '/' separators", raw)
	}

	item := pattern{raw: raw}
	value := raw
	if strings.HasPrefix(value, "/") {
		item.anchored = true
		value = strings.TrimPrefix(value, "/")
	}
	if strings.HasSuffix(value, "/") {
		item.dirOnly = true
		value = strings.TrimSuffix(value, "/")
	}
	if value == "" {
		return pattern{}, fmt.Errorf("ignore pattern %q does not name a path", raw)
	}

	parts := strings.Split(value, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return pattern{}, fmt.Errorf("ignore pattern %q contains an invalid path component", raw)
		}
		if strings.Contains(part, "**") && part != "**" {
			return pattern{}, fmt.Errorf("ignore pattern %q must use ** as a complete path component", raw)
		}
		if part != "**" {
			if _, err := path.Match(part, "probe"); err != nil {
				return pattern{}, fmt.Errorf("ignore pattern %q: %w", raw, err)
			}
		}
	}
	item.segments = parts
	return item, nil
}

func (p pattern) matches(candidate []string) bool {
	if p.anchored {
		return matchSegments(p.segments, candidate)
	}
	for start := 0; start < len(candidate); start++ {
		if matchSegments(p.segments, candidate[start:]) {
			return true
		}
	}
	return false
}

func matchSegments(patternSegments, candidate []string) bool {
	type key struct{ pattern, candidate int }
	memo := make(map[key]bool)
	seen := make(map[key]bool)

	var match func(int, int) bool
	match = func(pi, ci int) bool {
		state := key{pattern: pi, candidate: ci}
		if seen[state] {
			return memo[state]
		}
		seen[state] = true

		var result bool
		switch {
		case pi == len(patternSegments):
			result = ci == len(candidate)
		case patternSegments[pi] == "**":
			result = match(pi+1, ci) || (ci < len(candidate) && match(pi, ci+1))
		case ci < len(candidate):
			componentMatch, err := path.Match(patternSegments[pi], candidate[ci])
			result = err == nil && componentMatch && match(pi+1, ci+1)
		default:
			result = false
		}
		memo[state] = result
		return result
	}
	return match(0, 0)
}

func logicalSegments(relPath string) ([]string, bool) {
	if relPath == "" || strings.HasPrefix(relPath, "/") || strings.ContainsRune(relPath, '\x00') {
		return nil, false
	}
	parts := strings.Split(relPath, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return nil, false
		}
	}
	return parts, true
}

func containsReservedGit(parts []string) bool {
	for _, part := range parts {
		if part == ".git" {
			return true
		}
	}
	return false
}
