// Package diff implements deterministic, read-only comparison of Replay runs.
package diff

import (
	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

const defaultMaxStateChanges = 10000

// Dependencies are the local immutable evidence stores used by comparison.
type Dependencies struct {
	DB  *persistence.DB
	CAS *store.LocalStore
}

// Options controls bounded result materialization. A zero MaxStateChanges uses
// the conservative default; a negative value is rejected.
type Options struct {
	MaxStateChanges int
}

// Result is the deterministic multi-dimensional comparison of two sessions.
type Result struct {
	Left      SessionSummary `json:"left"`
	Right     SessionSummary `json:"right"`
	Lineage   LineageDiff    `json:"lineage"`
	State     StateDiff      `json:"state"`
	Timeline  TimelineDiff   `json:"timeline"`
	Process   StreamDiff     `json:"process"`
	Agent     StreamDiff     `json:"agent"`
	Outcome   OutcomeDiff    `json:"outcome"`
	Identical bool           `json:"identical"`
}

// SessionSummary exposes only stable run metadata needed to interpret a diff.
type SessionSummary struct {
	SessionID            string `json:"session_id"`
	ParentSessionID      string `json:"parent_session_id,omitempty"`
	ForkEventSeq         uint64 `json:"fork_event_seq,omitempty"`
	Status               string `json:"status"`
	InitialStateID       string `json:"initial_state_id,omitempty"`
	FinalStateID         string `json:"final_state_id,omitempty"`
	ReproducibilityLevel string `json:"reproducibility_level"`
	AdapterID            string `json:"adapter_id,omitempty"`
	AdapterVersion       string `json:"adapter_version,omitempty"`
}

// LineageDiff describes durable ancestry independently from event similarity.
type LineageDiff struct {
	Related               bool   `json:"related"`
	CommonSessionID       string `json:"common_session_id,omitempty"`
	LeftDepth             int    `json:"left_depth,omitempty"`
	RightDepth            int    `json:"right_depth,omitempty"`
	LeftForkEventSeq      uint64 `json:"left_fork_event_seq,omitempty"`
	RightForkEventSeq     uint64 `json:"right_fork_event_seq,omitempty"`
	SharedThroughEventSeq uint64 `json:"shared_through_event_seq,omitempty"`
}

// StateDiff compares the selected sessions' final published workspace states.
type StateDiff struct {
	Comparable       bool          `json:"comparable"`
	Reason           string        `json:"reason,omitempty"`
	LeftStateID      string        `json:"left_state_id,omitempty"`
	RightStateID     string        `json:"right_state_id,omitempty"`
	LeftRootTreeID   string        `json:"left_root_tree_id,omitempty"`
	RightRootTreeID  string        `json:"right_root_tree_id,omitempty"`
	Equal            bool          `json:"equal"`
	Added            int           `json:"added"`
	Removed          int           `json:"removed"`
	Modified         int           `json:"modified"`
	TypeChanged      int           `json:"type_changed"`
	TotalChanges     int           `json:"total_changes"`
	ChangesTruncated bool          `json:"changes_truncated,omitempty"`
	Changes          []StateChange `json:"changes,omitempty"`
}

// StateChange is one path-level tree difference. PathComponentsB64 is the
// lossless representation; DisplayPath is a human-readable rendering only.
type StateChange struct {
	PathComponentsB64 []string  `json:"path_components_b64"`
	DisplayPath       string    `json:"display_path"`
	Change            string    `json:"change"`
	Reason            string    `json:"reason,omitempty"`
	Left              *TreeNode `json:"left,omitempty"`
	Right             *TreeNode `json:"right,omitempty"`
}

// TreeNode is the evidence-bearing metadata for one tree entry.
type TreeNode struct {
	Kind     string `json:"kind"`
	Mode     uint32 `json:"mode"`
	Size     int64  `json:"size"`
	ObjectID string `json:"object_id"`
}

// TimelineDiff compares normalized technical events in sequence order. Runtime
// identity and envelope timestamps are excluded from the technical fingerprint.
type TimelineDiff struct {
	LeftEvents         int           `json:"left_events"`
	RightEvents        int           `json:"right_events"`
	CommonPrefixEvents int           `json:"common_prefix_events"`
	Equal              bool          `json:"equal"`
	LeftRemaining      int           `json:"left_remaining"`
	RightRemaining     int           `json:"right_remaining"`
	FirstLeft          *EventSummary `json:"first_left,omitempty"`
	FirstRight         *EventSummary `json:"first_right,omitempty"`
}

// StreamDiff is the same structural comparison restricted to one event
// namespace such as process.* or agent.*.
type StreamDiff struct {
	LeftEvents         int            `json:"left_events"`
	RightEvents        int            `json:"right_events"`
	CommonPrefixEvents int            `json:"common_prefix_events"`
	Equal              bool           `json:"equal"`
	LeftTypes          map[string]int `json:"left_types"`
	RightTypes         map[string]int `json:"right_types"`
	FirstLeft          *EventSummary  `json:"first_left,omitempty"`
	FirstRight         *EventSummary  `json:"first_right,omitempty"`
}

// EventSummary intentionally omits session-local timestamps and PIDs from the
// comparison surface while retaining the original event sequence for navigation.
type EventSummary struct {
	Seq                 uint64 `json:"seq"`
	Type                string `json:"type"`
	Source              string `json:"source"`
	StateBeforeRootTree string `json:"state_before_root_tree,omitempty"`
	StateAfterRootTree  string `json:"state_after_root_tree,omitempty"`
	PayloadFingerprint  string `json:"payload_fingerprint"`
	PrivacyClass        string `json:"privacy_class"`
	Redacted            bool   `json:"redacted,omitempty"`
}

// OutcomeDiff compares terminal session outcome independently from the event
// timeline and detailed workspace state changes.
type OutcomeDiff struct {
	Left  OutcomeSide `json:"left"`
	Right OutcomeSide `json:"right"`
	Equal bool        `json:"equal"`
}

// OutcomeSide is a compact final-run result.
type OutcomeSide struct {
	Status      string `json:"status"`
	ExitCode    *int   `json:"exit_code,omitempty"`
	Success     *bool  `json:"success,omitempty"`
	FinalRootID string `json:"final_root_tree_id,omitempty"`
}
