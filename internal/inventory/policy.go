package inventory

import (
	"encoding/hex"
	"strings"
	"time"
)

type ActivityState string

const (
	ActivityKnown     ActivityState = "known"
	ActivityUnknown   ActivityState = "unknown"
	ActivityAmbiguous ActivityState = "ambiguous"
)

type Activity struct {
	State ActivityState `json:"state"`
	Last  *time.Time    `json:"last_activity_at"`
}

// ResolveActivity reconciles authoritative claims about the SAME last-activity
// fact, not different events' start/end times. There is no proven AGY adapter
// yet. No drift tolerance is invented: unequal instants are ambiguous.
func ResolveActivity(claims []time.Time, authoritative bool) Activity {
	unknown := Activity{State: ActivityUnknown}
	if !authoritative || len(claims) == 0 {
		return unknown
	}
	for _, t := range claims {
		if !validTime(t) {
			return unknown
		}
	}
	for _, t := range claims[1:] {
		if !t.Equal(claims[0]) {
			return Activity{State: ActivityAmbiguous}
		}
	}
	t := claims[0].UTC()
	return Activity{State: ActivityKnown, Last: &t}
}
func validTime(t time.Time) bool { return !t.IsZero() && t.Year() >= 1 && t.Year() <= 9999 }
func validID(id string) bool {
	b, err := hex.DecodeString(strings.TrimPrefix(id, "sha256:"))
	return strings.HasPrefix(id, "sha256:") && err == nil && len(b) == 32
}

type IngestState string

const (
	IngestVerified    IngestState = "verified"
	IngestMissing     IngestState = "missing"
	IngestPartial     IngestState = "partial"
	IngestEvidenceGap IngestState = "incomplete_evidence"
	IngestMismatch    IngestState = "source_mismatch"
	IngestStale       IngestState = "stale_checkpoint"
	IngestUnsupported IngestState = "unsupported_evidence"
)

// IngestAssessment is a verification result, not a caller-provided certificate.
// Current ledger adapters NEVER return IngestVerified. Positive engine tests
// use synthetic assessments; these cannot be supplied via the CLI or JSON.
type IngestAssessment struct {
	State           IngestState `json:"state"`
	SourceID        string      `json:"source_id,omitempty"`
	Checkpoint      time.Time   `json:"checkpoint"`
	ActivityCovered *time.Time  `json:"activity_covered"`
}
type SourceState string

const (
	SourceVerified    SourceState = "verified_snapshot"
	SourceChanged     SourceState = "changed"
	SourceFailed      SourceState = "verification_failed"
	SourceUnsupported SourceState = "unsupported_schema"
)

type Blocker string

const (
	BlockedActivityUnknown          Blocker = "activity_unknown"
	BlockedActivityAmbiguous        Blocker = "activity_ambiguous"
	BlockedActive                   Blocker = "activity_after_evaluation"
	BlockedIncompleteIngestEvidence Blocker = "incomplete_ingest_evidence"
	BlockedIncompleteIngest         Blocker = "partial_ingest"
	BlockedMissingIngestEvidence    Blocker = "missing_ingest_evidence"
	BlockedStaleCheckpoint          Blocker = "stale_ingest_checkpoint"
	BlockedUnsupportedEvidence      Blocker = "unsupported_ingest_evidence"
	BlockedSourceChanged            Blocker = "source_changed"
	BlockedVerificationFailed       Blocker = "verification_failed"
	BlockedUnsupportedSchema        Blocker = "unsupported_schema"
	BlockedRetentionUnset           Blocker = "retention_unset"
	BlockedRetentionInvalid         Blocker = "retention_invalid"
	BlockedRetentionNotReached      Blocker = "retention_not_reached"
	BlockedEvaluationTime           Blocker = "evaluation_time_invalid"
)

type DecisionInput struct {
	SourceID     string
	PathRef      string
	Size         int64
	Activity     Activity
	Ingest       IngestAssessment
	Verification SourceState
	Retention    *time.Duration
	Now          time.Time
}
type Decision struct {
	SourceID           string           `json:"source_id"`
	PathRef            string           `json:"path_ref"`
	Activity           Activity         `json:"activity"`
	RetentionDeadline  *time.Time       `json:"retention_deadline"`
	Ingest             IngestAssessment `json:"ingest"`
	IngestVerified     bool             `json:"ingest_verified"`
	SourceVerification SourceState      `json:"source_verification"`
	SourceIssues       []string         `json:"source_issues"`
	SourceVerified     bool             `json:"source_verified"`
	Eligible           bool             `json:"eligible"`
	ReclaimableBytes   int64            `json:"reclaimable_bytes"`
	Blockers           []Blocker        `json:"blockers"`
}

// Evaluate has no filesystem, clock or rendering dependencies. The exact
// deadline is inclusive. Inactivity is represented only by authoritative last
// activity followed by the explicitly supplied positive retention interval.
func Evaluate(in DecisionInput) Decision {
	d := Decision{SourceID: in.SourceID, PathRef: in.PathRef, Activity: in.Activity, Ingest: in.Ingest, SourceVerification: in.Verification, Blockers: []Blocker{}}
	add := func(b Blocker) {
		for _, v := range d.Blockers {
			if v == b {
				return
			}
		}
		d.Blockers = append(d.Blockers, b)
	}
	if !validTime(in.Now) {
		add(BlockedEvaluationTime)
	}
	switch in.Verification {
	case SourceVerified:
		if validID(in.SourceID) && in.Size >= 0 {
			d.SourceVerified = true
		} else {
			add(BlockedVerificationFailed)
		}
	case SourceChanged:
		add(BlockedSourceChanged)
	case SourceUnsupported:
		add(BlockedUnsupportedSchema)
	default:
		add(BlockedVerificationFailed)
	}
	switch in.Activity.State {
	case ActivityKnown:
		if in.Activity.Last == nil || !validTime(*in.Activity.Last) {
			add(BlockedActivityUnknown)
		} else {
			t := in.Activity.Last.UTC()
			d.Activity.Last = &t
			if t.After(in.Now) {
				add(BlockedActive)
			}
		}
	case ActivityAmbiguous:
		add(BlockedActivityAmbiguous)
	default:
		add(BlockedActivityUnknown)
	}
	switch in.Ingest.State {
	case IngestVerified:
		if in.Ingest.SourceID != in.SourceID || !validID(in.Ingest.SourceID) {
			add(BlockedSourceChanged)
		} else if !validTime(in.Ingest.Checkpoint) || in.Ingest.Checkpoint.After(in.Now) || in.Activity.Last == nil || in.Ingest.Checkpoint.Before(*in.Activity.Last) {
			add(BlockedStaleCheckpoint)
		} else if in.Ingest.ActivityCovered == nil || !in.Ingest.ActivityCovered.Equal(*in.Activity.Last) {
			add(BlockedIncompleteIngestEvidence)
		} else {
			d.IngestVerified = true
		}
	case IngestMissing:
		add(BlockedMissingIngestEvidence)
	case IngestPartial:
		add(BlockedIncompleteIngest)
	case IngestMismatch:
		add(BlockedSourceChanged)
	case IngestStale:
		add(BlockedStaleCheckpoint)
	case IngestUnsupported:
		add(BlockedUnsupportedEvidence)
	default:
		add(BlockedIncompleteIngestEvidence)
	}
	if in.Retention == nil {
		add(BlockedRetentionUnset)
	} else if *in.Retention <= 0 {
		add(BlockedRetentionInvalid)
	} else if in.Activity.State == ActivityKnown && in.Activity.Last != nil && validTime(*in.Activity.Last) {
		deadline := in.Activity.Last.UTC().Add(*in.Retention)
		if !validTime(deadline) {
			add(BlockedRetentionInvalid)
		} else {
			d.RetentionDeadline = &deadline
			if in.Now.Before(deadline) {
				add(BlockedRetentionNotReached)
			}
		}
	}
	d.Eligible = len(d.Blockers) == 0 && d.SourceVerified && d.IngestVerified
	if d.Eligible {
		d.ReclaimableBytes = in.Size
	}
	return d
}
