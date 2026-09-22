package inventory

import (
	"strings"
	"testing"
	"time"
)

func instant() time.Time { return time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC) }
func positiveInput() DecisionInput {
	last := instant().Add(-48 * time.Hour)
	duration := 24 * time.Hour
	id := "sha256:" + strings.Repeat("a", 64)
	return DecisionInput{SourceID: id, Size: 42, Activity: ResolveActivity([]time.Time{last}, true), Ingest: IngestAssessment{State: IngestVerified, SourceID: id, Checkpoint: instant().Add(-time.Hour), ActivityCovered: &last}, Verification: SourceVerified, Retention: &duration, Now: instant()}
}
func hasBlock(d Decision, b Blocker) bool {
	for _, v := range d.Blockers {
		if v == b {
			return true
		}
	}
	return false
}

func TestActivityClaims(t *testing.T) {
	now := instant()
	for _, tc := range []struct {
		name          string
		claims        []time.Time
		authoritative bool
		want          ActivityState
	}{
		{"recent", []time.Time{now}, true, ActivityKnown},
		{"old", []time.Time{now.Add(-365 * 24 * time.Hour)}, true, ActivityKnown},
		{"missing", nil, true, ActivityUnknown},
		{"invalid", []time.Time{{}}, true, ActivityUnknown},
		{"out_of_range", []time.Time{time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)}, true, ActivityUnknown},
		{"conflict", []time.Time{now, now.Add(time.Second)}, true, ActivityAmbiguous},
		{"timezone", []time.Time{now, now.In(time.FixedZone("offset", 8*3600))}, true, ActivityKnown},
		{"unproven_semantics", []time.Time{now}, false, ActivityUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ResolveActivity(tc.claims, tc.authoritative)
			if got.State != tc.want {
				t.Fatalf("%+v", got)
			}
			if got.Last != nil && got.Last.Location() != time.UTC {
				t.Fatal("not UTC")
			}
		})
	}
}

func TestDecisionGatesAndBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*DecisionInput)
		want   Blocker
	}{
		{"complete synthetic proof", func(*DecisionInput) {}, ""},
		{"unset", func(in *DecisionInput) { in.Retention = nil }, BlockedRetentionUnset},
		{"zero", func(in *DecisionInput) { d := time.Duration(0); in.Retention = &d }, BlockedRetentionInvalid},
		{"before", func(in *DecisionInput) {
			in.Now = in.Activity.Last.Add(*in.Retention - time.Nanosecond)
			in.Ingest.Checkpoint = *in.Activity.Last
		}, BlockedRetentionNotReached},
		{"exact", func(in *DecisionInput) {
			in.Now = in.Activity.Last.Add(*in.Retention)
			in.Ingest.Checkpoint = *in.Activity.Last
		}, ""},
		{"after", func(in *DecisionInput) {
			in.Now = in.Activity.Last.Add(*in.Retention + time.Nanosecond)
			in.Ingest.Checkpoint = *in.Activity.Last
		}, ""},
		{"timezone", func(in *DecisionInput) { in.Now = in.Now.In(time.FixedZone("offset", -7*3600)) }, ""},
		{"unknown", func(in *DecisionInput) { in.Activity = Activity{State: ActivityUnknown} }, BlockedActivityUnknown},
		{"ambiguous", func(in *DecisionInput) { in.Activity = Activity{State: ActivityAmbiguous} }, BlockedActivityAmbiguous},
		{"partial", func(in *DecisionInput) { in.Ingest.State = IngestPartial }, BlockedIncompleteIngest},
		{"missing", func(in *DecisionInput) { in.Ingest.State = IngestMissing }, BlockedMissingIngestEvidence},
		{"gap", func(in *DecisionInput) { in.Ingest.State = IngestEvidenceGap }, BlockedIncompleteIngestEvidence},
		{"identity", func(in *DecisionInput) { in.Ingest.SourceID = "sha256:" + strings.Repeat("b", 64) }, BlockedSourceChanged},
		{"changed", func(in *DecisionInput) { in.Verification = SourceChanged }, BlockedSourceChanged},
		{"stale", func(in *DecisionInput) { in.Ingest.Checkpoint = in.Activity.Last.Add(-time.Second) }, BlockedStaleCheckpoint},
		{"uncovered activity", func(in *DecisionInput) { in.Ingest.ActivityCovered = nil }, BlockedIncompleteIngestEvidence},
		{"unsupported", func(in *DecisionInput) { in.Ingest.State = IngestUnsupported }, BlockedUnsupportedEvidence},
		{"unknown schema", func(in *DecisionInput) { in.Verification = SourceUnsupported }, BlockedUnsupportedSchema},
		{"failed", func(in *DecisionInput) { in.Verification = SourceFailed }, BlockedVerificationFailed},
		{"invalid clock", func(in *DecisionInput) { in.Now = time.Time{} }, BlockedEvaluationTime},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := positiveInput()
			tc.change(&in)
			d := Evaluate(in)
			if tc.want == "" {
				if !d.Eligible || d.ReclaimableBytes != 42 {
					t.Fatalf("%+v", d)
				}
			} else if d.Eligible || d.ReclaimableBytes != 0 || !hasBlock(d, tc.want) {
				t.Fatalf("%+v", d)
			}
		})
	}
}

func TestMixedReclaimSum(t *testing.T) {
	in := positiveInput()
	yes := Evaluate(in)
	in.Ingest.State = IngestMissing
	no := Evaluate(in)
	total, err := reclaimTotal([]Decision{yes, no, yes})
	if err != nil || total != 84 {
		t.Fatalf("%d %v", total, err)
	}
}
