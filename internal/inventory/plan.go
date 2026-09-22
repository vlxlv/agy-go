package inventory

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

type manifest struct {
	path, digest, status, scanStatus, parser string
	size, mtime, count, imported             int64
	complete                                 bool
	checkpoint                               time.Time
}

// readManifests is diagnostic only: schema v2 cannot certify complete ingest.
// Even a matching populated digest plus matching counts stays unverified.
func readManifests(ctx context.Context, path string) (map[string]manifest, error) {
	parent, err := New(filepath.Dir(path))
	if err != nil {
		return nil, errors.New("ledger_directory_unavailable")
	}
	defer parent.Close()
	result := map[string]manifest{}
	err = parent.snapshot(ctx, filepath.Base(path), &Entry{}, func(ctx context.Context, copy string, _ *Entry) error {
		dsn := (&url.URL{Scheme: "file", Path: copy}).String() + "?mode=ro&immutable=1&_pragma=query_only(ON)&_pragma=trusted_schema(OFF)"
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			return errors.New("ledger_open_failed")
		}
		defer db.Close()
		db.SetMaxOpenConns(1)
		var check string
		var version int
		if db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&check) != nil || check != "ok" {
			return errors.New("ledger_integrity_failed")
		}
		if db.QueryRowContext(ctx, "SELECT MAX(version) FROM schema_migrations").Scan(&version) != nil || version != 2 {
			return errors.New("ledger_schema_unsupported")
		}
		rows, err := db.QueryContext(ctx, `SELECT source_path,COALESCE(source_sha256,''),source_size,source_mtime_ns,is_complete,status,last_scan_status,parser_version,ingested_at,generation_count,
   (SELECT count(*) FROM usage_records r WHERE r.conversation_id=m.conversation_id)
   FROM ingest_manifests m`)
		if err != nil {
			return errors.New("ledger_schema_unsupported")
		}
		defer rows.Close()
		for rows.Next() {
			var m manifest
			var complete int
			var timestamp sql.NullTime
			if rows.Scan(&m.path, &m.digest, &m.size, &m.mtime, &complete, &m.status, &m.scanStatus, &m.parser, &timestamp, &m.count, &m.imported) != nil {
				return errors.New("ledger_evidence_invalid")
			}
			m.complete = complete == 1
			m.checkpoint = timestamp.Time
			if !filepath.IsAbs(m.path) {
				return errors.New("ledger_source_identity_ambiguous")
			}
			key := filepath.Clean(m.path)
			if _, exists := result[key]; exists {
				return errors.New("ledger_source_identity_ambiguous")
			}
			result[key] = m
		}
		if rows.Err() != nil {
			return errors.New("ledger_read_failed")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func assessManifest(m *manifest, e Entry, now time.Time) IngestAssessment {
	if m == nil {
		return IngestAssessment{State: IngestMissing}
	}
	a := IngestAssessment{State: IngestEvidenceGap, Checkpoint: m.checkpoint.UTC()}
	if m.digest != "" {
		id := "sha256:" + strings.ToLower(m.digest)
		if !validID(id) {
			a.State = IngestUnsupported
			return a
		}
		a.SourceID = id
		if id != e.SourceID {
			a.State = IngestMismatch
			return a
		}
	}
	if m.size != e.Size || m.mtime != e.Mtime.UnixNano() {
		a.State = IngestMismatch
		return a
	}
	if !m.complete || m.status != "completed" || m.scanStatus != "completed" || m.count != m.imported || m.count < 0 {
		a.State = IngestPartial
		return a
	}
	if !validTime(m.checkpoint) || m.checkpoint.After(now) || m.checkpoint.Before(e.Mtime) {
		a.State = IngestStale
		return a
	}
	if m.parser != "1.1.0" {
		a.State = IngestUnsupported
	}
	// Missing generation-bound digest/coverage/activity contract is a hard stop.
	return a
}

type PlanReport struct {
	FormatVersion         int        `json:"format_version"`
	DryRun                bool       `json:"dry_run"`
	EvaluatedAt           time.Time  `json:"evaluated_at"`
	RetentionSeconds      *int64     `json:"retention_seconds"`
	Sources               []Decision `json:"sources"`
	TotalReclaimableBytes int64      `json:"total_reclaimable_bytes"`
	EvidenceGaps          []string   `json:"evidence_gaps"`
	Errors                []string   `json:"errors"`
}

// Plan always takes fresh snapshots after discovery. No stored inventory or
// supplied JSON can authorize a positive verification result.
func (i *Inventory) Plan(ctx context.Context, ledger string, retention *time.Duration, now time.Time) (PlanReport, error) {
	baseline, err := i.Scan(ctx)
	if err != nil {
		return PlanReport{}, err
	}
	return i.plan(ctx, baseline, ledger, retention, now)
}
func (i *Inventory) plan(ctx context.Context, baseline []Entry, ledger string, retention *time.Duration, now time.Time) (PlanReport, error) {
	report := PlanReport{FormatVersion: 1, DryRun: true, EvaluatedAt: now.UTC(), Sources: []Decision{}, EvidenceGaps: []string{"authoritative_last_activity_contract_missing", "generation_bound_complete_ingest_contract_missing"}, Errors: []string{}}
	if retention != nil {
		seconds := int64(*retention / time.Second)
		report.RetentionSeconds = &seconds
	}
	var manifests map[string]manifest
	var ledgerErr error
	if ledger != "" {
		manifests, ledgerErr = readManifests(ctx, ledger)
		if ledgerErr != nil {
			report.Errors = append(report.Errors, ledgerErr.Error())
		}
	}
	for _, old := range baseline {
		if ctx.Err() != nil {
			return report, ctx.Err()
		}
		e := i.Inspect(ctx, old.name)
		state := SourceVerified
		if e.Schema == "unsupported" {
			state = SourceUnsupported
		} else if len(e.Blockers) > 3 {
			state = SourceFailed
		}
		if old.SourceID != "" && e.SourceID != old.SourceID {
			state = SourceChanged
		}
		for _, b := range e.Blockers {
			if b == "source_changed" {
				state = SourceChanged
			}
		}
		var m *manifest
		if value, ok := manifests[filepath.Join(i.path, old.name)]; ok {
			m = &value
		}
		ingest := assessManifest(m, e, now)
		if ledgerErr != nil {
			ingest.State = IngestUnsupported
		}
		// The known timestamps are event times, not a proven complete activity
		// watermark. Do not infer last activity from mtime or usage-only records.
		decision := Evaluate(DecisionInput{SourceID: e.SourceID, PathRef: e.PathRef, Size: e.Size, Activity: ResolveActivity(nil, false), Ingest: ingest, Verification: state, Retention: retention, Now: now})
		decision.SourceIssues = append([]string{}, e.Blockers[3:]...)
		report.Sources = append(report.Sources, decision)
	}
	total, err := reclaimTotal(report.Sources)
	if err != nil {
		return report, err
	}
	report.TotalReclaimableBytes = total
	return report, nil
}

func reclaimTotal(decisions []Decision) (int64, error) {
	var total int64
	for _, d := range decisions {
		if d.Eligible {
			if d.ReclaimableBytes < 0 || d.ReclaimableBytes > math.MaxInt64-total {
				return 0, errors.New("reclaim_total_overflow")
			}
			total += d.ReclaimableBytes
		}
	}
	return total, nil
}
