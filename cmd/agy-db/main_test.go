package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/vlxlv/agy-go/internal/inventory"
	"strings"
	"testing"
)

func TestCLISafeDefaults(t *testing.T) {
	for _, args := range [][]string{{}, {"--help"}, {"prune"}, {"delete"}, {"gc"}, {"cleanup"}, {"scan"}, {"inspect", "--source-dir", "/home/ezhang/.gemini", "secret.db"}, {"scan", "--secret-credential"}} {
		var out, errs bytes.Buffer
		code := run(context.Background(), args, &out, &errs)
		if len(args) > 0 && args[0] != "--help" && code == 0 {
			t.Fatalf("unsafe command succeeded: %v", args)
		}
		if len(args) == 0 || args[0] == "--help" {
			if !strings.Contains(out.String(), "read-only") || !strings.Contains(out.String(), "never modifies or deletes") || !strings.Contains(out.String(), "CP3 BLOCKED") {
				t.Fatal("help omits safety boundary")
			}
		}
		if strings.Contains(out.String()+errs.String(), "secret") {
			t.Fatal("private argument echoed")
		}
	}
}

func TestPlanRenderingAndArguments(t *testing.T) {
	r := inventory.PlanReport{FormatVersion: 1, DryRun: true, Sources: []inventory.Decision{{SourceID: "sha256:abc", PathRef: "path-sha256:def", Blockers: []inventory.Blocker{inventory.BlockedIncompleteIngestEvidence}}}, EvidenceGaps: []string{"generation_bound_complete_ingest_contract_missing"}}
	var out bytes.Buffer
	if err := renderPlan(&out, r, true); err != nil {
		t.Fatal(err)
	}
	var decoded inventory.PlanReport
	if json.Unmarshal(out.Bytes(), &decoded) != nil || !decoded.DryRun || decoded.TotalReclaimableBytes != 0 {
		t.Fatal("JSON contract")
	}
	out.Reset()
	if err := renderPlan(&out, r, false); err != nil || !strings.Contains(out.String(), "DRY-RUN ONLY") || !strings.Contains(out.String(), "incomplete_ingest_evidence") {
		t.Fatal("text report")
	}
	for _, duration := range []string{"0", "-1h", "1d", "secret", "999999999999999999999h", "1ns"} {
		out.Reset()
		var errs bytes.Buffer
		if run(context.Background(), []string{"plan", "--source-dir", "unused", "--retention", duration}, &out, &errs) != 2 {
			t.Fatal("bad retention accepted")
		}
		if strings.Contains(errs.String(), "secret") {
			t.Fatal("private input echoed")
		}
	}
}
