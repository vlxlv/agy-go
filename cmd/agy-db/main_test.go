package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/vlxlv/agy-go/internal/inventory"
)

func TestCLISafeDefaults(t *testing.T) {
	for _, args := range [][]string{{}, {"--help"}, {"prune"}, {"delete"}, {"gc"}, {"cleanup"}, {"restore"}, {"scan"}, {"verify-archive"}, {"inspect", "--source-dir", "/home/ezhang/.gemini", "secret.db"}, {"scan", "--secret-credential"}} {
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

func TestArchiveVerificationAndRegistryRendering(t *testing.T) {
	verified := time.Unix(100, 0).UTC()
	result := inventory.ArchiveVerificationResult{FormatVersion: 1, State: inventory.ArchiveValid, ArchiveID: "0123456789abcdef0123456789abcdef", ConversationID: "conversation", PathRef: "path-sha256:abc", ManifestSHA256: "def", Files: 2, Bytes: 10, VerifiedAt: verified}
	var out bytes.Buffer
	if err := renderArchiveVerification(&out, result, true); err != nil {
		t.Fatal(err)
	}
	var decoded inventory.ArchiveVerificationResult
	if json.Unmarshal(out.Bytes(), &decoded) != nil || decoded.State != inventory.ArchiveValid || decoded.Files != 2 {
		t.Fatal("verification JSON contract")
	}
	out.Reset()
	if err := renderArchiveVerification(&out, result, false); err != nil || !strings.Contains(out.String(), "valid") || strings.Contains(out.String(), "path-sha256") {
		t.Fatal("verification text contract")
	}
	report := inventory.ArchiveRegistryReport{FormatVersion: 1, RegistryRef: "path-sha256:registry", Archives: []inventory.ArchiveRegistryEntry{{ArchiveID: result.ArchiveID, ConversationID: result.ConversationID, PathRef: result.PathRef, CreatedAt: verified, VerificationState: inventory.ArchiveValid, VerifiedAt: &verified, Files: 2, Bytes: 10}}}
	out.Reset()
	if err := renderArchiveRegistry(&out, report, true); err != nil || !strings.Contains(out.String(), "registry_ref") {
		t.Fatal("registry JSON contract")
	}
	out.Reset()
	if err := renderArchiveRegistry(&out, report, false); err != nil || !strings.Contains(out.String(), "VERIFICATION") || !strings.Contains(out.String(), "valid") {
		t.Fatal("registry text contract")
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
