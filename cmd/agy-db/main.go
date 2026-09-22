package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"text/tabwriter"
	"time"

	"github.com/vlxlv/agy-go/internal/inventory"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, out, errOut io.Writer) int {
	if len(args) == 0 || args[0] == "--help" || args[0] == "help" {
		fmt.Fprintln(out, "agy-db scan|status --source-dir DIR [--json]\nagy-db inspect --source-dir DIR [--json] NAME.db\nagy-db plan --source-dir DIR [--ledger FILE] [--retention 720h] [--json]\nv1: read-only inventory, inspection and planning; never modifies or deletes conversation databases.\nPlan is always dry-run; retention has no default. CP3 BLOCKED.")
		return 0
	}
	cmd := args[0]
	if cmd != "scan" && cmd != "status" && cmd != "inspect" && cmd != "plan" {
		fmt.Fprintln(errOut, "unknown command")
		return 2
	}
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	// Flag parser errors can echo private input; emit a fixed diagnostic instead.
	fs.SetOutput(io.Discard)
	root := fs.String("source-dir", "", "Explicit source directory")
	asJSON := fs.Bool("json", false, "JSON output")
	var ledger, retentionText string
	if cmd == "plan" {
		fs.StringVar(&ledger, "ledger", "", "Explicit usage ledger file")
		fs.StringVar(&retentionText, "retention", "", "Positive whole-second duration, e.g. 720h; no default")
	}
	if fs.Parse(args[1:]) != nil || *root == "" || (cmd == "inspect" && fs.NArg() != 1) || (cmd != "inspect" && fs.NArg() != 0) {
		fmt.Fprintln(errOut, "invalid arguments; see agy-db --help")
		return 2
	}
	var retention *time.Duration
	if retentionText != "" {
		d, err := time.ParseDuration(retentionText)
		if err != nil || d <= 0 || d%time.Second != 0 {
			fmt.Fprintln(errOut, "retention must be a positive whole-second duration")
			return 2
		}
		retention = &d
	}
	inv, err := inventory.New(*root)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	defer inv.Close()
	if cmd == "plan" {
		report, err := inv.Plan(ctx, ledger, retention, time.Now().UTC())
		if err != nil {
			fmt.Fprintln(errOut, "plan interrupted or unavailable")
			return 1
		}
		if renderPlan(out, report, *asJSON) != nil {
			return 1
		}
		if len(report.Errors) > 0 {
			return 1
		}
		for _, d := range report.Sources {
			if !d.SourceVerified {
				return 1
			}
		}
		return 0
	}
	var entries []inventory.Entry
	if cmd == "inspect" {
		entries = []inventory.Entry{inv.Inspect(ctx, fs.Arg(0))}
	} else {
		entries, err = inv.Scan(ctx)
	}
	if err != nil {
		fmt.Fprintln(errOut, "inventory interrupted or unavailable")
		return 1
	}
	code := 0
	for _, e := range entries {
		if len(e.Blockers) > 3 {
			code = 1
		}
	}
	if *asJSON {
		if json.NewEncoder(out).Encode(struct {
			Version int               `json:"format_version"`
			Sources []inventory.Entry `json:"sources"`
		}{1, entries}) != nil {
			return 1
		}
		return code
	}
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "PATH REF (PREFIX)\tSOURCE ID (PREFIX)\tBYTES\tSCHEMA\tSTEPS\tGENERATIONS\tACTIVITY\tINGEST\tELIGIBLE\tBLOCKERS")
	for _, e := range entries {
		steps, gens := "unknown", "unknown"
		if e.Steps != nil {
			steps = fmt.Sprint(*e.Steps)
		}
		if e.Generations != nil {
			gens = fmt.Sprint(*e.Generations)
		}
		fmt.Fprintf(w, "%.24s\t%.19s\t%d\t%s\t%s\t%s\tunknown\t%s\tfalse\t%v\n", e.PathRef, e.SourceID, e.Size, e.Schema, steps, gens, e.Ingest, e.Blockers)
	}
	if w.Flush() != nil {
		return 1
	}
	return code
}

func renderPlan(out io.Writer, r inventory.PlanReport, asJSON bool) error {
	if asJSON {
		return json.NewEncoder(out).Encode(r)
	}
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "DRY-RUN ONLY — no files will be deleted")
	fmt.Fprintln(w, "PATH REF (PREFIX)\tSOURCE ID (PREFIX)\tLAST ACTIVITY\tDEADLINE\tINGEST\tSOURCE\tELIGIBLE\tBYTES\tBLOCKERS")
	for _, d := range r.Sources {
		activity, deadline := "unknown", "unknown"
		if d.Activity.Last != nil {
			activity = d.Activity.Last.UTC().Format(time.RFC3339Nano)
		}
		if d.RetentionDeadline != nil {
			deadline = d.RetentionDeadline.UTC().Format(time.RFC3339Nano)
		}
		fmt.Fprintf(w, "%.24s\t%.19s\t%s\t%s\t%s\t%s\t%t\t%d\t%v %v\n", d.PathRef, d.SourceID, activity, deadline, d.Ingest.State, d.SourceVerification, d.Eligible, d.ReclaimableBytes, d.Blockers, d.SourceIssues)
	}
	fmt.Fprintf(w, "Total reclaimable bytes: %d\nEvidence gaps: %v\n", r.TotalReclaimableBytes, r.EvidenceGaps)
	if len(r.Errors) > 0 {
		fmt.Fprintf(w, "Errors: %v\n", r.Errors)
	}
	return w.Flush()
}
