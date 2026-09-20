package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/vlxlv/agy-go/internal/tokei"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "ingest":
		runIngest(args)
	case "status":
		runStatus(args)
	case "verify":
		runVerify(args)
	case "summary":
		runSummary(args)
	case "help", "-h", "--help":
		printUsage()
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `agy-tokei: AGY token usage analytics and verification CLI

Usage:
  agy-tokei <command> [arguments]

Commands:
  ingest     Scan and ingest AGY conversation databases into usage ledger
  status     Display ledger status, indexed databases, and record counts
  verify     Validate ledger schema, manifest integrity, and token arithmetic
  summary    Display aggregate token statistics across projects and models

Global Flags:
  --data-dir            Custom data directory for usage.db (default: ~/.local/share/agy-tokei)
  --ledger-path         Custom path to usage.db
  --conversations-dir   Custom path to AGY conversations (default: ~/.gemini/antigravity-cli/conversations)
  --summaries-db        Custom path to conversation_summaries.db
  --json                Output results in JSON format
`)
}

func commonFlags(fs *flag.FlagSet) (*string, *string, *string, *string, *bool) {
	dataDir := fs.String("data-dir", "", "Custom data directory for usage.db")
	ledgerPath := fs.String("ledger-path", "", "Explicit path to usage.db")
	convDir := fs.String("conversations-dir", "", "Path to AGY conversations directory")
	sumDB := fs.String("summaries-db", "", "Path to conversation_summaries.db")
	asJSON := fs.Bool("json", false, "Output results in JSON format")
	return dataDir, ledgerPath, convDir, sumDB, asJSON
}

func getService(dataDir, ledgerPath, convDir, sumDB string, force, verbose bool) *tokei.Service {
	resolvedLedger := ledgerPath
	if resolvedLedger == "" && dataDir != "" {
		resolvedLedger = filepath.Join(dataDir, "usage.db")
	}
	return tokei.NewService(tokei.ServiceOptions{
		LedgerPath:       resolvedLedger,
		ConversationsDir: convDir,
		SummariesPath:    sumDB,
		Force:            force,
		Verbose:          verbose,
	})
}

func runIngest(args []string) {
	fs := flag.NewFlagSet("ingest", flag.ExitOnError)
	dataDir, ledgerPath, convDir, sumDB, asJSON := commonFlags(fs)
	force := fs.Bool("force", false, "Force re-ingestion of all databases even if up to date")
	verbose := fs.Bool("verbose", false, "Verbose output during ingestion")
	_ = fs.Parse(args)

	svc := getService(*dataDir, *ledgerPath, *convDir, *sumDB, *force, *verbose)
	stats, err := svc.Ingest()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ingest error: %v\n", err)
		os.Exit(1)
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(stats)
		return
	}

	fmt.Printf("Ingestion completed in %s\n", stats.Duration.Round(100*1000))
	fmt.Printf("  Discovered databases: %d\n", stats.TotalDiscovered)
	fmt.Printf("  Already up-to-date:   %d\n", stats.AlreadyUpToDate)
	fmt.Printf("  Ingested databases:   %d (completed: %d, active: %d, failed: %d)\n",
		stats.IngestedCount, stats.CompletedCount, stats.ActiveCount, stats.FailedCount)
	fmt.Printf("  New records ingested: %d\n", stats.NewRecords)
	fmt.Printf("  Total unique records: %d\n", stats.TotalRecords)
}

func runStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	dataDir, ledgerPath, convDir, sumDB, asJSON := commonFlags(fs)
	_ = fs.Parse(args)

	svc := getService(*dataDir, *ledgerPath, *convDir, *sumDB, false, false)
	st, err := svc.Status()
	if err != nil {
		fmt.Fprintf(os.Stderr, "status error: %v\n", err)
		os.Exit(1)
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(st)
		return
	}

	fmt.Printf("Usage Ledger Status\n")
	fmt.Printf("  Ledger Path:              %s\n", st.LedgerPath)
	fmt.Printf("  Ledger Size:              %s (%d bytes)\n", formatBytes(st.LedgerSize), st.LedgerSize)
	fmt.Printf("  Discovered Conversations: %d\n", st.DiscoveredConversations)
	fmt.Printf("  Indexed Conversations:    %d (completed: %d, active: %d, failed: %d)\n",
		st.IndexedConversations, st.CompletedConversations, st.ActiveConversations, st.FailedConversations)
	fmt.Printf("  Total Unique Generations: %d\n", st.TotalGenerations)
	fmt.Printf("  Total Tokens Processed:   %s\n", formatNumber(st.TotalTokens))
}

func runVerify(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	dataDir, ledgerPath, convDir, sumDB, asJSON := commonFlags(fs)
	_ = fs.Parse(args)

	svc := getService(*dataDir, *ledgerPath, *convDir, *sumDB, false, false)
	v, err := svc.Verify()
	if err != nil {
		fmt.Fprintf(os.Stderr, "verify error: %v\n", err)
		os.Exit(1)
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(v)
		if !v.Valid {
			os.Exit(1)
		}
		return
	}

	fmt.Printf("Ledger Invariant & Consistency Verification\n")
	fmt.Printf("  Schema Version:             %d\n", v.SchemaVersion)
	fmt.Printf("  Total Usage Records:        %d\n", v.TotalRecords)
	fmt.Printf("  Total Ingest Manifests:     %d\n", v.TotalManifests)
	fmt.Printf("  Discrepant Output Tokens:   %d\n", v.DiscrepantOutputTokens)
	fmt.Printf("  Discrepant Total Tokens:    %d\n", v.DiscrepantTotalTokens)
	fmt.Printf("  Duplicate Generation IDs:   %d\n", v.DuplicateGenerations)

	if !v.Valid {
		fmt.Printf("\nVerification: FAILED\n")
		for _, e := range v.Errors {
			fmt.Printf("  - %s\n", e)
		}
		os.Exit(1)
	} else {
		fmt.Printf("\nVerification: PASS (all invariants hold)\n")
	}
}

func runSummary(args []string) {
	fs := flag.NewFlagSet("summary", flag.ExitOnError)
	dataDir, ledgerPath, convDir, sumDB, asJSON := commonFlags(fs)
	all := fs.Bool("all", true, "Show overall, model, and project aggregates")
	_ = fs.Parse(args)

	svc := getService(*dataDir, *ledgerPath, *convDir, *sumDB, false, false)
	sum, err := svc.Summary()
	if err != nil {
		fmt.Fprintf(os.Stderr, "summary error: %v\n", err)
		os.Exit(1)
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(sum)
		return
	}

	fmt.Printf("AGY Authoritative Usage Summary (Conversations: %d, Generations: %s)\n\n",
		sum.Conversations, formatNumber(uint64(sum.Overall.GenerationCount)))

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TOKEN CATEGORY\tCOUNT")
	fmt.Fprintf(w, "Input (Uncached)\t%s\n", formatNumber(sum.Overall.InputTokens))
	fmt.Fprintf(w, "Cache Read\t%s\n", formatNumber(sum.Overall.CacheReadTokens))
	fmt.Fprintf(w, "Cache Creation\t%s\n", formatNumber(sum.Overall.CacheCreationTokens))
	fmt.Fprintf(w, "Visible Output\t%s\n", formatNumber(sum.Overall.VisibleOutputTokens))
	fmt.Fprintf(w, "Reasoning / Thinking\t%s\n", formatNumber(sum.Overall.ReasoningTokens))
	fmt.Fprintf(w, "Total Output\t%s\n", formatNumber(sum.Overall.TotalOutputTokens))
	fmt.Fprintf(w, "Total Prompt (Input+Cache)\t%s\n", formatNumber(sum.Overall.InputTokens+sum.Overall.CacheReadTokens))
	fmt.Fprintf(w, "TOTAL TOKENS\t%s\n", formatNumber(sum.Overall.TotalTokens))
	_ = w.Flush()

	if *all && len(sum.ByModel) > 0 {
		fmt.Printf("\nBreakdown by Model:\n")
		mw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(mw, "MODEL\tGENERATIONS\tINPUT\tCACHE READ\tOUTPUT\tREASONING\tTOTAL")

		var models []string
		for m := range sum.ByModel {
			models = append(models, m)
		}
		sort.Strings(models)

		for _, m := range models {
			t := sum.ByModel[m]
			fmt.Fprintf(mw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				m,
				formatNumber(uint64(t.GenerationCount)),
				formatNumber(t.InputTokens),
				formatNumber(t.CacheReadTokens),
				formatNumber(t.TotalOutputTokens),
				formatNumber(t.ReasoningTokens),
				formatNumber(t.TotalTokens),
			)
		}
		_ = mw.Flush()
	}

	if *all && len(sum.ByProject) > 0 {
		fmt.Printf("\nBreakdown by Project:\n")
		pw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(pw, "PROJECT\tGENERATIONS\tINPUT\tCACHE READ\tOUTPUT\tTOTAL")

		var projects []string
		for p := range sum.ByProject {
			projects = append(projects, p)
		}
		sort.Strings(projects)

		for _, p := range projects {
			t := sum.ByProject[p]
			displayProject := p
			if displayProject == "" {
				displayProject = "(none)"
			}
			fmt.Fprintf(pw, "%s\t%s\t%s\t%s\t%s\t%s\n",
				displayProject,
				formatNumber(uint64(t.GenerationCount)),
				formatNumber(t.InputTokens),
				formatNumber(t.CacheReadTokens),
				formatNumber(t.TotalOutputTokens),
				formatNumber(t.TotalTokens),
			)
		}
		_ = pw.Flush()
	}
}

func formatNumber(n uint64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	leading := len(s) % 3
	if leading > 0 {
		b.WriteString(s[:leading])
		s = s[leading:]
		if len(s) > 0 {
			b.WriteByte(',')
		}
	}
	for len(s) > 0 {
		b.WriteString(s[:3])
		s = s[3:]
		if len(s) > 0 {
			b.WriteByte(',')
		}
	}
	return b.String()
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
