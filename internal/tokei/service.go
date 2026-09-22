package tokei

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Default paths for AGY storage and tokei ledger.
var (
	DefaultDataDir          = filepath.Join(os.Getenv("HOME"), ".local", "share", "agy-tokei")
	DefaultLedgerPath       = filepath.Join(DefaultDataDir, "usage.db")
	DefaultConversationsDir = filepath.Join(os.Getenv("HOME"), ".gemini", "antigravity-cli", "conversations")
	DefaultSummariesDB      = filepath.Join(os.Getenv("HOME"), ".gemini", "antigravity-cli", "conversation_summaries.db")
)

// ServiceOptions configures the ingestion and query service.
type ServiceOptions struct {
	LedgerPath       string
	ConversationsDir string
	SummariesPath    string
	Force            bool
	Verbose          bool
}

// IngestStats records metrics from an ingestion run.
type IngestStats struct {
	TotalDiscovered int           `json:"total_discovered"`
	AlreadyUpToDate int           `json:"already_up_to_date"`
	IngestedCount   int           `json:"ingested_count"`
	CompletedCount  int           `json:"completed_count"`
	ActiveCount     int           `json:"active_count"`
	FailedCount     int           `json:"failed_count"`
	TotalRecords    int64         `json:"total_records"`
	NewRecords      int64         `json:"new_records"`
	Duration        time.Duration `json:"duration_ms"`
}

// Service coordinates discovery, ingestion, ledger management, and reporting.
type Service struct {
	opts ServiceOptions
}

// NewService creates a new Service instance.
func NewService(opts ServiceOptions) *Service {
	if opts.LedgerPath == "" {
		if env := os.Getenv("AGY_TOKEI_LEDGER_PATH"); env != "" {
			opts.LedgerPath = env
		} else if envDir := os.Getenv("AGY_TOKEI_DATA_DIR"); envDir != "" {
			opts.LedgerPath = filepath.Join(envDir, "usage.db")
		} else {
			opts.LedgerPath = DefaultLedgerPath
		}
	}
	if opts.ConversationsDir == "" {
		if env := os.Getenv("AGY_CONVERSATIONS_DIR"); env != "" {
			opts.ConversationsDir = env
		} else {
			opts.ConversationsDir = DefaultConversationsDir
		}
	}
	if opts.SummariesPath == "" {
		if env := os.Getenv("AGY_SUMMARIES_DB"); env != "" {
			opts.SummariesPath = env
		} else {
			opts.SummariesPath = DefaultSummariesDB
		}
	}
	return &Service{opts: opts}
}

// Discover returns all candidate AGY conversation SQLite databases in sorted order.
func (s *Service) Discover() ([]string, error) {
	if _, err := os.Stat(s.opts.ConversationsDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("conversations directory %s does not exist", s.opts.ConversationsDir)
	}

	entries, err := os.ReadDir(s.opts.ConversationsDir)
	if err != nil {
		return nil, fmt.Errorf("read conversations directory: %w", err)
	}

	var dbs []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".db") && !strings.HasSuffix(name, "-wal") && !strings.HasSuffix(name, "-shm") {
			dbs = append(dbs, filepath.Join(s.opts.ConversationsDir, name))
		}
	}
	sort.Strings(dbs)
	return dbs, nil
}

// Ingest performs an incremental scan and ingestion of all discovered AGY conversation DBs.
func (s *Service) Ingest() (*IngestStats, error) {
	startTime := time.Now()

	dbs, err := s.Discover()
	if err != nil {
		return nil, err
	}

	ledger, err := OpenLedger(s.opts.LedgerPath)
	if err != nil {
		return nil, err
	}
	defer ledger.Close()

	// Synchronize or load catalog metadata
	catalog := make(map[string]*ConversationMeta)
	summariesPath := s.opts.SummariesPath
	if fiSum, err := os.Stat(summariesPath); err == nil && !fiSum.IsDir() {
		catalogSync, err := ledger.GetCatalogSyncState()
		if err != nil {
			return nil, fmt.Errorf("get catalog sync state: %w", err)
		}

		wal, walErr := os.Stat(summariesPath + "-wal")
		noWAL := os.IsNotExist(walErr) || (walErr == nil && wal.Size() == 0)
		catalogUnchanged := noWAL && !s.opts.Force && catalogSync != nil &&
			catalogSync.CatalogPath == summariesPath &&
			catalogSync.CatalogSize == fiSum.Size() &&
			catalogSync.CatalogMtimeNs == fiSum.ModTime().UnixNano()

		if catalogUnchanged {
			catalog, err = ledger.GetAllConversationMetadata()
			if err != nil {
				return nil, fmt.Errorf("get cached conversation metadata: %w", err)
			}
		} else {
			sumReader := NewReader(summariesPath)
			catalog, err = sumReader.ReadCatalog()
			if err != nil {
				return nil, fmt.Errorf("read catalog: %w", err)
			}
			if err := ledger.SyncCatalog(summariesPath, fiSum.Size(), fiSum.ModTime().UnixNano(), catalog); err != nil {
				return nil, fmt.Errorf("sync catalog: %w", err)
			}
		}
	} else {
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		catalog, err = ledger.GetAllConversationMetadata()
		if err != nil {
			return nil, err
		}
	}

	reader := NewReader(summariesPath)
	manifests, err := ledger.GetManifests()
	if err != nil {
		return nil, err
	}

	stats := &IngestStats{
		TotalDiscovered: len(dbs),
	}

	for _, dbPath := range dbs {
		cid := strings.TrimSuffix(filepath.Base(dbPath), ".db")
		fi, err := os.Stat(dbPath)
		if err != nil {
			stats.FailedCount++
			_ = ledger.RecordScanFailure(cid, dbPath, err)
			continue
		}

		// Cheap incremental skip check
		if !s.opts.Force {
			if existing, ok := manifests[cid]; ok {
				walPath := dbPath + "-wal"
				hasActiveWAL := false
				if fiWal, err := os.Stat(walPath); err == nil && fiWal.Size() > 0 {
					hasActiveWAL = true
				}

				if existing.IsComplete && existing.ParserVersion == ParserVersion && !hasActiveWAL &&
					existing.LastScanStatus != "failed" &&
					existing.SourceSize == fi.Size() &&
					existing.SourceMtimeNs == fi.ModTime().UnixNano() {
					stats.AlreadyUpToDate++
					continue
				}
			}
		}

		// Ingest conversation
		meta := catalog[cid]
		readRes, err := reader.ReadConversation(dbPath, meta)
		if err != nil {
			stats.FailedCount++
			_ = ledger.RecordScanFailure(cid, dbPath, err)
			continue
		}

		manifest := &SourceManifest{
			ConversationID:    cid,
			SourcePath:        dbPath,
			SourceSize:        readRes.SourceSize,
			SourceMtimeNs:     readRes.SourceMtimeNs,
			SourceFingerprint: readRes.SourceFingerprint,
			HighestStepIndex:  readRes.HighestStepIndex,
			HighestGenIndex:   readRes.HighestGenIndex,
			IsComplete:        readRes.IsComplete,
			Status:            readRes.Status,
		}

		if err := ledger.CommitIngest(manifest, readRes.Records); err != nil {
			stats.FailedCount++
			continue
		}

		stats.IngestedCount++
		stats.NewRecords += int64(len(readRes.Records))
		if readRes.IsComplete {
			stats.CompletedCount++
		} else {
			stats.ActiveCount++
		}
	}

	st, err := ledger.GetStatus(len(dbs))
	if err == nil {
		stats.TotalRecords = st.TotalGenerations
	}

	stats.Duration = time.Since(startTime)
	return stats, nil
}

// Status returns a high-level status of the ledger and discovery state.
func (s *Service) Status() (*StatusReport, error) {
	dbs, err := s.Discover()
	if err != nil {
		return nil, err
	}
	discoveredCount := len(dbs)

	ledger, err := OpenLedgerReadOnly(s.opts.LedgerPath)
	if err != nil {
		return nil, err
	}
	defer ledger.Close()

	return ledger.GetStatus(discoveredCount)
}

// Verify runs validation checks across the usage ledger.
func (s *Service) Verify() (*VerifyReport, error) {
	ledger, err := OpenLedgerReadOnly(s.opts.LedgerPath)
	if err != nil {
		return nil, err
	}
	defer ledger.Close()

	return ledger.Verify()
}

// Summary returns aggregated token totals across the dataset.
func (s *Service) Summary() (*SummaryReport, error) {
	ledger, err := OpenLedgerReadOnly(s.opts.LedgerPath)
	if err != nil {
		return nil, err
	}
	defer ledger.Close()

	return ledger.GetSummary()
}
