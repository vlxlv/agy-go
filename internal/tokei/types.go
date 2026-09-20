package tokei

import (
	"fmt"
	"strings"
	"time"
)

// ParserVersion records the parser format version for provenance tracking.
const ParserVersion = "1.0.0"

// DefaultModel is the fallback model name when none can be determined.
const DefaultModel = "gemini-internal-model"

// UsageRecord represents an authoritative, deduplicated LLM generation event.
type UsageRecord struct {
	ID                        int64     `json:"id,omitempty"`
	ConversationID            string    `json:"conversation_id"`
	GenerationID              string    `json:"generation_id"`
	StepIndex                 int64     `json:"step_index"`
	Timestamp                 time.Time `json:"timestamp"`
	WorkspaceURI              string    `json:"workspace_uri"`
	ProjectID                 string    `json:"project_id"`
	Model                     string    `json:"model"`
	ProviderID                uint64    `json:"provider_id"`
	ResponseID                string    `json:"response_id"`
	ProviderAssignedMessageID string    `json:"provider_assigned_message_id"`
	MessageID                 string    `json:"message_id"`

	InputTokens         uint64 `json:"input_tokens"`
	CacheReadTokens     uint64 `json:"cache_read_tokens"`
	CacheCreationTokens uint64 `json:"cache_creation_tokens"`
	VisibleOutputTokens uint64 `json:"visible_output_tokens"`
	ReasoningTokens     uint64 `json:"reasoning_tokens"`
	TotalOutputTokens   uint64 `json:"total_output_tokens"`
	TotalTokens         uint64 `json:"total_tokens"`
}

// Normalize validates and enforces token arithmetic invariants on the record.
func (u *UsageRecord) Normalize() {
	// TotalOutputTokens = VisibleOutputTokens + ReasoningTokens
	calculatedOutput := u.VisibleOutputTokens + u.ReasoningTokens
	if calculatedOutput > u.TotalOutputTokens {
		u.TotalOutputTokens = calculatedOutput
	} else if u.TotalOutputTokens > calculatedOutput {
		if u.ReasoningTokens > 0 && u.VisibleOutputTokens == 0 {
			u.VisibleOutputTokens = u.TotalOutputTokens - u.ReasoningTokens
		} else if u.VisibleOutputTokens > 0 && u.ReasoningTokens == 0 {
			u.ReasoningTokens = u.TotalOutputTokens - u.VisibleOutputTokens
		}
	}

	// TotalTokens = InputTokens + CacheReadTokens + TotalOutputTokens
	u.TotalTokens = u.InputTokens + u.CacheReadTokens + u.TotalOutputTokens

	u.Model = NormalizeModel(u.Model)
}

// IsTokenBearing returns true if any token counter is non-zero.
func (u *UsageRecord) IsTokenBearing() bool {
	return u.InputTokens > 0 ||
		u.TotalOutputTokens > 0 ||
		u.CacheReadTokens > 0 ||
		u.CacheCreationTokens > 0 ||
		u.ReasoningTokens > 0 ||
		u.VisibleOutputTokens > 0
}

// SourceManifest records ingestion state, completeness, and provenance for one conversation DB.
type SourceManifest struct {
	ConversationID      string     `json:"conversation_id"`
	SourcePath          string     `json:"source_path"`
	SourceSize          int64      `json:"source_size"`
	SourceMtimeNs       int64      `json:"source_mtime_ns"`
	SourceFingerprint   string     `json:"source_fingerprint"`
	SourceSHA256        *string    `json:"source_sha256,omitempty"`
	HighestStepIndex    int64      `json:"highest_step_index"`
	HighestGenIndex     int64      `json:"highest_gen_index"`
	GenerationCount     int64      `json:"generation_count"`
	FirstUsageTimestamp *time.Time `json:"first_usage_timestamp,omitempty"`
	LastUsageTimestamp  *time.Time `json:"last_usage_timestamp,omitempty"`
	ParserVersion       string     `json:"parser_version"`
	IngestedAt          time.Time  `json:"ingested_at"`
	IsComplete          bool       `json:"is_complete"`
	Status              string     `json:"status"` // "completed", "active", "failed"
	ErrorMessage        string     `json:"error_message,omitempty"`
	LastScanAt          *time.Time `json:"last_scan_at,omitempty"`
	LastScanStatus      string     `json:"last_scan_status,omitempty"`
	LastScanError       string     `json:"last_scan_error,omitempty"`
}

// ConversationMeta holds workspace and project mappings from conversation_summaries.db.
type ConversationMeta struct {
	ConversationID string    `json:"conversation_id"`
	Title          string    `json:"title"`
	WorkspaceURI   string    `json:"workspace_uri"`
	WorkspaceURIs  string    `json:"workspace_uris,omitempty"`
	ProjectID      string    `json:"project_id"`
	AgentName      string    `json:"agent_name"`
	LastModified   time.Time `json:"last_modified"`
	StepCount      int64     `json:"step_count"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
}

// CatalogSyncState records the sync checkpoint for conversation_summaries.db.
type CatalogSyncState struct {
	CatalogPath       string    `json:"catalog_path"`
	CatalogSize       int64     `json:"catalog_size"`
	CatalogMtimeNs    int64     `json:"catalog_mtime_ns"`
	CatalogSHA256     string    `json:"catalog_sha256,omitempty"`
	ConversationCount int       `json:"conversation_count"`
	SyncedAt          time.Time `json:"synced_at"`
}

// ModelNameFromID maps known numeric model IDs in AGY protobufs to canonical model strings.
func ModelNameFromID(modelID uint64) string {
	switch modelID {
	case 246:
		return "gemini-2.5-pro"
	case 312:
		return "gemini-2.5-flash"
	case 313, 329:
		return "gemini-2.5-flash-thinking"
	case 330:
		return "gemini-2.5-flash-lite"
	case 281, 282:
		return "claude-4-sonnet"
	case 290, 291:
		return "claude-4-opus"
	case 333, 334:
		return "claude-4.5-sonnet"
	case 340, 341:
		return "claude-4.5-haiku"
	case 342:
		return "gpt-oss-120b-medium"
	case 1318:
		return "gemini-3.8-flash"
	default:
		if modelID >= 1000 {
			return fmt.Sprintf("model_placeholder_m%d", modelID-1000)
		}
		return fmt.Sprintf("antigravity-model-%d", modelID)
	}
}

// NormalizeModel canonicalizes model names and removes ephemeral suffixes or parenthesis decorators.
func NormalizeModel(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return DefaultModel
	}
	lower := strings.ToLower(trimmed)
	if idx := strings.Index(lower, "("); idx != -1 {
		lower = strings.TrimSpace(lower[:idx])
	}
	switch lower {
	case "gemini 3.8 flash", "gemini-3.8-flash":
		return "gemini-3.8-flash"
	case "gemini 3.7 flash", "gemini-3.7-flash", "gemini 3.7 flash thinking":
		return "gemini-3.7-flash"
	case "gemini 3.7 pro", "gemini-3.7-pro", "gemini 3.7 pro thinking":
		return "gemini-3.7-pro"
	case "gemini 3.6 flash", "gemini-3.6-flash", "gemini 3 flash":
		return "gemini-3.6-flash"
	case "gemini 3.6 pro", "gemini-3.6-pro":
		return "gemini-3.6-pro"
	case "gemini 3 pro", "gemini-3-pro", "gemini 3 pro thinking":
		return "gemini-3-pro"
	case "gemini 2.5 flash", "gemini-2.5-flash":
		return "gemini-2.5-flash"
	case "gemini 2.5 pro", "gemini-2.5-pro":
		return "gemini-2.5-pro"
	case "gemini 2.0 flash", "gemini-2.0-flash", "gemini 2 flash":
		return "gemini-2.0-flash"
	case "gemini 2.0 pro", "gemini-2.0-pro":
		return "gemini-2.0-pro"
	case "gemini 1.5 flash", "gemini-1.5-flash":
		return "gemini-1.5-flash"
	case "gemini 1.5 pro", "gemini-1.5-pro":
		return "gemini-1.5-pro"
	default:
		return strings.ReplaceAll(lower, " ", "-")
	}
}
