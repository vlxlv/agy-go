package tokei

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Reader provides read-only extraction and deduplication of AGY conversation databases.
type Reader struct {
	summariesPath string
}

// NewReader creates an AGY database reader with the given summaries catalog path.
func NewReader(summariesPath string) *Reader {
	return &Reader{
		summariesPath: summariesPath,
	}
}

// ReadCatalog reads project and workspace mappings from conversation_summaries.db.
func (r *Reader) ReadCatalog() (map[string]*ConversationMeta, error) {
	result := make(map[string]*ConversationMeta)
	if r.summariesPath == "" {
		return result, nil
	}
	if _, err := os.Stat(r.summariesPath); os.IsNotExist(err) {
		return result, nil
	}

	dsn := fmt.Sprintf("file:%s?mode=ro&_pragma=busy_timeout(5000)&_pragma=query_only(ON)", r.summariesPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open conversation_summaries: %w", err)
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT conversation_id, title, workspace_uris, project_id, agent_name, last_modified_time, step_count
		FROM conversation_summaries
	`)
	if err != nil {
		return nil, fmt.Errorf("query conversation catalog: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid, title, wsUris, projID, agent string
			lastModStr                        string
			stepCount                         int64
		)
		if err := rows.Scan(&cid, &title, &wsUris, &projID, &agent, &lastModStr, &stepCount); err != nil {
			return nil, err
		}

		cleanURI := ""
		var uris []string
		if err := json.Unmarshal([]byte(wsUris), &uris); err == nil && len(uris) > 0 {
			cleanURI = uris[0]
		} else if strings.HasPrefix(wsUris, "file://") || strings.HasPrefix(wsUris, "/") {
			cleanURI = wsUris
		}

		lastMod, _ := time.Parse(time.RFC3339, lastModStr)
		if lastMod.IsZero() {
			lastMod, _ = time.Parse("2006-01-02 15:04:05.999999999-07:00", lastModStr)
		}

		result[cid] = &ConversationMeta{
			ConversationID: cid,
			Title:          title,
			WorkspaceURI:   cleanURI,
			WorkspaceURIs:  wsUris,
			ProjectID:      projID,
			AgentName:      agent,
			LastModified:   lastMod,
			StepCount:      stepCount,
		}
	}

	return result, rows.Err()
}

// ConversationReadResult holds the extracted records and source metadata for a single database.
type ConversationReadResult struct {
	ConversationID    string
	Records           []*UsageRecord
	HighestStepIndex  int64
	HighestGenIndex   int64
	SourceSize        int64
	SourceMtimeNs     int64
	SourceFingerprint string
	IsComplete        bool
	Status            string
}

// ReadConversation reads and deduplicates all usage records from one AGY conversation database.
func (r *Reader) ReadConversation(dbPath string, meta *ConversationMeta) (*ConversationReadResult, error) {
	fiBefore, err := os.Stat(dbPath)
	if err != nil {
		return nil, fmt.Errorf("stat database %s: %w", dbPath, err)
	}
	if fiBefore.Size() == 0 {
		return nil, fmt.Errorf("empty database %s", dbPath)
	}

	cid := strings.TrimSuffix(filepath.Base(dbPath), ".db")
	dsn := fmt.Sprintf("file:%s?mode=ro&_pragma=busy_timeout(5000)&_pragma=query_only(ON)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open read-only sqlite %s: %w", dbPath, err)
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin source snapshot: %w", err)
	}
	defer tx.Rollback()

	var schemaVer int
	if err := tx.QueryRow("PRAGMA schema_version;").Scan(&schemaVer); err != nil {
		return nil, fmt.Errorf("validate sqlite %s: %w", dbPath, err)
	}

	// 1. Fallback base timestamp from trajectory_metadata_blob
	var baseTimestamp time.Time
	var trajBlob []byte
	row := tx.QueryRow("SELECT data FROM trajectory_metadata_blob WHERE id = 'main'")
	if err := row.Scan(&trajBlob); err == nil && len(trajBlob) > 0 {
		if t, ok := ParseTrajectoryTimestamp(trajBlob); ok {
			baseTimestamp = t
		}
	}
	if baseTimestamp.IsZero() {
		baseTimestamp = fiBefore.ModTime().UTC()
	}

	workspaceURI := ""
	projectID := "unknown"
	if meta != nil {
		workspaceURI = meta.WorkspaceURI
		if meta.ProjectID != "" {
			projectID = meta.ProjectID
		}
	}

	// 2. Read steps table
	stepRows, err := tx.Query("SELECT idx, metadata FROM steps WHERE metadata IS NOT NULL ORDER BY idx ASC")
	var stepEvents []*StepEvent
	var highestStepIdx int64
	if err != nil {
		return nil, fmt.Errorf("read step metadata: %w", err)
	}
	defer stepRows.Close()
	for stepRows.Next() {
		var idx int64
		var blob []byte
		if err := stepRows.Scan(&idx, &blob); err != nil {
			return nil, err
		}
		if idx > highestStepIdx {
			highestStepIdx = idx
		}
		if len(blob) > 0 {
			ev, err := ParseStepMetadata(idx, blob)
			if err != nil {
				return nil, fmt.Errorf("decode step %d: %w", idx, err)
			}
			stepEvents = append(stepEvents, ev)
		}
	}

	if err := stepRows.Err(); err != nil {
		return nil, err
	}
	stepRows.Close()

	// 3. Read gen_metadata table
	genRows, err := tx.Query("SELECT idx, data FROM gen_metadata ORDER BY idx ASC")
	var genEvents []*GenEvent
	var highestGenIdx int64
	if err != nil {
		return nil, fmt.Errorf("read gen metadata: %w", err)
	}
	defer genRows.Close()
	for genRows.Next() {
		var idx int64
		var blob []byte
		if err := genRows.Scan(&idx, &blob); err != nil {
			return nil, err
		}
		if idx > highestGenIdx {
			highestGenIdx = idx
		}
		if len(blob) > 0 {
			ev, err := ParseGenMetadata(idx, blob)
			if err != nil {
				return nil, fmt.Errorf("decode gen %d: %w", idx, err)
			}
			genEvents = append(genEvents, ev)
		}
	}

	if err := genRows.Err(); err != nil {
		return nil, err
	}
	genRows.Close()

	// 4. Deduplicate across steps and gen_metadata
	// Key: Generation identity (e.g. "response:<id>", "provider:<id>", "message:<id>")
	recordMap := make(map[string]*UsageRecord)
	var orderedKeys []string

	// Join all known aliases before counting: an event may acquire a response ID later.
	parents := make(map[string]string)
	var root func(string) string
	root = func(id string) string {
		if parents[id] == "" {
			parents[id] = id
		}
		if parents[id] != id {
			parents[id] = root(parents[id])
		}
		return parents[id]
	}
	link := func(u *rawUsage) error {
		if u == nil {
			return nil
		}
		ids := []string{}
		if u.responseID != "" {
			ids = append(ids, "response:"+u.responseID)
		}
		if u.providerAssignedMessageID != "" {
			ids = append(ids, "provider:"+u.providerAssignedMessageID)
		}
		if u.messageID != "" {
			ids = append(ids, "message:"+u.messageID)
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids[1:] {
			a, b := root(ids[0]), root(id)
			if a != b && ((strings.HasPrefix(a, "response:") && strings.HasPrefix(b, "response:")) ||
				(strings.HasPrefix(a, "provider:") && strings.HasPrefix(b, "provider:"))) {
				return fmt.Errorf("ambiguous generation aliases: distinct generation IDs share an alias")
			}
			// Lexical order prefers response, then provider, then message identities.
			if a < b {
				a, b = b, a
			}
			parents[b] = a
		}
		return nil
	}
	for _, ev := range stepEvents {
		if err := link(ev.Usage); err != nil {
			return nil, err
		}
		for _, u := range ev.Retries {
			if err := link(u); err != nil {
				return nil, err
			}
		}
	}
	for _, ev := range genEvents {
		if err := link(ev.Usage); err != nil {
			return nil, err
		}
		for _, u := range ev.Retries {
			if err := link(u); err != nil {
				return nil, err
			}
		}
	}

	upsert := func(u *rawUsage, stepIdx int64, ts time.Time, modelName string, modelID uint64, provider uint64) {
		if u == nil || !u.isTokenBearing() {
			return
		}
		id := u.primaryIdentity()
		if id != "" {
			id = root(id)
		}
		if id == "" {
			// Fallback generation identity if response_id and message_id are both absent
			id = fmt.Sprintf("step:%s:%d", cid, stepIdx)
		}

		if ts.IsZero() {
			ts = baseTimestamp
		}

		resolvedModel := modelName
		if resolvedModel == "" && modelID != 0 {
			resolvedModel = ModelNameFromID(modelID)
		}
		if resolvedModel == "" && u.modelID != 0 {
			resolvedModel = ModelNameFromID(u.modelID)
		}
		if resolvedModel == "" {
			resolvedModel = DefaultModel
		}

		resolvedProvider := provider
		if resolvedProvider == 0 {
			resolvedProvider = u.providerID
		}

		existing, found := recordMap[id]
		if !found {
			rec := &UsageRecord{
				ConversationID:            cid,
				GenerationID:              id,
				StepIndex:                 stepIdx,
				Timestamp:                 ts,
				WorkspaceURI:              workspaceURI,
				ProjectID:                 projectID,
				Model:                     resolvedModel,
				ProviderID:                resolvedProvider,
				ResponseID:                u.responseID,
				ProviderAssignedMessageID: u.providerAssignedMessageID,
				MessageID:                 u.messageID,
				InputTokens:               u.inputTokens,
				CacheReadTokens:           u.cacheReadTokens,
				CacheCreationTokens:       u.cacheCreationTokens,
				VisibleOutputTokens:       u.visibleOutputTokens,
				ReasoningTokens:           u.reasoningTokens,
				TotalOutputTokens:         u.totalOutputTokens,
			}
			rec.Normalize()
			recordMap[id] = rec
			orderedKeys = append(orderedKeys, id)
			return
		}

		// Merge conservative: take max counters
		if u.inputTokens > existing.InputTokens {
			existing.InputTokens = u.inputTokens
		}
		if u.cacheReadTokens > existing.CacheReadTokens {
			existing.CacheReadTokens = u.cacheReadTokens
		}
		if u.cacheCreationTokens > existing.CacheCreationTokens {
			existing.CacheCreationTokens = u.cacheCreationTokens
		}
		if u.visibleOutputTokens > existing.VisibleOutputTokens {
			existing.VisibleOutputTokens = u.visibleOutputTokens
		}
		if u.reasoningTokens > existing.ReasoningTokens {
			existing.ReasoningTokens = u.reasoningTokens
		}
		if u.totalOutputTokens > existing.TotalOutputTokens {
			existing.TotalOutputTokens = u.totalOutputTokens
		}

		// Prefer explicit non-default model name
		if existing.Model == DefaultModel && resolvedModel != DefaultModel {
			existing.Model = resolvedModel
		}
		if existing.ProviderID == 0 && resolvedProvider != 0 {
			existing.ProviderID = resolvedProvider
		}

		// Prefer earlier non-zero timestamp
		if existing.Timestamp.IsZero() || (!ts.IsZero() && ts.Before(existing.Timestamp)) {
			existing.Timestamp = ts
		}

		existing.Normalize()
	}

	// Ingest steps (authoritative for timestamps and step order)
	for _, ev := range stepEvents {
		if ev.Usage != nil {
			upsert(ev.Usage, ev.StepIndex, ev.Timestamp, ev.ModelName, ev.ModelID, ev.Provider)
		}
		for _, ru := range ev.Retries {
			upsert(ru, ev.StepIndex, ev.Timestamp, ev.ModelName, ev.ModelID, ev.Provider)
		}
	}

	// Ingest gen_metadata (supplements any steps not captured in steps table)
	for _, ev := range genEvents {
		if ev.Usage != nil {
			upsert(ev.Usage, ev.GenIndex, ev.Timestamp, ev.ModelName, ev.ModelID, 0)
		}
		for _, ru := range ev.Retries {
			upsert(ru, ev.GenIndex, ev.Timestamp, ev.ModelName, ev.ModelID, 0)
		}
	}

	var records []*UsageRecord
	for _, k := range orderedKeys {
		r := recordMap[k]
		if r.IsTokenBearing() {
			records = append(records, r)
		}
	}

	// 5. Active database safety check: verify file did not change during extraction
	fiAfter, err := os.Stat(dbPath)
	if err != nil {
		return nil, fmt.Errorf("stat database after reading %s: %w", dbPath, err)
	}

	// Check if active WAL or SHM file exists
	walPath := dbPath + "-wal"
	hasActiveWAL := false
	if fiWal, err := os.Stat(walPath); err == nil && fiWal.Size() > 0 {
		hasActiveWAL = true
	}

	sourceChanged := fiAfter.Size() != fiBefore.Size() || fiAfter.ModTime().UnixNano() != fiBefore.ModTime().UnixNano()
	isComplete := !sourceChanged && !hasActiveWAL
	status := "completed"
	if !isComplete {
		status = "active"
	}

	fingerprint := fmt.Sprintf("size=%d:mtime=%d:steps=%d:gen=%d",
		fiAfter.Size(), fiAfter.ModTime().UnixNano(), highestStepIdx, highestGenIdx)

	return &ConversationReadResult{
		ConversationID:    cid,
		Records:           records,
		HighestStepIndex:  highestStepIdx,
		HighestGenIndex:   highestGenIdx,
		SourceSize:        fiAfter.Size(),
		SourceMtimeNs:     fiAfter.ModTime().UnixNano(),
		SourceFingerprint: fingerprint,
		IsComplete:        isComplete,
		Status:            status,
	}, nil
}
