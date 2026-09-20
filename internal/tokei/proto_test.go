package tokei

import (
	"bytes"
	"testing"
	"time"
)

// Helper to encode varint wire field
func putVarintField(buf *bytes.Buffer, fieldNum uint32, val uint64) {
	tag := (fieldNum << 3) | 0
	putVarint(buf, uint64(tag))
	putVarint(buf, val)
}

// Helper to encode length-delimited wire field
func putBytesField(buf *bytes.Buffer, fieldNum uint32, data []byte) {
	tag := (fieldNum << 3) | 2
	putVarint(buf, uint64(tag))
	putVarint(buf, uint64(len(data)))
	buf.Write(data)
}

func putVarint(buf *bytes.Buffer, val uint64) {
	for val >= 0x80 {
		buf.WriteByte(byte(val) | 0x80)
		val >>= 7
	}
	buf.WriteByte(byte(val))
}

func buildTimestamp(sec int64, nanos int64) []byte {
	var buf bytes.Buffer
	putVarintField(&buf, 1, uint64(sec))
	putVarintField(&buf, 2, uint64(nanos))
	return buf.Bytes()
}

func buildModelUsage(u *rawUsage) []byte {
	var buf bytes.Buffer
	if u.modelID != 0 {
		putVarintField(&buf, 1, u.modelID)
	}
	if u.inputTokens != 0 {
		putVarintField(&buf, 2, u.inputTokens)
	}
	if u.totalOutputTokens != 0 {
		putVarintField(&buf, 3, u.totalOutputTokens)
	}
	if u.cacheCreationTokens != 0 {
		putVarintField(&buf, 4, u.cacheCreationTokens)
	}
	if u.cacheReadTokens != 0 {
		putVarintField(&buf, 5, u.cacheReadTokens)
	}
	if u.providerID != 0 {
		putVarintField(&buf, 6, u.providerID)
	}
	if u.messageID != "" {
		putBytesField(&buf, 7, []byte(u.messageID))
	}
	if u.reasoningTokens != 0 {
		putVarintField(&buf, 9, u.reasoningTokens)
	}
	if u.visibleOutputTokens != 0 {
		putVarintField(&buf, 10, u.visibleOutputTokens)
	}
	if u.responseID != "" {
		putBytesField(&buf, 11, []byte(u.responseID))
	}
	if u.providerAssignedMessageID != "" {
		putBytesField(&buf, 12, []byte(u.providerAssignedMessageID))
	}
	return buf.Bytes()
}

func buildStepMetadata(startTime, endTime time.Time, modelName string, modelID uint64, usage *rawUsage, retries []*rawUsage) []byte {
	var buf bytes.Buffer
	if !startTime.IsZero() {
		putBytesField(&buf, 1, buildTimestamp(startTime.Unix(), int64(startTime.Nanosecond())))
	}
	if !endTime.IsZero() {
		putBytesField(&buf, 8, buildTimestamp(endTime.Unix(), int64(endTime.Nanosecond())))
	}
	if usage != nil {
		putBytesField(&buf, 9, buildModelUsage(usage))
	}
	if modelName != "" || modelID != 0 {
		var mbuf bytes.Buffer
		if modelID != 0 {
			putVarintField(&mbuf, 1, modelID)
		}
		if modelName != "" {
			putBytesField(&mbuf, 8, []byte(modelName))
		}
		putBytesField(&buf, 24, mbuf.Bytes())
	}
	for _, r := range retries {
		var rbuf bytes.Buffer
		putBytesField(&rbuf, 2, buildModelUsage(r))
		putBytesField(&buf, 28, rbuf.Bytes())
	}
	return buf.Bytes()
}

func buildGenMetadata(ts time.Time, modelName string, modelID uint64, usage *rawUsage, retries []*rawUsage) []byte {
	var chatBuf bytes.Buffer
	if modelID != 0 {
		putVarintField(&chatBuf, 3, modelID)
	}
	if usage != nil {
		putBytesField(&chatBuf, 4, buildModelUsage(usage))
	}
	if !ts.IsZero() {
		var genInfo bytes.Buffer
		putBytesField(&genInfo, 4, buildTimestamp(ts.Unix(), int64(ts.Nanosecond())))
		putBytesField(&chatBuf, 9, genInfo.Bytes())
	}
	for _, r := range retries {
		var rbuf bytes.Buffer
		putBytesField(&rbuf, 2, buildModelUsage(r))
		putBytesField(&chatBuf, 17, rbuf.Bytes())
	}
	if modelName != "" {
		putBytesField(&chatBuf, 19, []byte(modelName))
	}

	var rootBuf bytes.Buffer
	putBytesField(&rootBuf, 1, chatBuf.Bytes())
	return rootBuf.Bytes()
}

func TestProto_DecodeStepMetadata(t *testing.T) {
	ts := time.Date(2026, 9, 20, 12, 0, 0, 500000000, time.UTC)
	u := &rawUsage{
		modelID:             1318,
		inputTokens:         1500,
		cacheReadTokens:     8000,
		reasoningTokens:     300,
		visibleOutputTokens: 200,
		totalOutputTokens:   500,
		responseID:          "resp-12345",
		messageID:           "msg-12345",
	}

	blob := buildStepMetadata(ts, ts, "gemini-3.8-flash", 1318, u, nil)

	ev, err := ParseStepMetadata(1, blob)
	if err != nil {
		t.Fatalf("ParseStepMetadata failed: %v", err)
	}

	if ev.StepIndex != 1 {
		t.Errorf("expected step index 1, got %d", ev.StepIndex)
	}
	if !ev.Timestamp.Equal(ts) {
		t.Errorf("expected timestamp %v, got %v", ts, ev.Timestamp)
	}
	if ev.ModelName != "gemini-3.8-flash" {
		t.Errorf("expected model name gemini-3.8-flash, got %s", ev.ModelName)
	}
	if ev.Usage == nil {
		t.Fatalf("expected usage to be non-nil")
	}
	if ev.Usage.inputTokens != 1500 {
		t.Errorf("expected input tokens 1500, got %d", ev.Usage.inputTokens)
	}
	if ev.Usage.cacheReadTokens != 8000 {
		t.Errorf("expected cache read tokens 8000, got %d", ev.Usage.cacheReadTokens)
	}
	if ev.Usage.reasoningTokens != 300 {
		t.Errorf("expected reasoning tokens 300, got %d", ev.Usage.reasoningTokens)
	}
	if ev.Usage.visibleOutputTokens != 200 {
		t.Errorf("expected visible tokens 200, got %d", ev.Usage.visibleOutputTokens)
	}
	if ev.Usage.totalOutputTokens != 500 {
		t.Errorf("expected total output tokens 500, got %d", ev.Usage.totalOutputTokens)
	}
	if ev.Usage.responseID != "resp-12345" {
		t.Errorf("expected responseID resp-12345, got %s", ev.Usage.responseID)
	}
}

func TestProto_DecodeGenMetadata(t *testing.T) {
	ts := time.Date(2026, 9, 20, 14, 30, 0, 0, time.UTC)
	u := &rawUsage{
		modelID:             1318,
		inputTokens:         2500,
		cacheReadTokens:     12000,
		reasoningTokens:     400,
		visibleOutputTokens: 150,
		totalOutputTokens:   550,
		responseID:          "resp-gen-6789",
	}

	blob := buildGenMetadata(ts, "gemini-3.8-flash", 1318, u, nil)

	ev, err := ParseGenMetadata(42, blob)
	if err != nil {
		t.Fatalf("ParseGenMetadata failed: %v", err)
	}

	if ev.GenIndex != 42 {
		t.Errorf("expected gen index 42, got %d", ev.GenIndex)
	}
	if !ev.Timestamp.Equal(ts) {
		t.Errorf("expected timestamp %v, got %v", ts, ev.Timestamp)
	}
	if ev.ModelName != "gemini-3.8-flash" {
		t.Errorf("expected model name gemini-3.8-flash, got %s", ev.ModelName)
	}
	if ev.Usage == nil || ev.Usage.responseID != "resp-gen-6789" {
		t.Errorf("expected usage response id resp-gen-6789")
	}
}

func TestProto_UnknownFieldsAndForwardCompatibility(t *testing.T) {
	// Inject unrecognized field tags (e.g. tag 99, wire type 0, and tag 100, wire type 2)
	var buf bytes.Buffer
	putVarintField(&buf, 99, 123456)
	putBytesField(&buf, 100, []byte("future compatibility payload"))

	// Valid usage
	u := &rawUsage{inputTokens: 100, totalOutputTokens: 200, responseID: "test"}
	putBytesField(&buf, 9, buildModelUsage(u))

	ev, err := ParseStepMetadata(5, buf.Bytes())
	if err != nil {
		t.Fatalf("unexpected error parsing protobuf with unknown fields: %v", err)
	}
	if ev.Usage == nil || ev.Usage.inputTokens != 100 {
		t.Errorf("failed to extract usage amidst unknown fields")
	}
}

func TestProto_MalformedAndTruncatedWire(t *testing.T) {
	// Truncated length-delimited
	badWire := []byte{0x4A, 0x10, 0x01, 0x02} // tag 9, length 16, but only 2 bytes payload
	_, err := ParseStepMetadata(1, badWire)
	if err == nil {
		t.Errorf("expected error on truncated wire data, got nil")
	}

	// Varint overflow (> 10 bytes with MSB set)
	overflowVarint := bytes.Repeat([]byte{0x80}, 12)
	_, _, err = decodeVarint(overflowVarint)
	if err != errVarintOverflow {
		t.Errorf("expected errVarintOverflow, got %v", err)
	}
}

func TestUsageRecord_NormalizationAndAccounting(t *testing.T) {
	rec := &UsageRecord{
		ConversationID:      "conv-1",
		GenerationID:        "response:resp-1",
		InputTokens:         1000,
		CacheReadTokens:     5000,
		VisibleOutputTokens: 300,
		ReasoningTokens:     700,
		TotalOutputTokens:   0, // will be computed
		Model:               "Gemini 3.8 Flash (Preview)",
	}

	rec.Normalize()

	if rec.TotalOutputTokens != 1000 {
		t.Errorf("expected TotalOutputTokens = 1000, got %d", rec.TotalOutputTokens)
	}
	if rec.TotalTokens != 7000 { // 1000 + 5000 + 1000
		t.Errorf("expected TotalTokens = 7000, got %d", rec.TotalTokens)
	}
	if rec.Model != "gemini-3.8-flash" {
		t.Errorf("expected normalized model gemini-3.8-flash, got %s", rec.Model)
	}
}

func TestUsageRecord_ZeroUsageDrop(t *testing.T) {
	rec := &UsageRecord{
		ConversationID: "conv-empty",
		GenerationID:   "response:empty",
	}
	if rec.IsTokenBearing() {
		t.Errorf("expected zero-usage record to return IsTokenBearing() == false")
	}
}
