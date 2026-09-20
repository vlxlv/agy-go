package tokei

import (
	"errors"
	"fmt"
	"io"
	"time"
)

const (
	maxWireLength     = 100 * 1024 * 1024 // 100MB hard limit per blob
	maxRecursionDepth = 10                // bounded recursion depth
	maxVarintBytes    = 10                // standard 64-bit protobuf varint length
)

var (
	errWireTruncated  = errors.New("protobuf wire: truncated data")
	errVarintOverflow = errors.New("protobuf wire: varint overflow")
	errRecursionLimit = errors.New("protobuf wire: max recursion depth exceeded")
	errBlobTooLarge   = errors.New("protobuf wire: blob exceeds maximum allowed length")
)

// protoField represents a raw wire field decoded from a protobuf buffer.
type protoField struct {
	num      uint32
	wireType int
	varint   uint64
	bytes    []byte
}

// decodeVarint decodes an unsigned varint from a byte slice.
func decodeVarint(b []byte) (uint64, int, error) {
	var val uint64
	var shift uint
	for i, byteVal := range b {
		if i >= maxVarintBytes {
			return 0, 0, errVarintOverflow
		}
		val |= uint64(byteVal&0x7F) << shift
		if (byteVal & 0x80) == 0 {
			return val, i + 1, nil
		}
		shift += 7
	}
	return 0, 0, errWireTruncated
}

// decodeFields decodes all top-level wire fields from a slice with recursion depth bounds.
func decodeFields(b []byte, depth int) ([]protoField, error) {
	if depth > maxRecursionDepth {
		return nil, errRecursionLimit
	}
	if len(b) > maxWireLength {
		return nil, errBlobTooLarge
	}

	var fields []protoField
	offset := 0
	for offset < len(b) {
		tag, n, err := decodeVarint(b[offset:])
		if err != nil {
			return nil, err
		}
		offset += n

		fieldNum := uint32(tag >> 3)
		wireType := int(tag & 0x07)
		if fieldNum == 0 {
			return nil, errors.New("protobuf wire: field number 0 is invalid")
		}

		f := protoField{num: fieldNum, wireType: wireType}

		switch wireType {
		case 0: // Varint
			val, vn, err := decodeVarint(b[offset:])
			if err != nil {
				return nil, err
			}
			f.varint = val
			offset += vn
		case 1: // Fixed64
			if offset+8 > len(b) {
				return nil, errWireTruncated
			}
			f.bytes = b[offset : offset+8]
			offset += 8
		case 2: // Length-delimited
			length, ln, err := decodeVarint(b[offset:])
			if err != nil {
				return nil, err
			}
			offset += ln
			if uint64(len(b)-offset) < length {
				return nil, errWireTruncated
			}
			f.bytes = b[offset : offset+int(length)]
			offset += int(length)
		case 5: // Fixed32
			if offset+4 > len(b) {
				return nil, errWireTruncated
			}
			f.bytes = b[offset : offset+4]
			offset += 4
		default:
			// Unsupported or deprecated wire type (groups 3, 4); terminate safely
			return nil, fmt.Errorf("protobuf wire: unsupported wire type %d", wireType)
		}

		fields = append(fields, f)
	}

	return fields, nil
}

// rawUsage captures raw counters and identities from a ModelUsage protobuf message.
type rawUsage struct {
	modelID                   uint64
	inputTokens               uint64
	totalOutputTokens         uint64
	cacheCreationTokens       uint64
	cacheReadTokens           uint64
	providerID                uint64
	messageID                 string
	reasoningTokens           uint64
	visibleOutputTokens       uint64
	responseID                string
	providerAssignedMessageID string
}

// isTokenBearing returns true if any token count is greater than zero.
func (u *rawUsage) isTokenBearing() bool {
	return u.inputTokens > 0 ||
		u.totalOutputTokens > 0 ||
		u.cacheCreationTokens > 0 ||
		u.cacheReadTokens > 0 ||
		u.reasoningTokens > 0 ||
		u.visibleOutputTokens > 0
}

// primaryIdentity returns the canonical deduplication identity string.
func (u *rawUsage) primaryIdentity() string {
	if u.responseID != "" {
		return "response:" + u.responseID
	}
	if u.providerAssignedMessageID != "" {
		return "provider:" + u.providerAssignedMessageID
	}
	if u.messageID != "" {
		return "message:" + u.messageID
	}
	return ""
}

// parseTimestampMessage decodes a google.protobuf.Timestamp (field 1: seconds, field 2: nanos).
func parseTimestampMessage(b []byte, depth int) (time.Time, bool) {
	fields, err := decodeFields(b, depth+1)
	if err != nil {
		return time.Time{}, false
	}
	var sec int64
	var nanos int64
	hasSec := false
	for _, f := range fields {
		if f.num == 1 && f.wireType == 0 {
			sec = int64(f.varint)
			hasSec = true
		} else if f.num == 2 && f.wireType == 0 {
			nanos = int64(f.varint)
		}
	}
	if !hasSec || sec <= 0 {
		return time.Time{}, false
	}
	if nanos < 0 || nanos > 999_999_999 {
		nanos = 0
	}
	return time.Unix(sec, nanos).UTC(), true
}

// parseModelUsage decodes a ModelUsage message.
func parseModelUsage(b []byte, depth int) (*rawUsage, error) {
	fields, err := decodeFields(b, depth+1)
	if err != nil {
		return nil, err
	}
	u := &rawUsage{}
	for _, f := range fields {
		switch f.num {
		case 1:
			if f.wireType == 0 {
				u.modelID = f.varint
			}
		case 2:
			if f.wireType == 0 {
				u.inputTokens = f.varint
			}
		case 3:
			if f.wireType == 0 {
				u.totalOutputTokens = f.varint
			}
		case 4:
			if f.wireType == 0 {
				u.cacheCreationTokens = f.varint
			}
		case 5:
			if f.wireType == 0 {
				u.cacheReadTokens = f.varint
			}
		case 6:
			if f.wireType == 0 {
				u.providerID = f.varint
			}
		case 7:
			if f.wireType == 2 {
				u.messageID = string(f.bytes)
			}
		case 9:
			if f.wireType == 0 {
				u.reasoningTokens = f.varint
			}
		case 10:
			if f.wireType == 0 {
				u.visibleOutputTokens = f.varint
			}
		case 11:
			if f.wireType == 2 {
				u.responseID = string(f.bytes)
			}
		case 12:
			if f.wireType == 2 {
				u.providerAssignedMessageID = string(f.bytes)
			}
		}
	}
	return u, nil
}

// StepEvent represents an extracted usage event from steps.metadata.
type StepEvent struct {
	StepIndex int64
	Timestamp time.Time
	ModelName string
	ModelID   uint64
	Provider  uint64
	Usage     *rawUsage
	Retries   []*rawUsage
}

// ParseStepMetadata parses the steps.metadata blob.
func ParseStepMetadata(stepIdx int64, b []byte) (*StepEvent, error) {
	fields, err := decodeFields(b, 0)
	if err != nil {
		return nil, fmt.Errorf("decode step %d metadata: %w", stepIdx, err)
	}

	ev := &StepEvent{StepIndex: stepIdx}
	var startTime, endTime time.Time

	for _, f := range fields {
		switch f.num {
		case 1: // start timestamp
			if f.wireType == 2 {
				if t, ok := parseTimestampMessage(f.bytes, 0); ok {
					startTime = t
				}
			}
		case 8: // end timestamp
			if f.wireType == 2 {
				if t, ok := parseTimestampMessage(f.bytes, 0); ok {
					endTime = t
				}
			}
		case 9: // usage
			if f.wireType == 2 {
				u, err := parseModelUsage(f.bytes, 0)
				if err == nil {
					ev.Usage = u
				}
			}
		case 24: // model info
			if f.wireType == 2 {
				mFields, err := decodeFields(f.bytes, 1)
				if err == nil {
					for _, mf := range mFields {
						switch mf.num {
						case 1:
							if mf.wireType == 0 {
								ev.ModelID = mf.varint
							}
						case 7:
							if mf.wireType == 0 {
								ev.Provider = mf.varint
							}
						case 8, 12:
							if mf.wireType == 2 && ev.ModelName == "" {
								ev.ModelName = string(mf.bytes)
							}
						}
					}
				}
			}
		case 28: // retry usages
			if f.wireType == 2 {
				rFields, err := decodeFields(f.bytes, 1)
				if err == nil {
					for _, rf := range rFields {
						if rf.num == 2 && rf.wireType == 2 {
							ru, err := parseModelUsage(rf.bytes, 1)
							if err == nil && ru.isTokenBearing() {
								ev.Retries = append(ev.Retries, ru)
							}
						}
					}
				}
			}
		}
	}

	if !endTime.IsZero() {
		ev.Timestamp = endTime
	} else {
		ev.Timestamp = startTime
	}

	return ev, nil
}

// GenEvent represents an extracted usage event from gen_metadata.data.
type GenEvent struct {
	GenIndex  int64
	Timestamp time.Time
	ModelName string
	ModelID   uint64
	Usage     *rawUsage
	Retries   []*rawUsage
}

// ParseGenMetadata parses the gen_metadata.data blob.
func ParseGenMetadata(genIdx int64, b []byte) (*GenEvent, error) {
	fields, err := decodeFields(b, 0)
	if err != nil {
		return nil, fmt.Errorf("decode gen_metadata %d: %w", genIdx, err)
	}

	ev := &GenEvent{GenIndex: genIdx}

	for _, f := range fields {
		if f.num == 1 && f.wireType == 2 {
			chatFields, err := decodeFields(f.bytes, 1)
			if err != nil {
				continue
			}
			for _, cf := range chatFields {
				switch cf.num {
				case 3:
					if cf.wireType == 0 {
						ev.ModelID = cf.varint
					}
				case 4:
					if cf.wireType == 2 {
						u, err := parseModelUsage(cf.bytes, 2)
						if err == nil {
							ev.Usage = u
						}
					}
				case 9:
					if cf.wireType == 2 {
						gFields, err := decodeFields(cf.bytes, 2)
						if err == nil {
							for _, gf := range gFields {
								if gf.num == 4 && gf.wireType == 2 {
									if t, ok := parseTimestampMessage(gf.bytes, 3); ok {
										ev.Timestamp = t
									}
								}
							}
						}
					}
				case 17:
					if cf.wireType == 2 {
						rFields, err := decodeFields(cf.bytes, 2)
						if err == nil {
							for _, rf := range rFields {
								if rf.num == 2 && rf.wireType == 2 {
									ru, err := parseModelUsage(rf.bytes, 3)
									if err == nil && ru.isTokenBearing() {
										ev.Retries = append(ev.Retries, ru)
									}
								}
							}
						}
					}
				case 19, 21:
					if cf.wireType == 2 && ev.ModelName == "" {
						ev.ModelName = string(cf.bytes)
					}
				}
			}
		}
	}

	return ev, nil
}

// ParseTrajectoryTimestamp extracts the fallback base timestamp from trajectory_metadata_blob.
func ParseTrajectoryTimestamp(b []byte) (time.Time, bool) {
	fields, err := decodeFields(b, 0)
	if err != nil {
		return time.Time{}, false
	}
	for _, f := range fields {
		if f.num == 2 && f.wireType == 2 {
			return parseTimestampMessage(f.bytes, 1)
		}
	}
	return time.Time{}, false
}

// Suppress unused imports check
var _ = io.EOF
