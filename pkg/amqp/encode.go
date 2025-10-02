package amqp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

// encode helpers
func encodeShort(v uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return b
}
func encodeLong(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}
func encodeLongLong(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
func encodeLongStr(s string) []byte {
	b := make([]byte, 4+len(s))
	binary.BigEndian.PutUint32(b[0:4], uint32(len(s)))
	copy(b[4:], []byte(s))
	return b
}
func encodeFieldTableEmpty() []byte {
	return []byte{0, 0, 0, 0}
}

// shortstr: 1-byte length + bytes
func encodeShortStr(s string) []byte {
	if len(s) > 255 {
		s = s[:255]
	}
	b := make([]byte, 1+len(s))
	b[0] = byte(len(s))
	copy(b[1:], []byte(s))
	return b
}

// Build a connection.start method arguments
func buildStartArgs() []byte {
	var buf bytes.Buffer
	buf.WriteByte(0) // version-major
	buf.WriteByte(9) // version-minor (we advertise 0.9)
	buf.Write(encodeFieldTableEmpty())
	buf.Write(encodeLongStr("PLAIN"))
	buf.Write(encodeLongStr("en_US"))
	return buf.Bytes()
}

// Build a connection.tune args
func buildTuneArgs(channelMax uint16, frameMax uint32, heartbeat uint16) []byte {
	var buf bytes.Buffer
	buf.Write(encodeShort(channelMax))
	buf.Write(encodeLong(frameMax))
	buf.Write(encodeShort(heartbeat))
	return buf.Bytes()
}

// Build an ack method args: delivery-tag (longlong) + bit for multiple
func buildAckArgs(deliveryTag uint64, multiple bool) []byte {
	var buf bytes.Buffer
	buf.Write(encodeLongLong(deliveryTag))
	var b byte
	if multiple {
		b = 1
	}
	buf.WriteByte(b)
	return buf.Bytes()
}

// Build a nack method args: delivery-tag (longlong) + bit for multiple + bit for requeue
func buildNackArgs(deliveryTag uint64, multiple bool, requeue bool) []byte {
	var buf bytes.Buffer
	buf.Write(encodeLongLong(deliveryTag))
	var b byte
	if multiple {
		b |= 1
	}
	if requeue {
		b |= 2
	}
	buf.WriteByte(b)
	return buf.Bytes()
}

// Build a reject method args: delivery-tag (longlong) + bit for requeue
func buildRejectArgs(deliveryTag uint64, requeue bool) []byte {
	var buf bytes.Buffer
	buf.Write(encodeLongLong(deliveryTag))
	var b byte
	if requeue {
		b = 1
	}
	buf.WriteByte(b)
	return buf.Bytes()
}

// Build a content header frame payload for classID and bodySize. properties are omitted.
func buildContentHeaderPayload(classID uint16, bodySize uint64) []byte {
	// class-id (short), weight (short=0), body-size (longlong), property-flags (short=0)
	var buf bytes.Buffer
	buf.Write(encodeShort(classID))
	buf.Write(encodeShort(0))
	buf.Write(encodeLongLong(bodySize))
	buf.Write(encodeShort(0))
	return buf.Bytes()
}

// Exported wrappers
func EncodeShortStr(s string) []byte { return encodeShortStr(s) }
func EncodeLongLong(v uint64) []byte { return encodeLongLong(v) }
func EncodeShort(v uint16) []byte    { return encodeShort(v) }
func BuildContentHeaderPayload(classID uint16, bodySize uint64) []byte {
	return buildContentHeaderPayload(classID, bodySize)
}

// Field table parsing/serialization
// Supports a subset of AMQP field-value types commonly used in headers:
//  - 't' boolean (1 octet)
//  - 'b' signed 8-bit
//  - 'B' unsigned 8-bit
//  - 'I' signed 32-bit
//  - 'l' signed 64-bit
//  - 's' shortstr (1-byte length + bytes)
//  - 'S' longstr (4-byte length + bytes)
//  - 'F' nested field table
//  - 'A' array (array of field-values)
//  - 'T' timestamp (64-bit)
// Unknown types return an error.

func writeFieldTable(tbl map[string]interface{}) []byte {
	var buf bytes.Buffer
	// placeholder for length
	buf.Write(make([]byte, 4))
	start := buf.Len()
	for k, v := range tbl {
		// field-name shortstr
		buf.Write(encodeShortStr(k))
		switch val := v.(type) {
		case bool:
			buf.WriteByte('t')
			if val {
				buf.WriteByte(1)
			} else {
				buf.WriteByte(0)
			}
		case int8:
			buf.WriteByte('b')
			buf.WriteByte(byte(val))
		case uint8:
			buf.WriteByte('B')
			buf.WriteByte(byte(val))
		case int32:
			buf.WriteByte('I')
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, uint32(val))
			buf.Write(b)
		case int64:
			buf.WriteByte('l')
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, uint64(val))
			buf.Write(b)
		case string:
			// write as longstr
			buf.WriteByte('S')
			bs := []byte(val)
			l := make([]byte, 4)
			binary.BigEndian.PutUint32(l, uint32(len(bs)))
			buf.Write(l)
			buf.Write(bs)
		case map[string]interface{}:
			buf.WriteByte('F')
			ft := writeFieldTable(val)
			buf.Write(ft)
		case []interface{}:
			buf.WriteByte('A')
			// write array as field-value array: 4-byte length then sequence of field-values
			var arr bytes.Buffer
			for _, item := range val {
				// for simplicity, encode each item as a longstr for strings or int for ints
				switch it := item.(type) {
				case string:
					arr.WriteByte('S')
					bs := []byte(it)
					l := make([]byte, 4)
					binary.BigEndian.PutUint32(l, uint32(len(bs)))
					arr.Write(l)
					arr.Write(bs)
				case int32:
					arr.WriteByte('I')
					b := make([]byte, 4)
					binary.BigEndian.PutUint32(b, uint32(it))
					arr.Write(b)
				default:
					// skip unknown
				}
			}
			al := make([]byte, 4)
			binary.BigEndian.PutUint32(al, uint32(arr.Len()))
			buf.Write(al)
			buf.Write(arr.Bytes())
		case uint64:
			buf.WriteByte('l')
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, val)
			buf.Write(b)
		default:
			// best-effort: try string
			s := fmt.Sprintf("%v", val)
			buf.WriteByte('S')
			bs := []byte(s)
			l := make([]byte, 4)
			binary.BigEndian.PutUint32(l, uint32(len(bs)))
			buf.Write(l)
			buf.Write(bs)
		}
	}
	// fill length
	total := buf.Len() - start
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(total))
	out := buf.Bytes()
	copy(out[0:4], b)
	return out
}

func parseFieldTable(data []byte) (map[string]interface{}, int, error) {
	if len(data) < 4 {
		return nil, 0, fmt.Errorf("field-table too short")
	}
	tableLen := int(binary.BigEndian.Uint32(data[0:4]))
	if len(data) < 4+tableLen {
		return nil, 0, fmt.Errorf("field-table truncated")
	}
	pos := 4
	end := 4 + tableLen
	out := map[string]interface{}{}
	for pos < end {
		if pos >= len(data) {
			break
		}
		nameLen := int(data[pos])
		pos++
		if pos+nameLen > len(data) {
			return nil, 0, fmt.Errorf("field-name truncated")
		}
		name := string(data[pos : pos+nameLen])
		pos += nameLen
		if pos >= len(data) {
			return nil, 0, fmt.Errorf("missing field-type for %s", name)
		}
		typ := data[pos]
		pos++
		switch typ {
		case 't':
			out[name] = data[pos] != 0
			pos++
		case 'b':
			out[name] = int8(data[pos])
			pos++
		case 'B':
			out[name] = uint8(data[pos])
			pos++
		case 'I':
			if pos+4 > len(data) {
				return nil, 0, fmt.Errorf("int32 truncated")
			}
			out[name] = int32(binary.BigEndian.Uint32(data[pos : pos+4]))
			pos += 4
		case 'l':
			if pos+8 > len(data) {
				return nil, 0, fmt.Errorf("int64 truncated")
			}
			out[name] = int64(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
		case 'S':
			if pos+4 > len(data) {
				return nil, 0, fmt.Errorf("longstr truncated")
			}
			slen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
			pos += 4
			if pos+slen > len(data) {
				return nil, 0, fmt.Errorf("longstr bytes truncated")
			}
			out[name] = string(data[pos : pos+slen])
			pos += slen
		case 's':
			if pos >= len(data) {
				return nil, 0, fmt.Errorf("shortstr truncated")
			}
			slen := int(data[pos])
			pos++
			if pos+slen > len(data) {
				return nil, 0, fmt.Errorf("shortstr bytes truncated")
			}
			out[name] = string(data[pos : pos+slen])
			pos += slen
		case 'F':
			// nested table
			if pos+4 > len(data) {
				return nil, 0, fmt.Errorf("nested table header truncated")
			}
			// call recursively
			nested, consumed, err := parseFieldTable(data[pos:])
			if err != nil {
				return nil, 0, err
			}
			out[name] = nested
			pos += consumed
		case 'A':
			if pos+4 > len(data) {
				return nil, 0, fmt.Errorf("array header truncated")
			}
			arrLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
			pos += 4
			arrEnd := pos + arrLen
			var arr []interface{}
			for pos < arrEnd {
				if pos >= len(data) {
					break
				}
				vtype := data[pos]
				pos++
				switch vtype {
				case 'S':
					if pos+4 > len(data) {
						return nil, 0, fmt.Errorf("array longstr truncated")
					}
					slen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
					pos += 4
					if pos+slen > len(data) {
						return nil, 0, fmt.Errorf("array longstr bytes truncated")
					}
					arr = append(arr, string(data[pos:pos+slen]))
					pos += slen
				case 'I':
					if pos+4 > len(data) {
						return nil, 0, fmt.Errorf("array int truncated")
					}
					arr = append(arr, int32(binary.BigEndian.Uint32(data[pos:pos+4])))
					pos += 4
				default:
					// skip unknown: stop parsing array
					pos = arrEnd
				}
			}
			out[name] = arr
		case 'T':
			if pos+8 > len(data) {
				return nil, 0, fmt.Errorf("timestamp truncated")
			}
			ts := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
			out[name] = time.Unix(ts, 0)
			pos += 8
		default:
			return nil, 0, fmt.Errorf("unsupported field value type: %c", typ)
		}
	}
	return out, end, nil
}
