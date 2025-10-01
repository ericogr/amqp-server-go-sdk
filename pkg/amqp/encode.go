package amqp

import (
	"bytes"
	"encoding/binary"
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
