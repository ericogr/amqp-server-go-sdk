package amqp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	frameMethod    = 1
	frameHeader    = 2
	frameBody      = 3
	frameHeartbeat = 8
	frameEnd       = 0xCE
)

// limits and well-known classes/methods
const (
	MaxFrameSize = 1 << 20 // 1MB

	classConnection = 10
	classChannel    = 20
	classBasic      = 60
	classConfirm    = 85

	methodConnStart   = 10
	methodConnStartOk = 11
	methodConnTune    = 30
	methodConnTuneOk  = 31
	methodConnOpen    = 40
	methodConnOpenOk  = 41
	methodConnClose   = 50
	methodConnCloseOk = 51

	methodChannelOpen    = 10
	methodChannelOpenOk  = 11
	methodChannelClose   = 40
	methodChannelCloseOk = 41

	methodBasicPublish    = 40
	methodBasicAck        = 80
	methodConfirmSelect   = 10
	methodConfirmSelectOk = 11
)

// Frame represents a raw AMQP frame
type Frame struct {
	Type    uint8
	Channel uint16
	Payload []byte
}

// ReadFrame reads a single frame from r
func ReadFrame(r io.Reader) (Frame, error) {
	var hdr [7]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, err
	}
	t := hdr[0]
	ch := binary.BigEndian.Uint16(hdr[1:3])
	size := binary.BigEndian.Uint32(hdr[3:7])
	if size > MaxFrameSize {
		return Frame{}, fmt.Errorf("frame size %d exceeds limit %d", size, MaxFrameSize)
	}
	payload := make([]byte, size)
	if size > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return Frame{}, err
		}
	}
	// read frame-end octet
	var end [1]byte
	if _, err := io.ReadFull(r, end[:]); err != nil {
		return Frame{}, err
	}
	if end[0] != frameEnd {
		return Frame{}, errors.New("invalid frame end")
	}
	return Frame{Type: t, Channel: ch, Payload: payload}, nil
}

// WriteFrame writes a frame to w
func WriteFrame(w io.Writer, f Frame) error {
	var hdr [7]byte
	hdr[0] = f.Type
	binary.BigEndian.PutUint16(hdr[1:3], f.Channel)
	binary.BigEndian.PutUint32(hdr[3:7], uint32(len(f.Payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(f.Payload) > 0 {
		if _, err := w.Write(f.Payload); err != nil {
			return err
		}
	}
	// frame end
	if _, err := w.Write([]byte{frameEnd}); err != nil {
		return err
	}
	return nil
}

// helper to write a method frame (type 1). args does NOT include class/method ids.
func WriteMethod(w io.Writer, channel uint16, classID, methodID uint16, args []byte) error {
	payload := make([]byte, 4+len(args))
	binary.BigEndian.PutUint16(payload[0:2], classID)
	binary.BigEndian.PutUint16(payload[2:4], methodID)
	copy(payload[4:], args)
	return WriteFrame(w, Frame{Type: frameMethod, Channel: channel, Payload: payload})
}

// ParseMethod parses a method frame payload and returns class, method and remaining args
func ParseMethod(payload []byte) (classID, methodID uint16, args []byte, err error) {
	if len(payload) < 4 {
		return 0, 0, nil, fmt.Errorf("method payload too short")
	}
	classID = binary.BigEndian.Uint16(payload[0:2])
	methodID = binary.BigEndian.Uint16(payload[2:4])
	args = payload[4:]
	return classID, methodID, args, nil
}

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

// Serve starts an AMQP-like server on addr. handler is invoked when a basic.publish message body is received.
// This is a minimal implementation that implements enough of the wire protocol
// to accept a connection, open a channel and receive basic.publish + content frames.
// The original Serve kept the simple signature; it now delegates to ServeWithAuth
// with a nil AuthHandler for backwards compatibility.
func Serve(addr string, handler func(channel uint16, body []byte) error) error {
	return ServeWithAuth(addr, handler, nil)
}

// AuthHandler is called during the connection handshake when the client
// sends Connection.Start-Ok. The handler receives the selected SASL
// mechanism and the raw response bytes. If the handler returns a non-nil
// error the server will close the connection.
type AuthHandler func(mechanism string, response []byte) error

// ServeWithAuth starts the server like Serve but allows providing an
// AuthHandler to validate credentials during the handshake.
func ServeWithAuth(addr string, handler func(channel uint16, body []byte) error, auth AuthHandler) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go handleConnWithAuth(conn, handler, auth)
	}
}

// helper to write Connection.Close with reply-code/text and the offending class/method
func writeConnectionClose(w io.Writer, replyCode uint16, replyText string, classID uint16, methodID uint16) error {
	var buf bytes.Buffer
	buf.Write(encodeShort(replyCode))
	buf.Write(encodeLongStr(replyText))
	buf.Write(encodeShort(classID))
	buf.Write(encodeShort(methodID))
	return WriteMethod(w, 0, classConnection, methodConnClose, buf.Bytes())
}

// parseStartOkArgs extracts mechanism and response from Connection.Start-Ok args
func parseStartOkArgs(args []byte) (mechanism string, response []byte, locale string, err error) {
	if len(args) < 4 {
		return "", nil, "", fmt.Errorf("start-ok args too short")
	}
	// client-properties field-table: 4-byte length + table
	tableLen := int(binary.BigEndian.Uint32(args[0:4]))
	idx := 4 + tableLen
	if idx > len(args) {
		return "", nil, "", fmt.Errorf("start-ok truncated client-properties")
	}
	// mechanism (shortstr)
	if idx >= len(args) {
		return "", nil, "", fmt.Errorf("start-ok missing mechanism")
	}
	mlen := int(args[idx])
	if idx+1+mlen > len(args) {
		return "", nil, "", fmt.Errorf("start-ok truncated mechanism")
	}
	mechanism = string(args[idx+1 : idx+1+mlen])
	idx = idx + 1 + mlen
	// response (longstr)
	if idx+4 > len(args) {
		return mechanism, nil, "", fmt.Errorf("start-ok missing response")
	}
	rlen := int(binary.BigEndian.Uint32(args[idx : idx+4]))
	idx = idx + 4
	if idx+rlen > len(args) {
		return mechanism, nil, "", fmt.Errorf("start-ok truncated response")
	}
	response = args[idx : idx+rlen]
	idx = idx + rlen
	// locale (shortstr) if present
	if idx < len(args) {
		llen := int(args[idx])
		if idx+1+llen <= len(args) {
			locale = string(args[idx+1 : idx+1+llen])
		}
	}
	return mechanism, response, locale, nil
}

// handleConn is kept as a compatibility wrapper for tests/examples that call it
// directly. It delegates to handleConnWithAuth with a nil AuthHandler.
func handleConn(conn net.Conn, handler func(channel uint16, body []byte) error) {
	handleConnWithAuth(conn, handler, nil)
}

// handleConnWithAuth is the same as the previous handleConn but performs
// optional authentication using the provided AuthHandler during Start-Ok.
func handleConnWithAuth(conn net.Conn, handler func(channel uint16, body []byte) error, auth AuthHandler) {
	defer conn.Close()
	// set a deadline for initial header
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	// read 8-byte protocol header
	hdr := make([]byte, 8)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return
	}
	// we expect header starting with AMQP
	if len(hdr) >= 4 && string(hdr[:4]) != "AMQP" {
		return
	}

	// send Connection.Start (class 10, method 10)
	if err := WriteMethod(conn, 0, classConnection, methodConnStart, buildStartArgs()); err != nil {
		fmt.Printf("[server] write start error: %v\n", err)
		return
	}

	// read frames until we see Start-Ok (class 10 method 11). If auth is
	// provided, parse mechanism/response and call it; if auth fails, close.
	for {
		f, err := ReadFrame(conn)
		if err != nil {
			return
		}
		if f.Type != frameMethod {
			continue
		}
		classID, methodID, args, err := ParseMethod(f.Payload)
		if err != nil {
			return
		}
		fmt.Printf("[server] recv method chan=%d class=%d method=%d args=%d\n", f.Channel, classID, methodID, len(args))
		if classID == classConnection && methodID == methodConnStartOk {
			if auth != nil {
				mech, resp, _, err := parseStartOkArgs(args)
				if err != nil {
					fmt.Printf("[server] parse start-ok error: %v\n", err)
					// close with error
					_ = writeConnectionClose(conn, 540, "Malformed start-ok", 0, 0)
					return
				}
				if err := auth(mech, resp); err != nil {
					// authentication failed: send Connection.Close and return
					_ = writeConnectionClose(conn, 403, "ACCESS_REFUSED", classConnection, methodConnStartOk)
					return
				}
			}
			break
		}
	}

	// send Tune
	if err := WriteMethod(conn, 0, classConnection, methodConnTune, buildTuneArgs(0, 131072, 0)); err != nil {
		fmt.Printf("[server] write tune error: %v\n", err)
		return
	}

	// wait for Tune-Ok
	for {
		f, err := ReadFrame(conn)
		if err != nil {
			return
		}
		if f.Type != frameMethod {
			continue
		}
		classID, methodID, args, err := ParseMethod(f.Payload)
		if err != nil {
			return
		}
		fmt.Printf("[server] recv method chan=%d class=%d method=%d args=%d\n", f.Channel, classID, methodID, len(args))
		if classID == classConnection && methodID == methodConnTuneOk {
			break
		}
	}

	// send Open-OK? Wait for client open first
	for {
		f, err := ReadFrame(conn)
		if err != nil {
			return
		}
		if f.Type != frameMethod {
			continue
		}
		classID, methodID, args, err := ParseMethod(f.Payload)
		if err != nil {
			return
		}
		fmt.Printf("[server] recv method chan=%d class=%d method=%d args=%d\n", f.Channel, classID, methodID, len(args))
		if classID == classConnection && methodID == methodConnOpen {
			// client requested connection.open
			// parse virtual-host (path = shortstr)
			vhost := ""
			if len(args) > 0 {
				l := int(args[0])
				if 1+l <= len(args) {
					vhost = string(args[1 : 1+l])
				}
			}
			fmt.Printf("[server] connection.open vhost=%q\n", vhost)
			// connection.open-ok expects a shortstr reserved-1 (empty)
			if err := WriteMethod(conn, 0, classConnection, methodConnOpenOk, []byte{0}); err != nil {
				fmt.Printf("[server] write open-ok error: %v\n", err)
				return
			}
			fmt.Printf("[server] send method chan=0 class=%d method=%d (open-ok)\n", classConnection, methodConnOpenOk)
			// clear deadline after handshake
			conn.SetDeadline(time.Time{})
			break
		}
	}

	// channel states: track confirming mode and publish sequence per channel
	type channelState struct {
		confirming bool
		publishSeq uint64
	}
	channelStates := map[uint16]*channelState{}

	// now handle channel opens and basic.publish
	for {
		f, err := ReadFrame(conn)
		if err != nil {
			return
		}
		switch f.Type {
		case frameMethod:
			classID, methodID, args, err := ParseMethod(f.Payload)
			if err != nil {
				return
			}
			fmt.Printf("[server] recv method chan=%d class=%d method=%d args=%d\n", f.Channel, classID, methodID, len(args))
			// handle Connection.Close (class 10 method 50)
			if classID == classConnection && methodID == methodConnClose {
				// reply-code (short) may be present in args
				var replyCode uint16
				if len(args) >= 2 {
					replyCode = binary.BigEndian.Uint16(args[0:2])
				}
				fmt.Printf("[server] recv connection.close reply-code=%d args-len=%d\n", replyCode, len(args))
				// respond with Connection.Close-Ok (class 10 method 51) and close connection
				if err := WriteMethod(conn, 0, classConnection, methodConnCloseOk, []byte{}); err != nil {
					fmt.Printf("[server] write close-ok error: %v\n", err)
				}
				fmt.Printf("[server] send method chan=0 class=%d method=%d (close-ok)\n", classConnection, methodConnCloseOk)
				return
			}
			// channel open
			if classID == classChannel && methodID == methodChannelOpen {
				// create channel state
				channelStates[f.Channel] = &channelState{confirming: false, publishSeq: 0}
				// respond with channel.open-ok on same channel (reserved longstr)
				if err := WriteMethod(conn, f.Channel, classChannel, methodChannelOpenOk, encodeLongStr("")); err != nil {
					fmt.Printf("[server] write channel.open-ok error: %v\n", err)
					return
				}
				fmt.Printf("[server] send method chan=%d class=%d method=%d (channel.open-ok)\n", f.Channel, classChannel, methodChannelOpenOk)
				continue
			}
			// channel close: reply with channel.close-ok on same channel
			if classID == classChannel && methodID == methodChannelClose {
				if err := WriteMethod(conn, f.Channel, classChannel, methodChannelCloseOk, []byte{}); err != nil {
					fmt.Printf("[server] write channel.close-ok error: %v\n", err)
					return
				}
				fmt.Printf("[server] send method chan=%d class=%d method=%d (channel.close-ok)\n", f.Channel, classChannel, methodChannelCloseOk)
				continue
			}
			// confirm.select
			if classID == classConfirm && methodID == methodConfirmSelect {
				// Put channel into confirm mode. We ignore args (nowait) for simplicity.
				st, ok := channelStates[f.Channel]
				if !ok {
					st = &channelState{confirming: false, publishSeq: 0}
					channelStates[f.Channel] = st
				}
				st.confirming = true
				st.publishSeq = 0
				// respond with select-ok
				if err := WriteMethod(conn, f.Channel, classConfirm, methodConfirmSelectOk, []byte{}); err != nil {
					fmt.Printf("[server] write confirm.select-ok error: %v\n", err)
					return
				}
				fmt.Printf("[server] send method chan=%d class=%d method=%d (confirm.select-ok)\n", f.Channel, classConfirm, methodConfirmSelectOk)
				continue
			}

			// basic.publish
			if classID == classBasic && methodID == methodBasicPublish {
				// parse fields: reserved-1 (short), exchange (shortstr), routing-key (shortstr)
				idx := 0
				exch := ""
				rkey := ""
				if len(args) >= 2 {
					idx = 2
				}
				if idx < len(args) {
					l := int(args[idx])
					if idx+1+l <= len(args) {
						exch = string(args[idx+1 : idx+1+l])
						idx = idx + 1 + l
					}
				}
				if idx < len(args) {
					l := int(args[idx])
					if idx+1+l <= len(args) {
						rkey = string(args[idx+1 : idx+1+l])
						idx = idx + 1 + l
					}
				}
				fmt.Printf("[server] basic.publish exchange=%q routing-key=%q\n", exch, rkey)
				// next frame must be header
				hf, err := ReadFrame(conn)
				if err != nil {
					return
				}
				if hf.Type != frameHeader {
					return
				}
				// parse body-size at payload[4:12]
				if len(hf.Payload) < 12 {
					return
				}
				bodySize := binary.BigEndian.Uint64(hf.Payload[4:12])
				// read body frames until bodySize reached
				var got uint64
				var body bytes.Buffer
				for got < bodySize {
					bf, err := ReadFrame(conn)
					if err != nil {
						return
					}
					if bf.Type != frameBody {
						return
					}
					body.Write(bf.Payload)
					got += uint64(len(bf.Payload))
				}
				// invoke handler
				_ = handler(f.Channel, body.Bytes())

				// determine ack behavior: if channel is in confirm mode send a
				// publisher-confirm Basic.Ack with an incrementing delivery tag
				st, ok := channelStates[f.Channel]
				if ok && st.confirming {
					st.publishSeq++
					tag := st.publishSeq
					if err := WriteMethod(conn, f.Channel, classBasic, methodBasicAck, buildAckArgs(tag, false)); err != nil {
						fmt.Printf("[server] write basic.ack error: %v\n", err)
						return
					}
					fmt.Printf("[server] send method chan=%d class=%d method=%d (basic.ack, tag=%d)\n", f.Channel, classBasic, methodBasicAck, tag)
					continue
				}

				// backwards-compatible behavior: when not in confirm mode send an ack with tag 1
				if err := WriteMethod(conn, f.Channel, classBasic, methodBasicAck, buildAckArgs(1, false)); err != nil {
					fmt.Printf("[server] write basic.ack error: %v\n", err)
					return
				}
				fmt.Printf("[server] send method chan=%d class=%d method=%d (basic.ack)\n", f.Channel, classBasic, methodBasicAck)
				continue
			}
		case frameHeartbeat:
			// ignore heartbeats
		default:
			// ignore other frames
		}
	}
}
