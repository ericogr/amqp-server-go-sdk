package amqp

import (
	"bytes"
	"crypto/tls"
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
	classExchange   = 40
	classQueue      = 50
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
	// exchange methods (class 40)
	methodExchangeDeclare   = 10
	methodExchangeDeclareOk = 11
	methodExchangeDelete    = 20
	methodExchangeDeleteOk  = 21
	methodExchangeBind      = 30
	methodExchangeBindOk    = 31
	methodExchangeUnbind    = 40
	methodExchangeUnbindOk  = 51

	// queue methods (class 50)
	methodQueueDeclare   = 10
	methodQueueDeclareOk = 11
	methodQueueBind      = 20
	methodQueueBindOk    = 21
	methodQueuePurge     = 30
	methodQueuePurgeOk   = 31
	methodQueueDelete    = 40
	methodQueueDeleteOk  = 41

	// basic methods (class 60)
	methodBasicQos       = 10
	methodBasicConsume   = 20
	methodBasicConsumeOk = 21
	methodBasicCancel    = 30
	methodBasicCancelOk  = 31
	methodBasicReturn    = 50
	methodBasicDeliver   = 60
	methodBasicGet       = 70
	methodBasicGetOk     = 71
	methodBasicGetEmpty  = 72
	methodBasicReject    = 90
	methodBasicNack      = 120
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

// Serve starts an AMQP-like server on addr. handler is invoked when a basic.publish message body is received.
// This is a minimal implementation that implements enough of the wire protocol
// to accept a connection, open a channel and receive basic.publish + content frames.
// The original Serve kept the simple signature; it now delegates to ServeWithAuth
// with a nil AuthHandler for backwards compatibility.
// ConnContext provides connection-scoped helpers for handlers
type ConnContext struct {
	Conn        net.Conn
	Vhost       string
	TLSState    *tls.ConnectionState
	WriteMethod func(channel uint16, classID, methodID uint16, args []byte) error
	WriteFrame  func(f Frame) error
}

// ServerHandlers allows the server application to handle protocol operations
type ServerHandlers struct {
	OnExchangeDeclare func(ctx ConnContext, channel uint16, exchange, kind string, args []byte) error
	// OnExchangeDelete is called when a client issues exchange.delete.
	// Flags `ifUnused` and `nowait` are parsed and repassed to the handler.
	OnExchangeDelete func(ctx ConnContext, channel uint16, exchange string, ifUnused bool, nowait bool, args []byte) error
	OnExchangeBind   func(ctx ConnContext, channel uint16, destination, source, routingKey string, nowait bool, args []byte) error
	OnExchangeUnbind func(ctx ConnContext, channel uint16, destination, source, routingKey string, nowait bool, args []byte) error
	OnQueueDeclare   func(ctx ConnContext, channel uint16, queue string, args []byte) error
	// OnQueueDelete is called when a client issues queue.delete. The handler
	// should return the number of messages deleted; the SDK will include that
	// count in the queue.delete-ok payload. Flags `ifUnused`, `ifEmpty` and
	// `nowait` are parsed and repassed to the handler.
	OnQueueDelete  func(ctx ConnContext, channel uint16, queue string, ifUnused bool, ifEmpty bool, nowait bool, args []byte) (int, error)
	OnQueuePurge   func(ctx ConnContext, channel uint16, queue string, args []byte) (int, error)
	OnQueueBind    func(ctx ConnContext, channel uint16, queue, exchange, rkey string, args []byte) error
	OnBasicConsume func(ctx ConnContext, channel uint16, queue, consumerTag string, flags byte, args []byte) (serverTag string, err error)
	// OnBasicPublish returns (nack, err). If nack==true and the channel is in confirm mode
	// the SDK will send a Basic.Nack for the publishing tag.
	OnBasicPublish func(ctx ConnContext, channel uint16, exchange, rkey string, properties []byte, body []byte) (nack bool, err error)
	OnBasicGet     func(ctx ConnContext, channel uint16, queue string, noAck bool) (found bool, deliveryTag uint64, body []byte, err error)
	// Incoming client-to-server notifications
	OnBasicNack   func(ctx ConnContext, channel uint16, deliveryTag uint64, multiple bool, requeue bool) error
	OnBasicReject func(ctx ConnContext, channel uint16, deliveryTag uint64, requeue bool) error
}

func Serve(addr string, handler func(ctx ConnContext, channel uint16, body []byte) error) error {
	return ServeWithAuth(addr, handler, nil, nil)
}

// AuthHandler is called during the connection handshake to validate
// credentials. The handler receives a connection context (`ConnContext`) so it
// can inspect the requested virtual-host and TLS state, plus the selected SASL
// mechanism and the raw response bytes (from Start-Ok). If the handler
// returns a non-nil error the server will close the connection.
type AuthHandler func(ctx ConnContext, mechanism string, response []byte) error

// ServeWithAuth starts the server like Serve but allows providing an
// AuthHandler to validate credentials during the handshake.
func ServeWithAuth(addr string, handler func(ctx ConnContext, channel uint16, body []byte) error, auth AuthHandler, handlers *ServerHandlers) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	return ServeWithListener(ln, handler, auth, handlers)
}

// ServeWithListener accepts an existing net.Listener (possibly TLS) and
// serves AMQP on it. This allows callers to provide a tls.Listener when
// they want TLS-enabled transport.
func ServeWithListener(ln net.Listener, handler func(ctx ConnContext, channel uint16, body []byte) error, auth AuthHandler, handlers *ServerHandlers) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go handleConnWithAuth(conn, handler, auth, handlers)
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
func handleConn(conn net.Conn, handler func(ctx ConnContext, channel uint16, body []byte) error) {
	handleConnWithAuth(conn, handler, nil, nil)
}

// handleConnWithAuth is the same as the previous handleConn but performs
// optional authentication using the provided AuthHandler during Start-Ok.
func handleConnWithAuth(conn net.Conn, handler func(ctx ConnContext, channel uint16, body []byte) error, auth AuthHandler, handlers *ServerHandlers) {
	defer conn.Close()
	// set a deadline for initial header and TLS handshake
	conn.SetDeadline(time.Now().Add(30 * time.Second))

	// If this connection is TLS, perform the TLS handshake now so we can
	// obtain the TLS connection state for handlers/auth.
	var tlsState *tls.ConnectionState
	if tc, ok := conn.(*tls.Conn); ok {
		if err := tc.Handshake(); err != nil {
			fmt.Printf("[server] tls handshake error: %v\n", err)
			return
		}
		st := tc.ConnectionState()
		tlsState = &st
	}

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

	// connection-scoped context for handlers (declare early)
	var ctx ConnContext
	// populated after Connection.Open
	var vhost string
	// authentication info parsed from Start-Ok (if auth handler provided)
	var authMech string
	var authResp []byte
	// tls state populated earlier if this was a TLS connection
	var connTLSState *tls.ConnectionState = tlsState

	// read frames until we see Start-Ok (class 10 method 11). If auth is
	// provided, parse mechanism/response and store them; actual authentication
	// will be performed after Connection.Open so the handler receives the
	// requested vhost as well.
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
		// build connection-scoped context for handlers
		ctx = ConnContext{Conn: conn, Vhost: vhost, TLSState: connTLSState, WriteMethod: func(ch, cid, mid uint16, a []byte) error { return WriteMethod(conn, ch, cid, mid, a) }, WriteFrame: func(fr Frame) error { return WriteFrame(conn, fr) }}
		if classID == classConnection && methodID == methodConnStartOk {
			if auth != nil {
				mech, resp, _, err := parseStartOkArgs(args)
				if err != nil {
					fmt.Printf("[server] parse start-ok error: %v\n", err)
					// close with error
					_ = writeConnectionClose(conn, 540, "Malformed start-ok", 0, 0)
					return
				}
				// store mech/resp to use after Connection.Open when vhost is known
				authMech = mech
				authResp = append([]byte(nil), resp...)
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
			vhost = ""
			if len(args) > 0 {
				l := int(args[0])
				if 1+l <= len(args) {
					vhost = string(args[1 : 1+l])
				}
			}
			fmt.Printf("[server] connection.open vhost=%q\n", vhost)
			// build connection-scoped context for handlers (we now know vhost/TLS)
			ctx = ConnContext{Conn: conn, Vhost: vhost, TLSState: connTLSState, WriteMethod: func(ch, cid, mid uint16, a []byte) error { return WriteMethod(conn, ch, cid, mid, a) }, WriteFrame: func(fr Frame) error { return WriteFrame(conn, fr) }}
			// if an auth handler was provided, run it now that we know the vhost
			if auth != nil {
				if authMech == "" {
					// client did not provide Start-Ok credentials
					_ = writeConnectionClose(conn, 540, "Missing start-ok", 0, 0)
					return
				}
				if err := auth(ctx, authMech, authResp); err != nil {
					_ = writeConnectionClose(conn, 403, "ACCESS_REFUSED", classConnection, methodConnStartOk)
					return
				}
			}
			// connection.open-ok expects a shortstr reserved-1 (empty)
			if err := WriteMethod(conn, 0, classConnection, methodConnOpenOk, []byte{0}); err != nil {
				fmt.Printf("[server] write open-ok error: %v\n", err)
				return
			}
			fmt.Printf("[server] send method chan=0 class=%d method=%d (open-ok)\n", classConnection, methodConnOpenOk)
			// update connection-scoped context with negotiated vhost
			ctx = ConnContext{Conn: conn, Vhost: vhost, WriteMethod: func(ch, cid, mid uint16, a []byte) error { return WriteMethod(conn, ch, cid, mid, a) }, WriteFrame: func(fr Frame) error { return WriteFrame(conn, fr) }}
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

	// No in-memory broker state in SDK: delegate behavior to ServerHandlers.

	// now handle channel opens and methods
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

			// handle incoming client Basic.Nack (class 60 method 120)
			if classID == classBasic && methodID == methodBasicNack {
				if len(args) < 9 {
					continue
				}
				dtag := binary.BigEndian.Uint64(args[0:8])
				var multiple bool
				if len(args) >= 9 && args[8]&1 == 1 {
					multiple = true
				}
				var requeue bool
				if len(args) >= 9 && args[8]&2 == 2 {
					requeue = true
				}
				if handlers != nil && handlers.OnBasicNack != nil {
					_ = handlers.OnBasicNack(ctx, f.Channel, dtag, multiple, requeue)
				}
				continue
			}

			// handle incoming client Basic.Reject (class 60 method 90)
			if classID == classBasic && methodID == methodBasicReject {
				if len(args) < 9 {
					continue
				}
				dtag := binary.BigEndian.Uint64(args[0:8])
				var requeue bool
				if len(args) >= 9 && args[8]&1 == 1 {
					requeue = true
				}
				if handlers != nil && handlers.OnBasicReject != nil {
					_ = handlers.OnBasicReject(ctx, f.Channel, dtag, requeue)
				}
				continue
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

			// exchange.declare (delegate to handlers)
			if classID == classExchange && methodID == methodExchangeDeclare {
				// parse args: reserved-1 then exchange shortstr then type shortstr
				idx := 0
				if len(args) >= 2 {
					idx = 2
				}
				exch := ""
				if idx < len(args) {
					l := int(args[idx])
					if idx+1+l <= len(args) {
						exch = string(args[idx+1 : idx+1+l])
						idx = idx + 1 + l
					}
				}
				kind := ""
				if idx < len(args) {
					l := int(args[idx])
					if idx+1+l <= len(args) {
						kind = string(args[idx+1 : idx+1+l])
						idx = idx + 1 + l
					}
				}
				if handlers != nil && handlers.OnExchangeDeclare != nil {
					if err := handlers.OnExchangeDeclare(ctx, f.Channel, exch, kind, args); err != nil {
						_ = writeConnectionClose(conn, 504, "exchange.declare failed", classExchange, methodExchangeDeclare)
						return
					}
				}
				if err := WriteMethod(conn, f.Channel, classExchange, methodExchangeDeclareOk, []byte{}); err != nil {
					fmt.Printf("[server] write exchange.declare-ok error: %v\n", err)
					return
				}
				continue
			}

			// exchange.bind (delegate)
			if classID == classExchange && methodID == methodExchangeBind {
				idx := 0
				if len(args) >= 2 {
					idx = 2
				}
				dest := ""
				if idx < len(args) {
					l := int(args[idx])
					if idx+1+l <= len(args) {
						dest = string(args[idx+1 : idx+1+l])
						idx = idx + 1 + l
					}
				}
				src := ""
				if idx < len(args) {
					l := int(args[idx])
					if idx+1+l <= len(args) {
						src = string(args[idx+1 : idx+1+l])
						idx = idx + 1 + l
					}
				}
				rkey := ""
				if idx < len(args) {
					l := int(args[idx])
					if idx+1+l <= len(args) {
						rkey = string(args[idx+1 : idx+1+l])
						idx = idx + 1 + l
					}
				}
				// nowait flag (optional)
				var nowait bool
				if idx < len(args) {
					flags := args[idx]
					if flags&1 == 1 {
						nowait = true
					}
				}
				if handlers != nil && handlers.OnExchangeBind != nil {
					if err := handlers.OnExchangeBind(ctx, f.Channel, dest, src, rkey, nowait, args); err != nil {
						_ = writeConnectionClose(conn, 504, "exchange.bind failed", classExchange, methodExchangeBind)
						return
					}
				}
				if !nowait {
					if err := WriteMethod(conn, f.Channel, classExchange, methodExchangeBindOk, []byte{}); err != nil {
						fmt.Printf("[server] write exchange.bind-ok error: %v\n", err)
						return
					}
				}
				continue
			}

			// exchange.unbind (delegate)
			if classID == classExchange && methodID == methodExchangeUnbind {
				idx := 0
				if len(args) >= 2 {
					idx = 2
				}
				dest := ""
				if idx < len(args) {
					l := int(args[idx])
					if idx+1+l <= len(args) {
						dest = string(args[idx+1 : idx+1+l])
						idx = idx + 1 + l
					}
				}
				src := ""
				if idx < len(args) {
					l := int(args[idx])
					if idx+1+l <= len(args) {
						src = string(args[idx+1 : idx+1+l])
						idx = idx + 1 + l
					}
				}
				rkey := ""
				if idx < len(args) {
					l := int(args[idx])
					if idx+1+l <= len(args) {
						rkey = string(args[idx+1 : idx+1+l])
						idx = idx + 1 + l
					}
				}
				// no-wait (single octet)
				var nowait bool
				if idx < len(args) {
					if args[idx] != 0 {
						nowait = true
					}
				}
				if handlers != nil && handlers.OnExchangeUnbind != nil {
					if err := handlers.OnExchangeUnbind(ctx, f.Channel, dest, src, rkey, nowait, args); err != nil {
						_ = writeConnectionClose(conn, 504, "exchange.unbind failed", classExchange, methodExchangeUnbind)
						return
					}
				}
				if !nowait {
					if err := WriteMethod(conn, f.Channel, classExchange, methodExchangeUnbindOk, []byte{}); err != nil {
						fmt.Printf("[server] write exchange.unbind-ok error: %v\n", err)
						return
					}
				}
				continue
			}

			// exchange.delete (delegate)
			if classID == classExchange && methodID == methodExchangeDelete {
				idx := 0
				if len(args) >= 2 {
					idx = 2
				}
				exch := ""
				if idx < len(args) {
					l := int(args[idx])
					if idx+1+l <= len(args) {
						exch = string(args[idx+1 : idx+1+l])
						idx = idx + 1 + l
					}
				}
				// parse flags: if-unused (bit1), nowait (bit2)
				var ifUnused bool
				var nowait bool
				if idx < len(args) {
					flags := args[idx]
					if flags&1 == 1 {
						ifUnused = true
					}
					if flags&2 == 2 {
						nowait = true
					}
				}
				if handlers != nil && handlers.OnExchangeDelete != nil {
					if err := handlers.OnExchangeDelete(ctx, f.Channel, exch, ifUnused, nowait, args); err != nil {
						_ = writeConnectionClose(conn, 504, "exchange.delete failed", classExchange, methodExchangeDelete)
						return
					}
				}
				if !nowait {
					if err := WriteMethod(conn, f.Channel, classExchange, methodExchangeDeleteOk, []byte{}); err != nil {
						fmt.Printf("[server] write exchange.delete-ok error: %v\n", err)
						return
					}
				}
				continue
			}

			// queue.declare (delegate)
			if classID == classQueue && methodID == methodQueueDeclare {
				idx := 0
				if len(args) >= 2 {
					idx = 2
				}
				qname := ""
				if idx < len(args) {
					l := int(args[idx])
					if idx+1+l <= len(args) {
						qname = string(args[idx+1 : idx+1+l])
					}
				}
				if handlers != nil && handlers.OnQueueDeclare != nil {
					if err := handlers.OnQueueDeclare(ctx, f.Channel, qname, args); err != nil {
						_ = writeConnectionClose(conn, 504, "queue.declare failed", classQueue, methodQueueDeclare)
						return
					}
				}
				// queue.declare-ok: queue (shortstr), message-count (long), consumer-count (long)
				var dq bytes.Buffer
				dq.Write(encodeShortStr(qname))
				dq.Write(encodeLong(0))
				dq.Write(encodeLong(0))
				if err := WriteMethod(conn, f.Channel, classQueue, methodQueueDeclareOk, dq.Bytes()); err != nil {
					fmt.Printf("[server] write queue.declare-ok error: %v\n", err)
					return
				}
				continue
			}

			// queue.bind (delegate)
			if classID == classQueue && methodID == methodQueueBind {
				idx := 0
				if len(args) >= 2 {
					idx = 2
				}
				qname := ""
				if idx < len(args) {
					l := int(args[idx])
					if idx+1+l <= len(args) {
						qname = string(args[idx+1 : idx+1+l])
						idx = idx + 1 + l
					}
				}
				exch := ""
				if idx < len(args) {
					l := int(args[idx])
					if idx+1+l <= len(args) {
						exch = string(args[idx+1 : idx+1+l])
						idx = idx + 1 + l
					}
				}
				rkey := ""
				if idx < len(args) {
					l := int(args[idx])
					if idx+1+l <= len(args) {
						rkey = string(args[idx+1 : idx+1+l])
						idx = idx + 1 + l
					}
				}
				if handlers != nil && handlers.OnQueueBind != nil {
					if err := handlers.OnQueueBind(ctx, f.Channel, qname, exch, rkey, args); err != nil {
						_ = writeConnectionClose(conn, 504, "queue.bind failed", classQueue, methodQueueBind)
						return
					}
				}
				if err := WriteMethod(conn, f.Channel, classQueue, methodQueueBindOk, []byte{}); err != nil {
					fmt.Printf("[server] write queue.bind-ok error: %v\n", err)
					return
				}
				continue
			}

			// queue.purge (delegate)
			if classID == classQueue && methodID == methodQueuePurge {
				idx := 0
				if len(args) >= 2 {
					idx = 2
				}
				qname := ""
				if idx < len(args) {
					l := int(args[idx])
					if idx+1+l <= len(args) {
						qname = string(args[idx+1 : idx+1+l])
						idx = idx + 1 + l
					}
				}
				var msgCount int
				if handlers != nil && handlers.OnQueuePurge != nil {
					if c, err := handlers.OnQueuePurge(ctx, f.Channel, qname, args); err != nil {
						_ = writeConnectionClose(conn, 504, "queue.purge failed", classQueue, methodQueuePurge)
						return
					} else {
						msgCount = c
					}
				}
				// queue.purge-ok: message-count (long)
				if err := WriteMethod(conn, f.Channel, classQueue, methodQueuePurgeOk, encodeLong(uint32(msgCount))); err != nil {
					fmt.Printf("[server] write queue.purge-ok error: %v\n", err)
					return
				}
				continue
			}

			// queue.delete (delegate)
			if classID == classQueue && methodID == methodQueueDelete {
				idx := 0
				if len(args) >= 2 {
					idx = 2
				}
				qname := ""
				if idx < len(args) {
					l := int(args[idx])
					if idx+1+l <= len(args) {
						qname = string(args[idx+1 : idx+1+l])
						idx = idx + 1 + l
					}
				}
				// parse flags: if-unused (bit1), if-empty (bit2), nowait (bit4)
				var ifUnused bool
				var ifEmpty bool
				var nowait bool
				if idx < len(args) {
					flags := args[idx]
					if flags&1 == 1 {
						ifUnused = true
					}
					if flags&2 == 2 {
						ifEmpty = true
					}
					if flags&4 == 4 {
						nowait = true
					}
				}
				var delCount int
				if handlers != nil && handlers.OnQueueDelete != nil {
					c, err := handlers.OnQueueDelete(ctx, f.Channel, qname, ifUnused, ifEmpty, nowait, args)
					if err != nil {
						_ = writeConnectionClose(conn, 504, "queue.delete failed", classQueue, methodQueueDelete)
						return
					}
					delCount = c
				}
				// queue.delete-ok: message-count (long), unless nowait
				if !nowait {
					if err := WriteMethod(conn, f.Channel, classQueue, methodQueueDeleteOk, encodeLong(uint32(delCount))); err != nil {
						fmt.Printf("[server] write queue.delete-ok error: %v\n", err)
						return
					}
				}
				continue
			}

			// basic.consume
			if classID == classBasic && methodID == methodBasicConsume {
				idx := 0
				if len(args) >= 2 {
					idx = 2
				}
				qname := ""
				if idx < len(args) {
					l := int(args[idx])
					if idx+1+l <= len(args) {
						qname = string(args[idx+1 : idx+1+l])
						idx = idx + 1 + l
					}
				}
				consumerTag := ""
				if idx < len(args) {
					l := int(args[idx])
					if idx+1+l <= len(args) {
						consumerTag = string(args[idx+1 : idx+1+l])
						idx = idx + 1 + l
					}
				}
				var flags byte
				if idx < len(args) {
					flags = args[idx]
					idx++
				}
				// delegate to handler if provided
				serverTag := consumerTag
				if handlers != nil && handlers.OnBasicConsume != nil {
					if st, err := handlers.OnBasicConsume(ctx, f.Channel, qname, consumerTag, flags, args); err != nil {
						_ = writeConnectionClose(conn, 504, "basic.consume failed", classBasic, methodBasicConsume)
						return
					} else if st != "" {
						serverTag = st
					}
				}
				if serverTag == "" {
					serverTag = fmt.Sprintf("ctag-%d", time.Now().UnixNano())
				}
				if err := WriteMethod(conn, f.Channel, classBasic, methodBasicConsumeOk, encodeShortStr(serverTag)); err != nil {
					fmt.Printf("[server] write basic.consume-ok error: %v\n", err)
					return
				}
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

			// basic.get (delegate)
			if classID == classBasic && methodID == methodBasicGet {
				idx := 0
				if len(args) >= 2 {
					idx = 2
				}
				qname := ""
				if idx < len(args) {
					l := int(args[idx])
					if idx+1+l <= len(args) {
						qname = string(args[idx+1 : idx+1+l])
						idx = idx + 1 + l
					}
				}
				// delegate to handler
				if handlers != nil && handlers.OnBasicGet != nil {
					found, delTag, msg, err := handlers.OnBasicGet(ctx, f.Channel, qname, false)
					if err != nil {
						_ = writeConnectionClose(conn, 504, "basic.get failed", classBasic, methodBasicGet)
						return
					}
					if !found {
						if err := WriteMethod(conn, f.Channel, classBasic, methodBasicGetEmpty, []byte{}); err != nil {
							fmt.Printf("[server] write get-empty error: %v\n", err)
							return
						}
						continue
					}
					// send get-ok
					var payload bytes.Buffer
					payload.Write(encodeLongLong(delTag))
					payload.WriteByte(0)
					payload.Write(encodeShortStr(""))
					payload.Write(encodeShortStr(""))
					payload.Write(encodeLong(0))
					if err := WriteMethod(conn, f.Channel, classBasic, methodBasicGetOk, payload.Bytes()); err != nil {
						fmt.Printf("[server] write get-ok error: %v\n", err)
						return
					}
					if err := WriteFrame(conn, Frame{Type: frameHeader, Channel: f.Channel, Payload: buildContentHeaderPayload(classBasic, uint64(len(msg)))}); err != nil {
						fmt.Printf("[server] write header error: %v\n", err)
						return
					}
					if err := WriteFrame(conn, Frame{Type: frameBody, Channel: f.Channel, Payload: msg}); err != nil {
						fmt.Printf("[server] write body error: %v\n", err)
						return
					}
					continue
				}
				// default: no handler -> empty
				if err := WriteMethod(conn, f.Channel, classBasic, methodBasicGetEmpty, []byte{}); err != nil {
					fmt.Printf("[server] write get-empty error: %v\n", err)
					return
				}
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

				// build connection-scoped context for handlers
				ctx = ConnContext{Conn: conn, Vhost: vhost, TLSState: connTLSState, WriteMethod: func(ch, cid, mid uint16, a []byte) error { return WriteMethod(conn, ch, cid, mid, a) }, WriteFrame: func(f Frame) error { return WriteFrame(conn, f) }}
				if handler != nil {
					_ = handler(ctx, f.Channel, body.Bytes())
				}

				// delegate basic.publish processing to handlers if provided
				var publishNack bool
				if handlers != nil && handlers.OnBasicPublish != nil {
					nack, err := handlers.OnBasicPublish(ctx, f.Channel, exch, rkey, hf.Payload, body.Bytes())
					if err != nil {
						_ = writeConnectionClose(conn, 504, "basic.publish handler failed", classBasic, methodBasicPublish)
						return
					}
					publishNack = nack
				}

				// determine ack behavior for the publishing channel
				st, ok := channelStates[f.Channel]
				if ok && st.confirming {
					st.publishSeq++
					tag := st.publishSeq
					if publishNack {
						if err := WriteMethod(conn, f.Channel, classBasic, methodBasicNack, buildNackArgs(tag, false, false)); err != nil {
							fmt.Printf("[server] write basic.nack error: %v\n", err)
							return
						}
						fmt.Printf("[server] send method chan=%d class=%d method=%d (basic.nack, tag=%d)\n", f.Channel, classBasic, methodBasicNack, tag)
					} else {
						if err := WriteMethod(conn, f.Channel, classBasic, methodBasicAck, buildAckArgs(tag, false)); err != nil {
							fmt.Printf("[server] write basic.ack error: %v\n", err)
							return
						}
						fmt.Printf("[server] send method chan=%d class=%d method=%d (basic.ack, tag=%d)\n", f.Channel, classBasic, methodBasicAck, tag)
					}
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
