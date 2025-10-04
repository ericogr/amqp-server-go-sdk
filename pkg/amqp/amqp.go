package amqp

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

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

// BasicProperties represents parsed content header properties from a content header frame.
type BasicProperties struct {
	ContentType     string
	ContentEncoding string
	Headers         map[string]interface{}
	DeliveryMode    uint8
	Priority        uint8
	CorrelationId   string
	ReplyTo         string
	Expiration      string
	MessageId       string
	Timestamp       time.Time
	Type            string
	UserId          string
	AppId           string
	ClusterId       string
	Raw             []byte // raw property bytes
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
	OnQueueUnbind  func(ctx ConnContext, channel uint16, queue, exchange, rkey string, args []byte) error
	OnBasicConsume func(ctx ConnContext, channel uint16, queue, consumerTag string, flags byte, args []byte) (serverTag string, err error)
	// OnBasicPublish is called for incoming basic.publish. The SDK parses
	// method args, flags and content header properties and passes a structured
	// `BasicProperties` to the handler. The handler returns two booleans:
	//  - routed: whether the publication was routed/delivered (used to implement mandatory/immediate behavior)
	//  - nack: whether the server should send a publisher confirmation Nack
	// The SDK will send basic.return if the message is not routed and the
	// publisher requested mandatory/immediate, and will send confirm acks/nacks
	// when in confirm mode according to the handler's return values.
	OnBasicPublish func(ctx ConnContext, channel uint16, exchange, rkey string, mandatory bool, immediate bool, properties BasicProperties, body []byte) (routed bool, nack bool, err error)
	OnBasicGet     func(ctx ConnContext, channel uint16, queue string, noAck bool) (found bool, deliveryTag uint64, body []byte, err error)
	// Incoming client-to-server notifications
	OnBasicNack   func(ctx ConnContext, channel uint16, deliveryTag uint64, multiple bool, requeue bool) error
	OnBasicReject func(ctx ConnContext, channel uint16, deliveryTag uint64, requeue bool) error
	OnBasicAck    func(ctx ConnContext, channel uint16, deliveryTag uint64, multiple bool) error

	// Quality of Service (basic.qos)
	OnBasicQos func(ctx ConnContext, channel uint16, prefetchSize uint32, prefetchCount uint16, global bool) error

	// Channel flow control (channel.flow)
	OnChannelFlow func(ctx ConnContext, channel uint16, active bool) (bool, error)

	// OnChannelClose is called when a client issues channel.close. The
	// handler receives the channel id so adapters can clean up per-channel
	// resources (for example upstream channels).
	OnChannelClose func(ctx ConnContext, channel uint16) error

	// Notification when server issues a basic.return to a publisher
	OnBasicReturn func(ctx ConnContext, channel uint16, replyCode uint16, replyText string, exchange, routingKey string, properties BasicProperties, body []byte) error

	// OnConnClose is invoked when the connection handler is about to return
	// and the underlying net.Conn is being closed. Implementations can use
	// this to perform cleanup (for example closing upstream resources).
	OnConnClose func(ctx ConnContext)
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
	buf.Write(encodeShortStr(replyText))
	buf.Write(encodeShort(classID))
	buf.Write(encodeShort(methodID))
	return WriteMethod(w, 0, classConnection, methodConnClose, buf.Bytes())
}

// writeConnClose sends a Connection.Close using the provided connection
// context when available (so writes are serialized through ctx.WriteMethod).
// If ctx.Conn is nil or ctx.WriteMethod is not set, falls back to
// writeConnectionClose(w,...).
func writeConnClose(ctx ConnContext, w io.Writer, replyCode uint16, replyText string, classID uint16, methodID uint16) error {
	if ctx.Conn != nil && ctx.WriteMethod != nil {
		var buf bytes.Buffer
		buf.Write(encodeShort(replyCode))
		buf.Write(encodeShortStr(replyText))
		buf.Write(encodeShort(classID))
		buf.Write(encodeShort(methodID))
		return ctx.WriteMethod(0, classConnection, methodConnClose, buf.Bytes())
	}
	return writeConnectionClose(w, replyCode, replyText, classID, methodID)
}

// writeConnCloseAndWait sends a Connection.Close to the client and waits for
// a Connection.Close-Ok response (bounded timeout). This prevents closing the
// underlying socket before the client can reply and avoids client-side "use of
// closed network connection" errors.
func writeConnCloseAndWait(ctx ConnContext, conn net.Conn, replyCode uint16, replyText string, classID uint16, methodID uint16) error {
	if err := writeConnClose(ctx, conn, replyCode, replyText, classID, methodID); err != nil {
		return err
	}
	if conn == nil {
		return nil
	}
	// set a short read deadline while waiting for Close-Ok
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetReadDeadline(time.Time{})
	// wait for Connection.Close-Ok (class 10 method 51)
	_, _, err := waitForMethod(conn, classConnection, methodConnCloseOk)
	// ignore errors (timeout or io) but return underlying write errors were
	// already returned above. Log if wait failed.
	if err != nil {
		logger.Debug().Err(err).Msg("wait for connection.close-ok failed")
	}
	return nil
}

// SendConnectionClose sends a Connection.Close to the client using the
// provided ConnContext if available and serialized. It is exported for use
// by adapters that need to send a close to the client (for example when
// upstream becomes unavailable).
func SendConnectionClose(ctx ConnContext, replyCode uint16, replyText string, classID uint16, methodID uint16) error {
	return writeConnClose(ctx, ctx.Conn, replyCode, replyText, classID, methodID)
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

// channelState tracks per-channel confirmation state for publisher confirms.
type channelState struct {
	confirming bool
	publishSeq uint64
}

// newConnContext creates a ConnContext bound to conn with provided vhost/TLS state.
func newConnContext(conn net.Conn, vhost string, tlsState *tls.ConnectionState, writeMu *sync.Mutex) ConnContext {
	// If a write mutex is provided it will be used to serialize writes to
	// the underlying connection. If nil, writes are not synchronized.
	return ConnContext{
		Conn:     conn,
		Vhost:    vhost,
		TLSState: tlsState,
		WriteMethod: func(ch, cid, mid uint16, a []byte) error {
			if writeMu != nil {
				writeMu.Lock()
				defer writeMu.Unlock()
			}
			return WriteMethod(conn, ch, cid, mid, a)
		},
		WriteFrame: func(f Frame) error {
			if writeMu != nil {
				writeMu.Lock()
				defer writeMu.Unlock()
			}
			return WriteFrame(conn, f)
		},
	}
}

// performTLSHandshake performs TLS handshake if conn is a *tls.Conn.
// It returns the TLS connection state and a boolean indicating whether
// a TLS handshake was performed successfully.
func performTLSHandshake(conn net.Conn) (*tls.ConnectionState, bool) {
	if tc, ok := conn.(*tls.Conn); ok {
		if err := tc.Handshake(); err != nil {
			logger.Error().Err(err).Msg("[server] tls handshake error")
			return nil, false
		}
		st := tc.ConnectionState()
		return &st, true
	}
	return nil, false
}

// readProtocolHeader reads and validates the 8-byte AMQP protocol header.
func readProtocolHeader(conn net.Conn) error {
	hdr := make([]byte, 8)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return err
	}
	if len(hdr) >= 4 && string(hdr[:4]) != "AMQP" {
		return errors.New("invalid protocol header")
	}
	return nil
}

// waitForMethod reads frames until it sees the specified method (class/method)
// and returns the frame and the parsed method args.
func waitForMethod(conn net.Conn, targetClass, targetMethod uint16) (Frame, []byte, error) {
	for {
		f, err := ReadFrame(conn)
		if err != nil {
			return Frame{}, nil, err
		}
		if f.Type != frameMethod {
			continue
		}
		classID, methodID, args, err := ParseMethod(f.Payload)
		if err != nil {
			return Frame{}, nil, err
		}
		if classID == targetClass && methodID == targetMethod {
			return f, args, nil
		}
	}
}

// handleBasicPublish handles a client Basic.Publish method and its
// subsequent content frames. It delegates message processing to the
// provided ServerHandlers and the compatibility handler. Returns an error
// if a fatal protocol or write error occurs; callers should close the
// connection in that case.
func handleBasicPublish(ctx ConnContext, f Frame, args []byte, handlers *ServerHandlers, compatHandler func(ctx ConnContext, channel uint16, body []byte) error, channelStates map[uint16]*channelState) error {
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
	logger.Debug().Str("exchange", exch).Str("routing_key", rkey).Msg("basic.publish")

	// next frame must be header
	hf, err := ReadFrame(ctx.Conn)
	if err != nil {
		return err
	}
	if hf.Type != frameHeader {
		return errors.New("expected content header frame")
	}
	if len(hf.Payload) < 12 {
		return errors.New("invalid content header payload")
	}

	bodySize := binary.BigEndian.Uint64(hf.Payload[4:12])
	var props BasicProperties
	if len(hf.Payload) > 12 {
		pos := 12
		var propFlags uint16
		if pos+2 <= len(hf.Payload) {
			propFlags = binary.BigEndian.Uint16(hf.Payload[pos : pos+2])
			pos += 2
		}
		flagWords := []uint16{propFlags}
		for (flagWords[len(flagWords)-1] & 1) == 1 {
			if pos+2 > len(hf.Payload) {
				break
			}
			fw := binary.BigEndian.Uint16(hf.Payload[pos : pos+2])
			pos += 2
			flagWords = append(flagWords, fw)
		}
		var bits []bool
		for _, fw := range flagWords {
			for i := 15; i >= 1; i-- {
				bits = append(bits, (fw&(1<<uint(i))) != 0)
			}
		}
		propOrder := []string{"content-type", "content-encoding", "headers", "delivery-mode", "priority", "correlation-id", "reply-to", "expiration", "message-id", "timestamp", "type", "user-id", "app-id", "cluster-id"}
		for i, present := range bits {
			if i >= len(propOrder) {
				break
			}
			if !present {
				continue
			}
			name := propOrder[i]
			switch name {
			case "content-type":
				if pos < len(hf.Payload) {
					l := int(hf.Payload[pos])
					pos++
					if pos+l <= len(hf.Payload) {
						props.ContentType = string(hf.Payload[pos : pos+l])
						pos += l
					}
				}
			case "content-encoding":
				if pos < len(hf.Payload) {
					l := int(hf.Payload[pos])
					pos++
					if pos+l <= len(hf.Payload) {
						props.ContentEncoding = string(hf.Payload[pos : pos+l])
						pos += l
					}
				}
			case "headers":
				// parse field-table for headers
				if pos < len(hf.Payload) {
					if headers, consumed, err := parseFieldTable(hf.Payload[pos:]); err == nil {
						props.Headers = headers
						pos += consumed
					} else {
						// on error, leave headers nil and continue
						logger.Debug().Err(err).Msg("failed to parse headers field-table")
					}
				}
			case "delivery-mode":
				if pos < len(hf.Payload) {
					props.DeliveryMode = hf.Payload[pos]
					pos++
				}
			case "priority":
				if pos < len(hf.Payload) {
					props.Priority = hf.Payload[pos]
					pos++
				}
			case "correlation-id":
				if pos < len(hf.Payload) {
					l := int(hf.Payload[pos])
					pos++
					if pos+l <= len(hf.Payload) {
						props.CorrelationId = string(hf.Payload[pos : pos+l])
						pos += l
					}
				}
			case "reply-to":
				if pos < len(hf.Payload) {
					l := int(hf.Payload[pos])
					pos++
					if pos+l <= len(hf.Payload) {
						props.ReplyTo = string(hf.Payload[pos : pos+l])
						pos += l
					}
				}
			case "expiration":
				if pos < len(hf.Payload) {
					l := int(hf.Payload[pos])
					pos++
					if pos+l <= len(hf.Payload) {
						props.Expiration = string(hf.Payload[pos : pos+l])
						pos += l
					}
				}
			case "message-id":
				if pos < len(hf.Payload) {
					l := int(hf.Payload[pos])
					pos++
					if pos+l <= len(hf.Payload) {
						props.MessageId = string(hf.Payload[pos : pos+l])
						pos += l
					}
				}
			case "timestamp":
				if pos+8 <= len(hf.Payload) {
					ts := binary.BigEndian.Uint64(hf.Payload[pos : pos+8])
					props.Timestamp = time.Unix(int64(ts), 0)
					pos += 8
				}
			case "type":
				if pos < len(hf.Payload) {
					l := int(hf.Payload[pos])
					pos++
					if pos+l <= len(hf.Payload) {
						props.Type = string(hf.Payload[pos : pos+l])
						pos += l
					}
				}
			case "user-id":
				if pos < len(hf.Payload) {
					l := int(hf.Payload[pos])
					pos++
					if pos+l <= len(hf.Payload) {
						props.UserId = string(hf.Payload[pos : pos+l])
						pos += l
					}
				}
			case "app-id":
				if pos < len(hf.Payload) {
					l := int(hf.Payload[pos])
					pos++
					if pos+l <= len(hf.Payload) {
						props.AppId = string(hf.Payload[pos : pos+l])
						pos += l
					}
				}
			case "cluster-id":
				if pos < len(hf.Payload) {
					l := int(hf.Payload[pos])
					pos++
					if pos+l <= len(hf.Payload) {
						props.ClusterId = string(hf.Payload[pos : pos+l])
						pos += l
					}
				}
			}
		}
		props.Raw = hf.Payload[12:]
	}

	// read body frames until bodySize reached
	var got uint64
	var body bytes.Buffer
	for got < bodySize {
		bf, err := ReadFrame(ctx.Conn)
		if err != nil {
			return err
		}
		if bf.Type != frameBody {
			return errors.New("expected body frame")
		}
		body.Write(bf.Payload)
		got += uint64(len(bf.Payload))
	}

	// parse flags (mandatory/immediate) after exchange and rkey
	var mandatory, immediate bool
	{
		idx2 := 0
		if len(args) >= 2 {
			idx2 = 2
		}
		if idx2 < len(args) {
			l := int(args[idx2])
			if idx2+1+l <= len(args) {
				idx2 = idx2 + 1 + l
			}
		}
		if idx2 < len(args) {
			l := int(args[idx2])
			if idx2+1+l <= len(args) {
				idx2 = idx2 + 1 + l
			}
		}
		if idx2 < len(args) {
			flags := args[idx2]
			if flags&1 == 1 {
				mandatory = true
			}
			if flags&2 == 2 {
				immediate = true
			}
		}
	}

	// call compatibility handler
	if compatHandler != nil {
		if cerr := compatHandler(ctx, f.Channel, body.Bytes()); cerr != nil {
			if werr := writeConnCloseAndWait(ctx, ctx.Conn, 504, "compat handler failed", classBasic, methodBasicPublish); werr != nil {
				logger.Error().Err(werr).Msg("[server] write connection close error")
			}
			return cerr
		}
	}

	var publishNack bool
	var routed bool
	if handlers != nil && handlers.OnBasicPublish != nil {
		r, nack, err := handlers.OnBasicPublish(ctx, f.Channel, exch, rkey, mandatory, immediate, props, body.Bytes())
		if err != nil {
			if werr := writeConnCloseAndWait(ctx, ctx.Conn, 504, "basic.publish handler failed", classBasic, methodBasicPublish); werr != nil {
				logger.Error().Err(werr).Msg("[server] write connection close error")
			}
			return err
		}
		routed = r
		publishNack = nack
	}

	// handle mandatory/immediate returns
	if (mandatory || immediate) && !routed {
		var rbuf bytes.Buffer
		rbuf.Write(encodeShort(312))
		rbuf.Write(encodeShortStr("NO_ROUTE"))
		rbuf.Write(encodeShortStr(exch))
		rbuf.Write(encodeShortStr(rkey))
		if werr := ctx.WriteMethod(f.Channel, classBasic, methodBasicReturn, rbuf.Bytes()); werr != nil {
			logger.Error().Err(werr).Msg("[server] write basic.return method error")
		} else {
			if werr := ctx.WriteFrame(Frame{Type: frameHeader, Channel: f.Channel, Payload: buildContentHeaderPayload(classBasic, uint64(len(body.Bytes())))}); werr != nil {
				logger.Error().Err(werr).Msg("[server] write basic.return header error")
			}
			if werr := ctx.WriteFrame(Frame{Type: frameBody, Channel: f.Channel, Payload: body.Bytes()}); werr != nil {
				logger.Error().Err(werr).Msg("[server] write basic.return body error")
			}
		}
		// notify optional handler
		if handlers != nil && handlers.OnBasicReturn != nil {
			if herr := handlers.OnBasicReturn(ctx, f.Channel, 312, "NO_ROUTE", exch, rkey, props, body.Bytes()); herr != nil {
				logger.Error().Err(herr).Msg("[server] OnBasicReturn handler error")
			}
		}
	}

	// publisher confirms handling
	st, ok := channelStates[f.Channel]
	if ok && st.confirming {
		st.publishSeq++
		tag := st.publishSeq
		if publishNack {
			if err := ctx.WriteMethod(f.Channel, classBasic, methodBasicNack, buildNackArgs(tag, false, false)); err != nil {
				logger.Error().Err(err).Msg("[server] write basic.nack error")
				return err
			}
			logger.Debug().Uint16("chan", f.Channel).Int("class", int(classBasic)).Int("method", int(methodBasicNack)).Uint64("tag", tag).Msg("basic.nack")
		} else {
			if err := ctx.WriteMethod(f.Channel, classBasic, methodBasicAck, buildAckArgs(tag, false)); err != nil {
				logger.Error().Err(err).Msg("[server] write basic.ack error")
				return err
			}
			logger.Debug().Uint16("chan", f.Channel).Int("class", int(classBasic)).Int("method", int(methodBasicAck)).Uint64("tag", tag).Msg("basic.ack")
		}
		return nil
	}

	// not in confirm mode: send ack with tag 1
	if err := ctx.WriteMethod(f.Channel, classBasic, methodBasicAck, buildAckArgs(1, false)); err != nil {
		logger.Error().Err(err).Msg("[server] write basic.ack error")
		return err
	}
	logger.Debug().Uint16("chan", f.Channel).Int("class", int(classBasic)).Int("method", int(methodBasicAck)).Msg("basic.ack")
	return nil
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

	// per-connection write mutex used to serialize all outgoing frames.
	// This ensures multiple goroutines (heartbeats, consumer deliveries)
	// do not interleave writes and produce malformed frames.
	var writeMu sync.Mutex

	// use package-level writeConnClose helper (defined above) to send
	// connection.close; do not shadow the name here.

	// If this connection is TLS, perform the TLS handshake now so we can
	// obtain the TLS connection state for handlers/auth.
	var tlsState *tls.ConnectionState
	if st, ok := performTLSHandshake(conn); ok {
		tlsState = st
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
		logger.Error().Err(err).Msg("[server] write start error")
		return
	}

	// connection-scoped context for handlers (declare early)
	var ctx ConnContext
	// ensure connection-close notifications reach handlers that want them
	defer func() {
		if handlers != nil && handlers.OnConnClose != nil {
			handlers.OnConnClose(ctx)
		}
	}()
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
		logger.Debug().Uint16("chan", f.Channel).Int("class", int(classID)).Int("method", int(methodID)).Int("args", len(args)).Msg("recv method")
		// build connection-scoped context for handlers
		ctx = newConnContext(conn, vhost, connTLSState, &writeMu)
		if classID == classConnection && methodID == methodConnStartOk {
			if auth != nil {
				mech, resp, _, err := parseStartOkArgs(args)
				if err != nil {
					logger.Error().Err(err).Msg("[server] parse start-ok error")
					// close with error
					if werr := writeConnCloseAndWait(ctx, conn, 540, "Malformed start-ok", 0, 0); werr != nil {
						logger.Error().Err(werr).Msg("[server] write connection close error")
					}
					return
				}
				// store mech/resp to use after Connection.Open when vhost is known
				authMech = mech
				authResp = append([]byte(nil), resp...)
			}
			break
		}
	}

	// send Tune (advertise server heartbeat)
	serverHeartbeat := uint16(10) // seconds
	if err := WriteMethod(conn, 0, classConnection, methodConnTune, buildTuneArgs(0, 131072, serverHeartbeat)); err != nil {
		logger.Error().Err(err).Msg("[server] write tune error")
		return
	}

	// wait for Tune-Ok and negotiate heartbeat
	negotiatedHeartbeat := uint16(0)
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
		logger.Debug().Uint16("chan", f.Channel).Int("class", int(classID)).Int("method", int(methodID)).Int("args", len(args)).Msg("recv method")
		if classID == classConnection && methodID == methodConnTuneOk {
			// parse args: channelMax (short), frameMax (long), heartbeat (short)
			if len(args) >= 8 {
				clientHeartbeat := binary.BigEndian.Uint16(args[6:8])
				if serverHeartbeat == 0 || clientHeartbeat == 0 {
					negotiatedHeartbeat = serverHeartbeat
				} else if serverHeartbeat < clientHeartbeat {
					negotiatedHeartbeat = serverHeartbeat
				} else {
					negotiatedHeartbeat = clientHeartbeat
				}
			}
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
		logger.Debug().Uint16("chan", f.Channel).Int("class", int(classID)).Int("method", int(methodID)).Int("args", len(args)).Msg("recv method")
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
			logger.Debug().Str("vhost", vhost).Msg("connection.open")
			// build connection-scoped context for handlers (we now know vhost/TLS)
			ctx = newConnContext(conn, vhost, connTLSState, &writeMu)
			// if an auth handler was provided, run it now that we know the vhost
			if auth != nil {
				if authMech == "" {
					// client did not provide Start-Ok credentials
					if werr := writeConnCloseAndWait(ctx, conn, 540, "Missing start-ok", 0, 0); werr != nil {
						logger.Error().Err(werr).Msg("[server] write connection close error")
					}
					return
				}
				if err := auth(ctx, authMech, authResp); err != nil {
					// Authentication failed. To ensure the client receives the server's
					// custom reply-code and reply-text (instead of the client library
					// normalizing handshake failures), first complete the AMQP
					// connection handshake by sending Connection.Open-Ok, then send a
					// Connection.Close with the custom reply. This preserves protocol
					// correctness while allowing clients to observe the server-sent
					// reply text.
					replyCode := uint16(403) // Access Refused
					replyText := fmt.Sprintf("ACCESS_REFUSED: %s", err.Error())
					// send Open-Ok so the handshake completes from the client's view
					if werr := ctx.WriteMethod(0, classConnection, methodConnOpenOk, []byte{0}); werr != nil {
						logger.Error().Err(werr).Msg("[server] write open-ok error (auth failure path)")
						// fallthrough: attempt to send close even if open-ok failed
					}
					if werr := writeConnCloseAndWait(ctx, conn, replyCode, replyText, classConnection, methodConnStartOk); werr != nil {
						logger.Error().Err(werr).Msg("[server] write connection close error (auth failure path)")
					}
					return
				}
			}
			// connection.open-ok expects a shortstr reserved-1 (empty)
			if err := ctx.WriteMethod(0, classConnection, methodConnOpenOk, []byte{0}); err != nil {
				logger.Error().Err(err).Msg("[server] write open-ok error")
				return
			}
			logger.Debug().Uint16("chan", 0).Int("class", int(classConnection)).Int("method", int(methodConnOpenOk)).Msg("send method open-ok")
			// update connection-scoped context with negotiated vhost
			ctx = newConnContext(conn, vhost, connTLSState, &writeMu)
			// clear deadline after handshake
			conn.SetDeadline(time.Time{})
			// start heartbeat sender if negotiated
			if negotiatedHeartbeat > 0 {
				hb := negotiatedHeartbeat
				go func() {
					t := time.NewTicker(time.Duration(hb) * time.Second)
					defer t.Stop()
					for range t.C {
						if err := ctx.WriteFrame(Frame{Type: frameHeartbeat, Channel: 0, Payload: nil}); err != nil {
							logger.Error().Err(err).Msg("[server] heartbeat write error")
							return
						}
					}
				}()
			}
			break
		}
	}

	// channel states: track confirming mode and publish sequence per channel
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
			if err := handleMethodFrame(conn, ctx, f, handler, handlers, channelStates); err != nil {
				if err == io.EOF {
					return
				}
				logger.Debug().Err(err).Msg("[server] method handling error")
				return
			}
		case frameHeartbeat:
			// ignore heartbeats
		default:
			// ignore other frames
		}
	}
}
